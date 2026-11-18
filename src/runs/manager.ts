import { closeSync, mkdirSync, openSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import type { Account, Run } from "@quant/tools";
import type { Config } from "../config";
import type { Store } from "../db";
import { nameOf, requireThat, uid } from "../util";

const fields = "id,name,file,hash,account,mode,state,ready,started,ended,exitCode";
interface Child {
  proc: Bun.Subprocess;
  done: Promise<void>;
  token: string;
}
export class Runs {
  private children = new Map<string, Child>();
  private tokens = new Map<string, string>();
  private switching = new Set<string>();
  private stopping = new Set<string>();
  readonly root: string;
  constructor(
    private db: Store,
    private cfg: Config,
    private url: () => string,
  ) {
    this.root = join(cfg.dir, "runs");
    mkdirSync(this.root, { recursive: true, mode: 0o700 });
    db.tx(() => {
      db.run("UPDATE runs SET state='interrupted',ended=? WHERE state='running'", Date.now());
      db.run("DELETE FROM leases");
    });
  }
  get(ref: string) {
    const r =
      this.db.one<Run>(`SELECT ${fields} FROM runs WHERE id=?`, ref) ??
      this.db.one<Run>(
        `SELECT ${fields} FROM runs WHERE name=? ORDER BY (id IN (SELECT runId FROM leases)) DESC,started DESC,rowid DESC LIMIT 1`,
        ref,
      );
    requireThat(r, "Run not found", 404);
    return { ...r, ready: Boolean(r.ready) };
  }
  list() {
    return this.db
      .all<Run>(`SELECT ${fields} FROM runs ORDER BY started DESC,rowid DESC LIMIT 100`)
      .map((r) => ({ ...r, ready: Boolean(r.ready) }));
  }
  identity(token: string, write = false) {
    const id = this.tokens.get(token);
    requireThat(id, "Run credential expired or missing", 403);
    const r = this.get(id);
    if (write)
      requireThat(
        this.db.one("SELECT 1 FROM leases WHERE name=? AND runId=?", r.name, id) && r.state === "running",
        "Run has no execution lease",
        403,
      );
    return r;
  }
  private owner(name: string) {
    return this.db.one<{ runId: string }>("SELECT runId FROM leases WHERE name=?", name)?.runId;
  }
  start(file: string, name?: string, observe = false, account: Account = "paper") {
    requireThat(
      account === "paper" || (account === "binance" && this.cfg.binanceTrading),
      "Execution account unavailable",
    );
    requireThat(typeof file === "string" && file.endsWith(".ts"), "Expected a .ts entry");
    const entry = realpathSync(resolve(file)),
      source = readFileSync(entry, "utf8");
    const id = uid(),
      strategy = nameOf(name ?? id);
    requireThat(observe || !this.owner(strategy), "Name already has an executor", 409);
    const hash = new Bun.CryptoHasher("sha256").update(source).digest("hex"),
      dir = join(this.root, id);
    mkdirSync(dir, { mode: 0o700 });
    const snapshot = join(dir, "entry.ts");
    writeFileSync(snapshot, source, { mode: 0o600 });
    this.db.tx(() => {
      this.db.run(
        "INSERT INTO runs(id,name,file,hash,source,account,mode,state,started) VALUES (?,?,?,?,?,?,?,?,?)",
        id,
        strategy,
        entry,
        hash,
        source,
        account,
        observe ? "observe" : "execute",
        "running",
        Date.now(),
      );
      if (!observe) this.db.run("INSERT INTO leases VALUES (?,?)", strategy, id);
    });
    const token = uid() + uid();
    const stdout = openSync(join(dir, "stdout.log"), "w", 0o600),
      stderr = openSync(join(dir, "stderr.log"), "w", 0o600);
    try {
      const scriptEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => k.startsWith("SCRIPT_")));
      const proc = Bun.spawn(
        [process.execPath, "--no-env-file", "--preload", join(import.meta.dir, "preload.ts"), entry],
        {
          cwd: dirname(entry),
          env: {
            ...scriptEnv,
            PATH: process.env.PATH,
            LANG: process.env.LANG,
            TZ: process.env.TZ,
            HARNESS_URL: this.url(),
            HARNESS_ENTRY: entry,
            HARNESS_SNAPSHOT: snapshot,
            HARNESS_RUN_TOKEN: token,
            HARNESS_RUN_ID: id,
          },
          stdin: "ignore",
          stdout,
          stderr,
          ipc: (msg) => {
            if (msg === "harness:ready") this.db.run("UPDATE runs SET ready=1 WHERE id=? AND state='running'", id);
          },
        },
      );
      this.tokens.set(token, id);
      const done = proc.exited.then((code) => {
        const stopped = this.stopping.delete(id);
        this.tokens.delete(token);
        this.children.delete(id);
        this.db.tx(() => {
          this.db.run("DELETE FROM leases WHERE runId=?", id);
          this.db.run(
            "UPDATE runs SET state=?,ended=?,exitCode=? WHERE id=?",
            stopped ? "stopped" : code === 0 ? "exited" : "failed",
            Date.now(),
            code,
            id,
          );
        });
      });
      this.children.set(id, { proc, done, token });
    } catch (err) {
      this.db.run("DELETE FROM leases WHERE runId=?", id);
      this.db.run("UPDATE runs SET state='failed',ended=? WHERE id=?", Date.now(), id);
      throw err;
    } finally {
      closeSync(stdout);
      closeSync(stderr);
    }
    return this.get(id);
  }
  async stop(ref: string) {
    const run = this.get(ref),
      child = this.children.get(run.id);
    if (!child) return run;
    this.db.run("DELETE FROM leases WHERE runId=?", run.id);
    this.tokens.delete(child.token);
    this.stopping.add(run.id);
    child.proc.kill("SIGTERM");
    const timer = setTimeout(() => {
      if (child.proc.exitCode === null) child.proc.kill("SIGKILL");
    }, 1000);
    try {
      await child.done;
    } finally {
      clearTimeout(timer);
    }
    return this.get(run.id);
  }
  async replace(name: string, file: string, from?: string) {
    requireThat(!this.switching.has(name), "Replacement already in progress", 409);
    const old = this.owner(name);
    requireThat(old, "Strategy has no active executor", 409);
    const account = this.get(old).account,
      entry = realpathSync(file);
    const hash = new Bun.CryptoHasher("sha256").update(readFileSync(entry)).digest("hex");
    const observer = this.db.one<{ id: string }>(
      `SELECT id FROM runs WHERE name=? AND file=? AND hash=? AND account=?
      AND state='running' AND mode='observe' AND (? IS NULL OR id=?) ORDER BY started DESC LIMIT 1`,
      name,
      entry,
      hash,
      account,
      from ?? null,
      from ?? null,
    );
    requireThat(!from || observer, "Selected observer does not match the running source and account", 409);
    this.switching.add(name);
    let candidate: Run | undefined;
    let promoted = false;
    try {
      candidate = observer ? this.get(observer.id) : this.start(entry, name, true, account);
      const deadline = Date.now() + this.cfg.readyMs;
      while (!from && !this.get(candidate.id).ready) {
        requireThat(
          this.get(candidate.id).state === "running" && Date.now() < deadline,
          "Candidate exited or did not finish startup",
          409,
        );
        await Bun.sleep(15);
      }
      // Give a just-completed one-shot or synchronous startup exception time to report its exit.
      await Bun.sleep(60);
      const id = candidate.id;
      this.db.tx(() => {
        requireThat(
          this.owner(name) === old && this.children.get(id)?.proc.exitCode === null && !this.stopping.has(id),
          "Executor or candidate exited during handover",
          409,
        );
        this.db.run("UPDATE leases SET runId=? WHERE name=? AND runId=?", id, name, old);
        this.db.run("UPDATE runs SET mode='execute',ready=1 WHERE id=?", id);
        this.db.run("UPDATE runs SET mode='observe' WHERE id=?", old);
      });
      promoted = true;
      await this.stop(old);
      return this.get(id);
    } finally {
      this.switching.delete(name);
      if (!promoted && candidate && !observer) await this.stop(candidate.id);
    }
  }
  logs(ref: string) {
    const { id } = this.get(ref);
    return {
      id,
      stdout: readFileSync(join(this.root, id, "stdout.log"), "utf8"),
      stderr: readFileSync(join(this.root, id, "stderr.log"), "utf8"),
    };
  }
  async shutdown() {
    await Promise.all([...this.children.keys()].map((id) => this.stop(id)));
  }
}
