import type { Database } from "bun:sqlite";
import { lstat, mkdir, mkdtemp, readFile, readdir, realpath, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import type { WorkerCapabilities } from "@quant/tools";
import type { RunMode } from "../shared/config";
import { capabilities } from "../shared/capabilities";
import { atomicFile, sha256 } from "../shared/fs";
import { bad, Fault, ID, missing, oneOf, record, str, stringList } from "../shared/http";
import { validateFile } from "./validator";

const project = resolve(import.meta.dir, "../..");
const strategyIdPattern = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}@v[1-9][0-9]*$/;
const dimensions = ["regime", "style", "account_fit", "factor"] as const;
const lifecycles = ["backtested", "shadow", "live", "shelved", "deprecated"] as const;
const namePattern = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\.ts$/;
export interface Artifact { artifactPath: string; sourceHash: string; codeHash: string; sdkHash: string; strategyId?: string }

interface Strategy {
  strategy_id: string; family: string; version: string; file_path: string; source_hash: string; code_hash: string; sdk_hash: string;
  parent_id: string | null; lifecycle: string; diff_summary: string; created_at: number; updated_at: number;
}
type Tags = Partial<Record<typeof dimensions[number], string[]>>;

function tags(value: unknown): Tags {
  const obj = record(value ?? {});
  return Object.fromEntries(Object.entries(obj).map(([key, items]) => [
    oneOf(key === "factors" ? "factor" : key, "tag dimension", dimensions), stringList(items, key),
  ]));
}

export class StrategyRegistry {
  constructor(private readonly dir: string, private readonly db: Database, private readonly sdkDir = join(project, "packages/tools")) {}

  private async source(path: unknown, mode: RunMode) {
    const base = join(this.dir, mode === "worker" ? "scripts/workers" : "strategies/_drafts");
    const text = str(path, "entry path");
    const name = str(basename(text), "entry filename", namePattern);
    const absolute = isAbsolute(text) ? resolve(text) : text === name ? join(base, name) : resolve(this.dir, text);
    if (dirname(absolute) !== base) bad("Entry must be under " + relative(this.dir, base));
    const info = await lstat(absolute).catch(() => missing("Script not found"));
    if (!info.isFile() || info.size > 1024 * 1024 || (await realpath(absolute)) !== absolute) bad("Entry must be a regular .ts file up to 1 MiB");
    return { absolute, source: await readFile(absolute) };
  }

  private async compile(source: Uint8Array, mode: RunMode, capabilities?: WorkerCapabilities, build = true) {
    const temp = await mkdtemp(join(tmpdir(), "quant-build-"));
    try {
      const sdk = join(temp, "sdk");
      await mkdir(sdk);
      const filenames = (await readdir(this.sdkDir)).filter(name => /^[A-Za-z0-9_.-]+\.ts$/.test(name)).sort();
      const files = await Promise.all(filenames.map(async name => ({ name, bytes: await readFile(join(this.sdkDir, name)) })));
      const sdkHash = sha256(JSON.stringify(files.map(file => [file.name, sha256(file.bytes)])));
      await Promise.all(files.map(file => writeFile(join(sdk, file.name), file.bytes)));
      const entry = join(temp, "script.ts");
      await writeFile(entry, source);
      const sdkEntry = join(sdk, "index.ts");
      const diagnostics = validateFile(entry, sdkEntry, project, mode, capabilities);
      if (!build || diagnostics.some(item => item.level === "error")) return { diagnostics };
      const result = await Bun.build({
        entrypoints: [entry], root: temp, format: "esm", target: "bun", sourcemap: "none",
        plugins: [{ name: "pinned-quant-sdk", setup(builder) {
          builder.onResolve({ filter: /^@quant\/tools$/ }, () => ({ path: sdkEntry }));
        } }],
      });
      if (!result.success || result.outputs.length !== 1) throw new Error("Expected one bundled script artifact");
      const sourceHash = sha256(source);
      const content = Buffer.concat([
        Buffer.from("// quant-sdk " + sdkHash + " source " + sourceHash + " bun " + Bun.version + "\n"),
        Buffer.from(await result.outputs[0].arrayBuffer()),
      ]);
      const codeHash = sha256(content);
      const artifactPath = this.artifactPath(codeHash);
      try {
        await atomicFile(artifactPath, content, 0o444, true);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        await this.checkHash(artifactPath, codeHash);
      }
      return { diagnostics, artifact: { artifactPath, sourceHash, codeHash, sdkHash } satisfies Artifact };
    } finally {
      await rm(temp, { force: true, recursive: true });
    }
  }

  async validate(path: unknown, mode: RunMode, capabilities?: WorkerCapabilities) {
    const snapshot = await this.source(path, mode);
    const { diagnostics } = await this.compile(snapshot.source, mode, capabilities, false);
    return { valid: !diagnostics.some(diag => diag.level === "error"), diagnostics };
  }

  private async build(source: Uint8Array, mode: RunMode, capabilities?: WorkerCapabilities) {
    const { diagnostics, artifact } = await this.compile(source, mode, capabilities);
    if (!artifact) throw new Fault(422, "VALIDATION_FAILED", "Static checks failed", { diagnostics });
    return artifact;
  }

  private artifactPath(hash: string) {
    return join(this.dir, "artifacts", str(hash, "code hash", /^[a-f0-9]{64}$/), "entry.mjs");
  }

  private async checkHash(path: string, expected: string) {
    const info = await lstat(path).catch(() => undefined);
    if (!info?.isFile() || (info.mode & 0o777) !== 0o444 || await realpath(path) !== path || sha256(await readFile(path)) !== expected) {
      throw new Fault(422, "HASH_MISMATCH", "Immutable source or artifact is missing, writable or changed");
    }
  }

  get(id: unknown) {
    const strategyId = str(id, "strategyId", strategyIdPattern);
    return this.db.query<Strategy, [string]>("SELECT * FROM strategies WHERE strategy_id=?").get(strategyId) ?? missing("Unknown strategy " + strategyId);
  }

  async prepare(args: { mode: RunMode; strategyId?: unknown; path?: unknown; capabilities?: WorkerCapabilities }): Promise<Artifact> {
    if (args.strategyId !== undefined) {
      if (args.mode === "worker") bad("Workers require an entry path");
      const s = this.get(args.strategyId);
      const artifactPath = this.artifactPath(s.code_hash);
      await Promise.all([this.checkHash(join(this.dir, s.file_path), s.source_hash), this.checkHash(artifactPath, s.code_hash)]);
      return { strategyId: s.strategy_id, artifactPath, sourceHash: s.source_hash, codeHash: s.code_hash, sdkHash: s.sdk_hash };
    }
    if (args.mode === "live") throw new Fault(422, "STRATEGY_NOT_SEALED", "Live execution requires a sealed strategyId");
    const snapshot = await this.source(args.path, args.mode);
    return this.build(snapshot.source, args.mode, args.capabilities);
  }

  async seal(input: Record<string, unknown>) {
    const family = str(input.family, "family", ID);
    const summary = str(input.summary, "summary");
    const snapshot = await this.source(input.draftPath, "shadow");
    const draft = this.db.query<{ parent_id: string; inherited_tags_json: string }, [string]>(
      "SELECT parent_id,inherited_tags_json FROM strategy_drafts WHERE draft_path=?").get(relative(this.dir, snapshot.absolute));
    const tagValues = { ...tags(draft ? JSON.parse(draft.inherited_tags_json) : undefined), ...tags(input.tags) };
    const parent = input.parentId !== undefined ? this.get(input.parentId).strategy_id : draft?.parent_id ?? null;
    const lifecycle = oneOf(input.lifecycle ?? "backtested", "lifecycle", lifecycles);
    const artifact = await this.build(snapshot.source, "live", capabilities(input.capabilities));
    return this.publishSeal(snapshot.source, artifact, { family, summary, lifecycle, parent, tagValues });
  }

  private async publishSeal(source: Uint8Array, artifact: Artifact, options: {
    family: string; summary: string; lifecycle: string; parent: string | null; tagValues: Tags;
  }) {
    const { family, summary, lifecycle, parent, tagValues } = options;
    const folder = join(this.dir, "strategies", family);
    await mkdir(folder, { recursive: true, mode: 0o750 });
    if (await realpath(folder) !== folder) bad("Family directory must not be a symlink");
    const files = await readdir(folder);
    const reserved = this.db.transaction(() => {
      const published = this.db.query<{ version: string }, [string]>("SELECT version FROM strategies WHERE family=?").all(family);
      const pending = this.db.query<{ strategy_id: string }, [string]>("SELECT strategy_id FROM pending_seals WHERE strategy_id GLOB ?").all(family + "@*");
      const versions = [...files.filter(name => /^v\d+\.ts$/.test(name)).map(name => name.slice(0, -3)),
        ...published.map(row => row.version), ...pending.map(row => row.strategy_id.split("@")[1])];
      const version = "v" + (Math.max(0, ...versions.map(value => Number(value.slice(1)))) + 1);
      const now = Date.now();
      const s: Strategy = {
        strategy_id: family + "@" + version, family, version, file_path: join("strategies", family, version + ".ts"),
        source_hash: artifact.sourceHash, sdk_hash: artifact.sdkHash, code_hash: artifact.codeHash,
        parent_id: parent, lifecycle, diff_summary: summary, created_at: now, updated_at: now,
      };
      this.db.query("INSERT INTO pending_seals VALUES (?,?)").run(s.strategy_id, JSON.stringify({ s, tags: tagValues }));
      return s;
    }).immediate();
    await atomicFile(join(this.dir, reserved.file_path), source, 0o444, true);
    this.commitSeal(reserved, tagValues);
    return { ...artifact, strategyId: reserved.strategy_id, path: reserved.file_path };
  }

  private commitSeal(s: Strategy, values: Tags) {
    this.db.transaction(() => {
      this.db.query("INSERT INTO strategies VALUES (?,?,?,?,?,?,?,?,?,?,?,?)").run(
        s.strategy_id, s.family, s.version, s.file_path, s.source_hash, s.sdk_hash, s.code_hash, s.parent_id,
        s.lifecycle, s.diff_summary, s.created_at, s.updated_at);
      this.writeTags(s.strategy_id, values);
      this.db.query("DELETE FROM pending_seals WHERE strategy_id=?").run(s.strategy_id);
    }).immediate();
  }

  async recover() {
    for (const row of this.db.query<{ strategy_id: string; metadata_json: string }, []>("SELECT * FROM pending_seals").all()) {
      const { s, tags: values } = JSON.parse(row.metadata_json) as { s: Strategy; tags: Tags };
      const path = join(this.dir, s.file_path);
      try {
        await lstat(path);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        this.db.query("DELETE FROM pending_seals WHERE strategy_id=?").run(s.strategy_id);
        continue;
      }
      await this.checkHash(path, s.source_hash);
      await this.checkHash(this.artifactPath(s.code_hash), s.code_hash);
      this.commitSeal(s, values);
    }
  }

  private readTags(id: string) {
    const rows = this.db.query<{ dimension: keyof Tags; value: string }, [string]>(
      "SELECT dimension,value FROM strategy_tags WHERE strategy_id=? ORDER BY dimension,value").all(id);
    const result: Tags = {};
    for (const { dimension, value } of rows) (result[dimension] ??= []).push(value);
    return result;
  }

  private writeTags(id: string, values: Tags) {
    for (const [key, items] of Object.entries(values)) {
      this.db.query("DELETE FROM strategy_tags WHERE strategy_id=? AND dimension=?").run(id, key);
      for (const value of items) this.db.query("INSERT INTO strategy_tags VALUES (?,?,?)").run(id, key, value);
    }
  }

  search(filters: URLSearchParams) {
    const clauses: string[] = [];
    const params: string[] = [];
    for (const key of ["family", "lifecycle"]) {
      const val = filters.get(key);
      if (val) { clauses.push("s." + key + "=?"); params.push(val); }
    }
    for (const key of dimensions) {
      for (const val of filters.getAll(key)) {
        clauses.push("EXISTS(SELECT 1 FROM strategy_tags t WHERE t.strategy_id=s.strategy_id AND t.dimension=? AND t.value=?)");
        params.push(key, val);
      }
    }
    const rows = this.db.query<Strategy, string[]>("SELECT s.* FROM strategies s WHERE " +
      (clauses.join(" AND ") || "1") + " ORDER BY s.updated_at DESC LIMIT 500").all(...params);
    return rows.map(s => ({ ...s, tags: this.readTags(s.strategy_id),
      lastBacktest: this.db.query("SELECT * FROM backtest_runs WHERE target=? ORDER BY created_at DESC LIMIT 1").get(s.strategy_id) }));
  }

  async describe(id: unknown) {
    const s = this.get(id);
    await this.checkHash(join(this.dir, s.file_path), s.source_hash);
    const lineage: Strategy[] = [];
    for (let p = s.parent_id; p;) {
      const parent = this.get(p);
      lineage.push(parent);
      p = parent.parent_id;
    }
    return { ...s, source: await readFile(join(this.dir, s.file_path), "utf8"), lineage, tags: this.readTags(s.strategy_id),
      notes: this.db.query("SELECT * FROM strategy_notes WHERE strategy_id=? ORDER BY created_at").all(s.strategy_id),
      backtests: this.db.query("SELECT * FROM backtest_runs WHERE target=? ORDER BY created_at DESC LIMIT 100").all(s.strategy_id) };
  }

  async fork(input: Record<string, unknown>) {
    const parent = this.get(input.fromStrategyId);
    const name = str(input.draftName, "draftName");
    const filename = str(name.endsWith(".ts") ? name : name + ".ts", "draftName", namePattern);
    const path = join("strategies", "_drafts", filename);
    const reason = str(input.reason, "reason");
    await this.checkHash(join(this.dir, parent.file_path), parent.source_hash);
    try {
      await atomicFile(join(this.dir, path), await readFile(join(this.dir, parent.file_path)), 0o640, true);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") throw new Fault(409, "DRAFT_EXISTS", "Draft already exists");
      throw error;
    }
    try {
      this.db.query("INSERT INTO strategy_drafts VALUES (?,?,?,?,?)")
        .run(path, parent.strategy_id, JSON.stringify(this.readTags(parent.strategy_id)), reason, Date.now());
    } catch (error) {
      await unlink(join(this.dir, path));
      throw error;
    }
    return { draftPath: path, parentId: parent.strategy_id };
  }

  tag(id: unknown, input: Record<string, unknown>) {
    const { strategy_id: key, lifecycle: old } = this.get(id);
    const lifecycle = oneOf(input.lifecycle ?? old, "lifecycle", lifecycles);
    const note = input.note === undefined ? undefined : str(input.note, "note");
    const author = oneOf(input.author ?? "user", "author", ["user", "human", "cron_review", "script_triggered"]);
    const values = tags(input.tags);
    this.db.transaction(() => {
      const ts = Date.now();
      this.writeTags(key, values);
      if (note) this.db.query("INSERT INTO strategy_notes(strategy_id,author,note,created_at) VALUES (?,?,?,?)").run(key, author, note, ts);
      this.db.query("UPDATE strategies SET lifecycle=?,updated_at=? WHERE strategy_id=?").run(lifecycle, ts, key);
    }).immediate();
    return { strategyId: key, lifecycle, tags: this.readTags(key) };
  }
}
