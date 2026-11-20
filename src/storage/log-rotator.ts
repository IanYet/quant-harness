import { appendFile, mkdir, readFile, readdir, stat, unlink } from "node:fs/promises";
import { join } from "node:path";
import type { Database } from "bun:sqlite";
import { atomicFile } from "../shared/fs";
import { ID, missing, oneOf, str } from "../shared/http";

export class LogRotator {
  private readonly active = new Map<string, Promise<unknown>>();
  constructor(private readonly dir: string, private readonly db: Database, private readonly maxBytes = 1024 * 1024) {}

  append(input: Record<string, unknown>) {
    const runId = str(input.runId, "runId", ID);
    const mode = oneOf(input.mode, "mode", ["worker", "live", "shadow", "backtest"]);
    const name = str(input.name, "name", ID);
    const chunk = typeof input.text === "string" ? input.text : "";
    const pending = (this.active.get(runId) ?? Promise.resolve()).catch(() => {}).then(async () => {
      const relative = join(mode, name, runId + ".log");
      const path = join(this.dir, relative);
      const info = this.db.query<{ path: string }, [string]>("SELECT path FROM script_logs WHERE run_id=?").get(runId);
      if (info && info.path !== relative) throw new Error("Run log path changed");
      if (!info) this.db.query("INSERT INTO script_logs VALUES (?,?)").run(runId, relative);
      await mkdir(join(this.dir, mode, name), { recursive: true, mode: 0o750 });
      const prior = await stat(path).catch(() => undefined);
      const differentDay = prior && new Date(prior.mtime).toISOString().slice(0, 10) !== new Date().toISOString().slice(0, 10);
      if (prior?.size && (differentDay || prior.size + Buffer.byteLength(chunk) > this.maxBytes)) {
        const rotated = path + "." + Date.now() + ".gz";
        await atomicFile(rotated, Bun.gzipSync(await readFile(path)), 0o640, true);
        await unlink(path);
        const logs = (await readdir(join(this.dir, mode, name))).filter(file => file.startsWith(runId + ".log.") && file.endsWith(".gz")).sort();
        await Promise.all(logs.slice(0, -5).map(file => unlink(join(this.dir, mode, name, file))));
      }
      await appendFile(path, chunk, { mode: 0o640 });
    });
    this.active.set(runId, pending);
    return pending.finally(() => { if (this.active.get(runId) === pending) this.active.delete(runId); });
  }

  async tail(runId: string, limit: number, since: number, match?: string) {
    const info = this.db.query<{ path: string }, [string]>("SELECT path FROM script_logs WHERE run_id=?").get(runId);
    if (!info) missing("No logs for " + runId);
    await this.active.get(runId);
    const folder = join(this.dir, info.path, "..");
    const archives = (await readdir(folder)).filter(file => file.startsWith(runId + ".log.") && file.endsWith(".gz")).sort();
    const logs = [...archives.map(file => join(folder, file)), join(this.dir, info.path)];
    let lines: string[] = [];
    for (const path of logs.reverse()) {
      const bytes = await readFile(path).catch(() => undefined);
      if (!bytes) continue;
      const text = new TextDecoder().decode(path.endsWith(".gz") ? Bun.gunzipSync(bytes) : bytes);
      const selected = text.split("\n").filter(line => line && (!match || line.includes(match)) && (!since || Date.parse(line.slice(0, 24)) >= since));
      lines = [...selected, ...lines].slice(-limit);
      if (lines.length >= limit) break;
    }
    return { runId, lines };
  }
}
