import { mkdtemp, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { connect, type Account } from "@quant/tools";
import { config, type Config } from "../src/config";
import { startHost } from "../src/server";
export const project = resolve(import.meta.dir, "..");
export async function until<T>(read: () => T | Promise<T>, ms = 5000): Promise<NonNullable<T>> {
  const start = Date.now();
  while (Date.now() - start < ms) {
    const v = await read();
    if (v) return v as NonNullable<T>;
    await Bun.sleep(12);
  }
  throw new Error("Timed out waiting for " + String(read));
}
export async function sandbox(overrides: Partial<Config> = {}) {
  const root = await mkdtemp(join(tmpdir(), "quant-test-"));
  await symlink(join(project, "node_modules"), join(root, "node_modules"), "dir");
  const cfg: Config = {
    ...(await config({})),
    dir: join(root, "db"),
    port: 0,
    tickMs: 60000,
    healthMs: 30,
    reconnectMs: 50,
    ...overrides,
  };
  let host = await startHost(cfg);
  return {
    root,
    cfg,
    get host() {
      return host;
    },
    get sdk() {
      return connect(host.url, "");
    },
    async restart() {
      await host.stop();
      host = await startHost(cfg);
    },
    async actor(name: string, account: Account = "paper", observe = false) {
      const file = join(root, crypto.randomUUID() + ".ts"),
        output = file + ".token";
      await Bun.write(
        file,
        `await Bun.write(${JSON.stringify(output)},process.env.HARNESS_RUN_TOKEN!); setInterval(()=>{},60000);`,
      );
      const run = host.runs.start(file, name, observe, account);
      await until(() => host.runs.get(run.id).ready);
      const token = await Bun.file(output).text();
      return { run, file, token, client: connect(host.url, token) };
    },
    async close() {
      await host.stop();
      await rm(root, { recursive: true, force: true });
    },
  };
}
