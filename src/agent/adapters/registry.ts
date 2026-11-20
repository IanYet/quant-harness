import { join, resolve } from "node:path";
import { homedir } from "node:os";
import type { HarnessConfig } from "../../shared/config";
import { num, oneOf, record, str } from "../../shared/http";
import { CodexAdapter } from "./codex";
import { AntigravityAdapter, hiddenDirectory } from "./antigravity";
import type { AgentHarnessAdapter } from "./interface";

export function backends(config: HarnessConfig) {
  const root = resolve(import.meta.dir, "../../..");
  const backends = record(config.agent.backends ?? { codex: { type: "codex" } });
  const adapters = new Map<string, AgentHarnessAdapter>();
  for (const [name, raw] of Object.entries(backends)) {
    const value = record(raw);
    const type = oneOf(value.type, "agent backend type", ["codex", "antigravity"]);
    const opts = {
      cwd: resolve(str(value.cwd ?? root, "cwd")), dataDir: config.dataDir, mcpScript: join(root, "src/mcp/server.ts"),
      model: value.model === undefined ? undefined : str(value.model, "model"),
    };
    const rate = (key: string) => value[key] === undefined ? undefined : num(value[key], key, 0);
    if (type === "codex") adapters.set(name, new CodexAdapter(name, { ...opts, command: str(value.command ?? "codex", "command"),
      inputUsdPerMillion: rate("inputUsdPerMillion"), outputUsdPerMillion: rate("outputUsdPerMillion") }));
    else adapters.set(name, new AntigravityAdapter(name, { ...opts, command: str(value.command ?? "agy", "command"),
      project: value.project === undefined ? undefined : str(value.project, "project"),
      visibleAppDataDir: value.visibleAppDataDir === undefined ? join(homedir(), ".gemini", "antigravity") : resolve(str(value.visibleAppDataDir, "visibleAppDataDir")),
      hiddenAppDataDir: value.hiddenAppDataDir === undefined ? hiddenDirectory(config.dataDir, name) : resolve(str(value.hiddenAppDataDir, "hiddenAppDataDir")),
      authAppDataDir: value.authAppDataDir === undefined ? undefined : resolve(str(value.authAppDataDir, "authAppDataDir")),
      inputUsdPerMillion: rate("inputUsdPerMillion"), outputUsdPerMillion: rate("outputUsdPerMillion") }));
  }
  return adapters;
}
