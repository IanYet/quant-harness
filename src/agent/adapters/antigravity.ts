import { chmod, lstat, mkdir, mkdtemp, rm, symlink } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { join, relative, resolve } from "node:path";
import { Fault } from "../../shared/http";
import type { AdapterResult, AdapterStartOptions, AgentHarnessAdapter } from "./interface";
import { StdioRpc } from "./rpc";

type Active = { cancel(): void };
type AgentOpts = Omit<AdapterStartOptions, "prompt">;

export class AntigravityAdapter implements AgentHarnessAdapter {
  private readonly active = new Map<string, Active>();
  constructor(readonly name: string, private readonly settings: {
    command: string; cwd: string; project?: string; model?: string; visibleAppDataDir?: string;
    hiddenAppDataDir: string; mcpScript: string; dataDir: string;
    inputUsdPerMillion?: number; outputUsdPerMillion?: number; authAppDataDir?: string;
  }) {}

  private get geminiDir() { return resolve(process.env.ANTIGRAVITY_GEMINI_DIR ?? join(homedir(), ".gemini")); }

  private async reuseLogin(directory: string) {
    const ownerStore = this.settings.authAppDataDir ?? join(this.geminiDir, "antigravity-cli");
    if (resolve(ownerStore) === resolve(directory)) return;
    const filename = "antigravity-oauth-token";
    const credential = join(ownerStore, filename);
    const info = await lstat(credential).catch(() => undefined);
    if (!info?.isFile() || (info.mode & 0o077) !== 0) return;
    try { await symlink(credential, join(directory, filename)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  }

  async startSession(opts: AdapterStartOptions) {
    return this.useCli(opts.visibility) ? this.visible(opts.prompt, opts) : this.acp(opts.prompt, opts);
  }

  async resumeSession(backendId: string, message: string, opts: AgentOpts) {
    const split = backendId.indexOf(":");
    const visibility = backendId.slice(0, split);
    const id = backendId.slice(split + 1);
    if (visibility !== opts.visibility || visibility === "ephemeral") throw new Fault(422, "VISIBILITY_UNSUPPORTED",
      "Antigravity can resume only in the original persistent session store");
    return this.useCli(opts.visibility) ? this.visible(message, opts, id) : this.acp(message, opts, id);
  }

  private useCli(visibility: AgentOpts["visibility"]) {
    return visibility === "visible" && (!this.settings.visibleAppDataDir ||
      this.settings.visibleAppDataDir === join(this.geminiDir, "antigravity-cli"));
  }

  private async visible(prompt: string, opts: AgentOpts, resume?: string): Promise<AdapterResult & { backendSessionId: string }> {
    opts.signal.throwIfAborted();
    const argv = [this.settings.command, "--print", prompt, "--output-format", "stream-json", "--print-timeout",
      opts.timeoutSec + "s", "--disable-slash-commands", "--add-dir", this.settings.dataDir];
    if (this.settings.project) argv.push("--project", this.settings.project);
    if (this.settings.model) argv.push("--model", this.settings.model);
    if (resume) argv.push("--conversation", resume);
    const child = Bun.spawn(argv, { cwd: this.settings.cwd, env: { ...process.env, ...opts.mcpEnv },
      stdin: "ignore", stdout: "pipe", stderr: "pipe" });
    const cancel = () => {
      child.kill("SIGINT");
      const kill = setTimeout(() => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); }, 5000);
      kill.unref();
    };
    let backendId = resume ? "visible:" + resume : undefined;
    if (backendId) this.active.set(backendId, { cancel });
    const decoder = new TextDecoder();
    let summary = "", terminal = "", buffered = "", units = 0, costUsd: number | undefined;
    opts.signal.addEventListener("abort", cancel, { once: true });
    const stderr = (async () => {
      let tail = "";
      const d = new TextDecoder();
      for await (const data of child.stderr) tail = (tail + d.decode(data, { stream: true })).slice(-1400);
      return tail;
    })();
    try {
      for await (const bytes of child.stdout) {
        buffered += decoder.decode(bytes, { stream: true });
        let eol: number;
        while ((eol = buffered.indexOf("\n")) !== -1) {
          const line = buffered.slice(0, eol);
          buffered = buffered.slice(eol + 1);
          if (!line.trim()) continue;
          let event: { event?: string; conversation_id?: string; step_update?: { step_type: string; state: string; text_delta?: string };
            result?: { conversation_id?: string; status: string; response?: string; num_turns?: number;
              usage?: { input_tokens?: number; output_tokens?: number }; total_cost_usd?: number } };
          try { event = JSON.parse(line); } catch { continue; }
          const id = event.conversation_id ?? event.result?.conversation_id;
          if (!backendId && id) {
            backendId = "visible:" + id;
            opts.onSession(backendId);
            this.active.set(backendId, { cancel });
          }
          const step = event.step_update;
          if (step && step.step_type === "agent_response") {
            if (step.text_delta) summary = (summary + step.text_delta).slice(-20_000);
            if (step.state === "DONE" && ++units > opts.maxTurns) { cancel(); throw new Fault(429, "TASK_TURN_LIMIT", "Antigravity exceeded its interaction allowance"); }
          }
          if (event.event === "result" && event.result) {
            const result = event.result;
            if (result.status !== "SUCCESS") throw new Fault(502, "AGENT_BACKEND_ERROR", result.response ?? "Antigravity task failed");
            if ((result.num_turns ?? 0) > opts.maxTurns) throw new Fault(429, "TASK_TURN_LIMIT", "Antigravity exceeded its interaction allowance");
            terminal = result.response ?? "";
            const inputRate = this.settings.inputUsdPerMillion, outputRate = this.settings.outputUsdPerMillion;
            costUsd = result.total_cost_usd ?? (result.usage && inputRate !== undefined && outputRate !== undefined ?
              ((result.usage.input_tokens ?? 0) * inputRate + (result.usage.output_tokens ?? 0) * outputRate) / 1_000_000 : undefined);
          }
        }
        if (buffered.length > 8 * 1024 * 1024) throw new Error("Antigravity output frame too large");
      }
      const code = await child.exited;
      opts.signal.throwIfAborted();
      const diagnostic = await stderr;
      if (code !== 0 || !backendId) throw new Fault(502, "AGENT_BACKEND_ERROR", diagnostic.trim().slice(-400) || "Antigravity did not return a session");
      return { backendSessionId: backendId, summary: (terminal || summary).slice(-20_000), costUsd };
    } finally {
      opts.signal.removeEventListener("abort", cancel);
      if (backendId) this.active.delete(backendId);
      if (child.exitCode === null && child.signalCode === null) cancel();
    }
  }

  private async acp(prompt: string, opts: AgentOpts, resume?: string): Promise<AdapterResult & { backendSessionId: string }> {
    opts.signal.throwIfAborted();
    const ephemeral = opts.visibility === "ephemeral";
    const dir = ephemeral ? await mkdtemp(join(tmpdir(), "quant-agy-")) :
      opts.visibility === "visible" ? this.settings.visibleAppDataDir! : this.settings.hiddenAppDataDir;
    if (opts.visibility === "hidden" && [this.settings.visibleAppDataDir, this.settings.authAppDataDir, join(this.geminiDir, "antigravity-cli")].includes(dir)) {
      throw new Fault(422, "VISIBILITY_UNSUPPORTED", "Hidden Agent storage must differ from app and login storage");
    }
    await mkdir(dir, { recursive: true, mode: 0o700 });
    let rpc: StdioRpc | undefined, backendId: string | undefined;
    try {
      if (opts.visibility !== "visible") await chmod(dir, 0o700);
      await this.reuseLogin(dir);
      rpc = new StdioRpc([this.settings.command, "acp", "--gemini-dir", this.geminiDir, "--app-data-dir", relative(this.geminiDir, dir),
        "--stdio", "--add-dir", this.settings.dataDir],
        this.settings.cwd, { ...process.env, ...opts.mcpEnv }, method =>
          method === "session/request_permission" ? { outcome: { outcome: "cancelled" } } : undefined);
      await rpc.request("initialize", { protocolVersion: 1, clientCapabilities: {}, clientInfo: { name: "quant-harness", version: "0.1.0" } });
      opts.signal.throwIfAborted();
      const params = { cwd: this.settings.cwd, mcpServers: [{ name: "quant", command: process.execPath,
        args: [this.settings.mcpScript], env: Object.entries(opts.mcpEnv).map(([name, value]) => ({ name, value })) }] };
      if (resume) await rpc.request("session/load", { sessionId: resume, ...params }, 15_000);
      const id = resume ?? (await rpc.request<{ sessionId: string }>("session/new", params, 30_000)).sessionId;
      backendId = opts.visibility + ":" + id;
      opts.onSession(backendId);
      if (this.settings.model) await rpc.request("session/set_model", { sessionId: id, modelId: this.settings.model });
      let count = 0, summary = "";
      let iterationLimit = false;
      const cancel = () => { if (rpc?.alive) { rpc.notify("session/cancel", { sessionId: id }); rpc.close(); } };
      opts.signal.addEventListener("abort", cancel, { once: true });
      this.active.set(backendId, { cancel });
      const unobserve = rpc.observe((method, raw) => {
        if (method !== "session/update") return;
        const event = raw as { sessionId?: string; update?: { sessionUpdate?: string; content?: { type?: string; text?: string } } };
        if (event.sessionId !== id) return;
        const u = event.update;
        if (u?.sessionUpdate === "agent_message_chunk" && u.content?.text) summary = (summary + u.content.text).slice(-20_000);
        if (u?.sessionUpdate === "tool_call" && ++count > opts.maxTurns) { iterationLimit = true; cancel(); }
      });
      try {
        const result = await rpc.request<{ stopReason?: string; _meta?: { costUsd?: number } }>("session/prompt",
          { sessionId: id, prompt: [{ type: "text", text: prompt }] }, opts.timeoutSec * 1000);
        opts.signal.throwIfAborted();
        if (result.stopReason === "cancelled") throw new Fault(422, "TASK_CANCELED", "Antigravity task canceled");
        return { backendSessionId: backendId, summary, costUsd: result._meta?.costUsd };
      } catch (error) {
        if (iterationLimit) throw new Fault(429, "TASK_TURN_LIMIT", "Antigravity exceeded its interaction allowance");
        opts.signal.throwIfAborted();
        throw error;
      } finally { opts.signal.removeEventListener("abort", cancel); unobserve(); }
    } finally {
      if (backendId) this.active.delete(backendId);
      rpc?.close();
      await rpc?.waitClosed();
      if (ephemeral) await rm(dir, { recursive: true, force: true });
    }
  }

  async setVisibility(id: string, visibility: "visible" | "hidden") {
    if (!id.startsWith(visibility + ":")) throw new Fault(422, "VISIBILITY_UNSUPPORTED", "Antigravity has separate persistent stores for visibility");
  }
  async cancel(id: string) { this.active.get(id)?.cancel(); }
  async health() {
    try {
      const result = Bun.spawnSync([this.settings.command, "--version"], { cwd: this.settings.cwd, stdout: "pipe", stderr: "ignore", timeout: 3000 });
      return result.exitCode === 0 ? { ok: true, detail: "CLI " + result.stdout.toString().trim() } : { ok: false, detail: "Antigravity CLI unavailable" };
    } catch { return { ok: false, detail: "Antigravity CLI unavailable" }; }
  }
  close() { for (const task of this.active.values()) task.cancel(); }
}

export function hiddenDirectory(dataDir: string, backend: string) {
  const name = createHash("sha256").update(backend).digest("hex").slice(0, 16);
  return join(dataDir, "agent-hidden", name);
}
