import { join } from "node:path";
import { Fault } from "../../shared/http";
import type { AdapterResult, AdapterStartOptions, AgentHarnessAdapter } from "./interface";
import { StdioRpc } from "./rpc";

type Session = { thread: { id: string } };

export class CodexAdapter implements AgentHarnessAdapter {
  private rpc?: StdioRpc;
  private starting?: Promise<StdioRpc>;
  private readonly turns = new Map<string, string>();
  constructor(readonly name: string, readonly settings: {
    command: string; model?: string; cwd: string; mcpScript: string; dataDir: string;
    inputUsdPerMillion?: number; outputUsdPerMillion?: number;
  }) {}

  private async connection() {
    if (this.rpc?.alive) return this.rpc;
    if (!this.starting) {
      this.starting = (async () => {
        const rpc = new StdioRpc([this.settings.command, "app-server", "--listen", "stdio://"], this.settings.cwd, process.env,
          method => method.endsWith("/requestApproval") ? { decision: "decline" } : undefined, false);
        try {
          await rpc.request("initialize", { clientInfo: { name: "quant-harness", title: "Quant Harness", version: "0.1.0" } });
          rpc.notify("initialized");
          this.rpc = rpc;
          return rpc;
        } catch (error) { rpc.close(); throw error; }
      })().finally(() => { this.starting = undefined; });
    }
    return this.starting;
  }

  private threadOptions(opts: Omit<AdapterStartOptions, "prompt">) {
    return {
      cwd: this.settings.cwd, model: this.settings.model, sandbox: "workspace-write", approvalPolicy: "never",
      config: {
        "shell_environment_policy.set": opts.mcpEnv,
        "sandbox_workspace_write.writable_roots": [this.settings.dataDir],
        "mcp_servers.quant.command": process.execPath,
        "mcp_servers.quant.args": [join(this.settings.mcpScript)],
        "mcp_servers.quant.env": opts.mcpEnv,
      },
    };
  }

  async startSession(opts: AdapterStartOptions) {
    const rpc = await this.connection();
    opts.signal.throwIfAborted();
    const { thread } = await rpc.request<Session>("thread/start", { ...this.threadOptions(opts), ephemeral: opts.visibility === "ephemeral" });
    opts.onSession(thread.id);
    if (opts.title && opts.visibility !== "ephemeral") await rpc.request("thread/name/set", { threadId: thread.id, name: opts.title });
    try {
      const result = await this.prompt(rpc, thread.id, opts.prompt, opts);
      return { backendSessionId: thread.id, ...result };
    } finally {
      if (opts.visibility === "hidden") await this.setVisibility(thread.id, "hidden");
    }
  }

  async resumeSession(id: string, message: string, opts: Omit<AdapterStartOptions, "prompt">) {
    const rpc = await this.connection();
    opts.signal.throwIfAborted();
    if (opts.previousVisibility === "hidden") await this.setVisibility(id, "visible");
    try {
      await rpc.request<Session>("thread/resume", { threadId: id, ...this.threadOptions(opts) });
      opts.onSession(id);
      return await this.prompt(rpc, id, message, opts);
    }
    finally { if (opts.visibility === "hidden") await this.setVisibility(id, "hidden"); }
  }

  private async prompt(rpc: StdioRpc, threadId: string, text: string, opts: Omit<AdapterStartOptions, "prompt">): Promise<AdapterResult> {
    opts.signal.throwIfAborted();
    let units = 0;
    let summary = "";
    let usage: { inputTokens?: number; outputTokens?: number } | undefined;
    const deferred = Promise.withResolvers<AdapterResult>();
    void deferred.promise.catch(() => {});
    const abort = () => {
      void this.cancel(threadId).catch(() => {});
      deferred.reject(opts.signal.reason ?? new Fault(422, "TASK_CANCELED", "Agent task canceled"));
    };
    opts.signal.addEventListener("abort", abort, { once: true });
    const removeClose = rpc.onClose(error => deferred.reject(error));
    const removeObserver = rpc.observe((method, raw) => {
      const p = raw as { threadId?: string; turn?: { id: string; status: string; error?: { message: string } };
        item?: { type: string; text?: string }; tokenUsage?: { last?: typeof usage } };
      if (!p || p.threadId !== threadId) return;
      if (method === "turn/started" && p.turn) this.turns.set(threadId, p.turn.id);
      if (method === "thread/tokenUsage/updated") usage = p.tokenUsage?.last;
      if (method === "item/completed" && p.item) {
        if (p.item.type === "agentMessage" && p.item.text) summary = p.item.text.slice(-20_000);
        if (["agentMessage", "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall"].includes(p.item.type)) {
          units++;
          if (units > opts.maxTurns) {
            void this.cancel(threadId).catch(() => {});
            deferred.reject(new Fault(429, "TASK_TURN_LIMIT", "Codex task exceeded its interaction allowance"));
          }
        }
      }
      if (method === "turn/completed") {
        if (p.turn?.status !== "completed") {
          deferred.reject(new Fault(502, "AGENT_BACKEND_ERROR", p.turn?.error?.message ?? "Codex turn " + p.turn?.status));
        } else {
          const { inputUsdPerMillion, outputUsdPerMillion } = this.settings;
          const costUsd = usage && inputUsdPerMillion !== undefined && outputUsdPerMillion !== undefined ?
            ((usage.inputTokens ?? 0) * inputUsdPerMillion + (usage.outputTokens ?? 0) * outputUsdPerMillion) / 1_000_000 : undefined;
          deferred.resolve({ summary, costUsd });
        }
      }
    });
    try {
      const { turn } = await rpc.request<{ turn: { id: string } }>("turn/start", {
        threadId, input: [{ type: "text", text, text_elements: [] }],
      }, 20_000);
      this.turns.set(threadId, turn.id);
      if (opts.signal.aborted) abort();
      return await deferred.promise;
    } finally {
      opts.signal.removeEventListener("abort", abort);
      removeObserver();
      removeClose();
      this.turns.delete(threadId);
    }
  }

  async setVisibility(id: string, visibility: "visible" | "hidden") {
    const rpc = await this.connection();
    try { await rpc.request(visibility === "hidden" ? "thread/archive" : "thread/unarchive", { threadId: id }); }
    catch (error) {
      if (!(visibility === "visible" && /not archived|already unarchived/i.test((error as Error).message))) throw error;
    }
  }

  async cancel(id: string) {
    const turn = this.turns.get(id);
    if (turn && this.rpc?.alive) await this.rpc.request("turn/interrupt", { threadId: id, turnId: turn }, 3500);
  }

  async health() {
    try {
      const rpc = await this.connection();
      const result = await rpc.request<{ account?: unknown; requiresOpenaiAuth?: boolean }>("account/read", { refreshToken: false }, 4500);
      return result.requiresOpenaiAuth && !result.account ? { ok: false, detail: "Codex requires sign-in" } : { ok: true };
    } catch (error) { return { ok: false, detail: (error as Error).message }; }
  }
  async close() { this.rpc?.close(); await this.rpc?.waitClosed(); }
}
