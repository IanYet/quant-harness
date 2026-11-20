import { Fault } from "../../shared/http";

type Document = { id?: string | number; method?: string; params?: unknown; result?: unknown; error?: { code?: number; message?: string } };
type Waiter = { method: string; resolve(value: unknown): void; reject(error: unknown): void; timer: ReturnType<typeof setTimeout> };

export class StdioRpc {
  private readonly child: ReturnType<typeof Bun.spawn<"pipe", "pipe", "pipe">>;
  private readonly pending = new Map<number | string, Waiter>();
  private readonly listeners = new Set<(method: string, params: unknown) => void>();
  private readonly closedListeners = new Set<(error: Fault) => void>();
  private counter = 0;
  private failure?: Fault;

  constructor(cmd: string[], cwd: string, env: NodeJS.ProcessEnv, private readonly onRequest?: (method: string, params: unknown) => unknown,
      private readonly jsonrpc = true) {
    this.child = Bun.spawn(cmd, { cwd, env, stdin: "pipe", stdout: "pipe", stderr: "pipe" });
    void this.read(this.child.stdout).catch(() => this.finish("Malformed RPC stream"));
    void this.drain(this.child.stderr);
    void this.child.exited.then(code => this.finish("Backend process exited (" + code + ")"));
  }

  get alive() { return !this.failure; }
  private send(value: Document) {
    if (this.failure) throw this.failure;
    this.child.stdin.write(JSON.stringify(this.jsonrpc ? { jsonrpc: "2.0", ...value } : value) + "\n");
    this.child.stdin.flush();
  }

  private async drain(stream: ReadableStream<Uint8Array>) {
    try { for await (const _ of stream) { /* Drain provider diagnostics without relaying credentials. */ } } catch {}
  }

  private async read(stream: ReadableStream<Uint8Array>) {
    const decoder = new TextDecoder();
    let buffered = "";
    for await (const bytes of stream) {
      buffered += decoder.decode(bytes, { stream: true });
      let end: number;
      while ((end = buffered.indexOf("\n")) !== -1) {
        const line = buffered.slice(0, end);
        buffered = buffered.slice(end + 1);
        if (!line.trim()) continue;
        let msg: Document;
        try { msg = JSON.parse(line) as Document; } catch { continue; }
        if (msg.method && msg.id !== undefined) {
          const result = this.onRequest?.(msg.method, msg.params);
          this.send(result === undefined ? { id: msg.id, error: { code: -32601, message: "Unsupported client request" } } : { id: msg.id, result });
        } else if (msg.method) {
          for (const listener of this.listeners) listener(msg.method, msg.params);
        } else if (msg.id !== undefined) {
          const waiter = this.pending.get(msg.id);
          if (!waiter) continue;
          this.pending.delete(msg.id);
          clearTimeout(waiter.timer);
          if (msg.error) waiter.reject(new Fault(502, "AGENT_BACKEND_ERROR", waiter.method + " · " + (msg.error.message ?? "Agent RPC failed")));
          else waiter.resolve(msg.result);
        }
      }
      if (buffered.length > 8 * 1024 * 1024) throw new Error("RPC frame too large");
    }
    this.finish("Backend stdout closed");
  }

  request<T>(method: string, params: unknown = {}, timeout = 10_000): Promise<T> {
    if (this.failure) return Promise.reject(this.failure);
    const id = ++this.counter;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Fault(503, "AGENT_BACKEND_TIMEOUT", "Backend request timed out at " + method));
      }, timeout);
      this.pending.set(id, { method, resolve: value => resolve(value as T), reject, timer });
      try { this.send({ id, method, params }); } catch (error) {
        this.pending.delete(id);
        clearTimeout(timer);
        reject(error);
      }
    });
  }

  notify(method: string, params?: unknown) { this.send({ method, params }); }
  observe(fn: (method: string, params: unknown) => void) { this.listeners.add(fn); return () => this.listeners.delete(fn); }
  onClose(fn: (error: Fault) => void) { this.closedListeners.add(fn); return () => this.closedListeners.delete(fn); }

  private finish(message: string) {
    if (this.failure) return;
    this.failure = new Fault(503, "AGENT_BACKEND_UNAVAILABLE", message);
    for (const waiter of this.pending.values()) { clearTimeout(waiter.timer); waiter.reject(this.failure); }
    for (const listener of this.closedListeners) listener(this.failure);
    this.pending.clear();
    this.listeners.clear();
    this.closedListeners.clear();
    this.child.kill("SIGTERM");
  }

  close() { this.finish("Backend closed"); }

  async waitClosed() {
    const timeout = setTimeout(() => this.child.kill("SIGKILL"), 3000);
    await this.child.exited.finally(() => clearTimeout(timeout));
  }
}
