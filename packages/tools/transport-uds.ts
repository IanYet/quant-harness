export type Params = Record<string, unknown>;
export type Endpoint = "runtime" | "storage";
export type Frame = { seq?: number; ts?: number; type?: string; throughSeq?: number; [key: string]: unknown };

export class QuantError extends Error {
  constructor(readonly code: string, message: string, readonly status?: number, readonly details?: unknown) {
    super(message);
  }
}

export function location(path: string, args?: Params) {
  const url = new URL(path, "http://localhost");
  for (const [key, value] of Object.entries(args ?? {})) {
    if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
  }
  return url;
}

export class UdsTransport {
  constructor(private readonly sockets: Record<Endpoint, string>, private readonly token?: string) {}

  private headers(json: boolean) {
    const headers = new Headers();
    if (this.token) headers.set("authorization", "Bearer " + this.token);
    if (json) headers.set("content-type", "application/json");
    return headers;
  }

  async request<T>(endpoint: Endpoint, path: string, args?: Params, method = "GET", body?: unknown, signal?: AbortSignal): Promise<T> {
    const result = await fetch(location(path, args), { unix: this.sockets[endpoint], method, redirect: "error",
      headers: this.headers(body !== undefined), body: body === undefined ? undefined : JSON.stringify(body),
      signal: signal ?? AbortSignal.timeout(30_000) });
    const value = await result.json() as { error?: { code: string; message: string; details?: unknown } };
    if (!result.ok) throw new QuantError(value.error?.code ?? "HTTP_ERROR", value.error?.message ?? result.statusText, result.status, value.error?.details);
    return value as T;
  }

  async *stream<T>(path: string, args: Params, opts: { reliable?: boolean; signal?: AbortSignal } = {}): AsyncGenerator<T> {
    let afterSeq = args.after_seq;
    const abort = new AbortController();
    const signal = opts.signal ? AbortSignal.any([abort.signal, opts.signal]) : abort.signal;
    try {
      while (!signal.aborted) {
        try {
          const res = await fetch(location(path, { ...args, after_seq: afterSeq }), { unix: this.sockets.runtime,
            headers: this.headers(false), redirect: "error", signal });
          if (!res.ok || !res.body) {
            const error = await res.json().catch(() => ({})) as { error?: { code: string; message: string } };
            throw new QuantError(error.error?.code ?? "STREAM_ERROR", error.error?.message ?? res.statusText, res.status);
          }
          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          let pending = "";
          try {
            while (!signal.aborted) {
              const { value, done } = await reader.read();
              if (done) break;
              pending += decoder.decode(value, { stream: true });
              let end: number;
              while ((end = pending.indexOf("\n")) !== -1) {
                const frame = JSON.parse(pending.slice(0, end)) as Frame;
                pending = pending.slice(end + 1);
                if (frame.throughSeq !== undefined) afterSeq = frame.throughSeq;
                if (frame.type === "gap") throw new QuantError("EVENT_GAP", "Historical market frames have expired", undefined, frame);
                if (frame.seq !== undefined) afterSeq = frame.seq;
                if (!opts.reliable || frame.seq !== undefined) yield frame as T;
              }
              if (pending.length > 4 * 1024 * 1024) throw new QuantError("FRAME_TOO_LARGE", "Stream frame exceeds the limit");
            }
          } finally {
            await reader.cancel().catch(() => {});
            reader.releaseLock();
          }
        } catch (error) {
          if (signal.aborted) return;
          if (error instanceof QuantError && (error.status === undefined || (error.status >= 400 && error.status < 500))) throw error;
        }
        await Bun.sleep(500);
      }
    } finally {
      abort.abort();
    }
  }
}
