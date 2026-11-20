import { chmod, lstat, mkdir, unlink } from "node:fs/promises";
import { connect } from "node:net";
import { dirname } from "node:path";
import { Fault } from "./http";

export async function serveUnix(socket: string, fetch: (req: Request) => Response | Promise<Response>) {
  await mkdir(dirname(socket), { mode: 0o750, recursive: true });
  try {
    if (!(await lstat(socket)).isSocket()) throw new Error("Expected a Unix socket at " + socket);
    const alive = await new Promise<boolean>((resolve, reject) => {
      const client = connect(socket);
      client.once("connect", () => { client.destroy(); resolve(true); });
      client.once("error", (error: NodeJS.ErrnoException) => {
        if (error.code === "ECONNREFUSED" || error.code === "ENOENT") resolve(false);
        else reject(error);
      });
    });
    if (alive) throw new Error("Socket in use: " + socket);
    await unlink(socket);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const server = Bun.serve({ unix: socket, maxRequestBodySize: 3 * 1024 * 1024, fetch(req, server) { server.timeout(req, 0); return fetch(req); } });
  await chmod(socket, 0o660);
  return server;
}

export function withQuery(path: string, params: Record<string, unknown> = {}): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) query.set(key, String(value));
  }
  return query.size ? path + "?" + query : path;
}

export async function unixFetch(socket: string, path: string, init: RequestInit = {}): Promise<Response> {
  return fetch("http://localhost" + path, { ...init, unix: socket, redirect: "error" });
}

export type CallOptions = { method?: string; body?: unknown; token?: string; timeoutMs?: number; headers?: HeadersInit };

export async function call<T = unknown>(socket: string, path: string, options: CallOptions = {}): Promise<T> {
  const { method = "GET", body, token, timeoutMs = 10_000 } = options;
  const headers = new Headers(options.headers);
  if (token) headers.set("authorization", "Bearer " + token);
  if (body !== undefined) headers.set("content-type", "application/json");
  let res: Response;
  try {
    res = await unixFetch(socket, path, { method, headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) });
  } catch {
    throw new Fault(503, "UNAVAILABLE", "Could not reach " + socket);
  }
  const value = await res.json() as { error?: { code: string; message: string; details: Record<string, unknown> } };
  if (!res.ok) throw new Fault(res.status, value.error?.code ?? "UPSTREAM_ERROR", value.error?.message ?? res.statusText, value.error?.details);
  return value as T;
}

export async function* readNdjson<T>(res: Response): AsyncGenerator<T> {
  if (!res.ok || !res.body) throw new Fault(res.status, "STREAM_UNAVAILABLE", "Stream HTTP " + res.status);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffered = "";
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buffered += decoder.decode(value, { stream: true });
      let end = buffered.indexOf("\n");
      while (end >= 0) {
        const line = buffered.slice(0, end);
        buffered = buffered.slice(end + 1);
        if (line) yield JSON.parse(line) as T;
        end = buffered.indexOf("\n");
      }
      if (buffered.length > 4 * 1024 * 1024) throw new Error("NDJSON frame too long");
    }
    if (buffered.trim()) throw new Error("Incomplete NDJSON frame");
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export function ndjson(frames: AsyncIterable<unknown>, onClose?: () => void): Response {
  const iterator = frames[Symbol.asyncIterator]();
  const encoder = new TextEncoder();
  return new Response(new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const frame = await iterator.next();
        if (frame.done) {
          onClose?.();
          controller.close();
        } else {
          controller.enqueue(encoder.encode(JSON.stringify(frame.value) + "\n"));
        }
      } catch (error) {
        onClose?.();
        controller.error(error);
      }
    },
    async cancel() {
      onClose?.();
      await iterator.return?.();
    },
  }, { highWaterMark: 0 }), { headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" } });
}
