export type Params = Record<string, string | number | boolean | undefined>;
export function endpoint(base: string, path: string, params?: Params) {
  const url = new URL(path, base);
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined) url.searchParams.set(key, String(value));
  }
  return url;
}

export async function check(response: Response) {
  if (!response.ok) throw new Error(`HTTP ${response.status} ${await response.text()}`);
  return response;
}

export async function* decodeLines<T>(body: ReadableStream<Uint8Array>): AsyncGenerator<T> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let pending = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      pending += decoder.decode(value, { stream: !done });
      let end: number;
      while ((end = pending.indexOf("\n")) >= 0) {
        const line = pending.slice(0, end).trim();
        pending = pending.slice(end + 1);
        if (line) yield JSON.parse(line) as T;
      }
      if (pending.length > 4_000_000) throw new Error("NDJSON line too large");
      if (done) {
        if (pending.trim()) throw new Error("Incomplete NDJSON line");
        break;
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export function transport(base: string, token?: string) {
  const headers = token ? { Authorization: `Bearer ${token}` } : undefined;
  async function get<T>(path: string, params?: Params): Promise<T> {
    return (await check(await fetch(endpoint(base, path, params), { headers }))).json() as Promise<T>;
  }
  async function post<T>(path: string, body: unknown): Promise<T> {
    return (
      await check(
        await fetch(endpoint(base, path), {
          method: "POST",
          headers: { ...headers, "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }),
      )
    ).json() as Promise<T>;
  }
  async function* stream<T>(path: string, params: Params, signal?: AbortSignal) {
    const abort = new AbortController();
    const combined = signal ? AbortSignal.any([signal, abort.signal]) : abort.signal;
    try {
      const response = await check(
        await fetch(endpoint(base, path, params), { headers, signal: combined, timeout: false }),
      );
      type Frame = { data: T } | { ready: true } | { error: string };
      for await (const frame of decodeLines<Frame>(response.body!)) {
        if ("error" in frame) throw new Error(frame.error);
        if ("data" in frame) yield frame.data;
        if ("ready" in frame && process.send) process.send("harness:ready");
      }
      if (!combined.aborted) throw new Error("Event stream disconnected");
    } finally {
      abort.abort();
    }
  }
  return { get, post, stream };
}
