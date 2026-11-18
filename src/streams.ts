type Listener = (item: any) => void;
export class Hub {
  private topics = new Map<string, Set<Listener>>();
  on(topic: string, fn: Listener) {
    let listeners = this.topics.get(topic);
    if (!listeners) this.topics.set(topic, (listeners = new Set()));
    listeners.add(fn);
    return () => {
      listeners.delete(fn);
      if (!listeners.size) this.topics.delete(topic);
    };
  }
  emit(topic: string, item: unknown) {
    for (const fn of this.topics.get(topic) ?? []) fn(item);
  }
  counts() {
    return Object.fromEntries(Array.from(this.topics, ([k, v]) => [k, v.size]));
  }
}

export interface Subscription<T> {
  release(): void;
  initial?: AsyncIterable<T>;
  select?: (item: T) => T | undefined;
}
export function ndjson<T>(req: Request, subscribe: (push: (item: T) => void) => Subscription<T>) {
  const queue: T[] = [];
  let wake: (() => void) | undefined;
  let closed = false;
  let heartbeat = false;
  let release: (() => void) | undefined;
  let timer: Timer | undefined;
  let controller: ReadableStreamDefaultController<Uint8Array>;
  const encoder = new TextEncoder();
  function cleanup() {
    if (closed) return;
    closed = true;
    clearInterval(timer);
    req.signal.removeEventListener("abort", abort);
    release?.();
    queue.length = 0;
    wake?.();
  }
  function abort() {
    cleanup();
  }
  const subscription = subscribe((item) => {
    if (closed) return;
    if (queue.length >= 256) {
      cleanup();
      controller.error(new Error("Slow subscriber: reconnect from the last cursor"));
      return;
    }
    queue.push(item);
    wake?.();
  });
  release = subscription.release;
  const initial = subscription.initial?.[Symbol.asyncIterator]();
  let ready = false;
  const frame = (value: unknown) => encoder.encode(JSON.stringify(value) + "\n");
  return new Response(
    new ReadableStream<Uint8Array>(
      {
        start(c) {
          controller = c;
          // Flush response headers immediately, even before the first hourly close.
          c.enqueue(encoder.encode("\n"));
          timer = setInterval(() => {
            heartbeat = true;
            wake?.();
          }, 15000);
          req.signal.addEventListener("abort", abort, { once: true });
          if (req.signal.aborted) cleanup();
        },
        async pull(c) {
          try {
            if (!ready) {
              const step = await initial?.next();
              if (closed) return c.close();
              if (step && !step.done) {
                c.enqueue(frame({ data: step.value }));
                return;
              }
              ready = true;
              c.enqueue(frame({ ready: true }));
              return;
            }
            while (!closed) {
              while (queue.length) {
                const item = queue.shift()!;
                const selected = subscription.select ? subscription.select(item) : item;
                if (selected !== undefined) {
                  c.enqueue(frame({ data: selected }));
                  return;
                }
              }
              if (heartbeat) {
                heartbeat = false;
                c.enqueue(encoder.encode("\n"));
                return;
              }
              await new Promise<void>((resolve) => {
                wake = resolve;
              });
            }
            c.close();
          } catch (error) {
            if (!closed) {
              c.enqueue(frame({ error: String(error) }));
              c.close();
            }
            cleanup();
          }
        },
        cancel() {
          cleanup();
          void initial?.return?.();
        },
      },
      { highWaterMark: 1 },
    ),
    { headers: { "Content-Type": "application/x-ndjson", "Cache-Control": "no-store" } },
  );
}
