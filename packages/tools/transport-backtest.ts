import { QuantError, UdsTransport } from "./transport-uds";
import type { Endpoint, Frame, Params } from "./transport-uds";

export class BacktestTransport extends UdsTransport {
  now = Number(process.env.QUANT_BACKTEST_START ?? 0);

  override request<T>(_endpoint: Endpoint, path: string, args?: Params, method = "GET", body?: unknown): Promise<T> {
    return super.request<T>("runtime", path, args, method, body);
  }

  override async *stream<T>(path: string, args: Params): AsyncGenerator<T> {
    if (path !== "/v1/stream") throw new QuantError("UNSUPPORTED_BACKTEST", "This stream is unavailable in a historical replay");
    const { id } = await this.request<{ id: string }>("runtime", "/v1/replay/subscriptions", args, "POST");
    try {
      for (;;) {
        const frame = await this.request<Frame | null>("runtime", "/v1/replay/subscriptions/" + id);
        if (frame === null) return;
        if (frame.ts !== undefined) this.now = frame.ts;
        yield frame as T;
      }
    } finally {
      await this.request("runtime", "/v1/replay/subscriptions/" + id, undefined, "DELETE").catch(() => {});
    }
  }
}
