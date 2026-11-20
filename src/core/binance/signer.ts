import { createHmac } from "node:crypto";
import { Fault } from "../../shared/http";

export class WeightBucket {
  private window = -1;
  private used = 0;
  private cooldown = 0;
  private readonly quota = new Map<string, { window: number; count: number }>();
  constructor(readonly limit: number, readonly reserve = Math.max(20, Math.floor(limit * 0.15))) {}

  take(weight: number, priority = false, group?: { key: string; limit: number }) {
    const time = Date.now();
    if (time < this.cooldown) throw new Fault(429, "RATE_LIMITED", "Binance retry window is still active");
    const minute = Math.floor(time / 60_000);
    if (minute !== this.window) { this.window = minute; this.used = 0; }
    if (this.used + weight > this.limit - (priority ? 0 : this.reserve)) {
      throw new Fault(429, "RATE_LIMITED", "Binance IP weight budget is exhausted");
    }
    if (group) {
      const window = Math.floor(time / 300_000);
      const previous = this.quota.get(group.key);
      const count = previous?.window === window ? previous.count : 0;
      if (count >= group.limit) throw new Fault(429, "RATE_LIMITED", "Binance endpoint request budget is exhausted");
      this.quota.set(group.key, { window, count: count + 1 });
    }
    this.used += weight;
  }

  observe(headers: Headers, status: number, requested: number) {
    const weight = Number(headers.get("x-mbx-used-weight-1m") ?? 0);
    const minute = Math.floor(Date.now() / 60_000);
    if (Number.isFinite(weight) && minute === this.window && minute === requested) this.used = Math.max(this.used, weight);
    if (status === 429 || status === 418) {
      const retry = Number(headers.get("retry-after"));
      this.cooldown = Date.now() + (retry > 0 ? Math.min(retry * 1000, 24 * 3600_000) : status === 418 ? 120_000 : 1000);
    }
  }
}

type Params = Record<string, string | number | boolean | undefined>;
type RequestOptions = {
  method?: string; signed?: boolean; apiKeyOnly?: boolean; weight?: number; priority?: boolean; uncertainWrite?: boolean;
  group?: { key: string; limit: number }; signal?: AbortSignal;
};
type Keys = { apiKey: string; apiSecret: string };

export class BinanceError extends Fault {
  constructor(readonly binanceCode: number | undefined, readonly upstreamStatus: number, message: string) {
    super(upstreamStatus === 429 || upstreamStatus === 418 ? 429 : 502,
      upstreamStatus === 429 || upstreamStatus === 418 ? "RATE_LIMITED" : "EXCHANGE_ERROR", message, { binanceCode, upstreamStatus });
  }
}

export function exchangeJson(text: string, exactIds: boolean): unknown {
  return JSON.parse(exactIds ? text.replace(/("(?:orderId|algoId|actualOrderId|tranId|id|i|aid|ai|t)"\s*:\s*)(-?\d{16,})(?=[,\s}])/g, '$1"$2"') : text);
}

export class BinanceRest {
  private offset = 0;
  private clockSync?: Promise<void>;
  constructor(readonly base: string, readonly bucket: WeightBucket, private readonly keys?: Keys) {}

  async request<T>(path: string, params: Params = {}, opts: RequestOptions = {}): Promise<T> {
    try { return await this.send<T>(path, params, opts); }
    catch (error) {
      if (opts.signed && error instanceof BinanceError && error.binanceCode === -1021) {
        this.clockSync ??= (async () => {
          const start = Date.now();
          const { serverTime } = await this.send<{ serverTime: number }>("/fapi/v1/time", {}, { weight: 1, priority: true });
          this.offset = serverTime - Math.round((Date.now() + start) / 2);
        })().finally(() => { this.clockSync = undefined; });
        await this.clockSync;
        return this.send<T>(path, params, opts);
      }
      throw error;
    }
  }

  private async send<T>(path: string, params: Params, opts: RequestOptions): Promise<T> {
    const url = new URL(this.base + path);
    const headers = new Headers({ accept: "application/json", "cache-control": "no-cache" });
    for (const [key, value] of Object.entries(params)) if (value !== undefined) url.searchParams.set(key, String(value));
    if (opts.signed || opts.apiKeyOnly) {
      if (!this.keys) throw new Error("Binance key missing");
      headers.set("x-mbx-apikey", this.keys.apiKey);
    }
    this.bucket.take(opts.weight ?? 1, opts.priority, opts.group);
    const minute = Math.floor(Date.now() / 60_000);
    if (opts.signed) {
      url.searchParams.set("timestamp", String(Date.now() + this.offset));
      url.searchParams.set("recvWindow", "5000");
      url.searchParams.set("signature", createHmac("sha256", this.keys!.apiSecret).update(url.searchParams.toString()).digest("hex"));
    }
    const timeout = AbortSignal.timeout(8000);
    const signal = opts.signal ? AbortSignal.any([timeout, opts.signal]) : timeout;
    let res: Response, data: unknown;
    try {
      res = await fetch(url, { method: opts.method ?? "GET", headers, signal, redirect: "error" });
      this.bucket.observe(res.headers, res.status, minute);
      const text = await res.text();
      try { data = exchangeJson(text, !!this.keys); }
      catch { if (res.ok) throw new Error("Invalid exchange JSON"); }
    } catch {
      throw new Fault(503, opts.uncertainWrite ? "ORDER_UNKNOWN" : "UNAVAILABLE", "Binance response unavailable");
    }
    const error = data as { code?: number; msg?: string } | undefined;
    if (!res.ok || (typeof error?.code === "number" && error.code < 0)) {
      const message = error?.msg || "Binance HTTP " + res.status;
      const uncertain = res.status === 408 || error?.code === -1007 || error?.code === -1006 ||
        (res.status >= 500 && ![-1008].includes(error?.code ?? 0) && !/^(Service Unavailable|Internal error; unable to process)/i.test(message));
      if (opts.uncertainWrite && uncertain) throw new Fault(503, "ORDER_UNKNOWN", message);
      throw new BinanceError(error?.code, res.status, message);
    }
    return data as T;
  }
}
