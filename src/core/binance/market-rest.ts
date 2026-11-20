import type { ExchangeSymbolInfo, Kline, KlineInterval } from "@quant/tools";
import type { BinanceConfig } from "../../shared/config";
import { bad, missing, oneOf, str, SYMBOL } from "../../shared/http";
import { klineIntervals } from "../../../packages/tools/types";
import { BinanceRest, WeightBucket } from "./signer";

export type Environment = "mainnet" | "testnet";
type Route = {
  path: string; keys?: string; weight: number | ((query: URLSearchParams) => number);
  ttl?: number; cache?: string; optionalSymbol?: boolean; kline?: boolean; group?: { key: string; limit: number };
};
const klineWeight = (params: URLSearchParams) => {
  const limit = Number(params.get("limit") ?? 500);
  if (limit > 1500) bad("Kline limit must be <= 1500");
  return limit < 100 ? 1 : limit < 500 ? 2 : limit <= 1000 ? 5 : 10;
};
const depthWeight = (params: URLSearchParams) => {
  const limit = oneOf(params.get("limit") ?? "100", "depth limit", ["5", "10", "20", "50", "100", "500", "1000"]);
  return Number(limit) <= 50 ? 2 : limit === "100" ? 5 : limit === "500" ? 10 : 20;
};
const kline = (path: string, by: string, extra = ""): Route =>
  ({ path: "/fapi/v1/" + path, keys: by + " interval startTime endTime limit " + extra, weight: klineWeight, ttl: 2000, kline: true });
const futures = (path: string, extra = "", by = "symbol"): Route =>
  ({ path: "/futures/data/" + path, keys: by + " period startTime endTime limit " + extra, weight: 0, ttl: 30_000,
    cache: "futuresData", group: { key: "futuresData", limit: 1000 } });
const funding = { key: "funding", limit: 500 };
const endpoints: Record<string, Route> = {
  "exchange-info": { path: "/fapi/v1/exchangeInfo", keys: "symbol", weight: 1, ttl: 300_000, cache: "exchangeInfo", optionalSymbol: true },
  depth: { path: "/fapi/v1/depth", keys: "symbol limit", weight: depthWeight, ttl: 1000, cache: "depth" },
  trades: { path: "/fapi/v1/trades", keys: "symbol limit", weight: 5 },
  "agg-trades": { path: "/fapi/v1/aggTrades", keys: "symbol fromId startTime endTime limit", weight: 20 },
  klines: kline("klines", "symbol"),
  "continuous-klines": kline("continuousKlines", "pair", "contractType"),
  "index-klines": kline("indexPriceKlines", "pair"),
  "mark-klines": kline("markPriceKlines", "symbol"),
  "premium-klines": kline("premiumIndexKlines", "symbol"),
  "premium-index": { path: "/fapi/v1/premiumIndex", keys: "symbol", weight: q => q.has("symbol") ? 1 : 10, ttl: 1000, optionalSymbol: true },
  "ticker-24h": { path: "/fapi/v1/ticker/24hr", keys: "symbol", weight: q => q.has("symbol") ? 1 : 40, ttl: 1000, optionalSymbol: true },
  "ticker-price": { path: "/fapi/v2/ticker/price", keys: "symbol", weight: q => q.has("symbol") ? 1 : 2, ttl: 1000, optionalSymbol: true },
  "book-ticker": { path: "/fapi/v1/ticker/bookTicker", keys: "symbol", weight: q => q.has("symbol") ? 2 : 5, ttl: 1000, optionalSymbol: true },
  "funding-rates": { path: "/fapi/v1/fundingRate", keys: "symbol startTime endTime limit", weight: 1, group: funding, ttl: 2000 },
  "funding-info": { path: "/fapi/v1/fundingInfo", keys: "symbol", weight: 1, group: funding, ttl: 300_000, cache: "fundingInfo", optionalSymbol: true },
  "open-interest": { path: "/fapi/v1/openInterest", keys: "symbol", weight: 1, ttl: 5000, cache: "openInterest" },
  "open-interest-hist": futures("openInterestHist"),
  "top-ls-accounts": futures("topLongShortAccountRatio"),
  "top-ls-positions": futures("topLongShortPositionRatio"),
  "global-ls-accounts": futures("globalLongShortAccountRatio"),
  "taker-volume": futures("takerlongshortRatio"),
  basis: futures("basis", "contractType", "pair"),
  "index-info": { path: "/fapi/v1/indexInfo", keys: "symbol", weight: 1, optionalSymbol: true, ttl: 1000 },
  constituents: { path: "/fapi/v1/constituents", keys: "symbol", weight: 2, ttl: 30_000 },
  "asset-index": { path: "/fapi/v1/assetIndex", keys: "symbol", weight: q => q.has("symbol") ? 1 : 10, optionalSymbol: true, ttl: 5000 },
};

export function decodeKline(symbol: string, interval: KlineInterval, values: unknown[], now = Date.now()): Kline {
  const [openTime, o, h, l, c, v, closeTime, quoteVolume] = values.map(Number);
  if (![openTime, o, h, l, c, closeTime].every(Number.isFinite)) throw new Error("Invalid Binance kline");
  return { symbol, interval, openTime, closeTime, o, h, l, c, v: v || 0, quoteVolume: quoteVolume || 0, closed: closeTime < now };
}

export class MarketRest {
  readonly clients: Record<Environment, BinanceRest>;
  private readonly cache = new Map<string, { expires: number; data: unknown }>();
  private readonly pending = new Map<string, Promise<unknown>>();
  private readonly revision: Record<Environment, number> = { mainnet: 0, testnet: 0 };

  constructor(private readonly config: BinanceConfig, private readonly defaultSymbol: string, bucket: WeightBucket) {
    this.clients = { mainnet: new BinanceRest(config.mainnetRest, bucket), testnet: new BinanceRest(config.testnetRest, bucket) };
  }

  invalidate(env: Environment) {
    this.revision[env]++;
    this.cache.delete(env + ":exchange-info");
    this.pending.delete(env + ":exchange-info");
  }

  async rules(symbol: string, env: Environment, priority = false) {
    const rules = await this.query("exchange-info", new URLSearchParams({ symbol }), env, priority) as ExchangeSymbolInfo[];
    return rules[0] ?? missing("Binance symbol rules unavailable for " + symbol);
  }

  async query(endpoint: string, request: URLSearchParams, env: Environment = "mainnet", priority = false) {
    if (!Object.hasOwn(endpoints, endpoint)) return missing("Unknown Binance market endpoint");
    const spec = endpoints[endpoint];
    const allowed = new Set(spec.keys?.split(" ").filter(Boolean));
    const query = new URLSearchParams();
    for (const [raw, v] of request) {
      const key = raw === "start" ? "startTime" : raw === "end" ? "endTime" : raw;
      if (!allowed.has(key) || query.has(key)) bad("Unexpected Binance parameter " + raw);
      if (key === "symbol" || key === "pair") str(v, key, SYMBOL);
      if (key === "interval") oneOf(v, key, klineIntervals);
      if (key === "contractType") oneOf(v, key, ["PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"]);
      if (key === "period") oneOf(v, key, ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]);
      if (["fromId", "startTime", "endTime", "limit"].includes(key) && (!/^\d{1,18}$/.test(v) || (key === "limit" && Number(v) === 0))) bad("Invalid " + key);
      query.set(key, v);
    }
    if (!spec.optionalSymbol && allowed.has("symbol") && !query.has("symbol")) query.set("symbol", this.defaultSymbol);
    if (allowed.has("pair") && !query.has("pair")) query.set("pair", this.defaultSymbol);
    if (allowed.has("period") && !query.has("period")) query.set("period", "5m");
    const symbol = query.get("symbol") ?? query.get("pair");
    if (endpoint === "exchange-info" || endpoint === "funding-info") query.delete("symbol");
    query.sort();
    const key = env + ":" + endpoint + (query.size ? "?" + query : "");
    const ttl = this.config.publicRestCacheTtlMs[spec.cache ?? ""] ?? spec.ttl ?? 0;
    const revision = this.revision[env];
    const weight = typeof spec.weight === "function" ? spec.weight(query) : spec.weight;
    const cached = this.cache.get(key);
    let raw: unknown;
    if (cached && cached.expires > Date.now()) raw = cached.data;
    else {
      let job = this.pending.get(key);
      if (!job) {
        job = this.clients[env].request(spec.path, Object.fromEntries(query), { weight, group: spec.group, priority })
          .then(value => {
            if (ttl && (endpoint !== "exchange-info" || revision === this.revision[env])) {
              if (this.cache.size >= 2048) this.cache.delete(this.cache.keys().next().value!);
              this.cache.set(key, { expires: Date.now() + ttl, data: value });
            }
            return value;
          }).finally(() => { if (this.pending.get(key) === job) this.pending.delete(key); });
        this.pending.set(key, job);
      }
      raw = await job;
    }
    if (endpoint === "exchange-info") return (raw as { symbols: ExchangeSymbolInfo[] }).symbols
      .filter(item => !symbol || item.symbol === symbol);
    if (endpoint === "funding-info" && symbol) return (raw as { symbol: string }[]).filter(item => item.symbol === symbol);
    if (spec.kline) return (raw as unknown[][]).map(row => decodeKline(str(symbol, "symbol"), oneOf(query.get("interval"), "interval", klineIntervals), row));
    return raw;
  }
}
