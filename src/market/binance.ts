import { periods, type AggregateTrade, type Interval, type Metric, type Range, type Spec } from "@quant/tools";
import { endpoint } from "@quant/tools/transport";
import type { Config } from "../config";
import { wireJSON } from "../util";
import { archiveDay } from "./archive";
import { MarketSocket } from "./socket";
import { groupOf, type FeedEvents, type Group, type Source, type Wire } from "./source";
const day = 86400000;
export const aggregate = (r: Wire): AggregateTrade => ({
  id: String(r.a),
  price: r.p,
  quantity: r.q,
  time: r.T,
  buyerMaker: r.m,
});
export class BinanceSource implements Source {
  private sockets = new Map<Group, MarketSocket>();
  private keys = new Set<string>();
  constructor(private cfg: Config) {}
  now() {
    return Date.now();
  }
  start(events: FeedEvents) {
    for (const group of ["public", "market"] as const)
      this.sockets.set(group, new MarketSocket(group, this.cfg, events));
    this.streams(this.keys);
  }
  stop() {
    for (const s of this.sockets.values()) s.stop();
  }
  streams(keys: Set<string>) {
    this.keys = keys;
    for (const [group, socket] of this.sockets) socket.set(new Set([...keys].filter((k) => groupOf(k) === group)));
  }
  async ready(key: string) {
    await this.sockets.get(groupOf(key))!.ready(key);
  }
  heartbeats() {
    return {
      public: this.sockets.get("public")?.lastPong ?? null,
      market: this.sockets.get("market")?.lastPong ?? null,
    };
  }
  status() {
    return {
      public: this.sockets.get("public")?.state ?? "disconnected",
      market: this.sockets.get("market")?.state ?? "disconnected",
    };
  }
  async rest(path: string, params = {}): Promise<any> {
    const res = await fetch(endpoint(this.cfg.rest, path, params), {
      signal: AbortSignal.timeout(this.cfg.timeoutMs),
    });
    if (!res.ok) throw new Error(`Market ${path}: HTTP ${res.status} ${await res.text()}`);
    return wireJSON(await res.text());
  }
  async bars(symbol: string, interval: Interval, { start, end }: Range) {
    const rows: any[][] = await this.rest("/fapi/v1/klines", {
      symbol,
      interval,
      startTime: start,
      endTime: end - 1,
      limit: 1000,
    });
    return rows
      .filter((r) => r[6] < this.now() && r[0] >= start && r[0] < end)
      .map((r) => ({
        symbol,
        interval,
        openTime: r[0],
        closeTime: r[6],
        open: r[1],
        high: r[2],
        low: r[3],
        close: r[4],
        volume: r[5],
      }));
  }
  async specs(): Promise<Spec[]> {
    const info = await this.rest("/fapi/v1/exchangeInfo");
    return info.symbols
      .filter((s: Wire) => s.contractType === "PERPETUAL" && s.status === "TRADING")
      .map((s: Wire) => {
        const f = Object.fromEntries(s.filters.map((v: Wire) => [v.filterType, v]));
        const lot = f.LOT_SIZE,
          market = f.MARKET_LOT_SIZE ?? lot;
        return {
          symbol: s.symbol,
          tickSize: f.PRICE_FILTER.tickSize,
          stepSize: lot.stepSize,
          minQty: lot.minQty,
          maxQty: lot.maxQty,
          marketStep: market.stepSize,
          marketMin: market.minQty,
          marketMax: market.maxQty,
          minNotional: f.MIN_NOTIONAL?.notional ?? "0",
        };
      });
  }
  async depth(symbol: string) {
    const r = await this.rest("/fapi/v1/depth", { symbol, limit: 1000 });
    return { lastUpdateId: String(r.lastUpdateId), bids: r.bids, asks: r.asks };
  }
  stats(symbol: string) {
    return this.rest("/fapi/v1/ticker/24hr", { symbol });
  }
  private async archive(kind: "oi" | "ratio" | "trades", symbol: string, range: Range, limit: number) {
    const rows: any[] = [];
    if (range.start >= range.end) return rows;
    for (let t = Math.floor(range.start / day) * day; t < range.end && rows.length < limit; t += day) {
      const date = new Date(t).toISOString().slice(0, 10);
      rows.push(...(await archiveDay(this.cfg, { ...range, kind, symbol, day: date, limit: limit - rows.length })));
    }
    return rows;
  }
  async trades(symbol: string, range: Range | undefined, limit: number): Promise<AggregateTrade[]> {
    if (!range) return (await this.rest("/fapi/v1/aggTrades", { symbol, limit })).map(aggregate);
    const split = Math.floor(this.now() / day) * day - day;
    const rows = await this.archive(
      "trades",
      symbol,
      { start: range.start, end: Math.min(split, range.end) },
      limit,
    );
    for (let t = Math.max(range.start, split); t < range.end && rows.length < limit; t += periods["1h"]) {
      const data: Wire[] = await this.rest("/fapi/v1/aggTrades", {
        symbol,
        startTime: t,
        endTime: Math.min(range.end, t + periods["1h"]) - 1,
        limit: limit - rows.length,
      });
      rows.push(...data.map(aggregate));
    }
    return rows;
  }
  async metric(kind: "oi" | "ratio" | "funding", symbol: string, range?: Range): Promise<Metric[]> {
    if (!range) {
      const r = await this.rest("/fapi/v1/openInterest", { symbol });
      return [{ time: r.time, value: r.openInterest, source: "rest" }];
    }
    const funding = kind === "funding";
    const split = funding ? range.start : Math.floor(this.now() / day) * day - 28 * day;
    const rows: Metric[] = funding
      ? []
      : await this.archive(kind, symbol, { start: range.start, end: Math.min(range.end, split) }, Infinity);
    const path = {
      oi: "/futures/data/openInterestHist",
      ratio: "/futures/data/globalLongShortAccountRatio",
      funding: "/fapi/v1/fundingRate",
    }[kind];
    let start = Math.max(split, range.start);
    while (start < range.end) {
      const end = Math.min(range.end, start + (funding ? 30 * day : 500 * 300000));
      const data: Wire[] = await this.rest(path, {
        symbol,
        startTime: start,
        endTime: end - 1,
        limit: 500,
        period: funding ? undefined : "5m",
      });
      for (const r of data)
        rows.push({
          time: r.fundingTime ?? r.timestamp,
          value: r[{ funding: "fundingRate", oi: "sumOpenInterest", ratio: "longShortRatio" }[kind]],
          source: "rest",
        });
      start = data.length === 500 ? Math.max(start + 1, rows.at(-1)!.time + 1) : end;
    }
    return rows;
  }
}
