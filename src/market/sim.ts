import { periods, type Bar, type Interval, type Range, type Spec } from "@quant/tools";
import type { FeedEvents, Source } from "./source";
const minute = 60000;
const basePrice = (t: number) => 60000 + (((Math.floor(t / minute) % 240) + 240) % 240);
const px = (v: number) => v.toFixed(2);
export function simBar(symbol: string, interval: Interval, time: number): Bar {
  const duration = periods[interval],
    count = duration / minute;
  const first = basePrice(time),
    last = basePrice(time + duration - minute);
  const wraps = count >= 240 || last < first;
  return {
    symbol,
    interval,
    openTime: time,
    closeTime: time + duration - 1,
    open: px(first),
    close: px(last + 0.2),
    high: px((wraps ? 60239 : last) + 0.5),
    low: px((wraps ? 60000 : first) - 0.5),
    volume: String(count * 10),
  };
}
export class SimSource implements Source {
  private keys = new Set<string>();
  private timer?: Timer;
  private events?: FeedEvents;
  private clock: number;
  constructor(
    private tickMs: number,
    private symbols: string[],
    resume = 0,
  ) {
    this.clock = Math.max(Math.floor(Date.now() / periods["1h"]) * periods["1h"] + 58 * minute, resume);
  }
  now() {
    return this.clock;
  }
  status() {
    return { public: "connected", market: "connected" };
  }
  async ready() {}
  streams(keys: Set<string>) {
    this.keys = keys;
  }
  start(events: FeedEvents) {
    this.events = events;
    this.step();
    this.timer = setInterval(() => this.step(), this.tickMs);
  }
  stop() {
    clearInterval(this.timer);
  }
  step() {
    this.clock += minute;
    for (const key of this.keys) {
      const [raw, stream] = key.split("@"),
        s = raw.toUpperCase(),
        E = this.clock;
      const price = basePrice(E),
        seq = E / minute;
      let event;
      if (stream === "bookTicker")
        event = { e: stream, E, s, u: seq, b: px(price), a: px(price + 0.1), B: "2", A: "2" };
      if (stream === "markPrice")
        event = { e: "markPriceUpdate", E, s, p: px(price), i: px(price), r: "0.0001", T: E + 28800000 };
      if (stream === "aggTrade") event = { e: stream, E, s, a: seq, p: px(price), q: "0.01", T: E, m: false };
      if (stream === "forceOrder" && seq % 10 === 0)
        event = { e: stream, E, o: { s, S: "SELL", p: px(price), q: "0.01", T: E } };
      if (stream === "depth")
        event = { e: "depthUpdate", E, s, U: seq - 1, u: seq, pu: seq - 1, b: [[px(price), "2"]], a: [] };
      if (stream.startsWith("kline_")) {
        const i = stream.slice(6) as Interval,
          step = periods[i];
        if (E % step === 0) {
          const bar = simBar(s, i, E - step);
          event = {
            e: "kline",
            E,
            s,
            k: {
              t: bar.openTime,
              T: bar.closeTime,
              i,
              o: bar.open,
              h: bar.high,
              l: bar.low,
              c: bar.close,
              v: bar.volume,
              x: true,
            },
          };
        }
      }
      if (event) this.events?.message(key, event);
    }
    this.events?.sample();
  }
  async bars(symbol: string, interval: Interval, { start, end }: Range) {
    const result: Bar[] = [];
    for (let t = start; t < end; t += periods[interval]) result.push(simBar(symbol, interval, t));
    return result;
  }
  async specs(): Promise<Spec[]> {
    return [...new Set([...this.symbols, "BTCUSDT", "ETHUSDT"])].map((symbol) => ({
      symbol,
      tickSize: "0.10",
      stepSize: "0.001",
      minQty: "0.001",
      maxQty: "1000",
      marketStep: "0.001",
      marketMin: "0.001",
      marketMax: "1000",
      minNotional: "5",
    }));
  }
  async depth(_symbol: string) {
    const p = basePrice(this.clock);
    return {
      lastUpdateId: String(this.clock / minute),
      bids: [[px(p), "2"]],
      asks: [[px(p + 0.1), "2"]],
    } as Awaited<ReturnType<Source["depth"]>>;
  }
  async trades(_symbol: string, range: Range | undefined, limit: number) {
    const end = Math.min(range?.end ?? this.clock + 1, this.clock + 1);
    const start = Math.ceil((range?.start ?? end - limit * minute) / minute) * minute;
    return Array.from({ length: Math.max(0, Math.min(limit, Math.ceil((end - start) / minute))) }, (_, i) => {
      const time = start + i * minute;
      return { id: String(time / minute), time, price: px(basePrice(time)), quantity: "0.01", buyerMaker: false };
    });
  }
  async metric(kind: "oi" | "ratio" | "funding", _symbol: string, range?: Range) {
    const step = kind === "funding" ? 8 * periods["1h"] : 5 * minute;
    const first = Math.ceil((range?.start ?? this.clock) / step) * step;
    const result = [];
    for (
      let time = range ? first : this.clock;
      time < (range?.end ?? this.clock + 1) && time <= this.clock;
      time += step
    ) {
      result.push({ time, value: { oi: "1234", ratio: "1.04", funding: "0.0001" }[kind], source: "sim" as const });
    }
    return result;
  }
  async stats(symbol: string) {
    return { symbol, lastPrice: px(basePrice(this.clock)), volume: "14400", closeTime: this.clock };
  }
}
