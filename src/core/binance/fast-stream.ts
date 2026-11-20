import type { FastMarketEvent } from "@quant/tools";
import { bad, integer, oneOf, record, str, SYMBOL } from "../../shared/http";
import { Mailbox } from "../../shared/mailbox";
import { ndjson } from "../../shared/uds";
import { klineIntervals } from "../../../packages/tools/types";
import { WsChannel } from "./ws-channel";

const all = {
  all_book_tickers: "!bookTicker", all_tickers_24h: "!ticker@arr", all_mini_tickers: "!miniTicker@arr",
  all_liquidations: "!forceOrder@arr", all_asset_indexes: "!assetIndex@arr",
} as const;
const individual = { agg_trade: "aggTrade", ticker_24h: "ticker", mini_ticker: "miniTicker", composite_index: "compositeIndex" } as const;
type Resolved = { type: FastMarketEvent["type"]; channel: "public" | "market"; stream: string; symbol?: string };

export function resolveStream(value: unknown, fallback: string): Resolved {
  const spec = record(value);
  const type = oneOf(spec.type, "stream type", [
    "depth", "depth_diff", "rpi_depth", "all_book_tickers", "agg_trade", "continuous_kline", "ticker_24h",
    "all_tickers_24h", "mini_ticker", "all_mini_tickers", "all_mark_prices", "all_liquidations",
    "composite_index", "asset_index", "all_asset_indexes",
  ]);
  const symbol = str(spec.symbol ?? spec.pair ?? spec.asset ?? fallback, "symbol", SYMBOL);
  const name = symbol.toLowerCase();
  if (type === "depth" || type === "depth_diff") {
    const levels = type === "depth" ? integer(spec.levels, "levels", 5, 20) : "";
    if (type === "depth" && ![5, 10, 20].includes(Number(levels))) bad("Depth levels are 5, 10 or 20");
    const speed = oneOf(spec.speed ?? "250ms", "speed", ["250ms", "100ms", "500ms"]);
    return { type, channel: "public", symbol, stream: name + "@depth" + levels + (speed === "250ms" ? "" : "@" + speed) };
  }
  if (type === "rpi_depth") return { type, channel: "public", symbol, stream: name + "@rpiDepth@500ms" };
  if (type in all) return { type, channel: type === "all_book_tickers" ? "public" : "market", stream: all[type as keyof typeof all] };
  if (type in individual) return { type, channel: "market", symbol, stream: name + "@" + individual[type as keyof typeof individual] };
  if (type === "all_mark_prices") return { type, channel: "market",
    stream: "!markPrice@arr" + (oneOf(spec.speed ?? "3s", "speed", ["1s", "3s"]) === "1s" ? "@1s" : "") };
  if (type === "asset_index") return { type, channel: "market", symbol: str(spec.asset, "asset", SYMBOL), stream: name + "@assetIndex" };
  const contract = oneOf(spec.contractType, "contractType", ["perpetual", "current_quarter", "next_quarter"]);
  const interval = oneOf(spec.interval, "interval", klineIntervals);
  return { type, channel: "market", symbol, stream: name + "_" + contract + "@continuousKline_" + interval };
}

export class FastStreams {
  private readonly channels: Record<Resolved["channel"], WsChannel>;
  private readonly refs = new Map<string, { spec: Resolved; users: Set<Mailbox<FastMarketEvent>> }>();
  constructor(ws: string, private readonly defaultSymbol: string) {
    this.channels = {
      public: new WsChannel(ws + "/public/stream", (stream, data) => this.receive(stream, data)),
      market: new WsChannel(ws + "/market/stream", (stream, data) => this.receive(stream, data)),
    };
  }

  private sync() {
    const streams = { public: [] as string[], market: [] as string[] };
    for (const { spec } of this.refs.values()) streams[spec.channel].push(spec.stream);
    this.channels.public.set(streams.public);
    this.channels.market.set(streams.market);
  }

  private receive(stream: string, payload: unknown) {
    const entry = this.refs.get(stream);
    if (!entry) return;
    const data = payload as { E?: number; T?: number; s?: string } | undefined;
    const ts = Date.now();
    const frame = { stream, type: entry.spec.type, symbol: entry.spec.symbol ?? data?.s,
      eventTs: data?.E ?? data?.T ?? ts, ingestedTs: ts, payload };
    for (const box of entry.users) box.offer(frame);
  }

  subscribe(value: unknown, signal: AbortSignal) {
    if (!Array.isArray(value) || value.length === 0 || value.length > 50) bad("Provide 1 to 50 stream specifications");
    const streams = [...new Map(value.map(raw => {
      const spec = resolveStream(raw, this.defaultSymbol);
      return [spec.stream, spec] as const;
    })).values()];
    if (new Set([...this.refs.keys(), ...streams.map(spec => spec.stream)]).size > 1024) bad("Binance fast stream limit reached");
    const box = new Mailbox<FastMarketEvent>(512, true);
    for (const spec of streams) {
      const entry = this.refs.get(spec.stream) ?? { spec, users: new Set<Mailbox<FastMarketEvent>>() };
      entry.users.add(box);
      this.refs.set(spec.stream, entry);
    }
    this.sync();
    const close = () => {
      signal.removeEventListener("abort", close);
      box.close();
      for (const { stream } of streams) {
        const entry = this.refs.get(stream);
        if (!entry) continue;
        entry.users.delete(box);
        if (!entry.users.size) this.refs.delete(stream);
      }
      this.sync();
    };
    signal.addEventListener("abort", close, { once: true });
    async function* frames() {
      let dropped = 0;
      for (;;) {
        const item = await box.take();
        if (item === undefined) return;
        const lost = box.dropped - dropped;
        dropped = box.dropped;
        yield lost ? { ...item, droppedCount: lost } : item;
      }
    }
    return ndjson(frames(), close);
  }

  health() { return Object.fromEntries([...this.refs].map(([stream, entry]) => [stream, entry.users.size])); }
  close() {
    for (const { users } of this.refs.values()) for (const queue of users) queue.close();
    this.refs.clear();
    for (const channel of Object.values(this.channels)) channel.close();
  }
}
