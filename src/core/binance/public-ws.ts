import type { Kline, MarketSnapshot, BookTicker, MarkPrice, Liquidation } from "@quant/tools";
import type { HarnessConfig } from "../../shared/config";
import { MINUTE, MinuteBars } from "../../shared/bars";
import { bad, missing, str, SYMBOL } from "../../shared/http";
import { EventBus } from "../event-bus";
import type { Markets } from "../gateway";
import { FastStreams } from "./fast-stream";
import { MarketRest } from "./market-rest";
import type { Environment } from "./market-rest";
import { WeightBucket } from "./signer";
import { WsChannel } from "./ws-channel";

type Json = Record<string, unknown>;
type Feed = { symbols: Set<string>; risk: Set<string>; snapshots: Map<string, MarketSnapshot>;
  freshness: Map<string, Record<string, number>>; bars: MinuteBars; book: WsChannel; market: WsChannel; gapAt?: number };
const streamSymbol = (name: string) => name.split("@")[0].toUpperCase();

export class PublicMarket implements Markets {
  readonly restProxy: MarketRest;
  private readonly feeds: Record<Environment, Feed>;
  private readonly fastStreams: FastStreams;
  private readonly warming = new Map<string, Promise<void>>();

  constructor(settings: HarnessConfig, private readonly bus: EventBus, bucket: WeightBucket) {
    this.restProxy = new MarketRest(settings.binance, settings.defaultSymbol, bucket);
    this.fastStreams = new FastStreams(settings.binance.mainnetWs, settings.defaultSymbol);
    const feed = (env: Environment, ws: string): Feed => ({
      symbols: new Set(), risk: new Set(), snapshots: new Map(), freshness: new Map(), bars: new MinuteBars(),
      book: new WsChannel(ws + "/public/stream", (stream, data) => this.frame(env, stream, data)),
      market: new WsChannel(ws + "/market/stream", (stream, data) => this.frame(env, stream, data), () => {
        const current = this.feeds[env];
        if (env === "mainnet" && current.gapAt) this.bus.publish("sys.market_gap", "binance.mainnet", {
          startTs: current.gapAt, endTs: Date.now(),
        });
        current.gapAt = undefined;
        for (const symbol of current.symbols) this.warm(env, symbol);
      }, () => { this.feeds[env].gapAt ??= Date.now(); }),
    });
    this.feeds = { mainnet: feed("mainnet", settings.binance.mainnetWs), testnet: feed("testnet", settings.binance.testnetWs) };
  }

  configure(settings: HarnessConfig) {
    const risks = (accounts: HarnessConfig["liveAccounts"]) => Object.values(accounts).flatMap(account => account.allowedSymbols);
    const live = [...new Set(risks(settings.liveAccounts))];
    const demo = [...new Set(risks(settings.shadowAccounts))];
    const size = 1 + settings.symbols.length * 3 + live.filter(symbol => !settings.symbols.includes(symbol)).length;
    if (size > 1024 || demo.length * 3 + 1 > 1024) bad("Binance base market stream limit exceeded");
    this.set("mainnet", settings.symbols, live);
    this.set("testnet", demo, demo);
  }

  private set(env: Environment, observed: string[], traded: string[]) {
    const feed = this.feeds[env];
    const old = feed.symbols;
    feed.symbols = new Set(observed);
    feed.risk = new Set(traded);
    for (const symbol of feed.snapshots.keys()) {
      if (feed.symbols.has(symbol) || feed.risk.has(symbol)) continue;
      feed.snapshots.delete(symbol);
      feed.freshness.delete(symbol);
    }
    for (const symbol of old) if (!feed.symbols.has(symbol)) feed.bars.remove(symbol);
    const all = new Set([...observed, ...traded]);
    const book = [...feed.symbols].map(symbol => symbol.toLowerCase() + "@bookTicker");
    const market = [...all].flatMap(symbol => {
      const ticker = symbol.toLowerCase();
      return !feed.symbols.has(symbol) ? [ticker + "@markPrice@1s"] :
        [ticker + "@kline_1m", ticker + "@markPrice@1s", ticker + "@forceOrder"];
    });
    if (market.length) market.push("!contractInfo");
    feed.book.set(book);
    feed.market.set(market);
    for (const symbol of feed.symbols) if (!old.has(symbol)) this.warm(env, symbol);
  }

  private warm(env: Environment, symbol: string) {
    const key = env + ":" + symbol;
    if (this.warming.has(key)) return;
    const start = Math.floor(Date.now() / (1440 * MINUTE)) * 1440 * MINUTE;
    const job = this.rest("klines", new URLSearchParams({
      symbol, interval: "1m", start: String(start), limit: "1500",
    }), env).then(bars => {
      const feed = this.feeds[env];
      if (!feed.symbols.has(symbol)) return;
      for (const bar of bars as Kline[]) feed.bars.add(bar);
    }).catch(error => console.warn("Kline warmup failed", env, symbol, (error as Error).message))
      .finally(() => this.warming.delete(key));
    this.warming.set(key, job);
  }

  private frame(env: Environment, stream: string, value: unknown) {
    const feed = this.feeds[env];
    const data = value as Json;
    const now = Date.now();
    if (stream === "!contractInfo") {
      this.restProxy.invalidate(env);
      if (env === "mainnet") this.bus.publish("market.contract_info", "binance.mainnet", { ...data, symbol: data.s });
      return;
    }
    const symbol = streamSymbol(stream);
    const snapshot = feed.snapshots.get(symbol) ?? { symbol };
    const times = feed.freshness.get(symbol) ?? {};
    const archive = env === "mainnet" && feed.symbols.has(symbol);
    const put = (name: string, payload: Json) => {
      times[name] = now;
      if (archive) this.bus.publish("market." + name, "binance.mainnet", payload);
    };
    if (stream.endsWith("@bookTicker")) {
      const book: BookTicker = { symbol, bid: Number(data.b), bidQty: Number(data.B), ask: Number(data.a), askQty: Number(data.A),
        last: snapshot.kline?.c, eventTs: Number(data.E ?? data.T ?? now) };
      if (!(book.bid > 0 && book.ask >= book.bid && book.bidQty >= 0 && book.askQty >= 0)) return;
      snapshot.book = book;
      put("book_ticker", { ...book });
    } else if (stream.endsWith("@markPrice@1s")) {
      const mark: MarkPrice = { symbol, markPrice: Number(data.p), indexPrice: Number(data.i), fundingRate: Number(data.r || 0),
        nextFundingTime: Number(data.T ?? 0), eventTs: Number(data.E ?? now) };
      if (!(mark.markPrice > 0 && mark.indexPrice > 0) || !Number.isFinite(mark.fundingRate) || !Number.isFinite(mark.eventTs)) return;
      snapshot.mark = mark;
      put("mark_price", { ...mark });
    } else if (stream.endsWith("@kline_1m")) {
      const k = data.k as Json;
      const bar: Kline = { symbol, interval: "1m", openTime: Number(k.t), closeTime: Number(k.T),
        o: Number(k.o), h: Number(k.h), l: Number(k.l), c: Number(k.c), v: Number(k.v), quoteVolume: Number(k.q), closed: k.x === true };
      if (![bar.openTime, bar.closeTime, bar.o, bar.h, bar.l, bar.c, bar.v, bar.quoteVolume].every(Number.isFinite)) return;
      snapshot.kline = bar;
      put("kline", { ...bar });
      for (const aggregate of feed.bars.add(bar)) put("kline", { ...aggregate });
    } else if (stream.endsWith("@forceOrder")) {
      const o = data.o as Json;
      const liquidation: Liquidation = { symbol, side: o.S === "BUY" ? "BUY" : "SELL", orderType: String(o.o), price: Number(o.p),
        avgPrice: Number(o.ap), qty: Number(o.q), filledQty: Number(o.z), tradeTime: Number(o.T), status: String(o.X) };
      if (![liquidation.price, liquidation.avgPrice, liquidation.qty, liquidation.filledQty, liquidation.tradeTime].every(Number.isFinite)) return;
      put("liquidation", { ...liquidation });
    }
    feed.snapshots.set(symbol, snapshot);
    feed.freshness.set(symbol, times);
  }

  mark(symbol: string, env: Environment) {
    const feed = this.feeds[env];
    const item = feed.snapshots.get(symbol)?.mark;
    const ts = feed.freshness.get(symbol)?.mark_price;
    if (!item || !ts) return undefined;
    return { price: item.markPrice, ts: Math.min(ts, item.eventTs) };
  }

  snapshot(symbol: string, env: Environment = "mainnet") {
    const feed = this.feeds[env];
    if (symbol === "*") return Object.fromEntries([...feed.symbols].map(id => [id, feed.snapshots.get(id) ?? { symbol: id }]));
    const id = str(symbol, "symbol", SYMBOL);
    if (!feed.symbols.has(id) && !feed.risk.has(id)) missing("Symbol has no current subscription");
    return feed.snapshots.get(id) ?? { symbol: id };
  }
  rest(endpoint: string, query: URLSearchParams, env?: Environment) { return this.restProxy.query(endpoint, query, env); }
  fast(specs: unknown, signal: AbortSignal) { return this.fastStreams.subscribe(specs, signal); }

  health() {
    return { freshness: Object.fromEntries(Object.entries(this.feeds).map(([env, feed]) => [
      env, Object.fromEntries([...feed.symbols].map(symbol => [symbol, feed.freshness.get(symbol) ?? {}])),
    ])), fastSubscriptions: this.fastStreams.health() };
  }

  close() {
    for (const feed of Object.values(this.feeds)) { feed.book.close(); feed.market.close(); }
    this.fastStreams.close();
  }
}
