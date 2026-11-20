import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes, randomUUID } from "node:crypto";
import type { DataSample, FundingRate, Kline, MarkPrice, OpenOrder } from "@quant/tools";
import { klineIntervals } from "../../packages/tools/types";
import type { HarnessConfig } from "../shared/config";
import { AGGREGATES, MINUTE, MinuteBars } from "../shared/bars";
import type { BusEvent } from "../shared/events";
import { matches } from "../shared/events";
import { bad, body, Fault, flag, handler, ID, integer, missing, num, oneOf, queryFlag, queryNumber, record, str, stringList, SYMBOL } from "../shared/http";
import { serveUnix } from "../shared/uds";
import { legs } from "../core/gateway";
import { reference, selection } from "../core/orders";
import { normalizeOrder } from "../core/risk";
import type { Storage } from "./db";
import type { HistoryBackfill } from "./history-backfill";
import { range, readFunding, readKlines, readLiquidations, readSamples, symbol } from "./queries";
import { ReplayBook } from "./replay-book";
import type { StrategyRegistry } from "./strategy-registry";

type Action =
  | { ts: number; funding: FundingRate }
  | { ts: number; bar: Kline; mark?: Kline }
  | { ts: number; archived: { topic: "market.mark_price" | "market.liquidation"; payload: Record<string, unknown> } }
  | { ts: number; sample: DataSample };
type Subscription = {
  filter: (event: BusEvent) => boolean;
  queue: BusEvent[];
  waiting?: { good: (value: BusEvent | null) => void; bad: (error: unknown) => void };
};

export class Replay {
  private now: number;
  private visibleSeq = 0;
  private eventSeq = 0;
  private cursor = 0;
  private checkpoint: unknown = null;
  private stateVersion = 0;
  private readonly bars = new MinuteBars();
  private readonly latest = new Map<string, Kline>();
  private readonly marks = new Map<string, Kline>();
  private readonly metricsAt = new Map<string, MarkPrice>();
  private readonly subscribers = new Map<string, Subscription>();
  private batch: BusEvent[] = [];
  private error?: unknown;
  private scheduled = false;
  private closed = false;
  private readonly book: ReplayBook;
  private seen = 0;

  constructor(private readonly opts: {
    start: number; symbols: string[]; settings: ConstructorParameters<typeof ReplayBook>[0]; actions: Action[];
    storage: Storage; history: HistoryBackfill; token: string;
  }) {
    this.now = opts.start - 1;
    this.book = new ReplayBook(opts.settings, (topic, payload) => this.emit(topic, payload));
    for (const ticker of opts.symbols) {
      const previous = readKlines(opts.storage, ticker, "1m", Math.max(0, opts.start - 1440 * MINUTE), this.now);
      for (const bar of previous) this.bars.add(bar);
      if (previous.length) this.latest.set(ticker, previous[previous.length - 1]);
    }
  }

  get finished() { return this.cursor >= this.opts.actions.length && !this.error; }
  get failure() { return this.error; }
  metrics() { return { bars: this.seen, ...this.book.metrics() }; }

  private emit(topic: string, payload: Record<string, unknown>, source = "backtest") {
    this.batch.push({ seq: ++this.eventSeq, ts: this.now, topic, source, payload });
  }

  private schedule() {
    if (this.scheduled || this.closed) return;
    this.scheduled = true;
    setImmediate(() => {
      this.scheduled = false;
      try { this.pump(); } catch (error) { this.close(error); }
    });
  }

  private pump() {
    for (let quota = 0; quota < 100; quota++) {
      const subscriptions = [...this.subscribers.values()];
      if (!subscriptions.length || subscriptions.some(sub => !sub.waiting || sub.queue.length)) return;
      if (this.cursor >= this.opts.actions.length) {
        for (const sub of subscriptions) { sub.waiting?.good(null); sub.waiting = undefined; }
        return;
      }
      this.now = this.opts.actions[this.cursor].ts;
      this.batch = [];
      while (this.opts.actions[this.cursor]?.ts === this.now) {
        const action = this.opts.actions[this.cursor++];
        if ("funding" in action) this.book.settleFunding(action.funding);
        else if ("sample" in action) {
          this.visibleSeq = Math.max(this.visibleSeq, action.sample.seq);
          this.emit("data.sample", { ...action.sample }, "archive");
        } else if ("archived" in action) {
          const { topic, payload } = action.archived;
          if (topic === "market.mark_price") this.metricsAt.set(String(payload.symbol), payload as unknown as MarkPrice);
          this.emit(topic, payload, "binance.mainnet");
        } else {
          this.seen++;
          this.book.advance(action.bar, action.mark);
          this.latest.set(action.bar.symbol, action.bar);
          if (action.mark) this.marks.set(action.bar.symbol, action.mark);
          this.emit("market.kline", { ...action.bar }, "binance.mainnet");
          for (const bar of this.bars.add(action.bar)) this.emit("market.kline", { ...bar }, "binance.mainnet");
        }
      }
      for (const sub of subscriptions) {
        sub.queue = this.batch.filter(sub.filter);
        if (sub.queue.length) {
          sub.waiting?.good(sub.queue.shift()!);
          sub.waiting = undefined;
        }
      }
    }
    this.schedule();
  }

  close(error?: unknown) {
    this.closed = true;
    this.error = error;
    for (const sub of this.subscribers.values()) {
      if (error) sub.waiting?.bad(error);
      else sub.waiting?.good(null);
    }
  }

  private open(url: URL) {
    const topics = (url.searchParams.get("topics") ?? "").split(",").filter(Boolean);
    const supported = ["market.kline", "market.mark_price", "market.liquidation", "data.sample", "trade.order", "trade.fill", "trade.algo_order"];
    if (!topics.length || topics.some(topic => !supported.includes(topic))) {
      throw new Fault(422, "UNSUPPORTED_BACKTEST", "Subscription requires fields that are not archived for replay");
    }
    const symbols = url.searchParams.get("symbols")?.split(",");
    const dataset = url.searchParams.get("dataset");
    const intervals = url.searchParams.get("intervals")?.split(",");
    const afterSeq = queryNumber(url, "after_seq", 0);
    const id = randomUUID();
    this.subscribers.set(id, { queue: [], filter: event => {
      const p = event.payload;
      return matches(event.topic, topics) && (!symbols || !p.symbol || symbols.includes(String(p.symbol))) &&
        (!intervals || !p.interval || intervals.includes(String(p.interval))) && (!dataset || p.dataset === dataset) &&
        (event.topic !== "data.sample" || Number(p.seq) > afterSeq);
    } });
    return { id };
  }

  async route(req: Request, url: URL) {
    if (req.headers.get("authorization") !== "Bearer " + this.opts.token) throw new Fault(401, "UNAUTHORIZED", "Invalid replay token");
    const path = url.pathname;
    const method = req.method;
    const { storage, history, symbols } = this.opts;
    const ticker = (raw: unknown) => {
      const value = str(raw ?? symbols[0], "symbol", SYMBOL);
      if (!symbols.includes(value)) bad("Symbol is outside the replay universe");
      return value;
    };

    if (method === "POST" && path === "/v1/replay/subscriptions") return this.open(url);
    if (path.startsWith("/v1/replay/subscriptions/")) {
      const id = path.slice(25);
      const sub = this.subscribers.get(id) ?? missing("Unknown replay subscription");
      if (method === "DELETE") {
        sub.waiting?.good(null);
        this.subscribers.delete(id);
        this.schedule();
        return { closed: true };
      }
      if (method === "GET") {
        if (this.error) throw this.error;
        if (sub.queue.length) return sub.queue.shift();
        if (this.closed || this.finished) return null;
        if (sub.waiting) bad("A replay subscription has one consumer");
        return new Promise<BusEvent | null>((good, bad) => {
          sub.waiting = { good, bad };
          this.schedule();
        });
      }
    }
    if (method === "GET" && path === "/v1/market/snapshot") {
      const view = (symbol: string) => ({ symbol, kline: this.latest.get(symbol), mark: this.metricsAt.get(symbol) ?? (this.marks.get(symbol) ? {
        symbol, markPrice: this.marks.get(symbol)!.c, eventTs: this.marks.get(symbol)!.closeTime,
      } : undefined) });
      return url.searchParams.get("symbol") === "*" ? Object.fromEntries(symbols.map(item => [item, view(item)])) : view(ticker(url.searchParams.get("symbol")));
    }
    if (method === "GET" && ["/v1/market/klines", "/v1/market/funding-rates", "/v1/market/liquidations"].includes(path)) {
      const { start, end } = range(url, this.now);
      const cutoff = Math.min(end, this.now);
      if (start > cutoff) return [];
      const selected = symbol(url, symbols[0]);
      if (path === "/v1/market/klines") {
        const interval = oneOf(url.searchParams.get("interval") ?? "1m", "interval", klineIntervals);
        if (start > 0 && (queryFlag(url, "backfill", true) || Object.hasOwn(AGGREGATES, interval))) await history.klines(selected, interval, start, cutoff);
        return readKlines(storage, selected, interval, start, cutoff);
      }
      if (path.endsWith("funding-rates")) {
        if (start > 0 && queryFlag(url, "backfill")) await history.funding(selected, start, cutoff);
        return readFunding(storage, selected, start, cutoff, queryNumber(url, "limit", 1000, 1, 10_000));
      }
      return readLiquidations(storage, selected, start, cutoff, queryNumber(url, "limit", 100, 1, 10_000));
    }
    if (method === "GET" && path === "/v1/data/query") {
      return readSamples(storage, str(url.searchParams.get("dataset"), "dataset", ID), url, this.now, this.visibleSeq);
    }
    if (method === "GET" && path === "/v1/state") return this.checkpoint;
    if (method === "PUT" && path === "/v1/state") {
      const input = await body(req);
      const stored = { namespace: "backtest", version: this.stateVersion + 1, epoch: 0, data: input.data,
        updatedAt: this.now, schemaVersion: integer(input.schemaVersion ?? 1, "schemaVersion", 1) };
      if (input.data === undefined) bad("Checkpoint data is required");
      if (Buffer.byteLength(JSON.stringify(stored)) > 1024 * 1024) throw new Fault(422, "CHECKPOINT_TOO_LARGE", "Checkpoint exceeds 1 MiB");
      this.stateVersion++;
      this.checkpoint = stored;
      return stored;
    }
    if (path.startsWith("/v1/trade/")) {
      const requested = url.searchParams.get("symbol");
      if (method === "GET") {
        if (path === "/v1/trade/positions") return requested === "*" ? this.book.positions : legs(this.book.positions, ticker(requested));
        if (path === "/v1/trade/account") return this.book.account();
        if (path === "/v1/trade/open-orders") return this.book.openOrders().filter(order =>
          (!requested || requested === order.symbol) && (queryFlag(url, "includeConditional", true) || order.kind === "STANDARD"));
        if (path === "/v1/trade/order") return this.book.get(ticker(requested), str(url.searchParams.get("clientOrderId"), "clientOrderId"));
      }
      const input = await body(req);
      if (method === "POST" && path === "/v1/trade/order") {
        const order = normalizeOrder(input, symbols[0]);
        ticker(order.symbol);
        return this.book.submit(order, this.now);
      }
      if (method === "POST" && path === "/v1/trade/leverage") {
        const symbol = ticker(input.symbol);
        const leverage = integer(input.leverage, "leverage", 1, 125);
        this.book.leverages[symbol] = leverage;
        return { symbol, leverage };
      }
      if (method === "DELETE" && path === "/v1/trade/order") {
        const list = selection(this.book.openOrders(), reference(input));
        if (list.length !== 1) return list.length ? bad("Order reference is ambiguous") : missing("No matching replay order");
        return this.book.cancel(list[0], this.now);
      }
      if (method === "DELETE" && path === "/v1/trade/orders") {
        const conditional = flag(input.includeConditional, "includeConditional");
        const list = this.book.openOrders().filter(order =>
          (!input.symbol || order.symbol === input.symbol) && (conditional || order.kind === "STANDARD"));
        for (const order of list) this.book.cancel(order, this.now);
        const count = (kind: OpenOrder["kind"]) => list.filter(order => order.kind === kind).length;
        return { canceledStandard: count("STANDARD"), canceledConditional: count("CONDITIONAL") };
      }
    }
    throw new Fault(422, "UNSUPPORTED_BACKTEST", "Endpoint is unavailable in historical replay");
  }
}

async function readTail(stream: ReadableStream<Uint8Array>) {
  let tail = "";
  const decoder = new TextDecoder();
  for await (const bytes of stream) tail = (tail + decoder.decode(bytes, { stream: true })).slice(-8000);
  return tail + decoder.decode();
}

export class Backtests {
  private count = 0;
  constructor(private readonly config: HarnessConfig, private readonly storage: Storage, private readonly history: HistoryBackfill,
    private readonly registry: StrategyRegistry) {}

  async run(input: Record<string, unknown>) {
    if (this.count >= 2) throw new Fault(429, "RATE_LIMITED", "Two standard backtests are already running");
    this.count++;
    try { return await this.runOne(input); } finally { this.count--; }
  }

  private async runOne(input: Record<string, unknown>) {
    const strategyId = input.strategyId === undefined ? undefined : str(input.strategyId, "strategyId");
    const draftPath = input.draftPath === undefined ? undefined : str(input.draftPath, "draftPath");
    if (Number(!!strategyId) + Number(!!draftPath) !== 1) bad("Specify one strategyId or draftPath");
    const start = integer(input.start, "start", 1);
    const end = integer(input.end, "end", start + 1, Date.now());
    const symbols = stringList(input.symbols ?? [this.config.defaultSymbol], "symbols", SYMBOL);
    if (!symbols.length || symbols.length * (end - start) / MINUTE > 100_000) bad("Range exceeds 100,000 minute bars across all symbols");
    const params = record(input.params ?? {});
    const settings = { initialBalance: num(params.initialBalance ?? 10_000, "initialBalance", 0.01),
      makerFee: num(params.makerFee ?? 0.0002, "makerFee", 0, 0.1),
      takerFee: num(params.takerFee ?? 0.0005, "takerFee", 0, 0.1),
      slippageBps: num(params.slippageBps ?? 2, "slippageBps", 0, 1000),
      triggerProtect: Object.fromEntries(Object.entries(record(params.triggerProtect ?? {})).map(([ticker, value]) =>
        [str(ticker, "symbol", SYMBOL), num(value, "triggerProtect", 0, 1)])),
    };
    const artifact = await this.registry.prepare({ mode: "shadow", strategyId, path: draftPath });
    const actions: Action[] = [];
    const warmup = Math.max(0, Math.floor(start / (1440 * MINUTE)) * 1440 * MINUTE);
    for (const ticker of symbols) {
      await this.history.klines(ticker, "1m", warmup, end);
      await this.history.marks(ticker, start, end);
      await this.history.funding(ticker, start, end);
      const bars = readKlines(this.storage, ticker, "1m", start, end);
      if (!bars.length) throw new Fault(422, "HISTORY_MISSING", "No closed mainnet bars for " + ticker);
      for (let i = 1; i < bars.length; i++) if (bars[i].openTime - bars[i - 1].openTime !== MINUTE) {
        throw new Fault(422, "HISTORY_MISSING", "Mainnet minute gap for " + ticker + " at " + bars[i - 1].closeTime);
      }
      const marks = this.storage.timeseries.query<Omit<Kline, "interval" | "closed" | "quoteVolume" | "v">, [string, number, number]>(
        "SELECT symbol,open_time AS openTime,close_time AS closeTime,open AS o,high AS h,low AS l,close AS c FROM mark_klines " +
        "WHERE symbol=? AND open_time>=? AND close_time<=? ORDER BY open_time").all(ticker, start, end);
      const byTime = new Map<number, Kline>(marks.map(mark => [mark.openTime, { ...mark, interval: "1m", closed: true, quoteVolume: 0, v: 0 }]));
      for (const bar of bars) actions.push({ ts: bar.closeTime, bar, mark: byTime.get(bar.openTime) });
      for (const funding of readFunding(this.storage, ticker, start, end, 10_000)) actions.push({ ts: funding.fundingTime, funding });
      const metrics = this.storage.timeseries.query<MarkPrice, [number, string, number, number]>(
        "SELECT symbol,mark_price AS markPrice,index_price AS indexPrice,funding_rate AS fundingRate," +
        "next_funding_time AS nextFundingTime,ts+? AS eventTs FROM market_metrics WHERE symbol=? AND ts BETWEEN ? AND ?")
        .all(MINUTE - 1, ticker, Math.max(0, start - MINUTE + 1), end - MINUTE + 1);
      for (const mark of metrics) actions.push({ ts: mark.eventTs, archived: { topic: "market.mark_price", payload: { ...mark } } });
      const liquidations = this.storage.timeseries.query<{ tradeTime: number; [key: string]: unknown }, [string, number, number]>(
        "SELECT symbol,side,order_type AS orderType,price,avg_price AS avgPrice,qty,filled_qty AS filledQty,trade_time AS tradeTime " +
        "FROM liquidations WHERE symbol=? AND trade_time BETWEEN ? AND ? ORDER BY trade_time,seq LIMIT 100001").all(ticker, start, end);
      if (liquidations.length > 100_000) bad("Historical liquidation replay exceeds 100,000 records for " + ticker);
      for (const event of liquidations) actions.push({ ts: event.tradeTime, archived: { topic: "market.liquidation", payload: event } });
    }
    const samples = this.storage.timeseries.query<DataSample & { json: string }, [number, number]>(
      "SELECT dataset,seq,ts,COALESCE(ingested_ts,ts) AS ingestedTs,sample_key AS key,payload_json AS json FROM data_samples " +
      "WHERE COALESCE(ingested_ts,ts) BETWEEN ? AND ? ORDER BY 4,seq LIMIT 100001").all(start, end);
    if (samples.length > 100_000) bad("Historical dataset replay exceeds 100,000 samples");
    for (const { json, ...sample } of samples) actions.push({ ts: sample.ingestedTs, sample: { ...sample, payload: JSON.parse(json) } });
    const order = (action: Action) => "funding" in action ? 0 : "sample" in action ? 1 : 2;
    actions.sort((a, b) => a.ts - b.ts || order(a) - order(b));
    const token = randomBytes(32).toString("hex");
    const runId = randomUUID();
    const replay = new Replay({ start, symbols, settings, actions, storage: this.storage, history: this.history, token });
    const limit = integer(input.timeoutSec ?? 300, "timeoutSec", 1, 900);
    const temp = await mkdtemp(join(tmpdir(), "quant-replay-"));
    const socket = join(temp, "replay.sock");
    const server = await serveUnix(socket, handler((req, url) => replay.route(req, url)));
    try {
      const child = Bun.spawn([process.execPath, "--no-env-file", "run", artifact.artifactPath], {
        cwd: this.config.dataDir, stdin: "ignore", stdout: "pipe", stderr: "pipe",
        env: { QUANT_SOCKET: socket, QUANT_BACKTEST_SOCKET: socket, QUANT_TOKEN: token, QUANT_MODE: "backtest",
          QUANT_BACKTEST_START: String(start - 1), QUANT_RUN_ID: runId, QUANT_ACCOUNT: "backtest", QUANT_EPOCH: "0" },
      });
      const tails = Promise.all([readTail(child.stdout), readTail(child.stderr)]);
      const timer = setTimeout(() => { replay.close(new Fault(422, "BACKTEST_TIMEOUT", "Replay deadline expired")); child.kill("SIGKILL"); }, limit * 1000);
      const exit = await child.exited.finally(() => clearTimeout(timer));
      const [stdout, stderr] = await tails;
      if (replay.failure) throw replay.failure;
      if (exit !== 0 || !replay.finished) throw new Fault(422, "BACKTEST_INCOMPLETE", "Strategy did not finish the historical stream",
        { exit, stderr, stdout, ...replay.metrics() });
      const metrics = replay.metrics();
      const target = strategyId ?? draftPath!;
      this.storage.ledger.query("INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?)")
        .run(runId, target, artifact.codeHash, start, end, JSON.stringify({ ...settings, symbols }), JSON.stringify(metrics), Date.now());
      return { runId, target, codeHash: artifact.codeHash, start, end, symbols, metrics, stdout, stderr };
    } finally {
      replay.close();
      await server.stop(true);
      await rm(temp, { recursive: true, force: true });
    }
  }
}
