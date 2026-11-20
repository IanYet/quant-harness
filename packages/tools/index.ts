import { QuantError, UdsTransport } from "./transport-uds";
import type { Params } from "./transport-uds";
import { LocalTransport } from "./transport-local";
import { BacktestTransport } from "./transport-backtest";
import { localDataDirectory } from "./paths";
import type {
  AccountFullState, AccountSnapshot, AccountTopic, AccountWatchEvent, AggTrade, AgentTaskStatus, AlgoOrderEvent,
  AssetIndexInfo, BasisPoint, CheckpointEnvelope, CompositeIndexInfo, DataSample, DataSampleEvent, ExchangeSymbolInfo,
  FastMarketEvent, FastStreamSpec, FundingInfo, FundingRate, FuturesPeriod, IndexConstituents, Kline, KlineInterval,
  Liquidation, LongShortRatioPoint, MarketEvent, MarketSnapshot, MarketTrade, NotifyRequest, OpenInterestPoint,
  OpenInterestSnapshot, OpenOrder, OrderBookSnapshot, OrderIntentStatus, OrderParams, OrderRef, PositionLeg,
  Positions, StopHandler, TakerVolumePoint, TaskRequest, Ticker24h, TradeAction, TradeFillEvent, WorkerCapabilities,
} from "./types";

export type * from "./types";
export { QuantError };

type Mode = "live" | "shadow" | "worker" | "backtest" | "local";
const token = process.env.QUANT_TOKEN;
const mode = (token ? process.env.QUANT_MODE : "local") as Mode;
const dir = mode === "local" ? localDataDirectory() : "/var/lib/quant-harness";
const sockets = {
  runtime: process.env.QUANT_SOCKET ?? dir + "/run/runtime.sock",
  storage: process.env.QUANT_STORAGE_SOCKET ?? dir + "/run/storage.sock",
};
const transport = mode === "backtest" ? new BacktestTransport(sockets, token) :
  mode === "local" ? new LocalTransport(sockets) : new UdsTransport(sockets, token);
let ready = Promise.resolve();
const rt = async <T>(path: string, args?: Params) => {
  await ready;
  return transport.request<T>("runtime", "/v1/" + path, args);
};
const write = async <T>(path: string, body?: unknown, method = "POST") => {
  await ready;
  return transport.request<T>("runtime", "/v1/" + path, undefined, method, body);
};
const history = <T>(path: string, args?: Params) => transport.request<T>("storage", "/v1/" + path, args);
async function* stream<T>(topics: string[], args: Params = {}) {
  await ready;
  yield* transport.stream<T>("/v1/stream", { ...args, topics: topics.join(",") }, { reliable: true });
}
const rest = <T>(path: string, args?: Params) => rt<T>("market/rest/" + path, args);
type TimeRange = { symbol?: string; start?: number; end?: number; limit?: number };
type PeriodParams = TimeRange & { period?: FuturesPeriod };
type Contract = "PERPETUAL" | "CURRENT_QUARTER" | "NEXT_QUARTER";
type MarketTopic = "kline" | "book_ticker" | "mark_price" | "liquidation" | "contract_info";

export const market = {
  snapshot<S extends string>(symbol?: S, opts?: { environment?: "mainnet" | "testnet" }) {
    return rt<S extends "*" ? Record<string, MarketSnapshot> : MarketSnapshot>("market/snapshot", { symbol, env: opts?.environment });
  },
  watch(topics: MarketTopic[], opts: { symbols?: string[]; intervals?: KlineInterval[]; afterSeq?: number } = {}) {
    return stream<MarketEvent>(topics.map(topic => "market." + topic), { symbols: opts.symbols, intervals: opts.intervals, after_seq: opts.afterSeq });
  },
  watchFast(streams: FastStreamSpec[]) {
    return transport.stream<FastMarketEvent>("/v1/market/fast-stream", { streams: JSON.stringify(streams) });
  },
  klines(interval: KlineInterval, opts: TimeRange & { start: number; end: number; backfill?: boolean }) {
    return history<Kline[]>("market/klines", { ...opts, interval });
  },
  fundingRates(opts?: TimeRange & { backfill?: boolean }) { return history<FundingRate[]>("market/funding-rates", opts); },
  liquidations(opts?: TimeRange) { return history<Liquidation[]>("market/liquidations", opts); },
  exchangeInfo(symbol?: string) { return rest<ExchangeSymbolInfo[]>("exchange-info", { symbol }); },
  depth(opts?: { symbol?: string; limit?: 5 | 10 | 20 | 50 | 100 | 500 | 1000 }) { return rest<OrderBookSnapshot>("depth", opts); },
  trades(opts?: { symbol?: string; limit?: number }) { return rest<MarketTrade[]>("trades", opts); },
  aggTrades(opts?: { symbol?: string; startTime?: number; endTime?: number; fromId?: number; limit?: number }) {
    return rest<AggTrade[]>("agg-trades", opts);
  },
  continuousKlines(opts: { pair: string; contractType: Contract; interval: KlineInterval; start?: number; end?: number; limit?: number }) {
    return rest<Kline[]>("continuous-klines", opts);
  },
  indexPriceKlines(opts: { pair: string; interval: KlineInterval; start?: number; end?: number; limit?: number }) {
    return rest<Kline[]>("index-klines", opts);
  },
  markPriceKlines(opts: TimeRange & { interval: KlineInterval }) { return rest<Kline[]>("mark-klines", opts); },
  premiumIndexKlines(opts: TimeRange & { interval: KlineInterval }) { return rest<Kline[]>("premium-klines", opts); },
  ticker24h(symbol?: string) { return rest<Ticker24h | Ticker24h[]>("ticker-24h", { symbol }); },
  fundingInfo(symbol?: string) { return rest<FundingInfo[]>("funding-info", { symbol }); },
  openInterest(symbol?: string) { return rest<OpenInterestSnapshot>("open-interest", { symbol }); },
  openInterestHist(opts?: PeriodParams) { return rest<OpenInterestPoint[]>("open-interest-hist", opts); },
  longShortRatio(kind: "top_accounts" | "top_positions" | "global_accounts", opts?: PeriodParams) {
    const paths = { top_accounts: "top-ls-accounts", top_positions: "top-ls-positions", global_accounts: "global-ls-accounts" };
    return rest<LongShortRatioPoint[]>(paths[kind], opts);
  },
  takerVolume(opts?: PeriodParams) { return rest<TakerVolumePoint[]>("taker-volume", opts); },
  basis(opts: { pair: string; contractType: Contract; period: FuturesPeriod; start?: number; end?: number; limit?: number }) {
    return rest<BasisPoint[]>("basis", opts);
  },
  indexInfo(symbol?: string) { return rest<CompositeIndexInfo[]>("index-info", { symbol }); },
  constituents(symbol?: string) { return rest<IndexConstituents>("constituents", { symbol }); },
  assetIndex(symbol?: string) { return rest<AssetIndexInfo[]>("asset-index", { symbol }); },
};

export const data = {
  publish(dataset: string, sample: { ts: number; key?: string; payload: Record<string, unknown>; ttlSec?: number }) {
    return write<{ seq: number }>("data/publish", { dataset, ...sample });
  },
  query(dataset: string, opts?: Omit<TimeRange, "symbol">) { return history<{ items: DataSample[]; maxSeq: number }>("data/query", { dataset, ...opts }); },
  watch(dataset: string, opts?: { afterSeq?: number }) { return stream<DataSampleEvent>(["data.sample"], { dataset, after_seq: opts?.afterSeq }); },
};

type FillRange = { afterSeq?: number; throughSeq?: number; symbol?: string };
export const accounts = {
  list: () => rt<Array<{ accountId: string; mode: "live" | "shadow"; allowedSymbols: string[] }>>("accounts"),
  state: (accountId: string) => rt<AccountFullState>("accounts/" + encodeURIComponent(accountId) + "/state"),
  fills(accountId: string, opts?: FillRange) {
    return rt<TradeFillEvent[]>("accounts/" + encodeURIComponent(accountId) + "/fills", {
      after_seq: opts?.afterSeq, through_seq: opts?.throughSeq, symbol: opts?.symbol,
    });
  },
  watch(ids: string[] | "*", topics: AccountTopic[], opts?: { afterSeq?: number }) {
    return stream<AccountWatchEvent>(topics.map(topic => "trade." + topic), { accounts: ids, after_seq: opts?.afterSeq });
  },
};

const order = (action: TradeAction, params: OrderParams) => write<OrderIntentStatus>("trade/order", { action, params });
const ownGroup = () => mode === "shadow" ? "shadow" : "trade";
export const trade = {
  openLong: (params: OrderParams) => order("openLong", params),
  closeLong: (params: OrderParams) => order("closeLong", params),
  openShort: (params: OrderParams) => order("openShort", params),
  closeShort: (params: OrderParams) => order("closeShort", params),
  cancel: (ref: OrderRef) => write<OpenOrder>("trade/order", ref, "DELETE"),
  cancelAll: (opts?: { symbol?: string; includeConditional?: boolean }) =>
    write<{ canceledStandard: number; canceledConditional: number }>("trade/orders", opts, "DELETE"),
  positions: (symbol?: string) => rt<{ long: PositionLeg; short: PositionLeg }>("trade/positions", { symbol }),
  allPositions: () => rt<Positions>("trade/positions", { symbol: "*" }),
  account: () => rt<AccountSnapshot>("trade/account"),
  setLeverage: (leverage: number, symbol?: string) => write<{ symbol: string; leverage: number }>("trade/leverage", { leverage, symbol }),
  getOrder: (clientOrderId: string, symbol?: string) => rt<OrderIntentStatus>("trade/order", { clientOrderId, symbol }),
  openOrders: (opts?: { symbol?: string; includeConditional?: boolean }) => rt<OpenOrder[]>("trade/open-orders", opts),
  watchFills: (opts?: { afterSeq?: number; symbol?: string }) => stream<TradeFillEvent>([ownGroup() + ".fill"], { after_seq: opts?.afterSeq, symbols: opts?.symbol }),
  watchAlgoOrders: (opts?: { afterSeq?: number; symbol?: string }) => stream<AlgoOrderEvent>([ownGroup() + ".algo_order"], { after_seq: opts?.afterSeq, symbols: opts?.symbol }),
};

export const notify = { send: (req: NotifyRequest) => write<{ queued: true; seq: number }>("notify", req) };
export const agents = {
  startTask: (req: TaskRequest) => write<{ taskId: string }>("agents/tasks", req),
  getTask: (taskId: string) => rt<AgentTaskStatus>("agents/tasks/" + encodeURIComponent(taskId)),
};

let handover: (() => void | Promise<void>) | undefined;
let stop: StopHandler | undefined;
let activeHandover: string | undefined;
export const state = {
  load: <T = Record<string, unknown>>() => rt<CheckpointEnvelope<T> | null>("state"),
  save: <T = Record<string, unknown>>(value: T, opts?: { schemaVersion?: number }) =>
    write<CheckpointEnvelope<T>>("state", { data: value, schemaVersion: opts?.schemaVersion, handoverId: activeHandover }, "PUT"),
};

export const harness = {
  mode,
  runId: process.env.QUANT_RUN_ID ?? "local",
  accountId: process.env.QUANT_ACCOUNT,
  epoch: process.env.QUANT_EPOCH === undefined ? undefined : Number(process.env.QUANT_EPOCH),
  capabilities: process.env.QUANT_CAPABILITIES ? JSON.parse(process.env.QUANT_CAPABILITIES) as WorkerCapabilities : undefined,
  now: () => transport instanceof BacktestTransport ? transport.now : Date.now(),
  onHandover(cb: () => void | Promise<void>) { handover = cb; },
  onStop(cb: StopHandler) { stop = cb; },
} as const;

type Handover = { handoverId: string; action: "stop" | "replace"; orders: OpenOrder[] };
let finishing: Promise<void> | undefined;
async function finish(request: Handover | null) {
  if (!request || finishing) return finishing;
  activeHandover = request.handoverId;
  finishing = (async () => {
    const deadline = setTimeout(() => process.exit(1), 2900);
    try {
      const decision = request.action === "stop" && stop ? await stop(request) : await handover?.();
      await write("handover/complete", { handoverId: request.handoverId, decision });
    } catch (error) {
      console.error("Handover callback failed", error);
      process.exit(1);
    } finally {
      clearTimeout(deadline);
    }
    process.exit(0);
  })();
  return finishing;
}

if (token && mode !== "backtest") {
  const handshake = Promise.withResolvers<void>();
  ready = handshake.promise;
  let lastHealthy = Date.now();
  process.on("SIGTERM", () => {
    if (finishing) return;
    void transport.request<Handover | null>("runtime", "/v1/handover").then(value => {
      if (!value) process.exit(0);
      handshake.resolve();
      return finish(value);
    })
      .catch(() => process.exit(1));
  });
  const check = async () => {
    try {
      await transport.request("runtime", "/v1/session/ready", undefined, "POST", undefined, AbortSignal.timeout(2000));
      lastHealthy = Date.now();
      handshake.resolve();
    } catch {
      if (Date.now() - lastHealthy >= 60_000) process.exit(1);
    }
  };
  void check();
  setInterval(() => void check(), 3000).unref();
  void (async () => {
    try {
      for await (const event of transport.stream<{ payload: Handover }>("/v1/stream",
        { topics: "sys.handover", after_seq: 0 }, { reliable: true })) {
        handshake.resolve();
        await finish(event.payload);
      }
    } catch (error) {
      if (!finishing) console.error("Lifecycle stream ended", error);
    }
  })();
}
