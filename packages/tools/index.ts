import { transport } from "./transport";
import type {
  Account,
  AggregateTrade,
  Bar,
  BarEvent,
  Cancel,
  Close,
  DepthFrame,
  DepthSnapshot,
  Instruction,
  Interval,
  Leverage,
  Metric,
  Order,
  Place,
  Position,
  Quote,
  Range,
  Run,
  Sample,
  Spec,
  Task,
} from "./types";
export * from "./types";

export function connect(
  base = process.env.HARNESS_URL ?? `http://127.0.0.1:${process.env.HARNESS_PORT ?? 8787}`,
  token = process.env.HARNESS_RUN_TOKEN,
) {
  const { get, post, stream } = transport(base, token);
  return {
    market: {
      quote: (symbol: string) => get<Quote>("/market/quote", { symbol }),
      bars: (symbol: string, interval: Interval, range: Range) =>
        get<Bar[]>("/market/bars", { symbol, interval, ...range }),
      watchBars: (symbol: string, interval: Interval, opts: { warmup?: number; signal?: AbortSignal } = {}) =>
        stream<BarEvent>("/stream/bars", { symbol, interval, warmup: opts.warmup }, opts.signal),
      watchQuotes: (symbol: string, signal?: AbortSignal) => stream<Quote>("/stream/quotes", { symbol }, signal),
      watchTrades: (symbol: string, signal?: AbortSignal) =>
        stream<AggregateTrade>("/stream/trades", { symbol }, signal),
      watchDepth: (symbol: string, signal?: AbortSignal) => stream<DepthFrame>("/stream/depth", { symbol }, signal),
      watchLiquidations: (symbol: string, signal?: AbortSignal) =>
        stream<unknown>("/stream/liquidations", { symbol }, signal),
      depth: (symbol: string) => get<DepthSnapshot>("/market/depth", { symbol }),
      trades: (symbol: string, range?: Range, limit?: number) =>
        get<AggregateTrade[]>("/market/trades", { symbol, ...range, limit }),
      funding: (symbol: string, range: Range) => get<Metric[]>("/market/funding", { symbol, ...range }),
      openInterest: (symbol: string, range?: Range) => get<Metric[]>("/market/oi", { symbol, ...range }),
      longShortRatio: (symbol: string, range: Range) => get<Metric[]>("/market/ratio", { symbol, ...range }),
      stats: (symbol: string) => get<Record<string, unknown>>("/market/stats", { symbol }),
      spec: (symbol: string) => get<Spec>("/market/spec", { symbol }),
    },
    trade: {
      positions: (account?: Account) => get<Position[]>("/trade/positions", { account }),
      openOrders: (account?: Account) => get<Order[]>("/trade/openOrders", { account }),
      place: (input: Place) => post<Instruction>("/trade/place", input),
      close: (input: Close) => post<Instruction>("/trade/close", input),
      cancel: (input: Cancel) => post<Instruction>("/trade/cancel", input),
      setLeverage: (input: Leverage) => post<Instruction>("/trade/setLeverage", input),
      instruction: (id: string) => get<Instruction>("/trade/instruction", { id }),
      order: (clientId: string, account?: Account) => get<Order>("/trade/order", { clientId, account }),
      fills: (clientId: string, account?: Account) =>
        get<{ tradeId: string; price: string; quantity: string; time: number }[]>("/trade/fills", {
          clientId,
          account,
        }),
    },
    data: {
      append: <T>(name: string, record: T) => post<Sample<T>>("/data/append", { name, record }),
      read: <T = unknown>(name: string, after = 0, limit = 500) =>
        get<Sample<T>[]>("/data/read", { name, after, limit }),
      watch: <T = unknown>(name: string, after = 0, signal?: AbortSignal) =>
        stream<Sample<T>>("/stream/data", { name, after }, signal),
    },
    notify: {
      send: (type: string, content: Record<string, unknown>) =>
        post<{ id: string; state: string }>("/notify/send", { type, content }),
    },
    agents: {
      startTask: (backend: string, prompt: string) => post<Task>("/agents/startTask", { backend, prompt }),
      sendMessage: (id: string, message: string) => post<Task>("/agents/sendMessage", { id, message }),
      getStatus: (id: string) => get<Task>("/agents/status", { id }),
    },
    currentRun: () => get<Run>("/run"),
  };
}
export const { market, trade, data, notify, agents, currentRun } = connect();
