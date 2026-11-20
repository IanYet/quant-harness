export interface BusEvent<T = Record<string, unknown>> {
  seq: number;
  ts: number;
  topic: string;
  source: string;
  payload: T;
}
export type ControlFrame =
  | { type: "gap"; topics: string[]; fromSeq: number; toSeq: number; fromTs?: number; toTs?: number }
  | { type: "watermark"; throughSeq: number }
  | { type: "heartbeat"; ts: number };
export type BusFrame = BusEvent | ControlFrame;

export const MARKET_TOPICS = ["market.kline", "market.book_ticker", "market.mark_price", "market.liquidation", "market.contract_info"];
export const TRADING_TOPICS = ["order", "algo_order", "fill", "position", "account", "income", "risk_reject"];
export const STORAGE_TOPICS = ["market.*", "trade.*", "shadow.*", "data.sample", "state.checkpoint", "sys.operation"];
export const CONSUMER_TOPICS = {
  storage: STORAGE_TOPICS,
  notifier: ["trade.fill", "shadow.fill", "trade.algo_order", "shadow.algo_order", "trade.risk_reject", "shadow.risk_reject",
    "sys.proposal", "sys.kill_switch", "sys.notify", "sys.alert", "sys.run_lifecycle"],
  agent: ["agent.request"],
} as const;

export function matches(topic: string, patterns: readonly string[]): boolean {
  return patterns.some(pattern => pattern === "*" || topic === pattern ||
    (pattern.endsWith(".*") && topic.startsWith(pattern.slice(0, -1))));
}
