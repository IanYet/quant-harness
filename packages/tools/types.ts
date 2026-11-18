export type Account = "paper" | "binance";
export type PositionSide = "BOTH" | "LONG" | "SHORT";
export const periods = {
  "1m": 60_000,
  "3m": 180_000,
  "5m": 300_000,
  "15m": 900_000,
  "30m": 1_800_000,
  "1h": 3_600_000,
  "2h": 7_200_000,
  "4h": 14_400_000,
  "6h": 21_600_000,
  "8h": 28_800_000,
  "12h": 43_200_000,
  "1d": 86_400_000,
} as const;
export type Interval = keyof typeof periods;
export interface Range {
  start: number;
  end: number;
}
export interface Bar {
  symbol: string;
  interval: Interval;
  openTime: number;
  closeTime: number;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
}
export interface BarEvent {
  source: "history" | "backfill" | "live";
  bar: Bar;
}
export interface Book {
  bid: string;
  bidQty: string;
  ask: string;
  askQty: string;
  eventTime: number;
  receivedAt: number;
}
export interface Mark {
  price: string;
  index: string;
  fundingRate: string;
  nextFundingTime: number;
  eventTime: number;
  receivedAt: number;
}
export interface Quote {
  symbol: string;
  book: Book | null;
  mark: Mark | null;
  bookStale: boolean;
  markStale: boolean;
}
export type Levels = [string, string][];
export interface DepthSnapshot {
  lastUpdateId: string;
  bids: Levels;
  asks: Levels;
}
export interface DepthFrame {
  kind: "snapshot" | "delta";
  symbol: string;
  first: string;
  last: string;
  previous?: string;
  bids: Levels;
  asks: Levels;
  time: number;
}
export interface AggregateTrade {
  id: string;
  time: number;
  price: string;
  quantity: string;
  buyerMaker: boolean;
}
export interface Metric {
  time: number;
  value: string;
  source: "rest" | "archive" | "sim";
}
export interface Spec {
  symbol: string;
  tickSize: string;
  stepSize: string;
  minQty: string;
  maxQty: string;
  marketStep: string;
  marketMin: string;
  marketMax: string;
  minNotional: string;
}
export interface Sample<T = unknown> {
  cursor: number;
  name: string;
  time: number;
  record: T;
}
export interface Position {
  symbol: string;
  positionSide: PositionSide;
  quantity: string;
}
export type OrderStatus = "UNKNOWN" | "NEW" | "PARTIALLY_FILLED" | "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED";
export interface Order {
  account: Account;
  clientId: string;
  exchangeId: string | null;
  symbol: string;
  side: "BUY" | "SELL";
  positionSide: PositionSide;
  type: "MARKET" | "LIMIT";
  quantity: string;
  price: string | null;
  executed: string;
  status: OrderStatus;
  reduceOnly: boolean;
  updated: number;
}
export interface Place {
  key: string;
  symbol: string;
  side: "BUY" | "SELL";
  type: "MARKET" | "LIMIT";
  quantity: string;
  price?: string;
  positionSide?: PositionSide;
  quote: Quote;
}
export interface Close {
  key: string;
  symbol: string;
  positionSide?: PositionSide;
  quote?: Quote;
}
export interface Cancel {
  key: string;
  symbol: string;
  clientId: string;
}
export interface Leverage {
  key: string;
  symbol: string;
  leverage: number;
}
export interface Instruction {
  id: string;
  account: Account;
  strategy: string;
  runId: string;
  key: string;
  action: string;
  params: Record<string, unknown>;
  decision: Quote | null;
  created: number;
  state: "pending" | "unknown" | "accepted" | "rejected";
  clientId: string | null;
  detail: string | null;
}
export interface Run {
  id: string;
  name: string;
  file: string;
  hash: string;
  account: Account;
  mode: "observe" | "execute";
  state: "running" | "stopped" | "exited" | "failed" | "interrupted";
  ready: boolean;
  started: number;
  ended: number | null;
  exitCode: number | null;
}
export interface Task {
  id: string;
  backend: string;
  sessionId: string | null;
  state: string;
  result: unknown;
}
