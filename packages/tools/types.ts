export type WorkingType = "MARK_PRICE" | "CONTRACT_PRICE";
export type TradeAction = "openLong" | "closeLong" | "openShort" | "closeShort";
export type TimeInForce = "GTC" | "IOC" | "FOK" | "GTX";
export type OrderParams = {
  symbol?: string;
  qty: number;
  clientOrderId: string;
  protection?: "take_profit" | "stop_loss";
} & (
  | { type: "MARKET" }
  | { type: "LIMIT"; price: number; timeInForce?: TimeInForce }
  | { type: "STOP_MARKET" | "TAKE_PROFIT_MARKET"; triggerPrice: number; workingType?: WorkingType; priceProtect?: boolean }
  | { type: "STOP" | "TAKE_PROFIT"; price: number; triggerPrice: number; timeInForce?: TimeInForce; workingType?: WorkingType; priceProtect?: boolean }
  | { type: "TRAILING_STOP_MARKET"; callbackRate: number; activationPrice?: number; workingType?: WorkingType; priceProtect?: boolean }
);
export type OrderRef = { orderId?: string; algoId?: string; clientOrderId?: string; symbol?: string };

export interface PositionLeg {
  symbol: string;
  side: "LONG" | "SHORT";
  qty: number;
  entryPrice: number;
  markPrice: number;
  unrealizedPnl: number;
  leverage: number;
  liquidationPrice: number;
}

export type Positions = Record<string, { long: PositionLeg; short: PositionLeg }>;

export interface AccountSnapshot {
  walletBalance: number;
  availableBalance: number;
  unrealizedPnl: number;
  marginType?: "CROSSED" | "ISOLATED";
  leverages: Record<string, number>;
}

export interface OpenOrder {
  kind: "STANDARD" | "CONDITIONAL";
  symbol: string;
  side: "BUY" | "SELL";
  positionSide: "LONG" | "SHORT";
  orderType: OrderParams["type"];
  status: string;
  qty: number;
  price?: number;
  triggerPrice?: number;
  filledQty?: number;
  exchangeOrderId?: string;
  algoId?: string;
  actualOrderId?: string;
  clientOrderId?: string;
  clientAlgoId?: string;
  exchangeClientId?: string;
  source: "strategy" | "manual" | "external";
  runId?: string;
  strategyId?: string;
  epoch?: number;
  createdAt: number;
  updatedAt: number;
}

export interface AccountFullState extends AccountSnapshot {
  accountId: string;
  mode: "live" | "shadow";
  positions: Positions;
  openOrders: OpenOrder[];
  reconciling: boolean;
}

export interface OrderIntentStatus {
  clientOrderId: string;
  exchangeClientId: string;
  status: "prepared" | "sending" | "accepted" | "rejected" | "unknown";
  order?: OpenOrder;
}

export interface CheckpointEnvelope<T = Record<string, unknown>> {
  namespace: string;
  accountId?: string;
  version: number;
  epoch: number;
  schemaVersion: number;
  updatedAt: number;
  data: T;
}

export interface WorkerCapabilities {
  accountRead?: string[] | "*";
  publishDatasets?: string[];
  notify?: boolean;
  agentTask?: boolean;
}

export const klineIntervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"] as const;
export type KlineInterval = typeof klineIntervals[number];
export type FuturesPeriod = "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "12h" | "1d";

export interface Kline {
  symbol: string;
  interval: KlineInterval;
  openTime: number;
  closeTime: number;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
  quoteVolume: number;
  closed: boolean;
}
export interface FundingRate { symbol: string; fundingTime: number; fundingRate: number; markPrice: number | null }
export interface MarkPrice { symbol: string; markPrice: number; indexPrice: number; fundingRate: number; nextFundingTime: number; eventTs: number }
export interface BookTicker { symbol: string; bid: number; ask: number; bidQty: number; askQty: number; last?: number; eventTs: number }
export interface Liquidation {
  symbol: string; side: "BUY" | "SELL"; orderType: string; price: number; avgPrice: number;
  qty: number; filledQty: number; tradeTime: number; status?: string;
}
export interface MarketSnapshot { symbol: string; book?: BookTicker; mark?: MarkPrice; kline?: Kline; [key: string]: unknown }
export interface OrderBookSnapshot { lastUpdateId: number; E?: number; T?: number; bids: [string, string][]; asks: [string, string][] }
export interface ExchangeSymbolInfo {
  symbol: string; status: string; contractType: string; filters: Array<{ filterType: string; [key: string]: string | number }>;
  baseAsset: string; quoteAsset: string; marginAsset: string;
}
export interface MarketTrade { id: number; price: string; qty: string; quoteQty: string; time: number; isBuyerMaker: boolean }
export interface AggTrade { a: number; p: string; q: string; f: number; l: number; T: number; m: boolean }
export interface Ticker24h { symbol: string; lastPrice: string; priceChangePercent: string; volume: string; quoteVolume: string }
export interface FundingInfo { symbol: string; adjustedFundingRateCap: string; adjustedFundingRateFloor: string; fundingIntervalHours: number }
export interface OpenInterestSnapshot { symbol: string; openInterest: string; time: number }
export interface OpenInterestPoint { symbol: string; sumOpenInterest: string; sumOpenInterestValue: string; timestamp: number }
export interface LongShortRatioPoint { symbol: string; longShortRatio: string; longAccount?: string; shortAccount?: string; timestamp: number }
export interface TakerVolumePoint { buySellRatio: string; buyVol: string; sellVol: string; timestamp: number }
export interface BasisPoint { pair: string; basis: string; basisRate: string; futuresPrice: string; indexPrice: string; timestamp: number }
export interface CompositeIndexInfo { symbol: string; time: number; component: Array<Record<string, unknown>> }
export interface IndexConstituents { symbol: string; time: number; constituents: Array<{ exchange: string; symbol: string; price: string; weight: string }> }
export interface AssetIndexInfo { symbol: string; time: number; index: string }

export type FastStreamSpec =
  | { type: "depth"; symbol?: string; levels: 5 | 10 | 20; speed?: "100ms" | "250ms" | "500ms" }
  | { type: "depth_diff"; symbol?: string; speed?: "100ms" | "250ms" | "500ms" }
  | { type: "rpi_depth" | "agg_trade" | "ticker_24h" | "mini_ticker" | "composite_index"; symbol?: string }
  | { type: "all_book_tickers" | "all_tickers_24h" | "all_mini_tickers" | "all_liquidations" | "all_asset_indexes" }
  | { type: "continuous_kline"; pair?: string; contractType: "perpetual" | "current_quarter" | "next_quarter"; interval: KlineInterval }
  | { type: "all_mark_prices"; speed?: "1s" | "3s" }
  | { type: "asset_index"; asset: string };

export interface FastMarketEvent {
  stream: string; type: FastStreamSpec["type"]; symbol?: string; eventTs: number; ingestedTs: number;
  droppedCount?: number; payload: unknown;
}

export interface EventFrame<T = Record<string, unknown>, Topic = string> {
  seq: number; ts: number; topic: Topic; source: string; payload: T;
}
export type MarketEvent =
  | EventFrame<Kline, "market.kline"> | EventFrame<BookTicker, "market.book_ticker"> |
  EventFrame<MarkPrice, "market.mark_price"> | EventFrame<Liquidation, "market.liquidation"> |
  EventFrame<Record<string, unknown>, "market.contract_info">;
export interface DataSample {
  dataset: string; seq: number; ts: number; ingestedTs: number; key?: string; payload: Record<string, unknown>;
}
export type DataSampleEvent = EventFrame<Omit<DataSample, "seq">, "data.sample">;
export type AccountTopic = "order" | "algo_order" | "fill" | "position" | "account" | "risk_reject";
export type AccountWatchEvent = EventFrame<Record<string, unknown> & { accountId: string }, "trade." | "shadow." | string>;
export interface TradeFill {
  accountId: string; symbol: string; tradeId: string; exchangeOrderId: string; clientOrderId?: string;
  qty: number; price: number; fee: number; feeAsset: string; realizedPnl: number; side: "BUY" | "SELL";
  positionSide: "LONG" | "SHORT"; ts: number; source: string;
}
export type TradeFillEvent = EventFrame<TradeFill, "trade.fill" | "shadow.fill">;
export type AlgoOrderEvent = EventFrame<OpenOrder & { accountId: string }, "trade.algo_order" | "shadow.algo_order">;
export interface NotifyRequest { level: "info" | "warn" | "critical"; title: string; body: string; dedupKey?: string; cooldownSec?: number }
export interface TaskRequest {
  dedupKey: string; prompt: string; title?: string; backend?: string;
  visibility?: "visible" | "hidden" | "ephemeral"; cooldownSec?: number; context?: Record<string, unknown>;
}
export interface AgentTaskStatus { taskId: string; status: string; sessionId?: string; summary?: string }
export type StopDecision = { retain?: OrderRef[]; cancel?: OrderRef[] };
export type StopHandler = (context: { handoverId: string; orders: OpenOrder[] }) => StopDecision | void | Promise<StopDecision | void>;
