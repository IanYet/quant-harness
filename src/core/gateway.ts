import type { AccountFullState, ExchangeSymbolInfo, MarketSnapshot, OpenOrder, PositionLeg, Positions } from "@quant/tools";
import type { AccountConfig, AccountMode } from "../shared/config";
import type { NormalizedOrder } from "./risk";

export interface ExchangeAccount {
  readonly mode: AccountMode;
  readonly id: string;
  config: AccountConfig;
  view(): AccountFullState;
  healthy(): { ready: boolean; privateWsAt: number; error?: string };
  rules(symbol: string, priority?: boolean): Promise<ExchangeSymbolInfo>;
  mark(symbol: string): { price: number; ts: number } | undefined;
  refresh(): Promise<void>;
  submit(order: NormalizedOrder, exchangeClientId: string): Promise<OpenOrder>;
  queryOrder(symbol: string, kind: OpenOrder["kind"], exchangeClientId: string): Promise<OpenOrder | null>;
  cancel(order: OpenOrder): Promise<OpenOrder>;
  cancelAll(symbol: string, conditional: boolean): Promise<void>;
  leverage(symbol: string, value: number): Promise<void>;
  close(): void;
}

export interface Markets {
  snapshot(symbol: string, env?: "mainnet" | "testnet"): MarketSnapshot | Record<string, MarketSnapshot>;
  rest(endpoint: string, query: URLSearchParams, env?: "mainnet" | "testnet"): Promise<unknown>;
  fast(specs: unknown, signal: AbortSignal): Response;
  health(): { freshness: unknown; fastSubscriptions: unknown };
}

export const accountKey = (mode: AccountMode, id: string) => mode + ":" + id;
export type AccountDirectory = Map<string, ExchangeAccount>;

export function emptyLeg(symbol: string, side: PositionLeg["side"], leverage = 1): PositionLeg {
  return { symbol, side, qty: 0, entryPrice: 0, markPrice: 0, unrealizedPnl: 0, leverage, liquidationPrice: 0 };
}
export function legs(positions: Positions, symbol: string, leverage?: number) {
  return positions[symbol] ?? { long: emptyLeg(symbol, "LONG", leverage), short: emptyLeg(symbol, "SHORT", leverage) };
}
