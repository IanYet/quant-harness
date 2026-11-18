import type { AggregateTrade, Bar, DepthSnapshot, Interval, Metric, Range, Spec } from "@quant/tools";
export type Wire = Record<string, any>;
export type Group = "public" | "market";
export function groupOf(key: string): Group {
  return /@(bookTicker|depth)/.test(key) ? "public" : "market";
}
export interface FeedEvents {
  message(key: string, event: Wire): void;
  down(group: Group, reason: string): void;
  up(group: Group): void;
  sample(): void;
}
export interface Source {
  now(): number;
  start(events: FeedEvents): void;
  stop(): void;
  streams(keys: Set<string>): void;
  ready(key: string): Promise<void>;
  heartbeats?(): Record<Group, number | null>;
  status(): Record<Group, string>;
  bars(symbol: string, interval: Interval, range: Range): Promise<Bar[]>;
  specs(): Promise<Spec[]>;
  depth(symbol: string): Promise<DepthSnapshot>;
  trades(symbol: string, range: Range | undefined, limit: number): Promise<AggregateTrade[]>;
  metric(kind: "oi" | "ratio" | "funding", symbol: string, range?: Range): Promise<Metric[]>;
  stats(symbol: string): Promise<Record<string, unknown>>;
}
