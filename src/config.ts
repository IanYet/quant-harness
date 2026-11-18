import { resolve } from "node:path";
import { requireThat, integer, symbolOf } from "./util";
export interface Destinations {
  bots?: Record<string, { url: string; tokenEnv?: string }>;
  notifications?: Record<string, { bot: string; target: string; template: string }>;
  agents?: Record<string, { url: string; tokenEnv?: string }>;
}
export interface Config {
  dir: string;
  port: number;
  symbols: string[];
  market: "sim" | "binance";
  binanceTrading: boolean;
  rest: string;
  ws: string;
  archive: string;
  apiKey: string;
  secret: string;
  tickMs: number;
  timeoutMs: number;
  reconnectMs: number;
  healthMs: number;
  reconcileMs: number;
  readyMs: number;
  destinations: Destinations;
}
export async function config(env = process.env): Promise<Config> {
  const market = env.HARNESS_MARKET ?? "sim";
  const trading = env.HARNESS_TRADING ?? "paper";
  requireThat(market === "binance" || market === "sim", "Unknown market mode");
  requireThat(trading === "paper" || trading === "binance", "Unknown trading mode");
  if (trading === "binance")
    requireThat(
      market === "binance" && env.BINANCE_API_KEY && env.BINANCE_API_SECRET,
      "Binance execution requires Binance market and host credentials",
    );
  return {
    dir: resolve(env.HARNESS_DIR ?? ".harness"),
    port: integer(env.HARNESS_PORT ?? 8787, 0, 65535),
    symbols: (env.HARNESS_SYMBOLS ?? "BTCUSDT").split(",").map((s) => symbolOf(s.trim())),
    market,
    binanceTrading: trading === "binance",
    rest: env.BINANCE_REST ?? "https://fapi.binance.com",
    ws: env.BINANCE_WS ?? "wss://fstream.binance.com",
    archive: env.BINANCE_ARCHIVE ?? "https://data.binance.vision/data/futures/um/daily",
    apiKey: env.BINANCE_API_KEY ?? "",
    secret: env.BINANCE_API_SECRET ?? "",
    tickMs: integer(env.HARNESS_SIM_TICK_MS ?? 100, 5, 60000),
    timeoutMs: 10000,
    reconnectMs: 1000,
    healthMs: 1000,
    reconcileMs: 15000,
    readyMs: integer(env.HARNESS_READY_MS ?? 30000, 100, 600000),
    destinations: env.HARNESS_CONNECTORS ? await Bun.file(env.HARNESS_CONNECTORS).json() : {},
  };
}
