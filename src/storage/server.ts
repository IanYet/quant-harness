import { statfs } from "node:fs/promises";
import { klineIntervals } from "../../packages/tools/types";
import type { ServiceAuth } from "../shared/auth";
import { capabilities } from "../shared/capabilities";
import type { HarnessConfig } from "../shared/config";
import { socketPath } from "../shared/config";
import { body, handler, ID, missing, oneOf, queryFlag, queryNumber, str } from "../shared/http";
import { serveUnix } from "../shared/uds";
import type { Storage } from "./db";
import { LogRotator } from "./log-rotator";
import { ledgerQuery, pnl, range, readFunding, readKlines, readLiquidations, readSamples, symbol } from "./queries";
import { StrategyRegistry } from "./strategy-registry";
import { join } from "node:path";
import type { KlineInterval } from "@quant/tools";

export type HistoryOperations = {
  klines(symbol: string, interval: KlineInterval, start: number, end: number): Promise<void>;
  funding(symbol: string, start: number, end: number): Promise<void>;
  backtest(input: Record<string, unknown>): Promise<unknown>;
};

export async function startStorage(config: HarnessConfig, storage: Storage, auth: ServiceAuth, registry: StrategyRegistry,
    history?: HistoryOperations) {
  const logs = new LogRotator(join(config.dataDir, "logs"), storage.ledger);
  const backfillRoles = ["cli", "mcp", "core", "storage"] as const;
  return serveUnix(socketPath(config.dataDir, "storage"), handler(async (req, url) => {
    const path = url.pathname;
    const method = req.method;
    if (method === "GET") {
      if (path === "/v1/meta/cursor") return storage.cursor();
      if (path === "/v1/health") {
        const disk = await statfs(config.dataDir);
        return { ok: true, pid: process.pid, ...storage.cursor(), availableBytes: disk.bavail * disk.bsize };
      }
      if (path === "/v1/market/klines") {
        const interval = oneOf(url.searchParams.get("interval") ?? "1m", "interval", klineIntervals);
        const ticker = symbol(url, config.defaultSymbol);
        const { start, end } = range(url);
        if (history && queryFlag(url, "backfill")) await history.klines(ticker, interval, start, end);
        return readKlines(storage, ticker, interval, start, Math.min(end, Date.now()), queryNumber(url, "limit", 100_000, 1, 100_000));
      }
      if (path === "/v1/market/funding-rates") {
        const ticker = symbol(url, config.defaultSymbol);
        const { start, end } = range(url);
        if (history && queryFlag(url, "backfill")) await history.funding(ticker, start, end);
        return readFunding(storage, ticker, start, Math.min(end, Date.now()), queryNumber(url, "limit", 1000, 1, 10_000));
      }
      if (path === "/v1/market/liquidations") {
        const { start, end } = range(url);
        return readLiquidations(storage, symbol(url, config.defaultSymbol), start, end, queryNumber(url, "limit", 100, 1, 10_000));
      }
      if (path === "/v1/data/datasets") return storage.timeseries.query(
        "SELECT d.*,(SELECT COUNT(*) FROM data_samples s WHERE s.dataset=d.dataset) AS sampleCount," +
        " updated_at+ttl_sec*1000<? AS expired FROM datasets d ORDER BY dataset").all(Date.now());
      if (path === "/v1/data/query") return readSamples(storage, str(url.searchParams.get("dataset"), "dataset", ID), url);
      if (path === "/v1/strategies/search") return registry.search(url.searchParams);
      if (path.startsWith("/v1/strategies/")) return registry.describe(decodeURIComponent(path.slice(15)));
      if (path.startsWith("/v1/logs/")) return logs.tail(str(path.slice(9), "runId", ID), queryNumber(url, "tail", 200, 1, 10_000),
        queryNumber(url, "since", 0), url.searchParams.get("grep") ?? undefined);
      if (path === "/v1/ledger/query") { auth.require(req, ["cli", "mcp", "core", "watchdog"]); return ledgerQuery(storage, url); }
      if (path === "/v1/ledger/pnl") { auth.require(req, ["cli", "mcp"]); return pnl(storage, url); }
      if (path === "/v1/checkpoints") {
        auth.require(req, ["cli", "mcp", "core"]);
        const namespace = str(url.searchParams.get("namespace"), "namespace");
        const version = queryNumber(url, "version", 0);
        return storage.ledger.query("SELECT * FROM checkpoints WHERE namespace=? AND (?=0 OR version=?) ORDER BY version DESC LIMIT ?")
          .all(namespace, version, version, queryNumber(url, "limit", 50, 1, 1000));
      }
    }

    if (method === "POST") {
      if (path === "/v1/logs/append") {
        auth.require(req, ["core", "storage"]);
        await logs.append(await body(req));
        return { written: true };
      }
      if (path === "/v1/scripts/prepare" || path === "/v1/scripts/validate") {
        auth.require(req, [...backfillRoles]);
        const input = await body(req);
        const mode = oneOf(input.mode, "mode", ["worker", "shadow", "live"]);
        const caps = capabilities(input.capabilities);
        return path === "/v1/scripts/prepare" ? registry.prepare({ mode, strategyId: input.strategyId, path: input.path, capabilities: caps }) :
          registry.validate(input.path, mode, caps);
      }
      if (path === "/v1/strategies/seal") { auth.require(req, ["cli", "mcp"]); return registry.seal(await body(req)); }
      if (path === "/v1/strategies/fork") { auth.require(req, ["cli", "mcp"]); return registry.fork(await body(req)); }
      if (path === "/v1/backtest/run" && history) { auth.require(req, ["cli", "mcp"]); return history.backtest(await body(req)); }
    }
    if (method === "PATCH" && /^\/v1\/strategies\/[^/]+\/tags$/.test(path)) {
      auth.require(req, ["cli", "mcp"]);
      return registry.tag(decodeURIComponent(path.split("/")[3]), await body(req));
    }
    return missing("Unknown storage endpoint");
  }));
}
