import type { SQLQueryBindings } from "bun:sqlite";
import type { FundingRate, Kline, KlineInterval, DataSample } from "@quant/tools";
import { bad, queryNumber, oneOf, str, SYMBOL } from "../shared/http";
import type { Storage } from "./db";

export function range(url: URL, now = Date.now()) {
  const start = queryNumber(url, "start", 0);
  const end = queryNumber(url, "end", now);
  if (start > end) bad("start must be <= end");
  return { start, end };
}
export function symbol(url: URL, fallback: string) {
  return str(url.searchParams.get("symbol") ?? fallback, "symbol", SYMBOL);
}
export function readKlines(storage: Storage, ticker: string, interval: KlineInterval, start: number, end: number, limit = 100_000) {
  return storage.timeseries.query<Kline, [string, string, number, number, number]>(
    "SELECT symbol,interval,open_time AS openTime,close_time AS closeTime,open AS o,high AS h,low AS l,close AS c," +
    "volume AS v,quote_volume AS quoteVolume,1 AS closed FROM klines WHERE symbol=? AND interval=? AND open_time>=? " +
    "AND close_time<=? ORDER BY open_time LIMIT ?").all(ticker, interval, start, end, limit).map(row => ({ ...row, closed: true }));
}
export function readFunding(storage: Storage, ticker: string, start: number, end: number, limit: number) {
  return storage.timeseries.query<FundingRate, [string, number, number, number]>(
    "SELECT symbol,funding_time AS fundingTime,funding_rate AS fundingRate,mark_price AS markPrice FROM funding_rates " +
    "WHERE symbol=? AND funding_time BETWEEN ? AND ? ORDER BY funding_time LIMIT ?").all(ticker, start, end, limit);
}
export function readLiquidations(storage: Storage, ticker: string, start: number, end: number, limit: number) {
  return storage.timeseries.query("SELECT symbol,side,order_type AS orderType,price,avg_price AS avgPrice,qty,filled_qty AS filledQty," +
    "trade_time AS tradeTime FROM liquidations WHERE symbol=? AND trade_time BETWEEN ? AND ? ORDER BY trade_time DESC LIMIT ?")
    .all(ticker, start, end, limit);
}
export function readSamples(storage: Storage, dataset: string, url: URL, asOf = queryNumber(url, "asOf", Number.MAX_SAFE_INTEGER),
    asOfSeq = queryNumber(url, "asOfSeq", Number.MAX_SAFE_INTEGER)) {
  const { start, end } = range(url, asOf);
  const limit = queryNumber(url, "limit", 100, 1, 10_000);
  return storage.timeseries.transaction(() => {
    const items = storage.timeseries.query<DataSample & { json: string }, [string, number, number, number, number, number, number]>(
      "SELECT dataset,seq,ts,ingested_ts AS ingestedTs,sample_key AS key,payload_json AS json FROM data_samples " +
      "WHERE dataset=? AND ts BETWEEN ? AND ? AND (COALESCE(ingested_ts,ts)<? OR (COALESCE(ingested_ts,ts)=? AND seq<=?)) " +
      "ORDER BY ts DESC,seq DESC LIMIT ?").all(dataset, start, end, asOf, asOf, asOfSeq, limit);
    const { maxSeq } = storage.timeseries.query<{ maxSeq: number }, [string, number, number, number]>(
      "SELECT COALESCE(MAX(seq),0) AS maxSeq FROM data_samples WHERE dataset=? AND " +
      "(COALESCE(ingested_ts,ts)<? OR (COALESCE(ingested_ts,ts)=? AND seq<=?))").get(dataset, asOf, asOf, asOfSeq)!;
    return { items: items.map(({ json, ...row }) => ({ ...row, payload: JSON.parse(json) })), maxSeq };
  })();
}

const ledgerTypes = {
  fills: ["fills", "ts", "event_seq"],
  orders: ["orders", "created_at", null],
  algo_orders: ["algo_orders", "created_at", null],
  income: ["account_income", "ts", null],
  risk_rejects: ["risk_rejects", "ts", "seq"],
  accounts: ["account_snapshots", "ts", "seq"],
  audit: ["operation_audit", "ts", "seq"],
} as const;

export function ledgerQuery(storage: Storage, url: URL) {
  const type = oneOf(url.searchParams.get("type") ?? "fills", "type", Object.keys(ledgerTypes) as (keyof typeof ledgerTypes)[]);
  const [table, date, seq] = ledgerTypes[type];
  const { start, end } = range(url);
  const conditions = [date + " BETWEEN ? AND ?"];
  const bindings: SQLQueryBindings[] = [start, end];
  const mode = url.searchParams.get("mode");
  const account = url.searchParams.get("accountId");
  const ticker = url.searchParams.get("symbol");
  if (type !== "audit" && mode) { conditions.push("mode=?"); bindings.push(oneOf(mode, "mode", ["live", "shadow"])); }
  if (type !== "audit" && account) { conditions.push("account_id=?"); bindings.push(account); }
  if (ticker && type !== "accounts" && type !== "risk_rejects" && type !== "audit") { conditions.push("symbol=?"); bindings.push(ticker); }
  const ordered = seq && (url.searchParams.has("after_seq") || url.searchParams.has("through_seq"));
  if (ordered) {
    conditions.push(seq + ">? AND " + seq + "<=?");
    bindings.push(queryNumber(url, "after_seq", 0), queryNumber(url, "through_seq", Number.MAX_SAFE_INTEGER));
  }
  return storage.ledger.query("SELECT * FROM " + table + " WHERE " + conditions.join(" AND ") + " ORDER BY " +
    (ordered ? seq + " ASC" : date + " DESC") + " LIMIT ?").all(...bindings, queryNumber(url, "limit", 50, 1, 10_000));
}

export function pnl(storage: Storage, url: URL) {
  const value = url.searchParams.get("window") ?? "7d";
  const span = /^([1-9]\d{0,4})([dhm])$/.exec(value);
  if (!span) bad("window must use d, h or m");
  const { start } = range(url);
  const since = start || Date.now() - Number(span[1]) * (span[2] === "d" ? 86_400_000 : span[2] === "h" ? 3_600_000 : 60_000);
  const target = url.searchParams.get("target");
  const ticker = url.searchParams.get("symbol");
  const mode = url.searchParams.get("mode");
  const modes = mode ? [oneOf(mode, "mode", ["live", "shadow"])] : ["live", "shadow"];
  return modes.map(mode => {
    const fills = storage.ledger.query<{ realized_pnl: number; fee: number; fee_asset: string }, SQLQueryBindings[]>(
      "SELECT realized_pnl,fee,fee_asset FROM fills WHERE mode=? AND ts>=? AND (? IS NULL OR symbol=?) " +
      "AND (? IS NULL OR account_id=? OR strategy_id=?) ORDER BY ts,event_seq").all(mode, since, ticker, ticker, target, target, target);
    const funding = storage.ledger.query<{ asset: string; amount: number }, SQLQueryBindings[]>(
      "SELECT asset,SUM(CAST(amount AS REAL)) AS amount FROM account_income WHERE mode=? AND ts>=? " +
      "AND income_type='FUNDING_FEE' AND (? IS NULL OR symbol=?) AND (? IS NULL OR account_id=?) GROUP BY asset")
      .all(mode, since, ticker, ticker, target, target);
    const fees: Record<string, number> = Object.create(null);
    let realizedPnl = 0, wins = 0, losses = 0, gains = 0, lost = 0, drawdown = 0, peak = 0, tradingEquity = 0;
    for (const fill of fills) {
      fees[fill.fee_asset] = (fees[fill.fee_asset] ?? 0) + fill.fee;
      realizedPnl += fill.realized_pnl;
      tradingEquity += fill.realized_pnl - (fill.fee_asset === "USDT" ? fill.fee : 0);
      peak = Math.max(peak, tradingEquity);
      drawdown = Math.max(drawdown, peak - tradingEquity);
      if (fill.realized_pnl > 0) { wins++; gains += fill.realized_pnl; }
      if (fill.realized_pnl < 0) { losses++; lost -= fill.realized_pnl; }
    }
    return { mode, fills: fills.length, realizedPnl, fees, funding, winRate: wins + losses ? wins / (wins + losses) : null,
      profitFactor: lost ? gains / lost : null, maxDrawdownUsdt: drawdown,
      netUsdt: realizedPnl - (fees.USDT ?? 0) + (funding.find(item => item.asset === "USDT")?.amount ?? 0) };
  });
}
