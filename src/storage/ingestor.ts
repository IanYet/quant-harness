import type { Database, SQLQueryBindings } from "bun:sqlite";
import type { BusEvent, ControlFrame } from "../shared/events";
import { CONSUMER_TOPICS } from "../shared/events";
import type { ServiceAuth } from "../shared/auth";
import { socketPath } from "../shared/config";
import { call, readNdjson, unixFetch, withQuery } from "../shared/uds";
import type { BusFrame } from "../shared/events";
import type { Storage } from "./db";

type Row = Record<string, SQLQueryBindings>;
type Gap = Extract<ControlFrame, { type: "gap" }>;

function insert(db: Database, table: string, row: Row, update = "") {
  const cols = Object.keys(row);
  db.query("INSERT INTO " + table + " (" + cols.join(",") + ") VALUES (" +
    cols.map(() => "?").join(",") + ") " + (update || "ON CONFLICT DO NOTHING")).run(...Object.values(row));
}

function orderConflict(table: "orders" | "algo_orders", id: string, status: string, fields: string[]) {
  return "ON CONFLICT(mode,account_id,symbol," + id + ") DO UPDATE SET " +
    fields.map(field => field + "=COALESCE(excluded." + field + "," + table + "." + field + ")").join(",") +
    " WHERE excluded.updated_at >= " + table + ".updated_at" +
    " AND (" + table + "." + status + " NOT IN ('FILLED','CANCELED','EXPIRED','EXPIRED_IN_MATCH','FINISHED','REJECTED')" +
    " OR excluded." + status + " = " + table + "." + status + ")";
}

const orderUpdate = orderConflict("orders", "exchange_order_id", "status",
  ["client_order_id", "price", "filled_qty", "avg_price", "status", "updated_at"]);
const algoUpdate = orderConflict("algo_orders", "algo_id", "algo_status",
  ["client_algo_id", "trigger_price", "price", "callback_rate", "working_type", "algo_status", "actual_order_id", "updated_at"]);

function ledgerEvent(db: Database, { topic, seq, ts, payload }: BusEvent) {
  const p = payload as Row;
  const [group, kind] = topic.split(".");
  const mode = group === "shadow" ? "shadow" : "live";
  const account = { mode, account_id: p.accountId };
  const owner = { source: p.source ?? "external", run_id: p.runId ?? null, strategy_id: p.strategyId ?? null };
  if (group === "trade" || group === "shadow") {
    switch (kind) {
      case "order":
        if (!p.exchangeOrderId) break;
        insert(db, "orders", {
          ...account, ...owner, epoch: p.epoch ?? null,
          symbol: p.symbol, exchange_order_id: p.exchangeOrderId, client_order_id: p.clientOrderId ?? null,
          side: p.side, position_side: p.positionSide, order_type: p.orderType, qty: p.qty,
          price: p.price ?? null, filled_qty: p.filledQty ?? 0, avg_price: p.avgPrice ?? null,
          status: p.status, created_at: p.createdAt ?? ts, updated_at: p.updatedAt ?? ts,
        }, orderUpdate);
        break;
      case "algo_order":
        if (!p.algoId) break;
        insert(db, "algo_orders", {
          ...account, ...owner, epoch: p.epoch ?? null, symbol: p.symbol, algo_id: p.algoId,
          client_algo_id: p.clientAlgoId ?? null, side: p.side, position_side: p.positionSide,
          order_type: p.orderType, qty: p.qty, trigger_price: p.triggerPrice ?? null, price: p.price ?? null,
          callback_rate: p.callbackRate ?? null, working_type: p.workingType ?? null, algo_status: p.status,
          actual_order_id: p.actualOrderId ?? null, created_at: p.createdAt ?? ts, updated_at: p.updatedAt ?? ts,
        }, algoUpdate);
        break;
      case "fill":
        insert(db, "fills", {
          ...account, ...owner, symbol: p.symbol, trade_id: p.tradeId, exchange_order_id: p.exchangeOrderId,
          client_order_id: p.clientOrderId ?? null, event_seq: seq, side: p.side, position_side: p.positionSide,
          price: p.price, qty: p.qty, fee: p.fee, fee_asset: p.feeAsset, realized_pnl: p.realizedPnl ?? 0, ts: p.ts ?? ts,
        }, "ON CONFLICT(mode,account_id,symbol,trade_id) DO UPDATE SET source=excluded.source,run_id=excluded.run_id," +
          "strategy_id=excluded.strategy_id,client_order_id=excluded.client_order_id WHERE fills.source='external' AND excluded.source<>'external'");
        break;
      case "income":
        insert(db, "account_income", {
          ...account, symbol: p.symbol ?? null, income_type: p.incomeType, tran_id: p.tranId,
          asset: p.asset, amount: String(p.amount), ts: p.ts ?? ts,
        });
        break;
      case "account":
        insert(db, "account_snapshots", {
          ...account, seq, wallet_balance: p.walletBalance, available_balance: p.availableBalance,
          unrealized_pnl: p.unrealizedPnl, positions_json: JSON.stringify(payload.positions ?? {}), ts: p.ts ?? ts,
        });
        break;
      case "risk_reject":
        insert(db, "risk_rejects", {
          ...account, seq, source: owner.source, run_id: owner.run_id, code: p.code,
          reason: p.reason, payload_json: JSON.stringify(p), ts,
        });
        break;
    }
  } else if (topic === "state.checkpoint") {
    insert(db, "checkpoints", {
      namespace: p.namespace, version: p.version, account_id: p.accountId ?? null, epoch: p.epoch,
      schema_version: p.schemaVersion, data_json: JSON.stringify(payload.data), updated_at: p.updatedAt,
    });
  } else if (topic === "sys.operation") {
    insert(db, "operation_audit", {
      seq, operation_id: p.operationId, action: p.action, actor: p.actor, source: p.source,
      target_json: JSON.stringify(payload.target ?? {}), confirmation_ref: p.confirmationRef ?? null,
      reason: p.reason ?? null, result: p.result, ts,
    });
  }
}

function seriesEvent(db: Database, { topic, seq, ts, source, payload }: BusEvent) {
  const p = payload as Row;
  if (topic === "data.sample") {
    const updated = Number(p.ingestedTs ?? ts);
    insert(db, "datasets", {
      dataset: p.dataset, schema_hint_json: null, ttl_sec: p.ttlSec ?? 300, last_sample_ts: p.ts, last_seq: seq, updated_at: updated,
    }, "ON CONFLICT(dataset) DO UPDATE SET last_sample_ts=excluded.last_sample_ts,last_seq=excluded.last_seq,updated_at=excluded.updated_at");
    insert(db, "data_samples", { dataset: p.dataset, seq, ts: p.ts, ingested_ts: updated, sample_key: p.key ?? null,
      payload_json: JSON.stringify(payload.payload) });
    return;
  }
  if (source !== "binance.mainnet") return;
  if (topic === "sys.market_gap") {
    insert(db, "market_gaps", { through_seq: seq, start_ts: p.startTs, end_ts: p.endTs });
    return;
  }
  switch (topic) {
    case "market.kline":
      if (payload.closed === true) insert(db, "klines", {
        symbol: p.symbol, interval: p.interval, open_time: p.openTime, close_time: p.closeTime,
        open: p.o, high: p.h, low: p.l, close: p.c, volume: p.v, quote_volume: p.quoteVolume,
      });
      break;
    case "market.mark_price": {
      const minute = Math.floor(Number(p.eventTs ?? ts) / 60_000) * 60_000;
      const book = db.query<{ bid: number; ask: number }, SQLQueryBindings[]>(
        "SELECT bid,ask FROM latest_books WHERE symbol=? AND ts<?").get(p.symbol, minute + 60_000);
      insert(db, "market_metrics", {
        symbol: p.symbol, ts: minute, mark_price: p.markPrice, index_price: p.indexPrice, funding_rate: p.fundingRate,
        next_funding_time: p.nextFundingTime, bid: book?.bid ?? null, ask: book?.ask ?? null,
      }, "ON CONFLICT(symbol,ts) DO UPDATE SET mark_price=excluded.mark_price,index_price=excluded.index_price," +
        "funding_rate=excluded.funding_rate,next_funding_time=excluded.next_funding_time");
      break;
    }
    case "market.book_ticker": {
      const time = Number(p.eventTs ?? ts);
      insert(db, "latest_books", { symbol: p.symbol, ts: time, bid: p.bid, ask: p.ask },
        "ON CONFLICT(symbol) DO UPDATE SET ts=excluded.ts,bid=excluded.bid,ask=excluded.ask WHERE excluded.ts >= latest_books.ts");
      db.query("UPDATE market_metrics SET bid=?,ask=? WHERE symbol=? AND ts=? AND ? >= (SELECT ts FROM latest_books WHERE symbol=?)")
        .run(p.bid, p.ask, p.symbol, Math.floor(time / 60_000) * 60_000, time, p.symbol);
      break;
    }
    case "market.funding_rate":
      insert(db, "funding_rates", { symbol: p.symbol, funding_time: p.fundingTime,
        funding_rate: p.fundingRate, mark_price: p.markPrice ?? null });
      break;
    case "market.liquidation":
      insert(db, "liquidations", {
        seq, symbol: p.symbol, side: p.side, order_type: p.orderType, price: p.price, avg_price: p.avgPrice,
        qty: p.qty, filled_qty: p.filledQty, trade_time: p.tradeTime,
      });
      break;
  }
}

export function writeLedger(db: Database, events: BusEvent[], throughSeq: number) {
  db.transaction(() => {
    const start = db.query<{ last_seq: number }, []>("SELECT last_seq FROM sync_cursor").get()!.last_seq;
    for (const event of events) if (event.seq > start && event.seq <= throughSeq) ledgerEvent(db, event);
    db.query("UPDATE sync_cursor SET last_seq=?,updated_at=? WHERE last_seq<?").run(throughSeq, Date.now(), throughSeq);
  }).immediate();
}

export function writeTimeseries(db: Database, events: BusEvent[], throughSeq: number, gaps: Gap[] = []) {
  db.transaction(() => {
    const cursor = db.query<{ last_seq: number; updated_at: number }, []>("SELECT last_seq,updated_at FROM sync_cursor").get()!;
    for (const event of events) if (event.seq > cursor.last_seq && event.seq <= throughSeq) seriesEvent(db, event);
    for (const gap of gaps) {
      if (gap.toSeq <= cursor.last_seq || !gap.topics.some(topic => topic.startsWith("market."))) continue;
      const end = gap.toTs ?? Date.now();
      const start = gap.fromTs ?? (cursor.updated_at || end - 3_600_000);
      insert(db, "market_gaps", { through_seq: gap.toSeq, start_ts: Math.min(start, end), end_ts: end });
    }
    db.query("UPDATE sync_cursor SET last_seq=?,updated_at=? WHERE last_seq<?").run(throughSeq, Date.now(), throughSeq);
  }).immediate();
}

export async function ingest(dir: string, storage: Storage, auth: ServiceAuth, signal: AbortSignal) {
  const socket = socketPath(dir, "admin");
  const token = auth.token("storage");
  while (!signal.aborted) {
    try {
      const res = await unixFetch(socket, withQuery("/v1/stream", {
        topics: CONSUMER_TOPICS.storage.join(","), after_seq: storage.cursor().lastSeq,
      }), { headers: { authorization: "Bearer " + token }, signal });

      let events: BusEvent[] = [];
      let gaps: Gap[] = [];
      let lastFlush = Date.now();
      let through = storage.cursor().lastSeq;
      let acking: Promise<void> | undefined;
      let acked = 0;
      let streamActive = true;
      function flush() {
        writeLedger(storage.ledger, events, through);
        writeTimeseries(storage.timeseries, events, through, gaps);
        events = [];
        gaps = [];
        lastFlush = Date.now();
        if (!acking && through > acked) {
          const written = storage.cursor().lastSeq;
          acking = call(socket, "/v1/stream/ack", { method: "POST", token, body: { throughSeq: written } })
            .then(() => { acked = written; })
            .catch(() => {})
            .finally(() => { acking = undefined; });
        }
      }
      const timer = setInterval(() => {
        if (streamActive && through > acked) {
          try { flush(); } catch (error) { console.error("Storage commit failed", error); }
        }
      }, 200);
      try {
        for await (const frame of readNdjson<BusFrame>(res)) {
          if ("seq" in frame) {
            events.push(frame);
            through = frame.seq;
          } else if (frame.type === "gap") gaps.push(frame);
          else if (frame.type === "watermark") through = Math.max(through, frame.throughSeq);
          if (events.length >= 200 || Date.now() - lastFlush >= 200) flush();
        }
      } finally {
        streamActive = false;
        clearInterval(timer);
      }
    } catch (error) {
      if (!signal.aborted) console.warn("Storage stream reconnecting", (error as Error).message);
    }
    if (!signal.aborted) await Bun.sleep(1000);
  }
}
