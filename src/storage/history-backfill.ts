import type { Kline, KlineInterval } from "@quant/tools";
import type { HarnessConfig } from "../shared/config";
import { config, socketPath } from "../shared/config";
import { bad } from "../shared/http";
import { call, withQuery } from "../shared/uds";
import { MINUTE } from "../shared/bars";
import type { Storage } from "./db";

type FundingRow = { symbol: string; fundingTime: number; fundingRate: string; markPrice?: string };
type Coverage = { start_ts: number; end_ts: number };
const DAY = 1440 * MINUTE;
const times = { "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "2h": 120, "4h": 240,
  "6h": 360, "8h": 480, "12h": 720, "1d": 1440, "3d": 4320, "1w": 10_080, "1M": 44_640 } as const;

export class HistoryBackfill {
  private readonly active = new Map<string, Promise<void>>();
  constructor(private readonly settings: HarnessConfig, private readonly dbs: Storage) {}

  private missing(kind: string, symbol: string, start: number, end: number) {
    const rows = this.dbs.timeseries.query<Coverage, [string, string, number, number]>(
      "SELECT start_ts,end_ts FROM history_coverage WHERE kind=? AND symbol=? AND end_ts>=? AND start_ts<=? ORDER BY start_ts")
      .all(kind, symbol, start, end);
    const gaps: Array<[number, number]> = [];
    let cursor = start;
    for (const row of rows) {
      if (row.start_ts > cursor) gaps.push([cursor, row.start_ts - 1]);
      cursor = Math.max(cursor, row.end_ts + 1);
    }
    if (cursor <= end) gaps.push([cursor, end]);
    return gaps;
  }

  private save(kind: string, symbol: string, start: number, end: number) {
    const db = this.dbs.timeseries;
    const overlap = db.query<{ lo: number | null; hi: number | null }, [string, string, number, number]>(
      "SELECT MIN(start_ts) AS lo, MAX(end_ts) AS hi FROM history_coverage WHERE kind=? AND symbol=? AND end_ts>=? AND start_ts<=?")
      .get(kind, symbol, start - 1, end + 1)!;
    db.query("DELETE FROM history_coverage WHERE kind=? AND symbol=? AND end_ts>=? AND start_ts<=?").run(kind, symbol, start - 1, end + 1);
    db.query("INSERT INTO history_coverage VALUES (?,?,?,?)").run(kind, symbol, Math.min(start, overlap.lo ?? start), Math.max(end, overlap.hi ?? end));
  }

  private serial(kind: string, symbol: string, work: () => Promise<void>) {
    const key = kind + ":" + symbol;
    const job = (this.active.get(key) ?? Promise.resolve()).catch(() => {}).then(work);
    this.active.set(key, job);
    return job.finally(() => { if (this.active.get(key) === job) this.active.delete(key); });
  }

  private mainnet<T>(endpoint: string, q: Record<string, unknown>) {
    return call<T>(socketPath(this.settings.dataDir, "admin"), withQuery("/v1/market/rest/" + endpoint, q), { timeoutMs: 15_000 });
  }

  klines(symbol: string, interval: KlineInterval, start: number, end: number) {
    return this.bars(symbol, interval, start, end, false);
  }

  marks(symbol: string, start: number, end: number) {
    return this.bars(symbol, "1m", start, end, true);
  }

  private bars(symbol: string, interval: KlineInterval, start: number, end: number, mark: boolean) {
    const step = times[interval] * MINUTE;
    end = Math.min(end, Math.floor(Date.now() / MINUTE) * MINUTE - 1);
    if (end < start) return Promise.resolve();
    if ((end - start) / step > 100_000) bad("History request exceeds 100,000 bars");
    const kind = mark ? "mark_klines" : "klines:" + interval;
    return this.serial(kind, symbol, async () => {
      const db = this.dbs.timeseries;
      for (const [from, to] of this.missing(kind, symbol, start, end)) {
        let cursor = from;
        while (cursor <= to) {
          const limit = 1000;
          const bars = await this.mainnet<Kline[]>(mark ? "mark-klines" : "klines", {
            symbol, interval, start: cursor, end: to, limit,
          });
          const next = bars.length === limit ? bars[bars.length - 1].openTime + 1 : to + 1;
          if (!Number.isSafeInteger(next) || next <= cursor) throw new Error("Historical kline pagination stalled");
          db.transaction(() => {
            const rows = bars.filter(row => row.closed && row.symbol === symbol && row.openTime >= cursor && row.openTime <= to);
            if (mark) {
              const put = db.query("INSERT INTO mark_klines VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING");
              for (const b of rows) put.run(symbol, b.openTime, b.closeTime, b.o, b.h, b.l, b.c);
            } else {
              const put = db.query("INSERT INTO klines VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING");
              for (const b of rows) put.run(symbol, interval, b.openTime, b.closeTime, b.o, b.h, b.l, b.c, b.v, b.quoteVolume);
            }
            this.save(kind, symbol, cursor, next - 1);
          }).immediate();
          cursor = next;
        }
      }
    });
  }

  funding(symbol: string, start: number, end: number) {
    end = Math.min(end, Date.now());
    if (end < start) return Promise.resolve();
    return this.serial("funding", symbol, async () => {
      const db = this.dbs.timeseries;
      for (const [from, to] of this.missing("funding", symbol, start, end)) {
        let cursor = from;
        while (cursor <= to) {
          const limit = 1000;
          const rows = await this.mainnet<FundingRow[]>("funding-rates", { symbol, start: cursor, end: to, limit });
          const next = rows.length === limit ? rows[rows.length - 1].fundingTime + 1 : to + 1;
          if (!Number.isSafeInteger(next) || next <= cursor) throw new Error("Historical funding pagination stalled");
          db.transaction(() => {
            const put = db.query("INSERT INTO funding_rates VALUES (?,?,?,?) ON CONFLICT DO NOTHING");
            for (const row of rows) if (row.symbol === symbol && row.fundingTime >= cursor && row.fundingTime <= to) {
              put.run(symbol, row.fundingTime, Number(row.fundingRate), row.markPrice === undefined ? null : Number(row.markPrice));
            }
            this.save("funding", symbol, cursor, next - 1);
          }).immediate();
          cursor = next;
        }
      }
    });
  }

  async run(signal: AbortSignal) {
    const db = this.dbs.timeseries;
    let nextFundingPoll = 0;
    while (!signal.aborted) {
      try {
        let symbols = this.settings.symbols;
        try { symbols = config().symbols; } catch {}
        const gap = db.query<{ through_seq: number; start_ts: number; end_ts: number }, []>(
          "SELECT * FROM market_gaps ORDER BY through_seq LIMIT 1").get();
        if (gap) {
          const start = Math.max(0, Math.floor(gap.start_ts / MINUTE) * MINUTE);
          const stop = Math.floor(gap.end_ts / MINUTE) * MINUTE - 1;
          for (const symbol of symbols) {
            if (signal.aborted) return;
            for (let from = start; from <= stop; from += 30 * DAY) {
              const end = Math.min(stop, from + 30 * DAY - 1);
              await this.klines(symbol, "1m", from, end);
              await this.funding(symbol, from, end);
              if (signal.aborted) return;
            }
          }
          db.query("DELETE FROM market_gaps WHERE through_seq=?").run(gap.through_seq);
          continue;
        }
        if (Date.now() >= nextFundingPoll) {
          const now = Date.now();
          nextFundingPoll = now + 5 * MINUTE;
          for (const symbol of symbols) {
            if (signal.aborted) return;
            await this.funding(symbol, now - DAY, now);
          }
        }
      } catch (error) { if (!signal.aborted) console.warn("Mainnet gap recovery delayed", (error as Error).message); }
      await Bun.sleep(2000);
    }
  }
}
