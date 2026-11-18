import {
  periods,
  type Bar,
  type BarEvent,
  type Book,
  type DepthFrame,
  type Interval,
  type Mark,
  type Quote,
  type Spec,
} from "@quant/tools";
import type { Store } from "../db";
import { Hub, type Subscription } from "../streams";
import { message, recordError, requireThat } from "../util";
import { aggregate } from "./binance";
import { DepthRecorder } from "./depth";
import { History } from "./history";
import { groupOf, type Group, type Source, type Wire } from "./source";
const streamKey = (s: string, suffix: string) => s.toLowerCase() + "@" + suffix;
const base = ["bookTicker", "markPrice@1s", "kline_1m", "forceOrder"];

export class Ticker {
  readonly history: History;
  readonly currentBars = new Map<string, Bar>();
  readonly specs = new Map<string, Spec>();
  private books = new Map<string, Book>();
  private bookIds = new Map<string, bigint>();
  private invalidBooks = new Set<string>();
  private invalidMarks = new Set<string>();
  private marks = new Map<string, Mark>();
  private exposure: string[] = [];
  private refs = new Map<string, number>();
  private keys = new Set<string>();
  private depths = new Map<string, DepthRecorder>();
  private watermarks = new Map<string, number>();
  private jobs = new Map<string, Promise<void>>();
  private reconnectCutoff = new Map<string, number>();
  private lastMinute = -1;
  private stopped = false;
  private repairJob?: Promise<void>;
  lastEvent: Partial<Record<Group, number>> = {};
  onBook?: (symbol: string, book: Book) => void;
  constructor(
    readonly source: Source,
    private db: Store,
    private hub: Hub,
    private fixed: string[],
  ) {
    this.history = new History(db, source);
  }
  async start() {
    await this.refreshSpecs();
    this.sync();
    this.source.start({
      message: (k, e) => this.event(k, e),
      sample: () => this.sample(),
      down: (g, r) => this.down(g, r),
      up: (g) => this.up(g),
    });
  }
  async refreshSpecs() {
    const specs = await this.source.specs();
    this.specs.clear();
    for (const spec of specs) this.specs.set(spec.symbol, spec);
  }
  spec(s: string) {
    const spec = this.specs.get(s);
    requireThat(spec, "Unknown perpetual contract " + s, 404);
    return spec;
  }
  track(symbols: string[]) {
    const next = [...new Set(symbols)].sort();
    if (next.join() === this.exposure.join()) return;
    this.exposure = next;
    this.sync();
  }
  private sync() {
    const keys = new Set(this.refs.keys());
    for (const s of [...this.fixed, ...this.exposure]) for (const suffix of base) keys.add(streamKey(s, suffix));
    for (const key of keys) {
      if (this.keys.has(key) || !key.includes("@kline_")) continue;
      const latest = this.db.one<{ time: number }>("SELECT time FROM stream_closes WHERE stream=?", key)?.time;
      if (latest !== undefined) this.watermarks.set(key, latest);
    }
    this.keys = keys;
    this.source.streams(keys);
  }
  private acquire(key: string) {
    this.refs.set(key, (this.refs.get(key) ?? 0) + 1);
    this.sync();
    return () => {
      const n = this.refs.get(key)! - 1;
      if (n) this.refs.set(key, n);
      else {
        this.refs.delete(key);
        this.depths.get(key)?.stop();
        this.depths.delete(key);
      }
      this.sync();
    };
  }
  quote(symbol: string): Quote {
    const book = this.books.get(symbol) ?? null,
      mark = this.marks.get(symbol) ?? null;
    const status = this.source.status();
    const stale = (r: Book | Mark | null, group: Group) =>
      !r ||
      status[group] !== "connected" ||
      this.source.now() - r.eventTime > 15000 ||
      Date.now() - r.receivedAt > 15000;
    return {
      symbol,
      book,
      mark,
      bookStale: this.invalidBooks.has(symbol) || stale(book, "public"),
      markStale: this.invalidMarks.has(symbol) || stale(mark, "market"),
    };
  }
  async latest(symbol: string) {
    this.spec(symbol);
    const quote = this.quote(symbol);
    if ((!quote.bookStale && !quote.markStale) || this.keys.has(streamKey(symbol, "bookTicker"))) return quote;
    const drops = ["bookTicker", "markPrice@1s"].map((k) => this.acquire(streamKey(symbol, k)));
    try {
      const deadline = Date.now() + 5000;
      while (
        !this.stopped &&
        Date.now() < deadline &&
        (this.quote(symbol).bookStale || this.quote(symbol).markStale)
      )
        await Bun.sleep(20);
      return this.quote(symbol);
    } finally {
      for (const drop of drops) drop();
    }
  }
  sample() {
    const minute = Math.floor(this.source.now() / 60000) * 60000;
    if (minute === this.lastMinute) return;
    this.lastMinute = minute;
    this.db.tx(() => {
      for (const s of new Set([
        ...this.fixed,
        ...this.exposure,
        ...[...this.keys].filter((k) => k.endsWith("@bookTicker")).map((k) => k.split("@")[0].toUpperCase()),
      ])) {
        this.db.run("INSERT OR IGNORE INTO snapshots VALUES (?,?,?)", s, minute, JSON.stringify(this.quote(s)));
      }
    });
  }
  private down(group: Group, reason: string) {
    this.db.gap(group, reason);
    delete this.lastEvent[group];
    if (group === "public") {
      for (const d of this.depths.values()) d.reset();
      this.invalidBooks = new Set(this.books.keys());
      this.bookIds.clear();
    }
    if (group === "market") this.invalidMarks = new Set(this.marks.keys());
  }
  private up(group: Group) {
    if (group !== "market") return;
    for (const key of this.keys) {
      if (!key.includes("@kline_")) continue;
      const [s, interval] = key.split("@kline_");
      const i = interval as Interval,
        cutoff = Math.floor(this.source.now() / periods[i]) * periods[i];
      this.reconnectCutoff.set(key, cutoff);
      this.enqueue(key, () => this.recover(key, s.toUpperCase(), i, cutoff));
    }
  }
  private enqueue(key: string, fn: () => Promise<void>) {
    const task = (this.jobs.get(key) ?? Promise.resolve())
      .then(() => (this.stopped ? undefined : fn()))
      .catch((err) => {
        if (!this.stopped) this.db.gap(key, message(err));
      });
    this.jobs.set(key, task);
    void task.then(() => {
      if (this.jobs.get(key) === task) this.jobs.delete(key);
    });
  }
  private async recover(key: string, s: string, i: Interval, until: number) {
    const step = periods[i],
      watermark = this.watermarks.get(key);
    if (watermark === undefined || watermark + step >= until) return;
    this.db.gap(key, `missing closes [${watermark + step},${until})`);
    for (let start = watermark + step; start < until && !this.stopped; start += 1000 * step) {
      await this.fillGap(key, s, i, start, Math.min(until, start + 1000 * step));
    }
  }
  private async fillGap(key: string, s: string, i: Interval, start: number, end: number) {
    const step = periods[i],
      retryAt = Date.now() + 60000;
    const defer = (from: number, to: number) =>
      this.db.run("INSERT OR REPLACE INTO bar_repairs VALUES (?,?,?,?,?)", s, i, from, to, retryAt);
    try {
      const bars = await this.history.bars(s, i, { start, end });
      this.db.tx(() => {
        this.db.run(
          "DELETE FROM bar_repairs WHERE symbol=? AND interval=? AND start=? AND end=?",
          s,
          i,
          start,
          end,
        );
        let next = start;
        for (const bar of bars) {
          if (next < bar.openTime) defer(next, bar.openTime);
          next = bar.openTime + step;
        }
        if (next < end) defer(next, end);
      });
      for (const bar of bars) {
        this.hub.emit(key, { source: "backfill", bar });
        if (bar.openTime > (this.watermarks.get(key) ?? -1)) this.watermarks.set(key, bar.openTime);
      }
    } catch (err) {
      defer(start, end);
      this.db.gap(key, message(err));
    }
  }
  retryGap() {
    if (this.repairJob || this.stopped) return;
    const gap = this.db.one<{ symbol: string; interval: Interval; start: number; end: number }>(
      "SELECT * FROM bar_repairs WHERE retryAt<=? ORDER BY retryAt LIMIT 1",
      Date.now(),
    );
    if (gap) {
      const key = streamKey(gap.symbol, "kline_" + gap.interval);
      this.repairJob = this.fillGap(key, gap.symbol, gap.interval, gap.start, gap.end).finally(() => {
        this.repairJob = undefined;
      });
    }
  }
  event(key: string, e: Wire) {
    if (this.stopped) return;
    const receivedAt = Date.now(),
      s = e.s ?? e.o?.s;
    this.lastEvent[groupOf(key)] = receivedAt;
    switch (e.e) {
      case "bookTicker": {
        if (e.u !== undefined) {
          const seq = BigInt(e.u);
          if (seq <= (this.bookIds.get(s) ?? -1n)) return;
          this.bookIds.set(s, seq);
        }
        this.invalidBooks.delete(s);
        const book = { bid: e.b, bidQty: e.B, ask: e.a, askQty: e.A, eventTime: e.E, receivedAt };
        this.books.set(s, book);
        this.onBook?.(s, book);
        this.hub.emit(streamKey(s, "quotes"), this.quote(s));
        break;
      }
      case "markPriceUpdate":
        if (e.E < (this.marks.get(s)?.eventTime ?? 0)) return;
        this.invalidMarks.delete(s);
        this.marks.set(s, {
          price: e.p,
          index: e.i,
          fundingRate: e.r,
          nextFundingTime: e.T,
          eventTime: e.E,
          receivedAt,
        });
        this.hub.emit(streamKey(s, "quotes"), this.quote(s));
        break;
      case "forceOrder":
        this.db.run("INSERT INTO liquidations(symbol,time,body) VALUES (?,?,?)", s, e.E, JSON.stringify(e));
        this.hub.emit(key, { kind: "publicLiquidationSnapshot", event: e });
        break;
      case "aggTrade":
        this.hub.emit(key, aggregate(e));
        break;
      case "depthUpdate":
        this.depths.get(key)?.event(e);
        break;
      case "kline": {
        const k = e.k,
          bar: Bar = {
            symbol: s,
            interval: k.i,
            openTime: k.t,
            closeTime: k.T,
            open: k.o,
            high: k.h,
            low: k.l,
            close: k.c,
            volume: k.v,
          };
        this.currentBars.set(key, bar);
        if (k.x !== true) break;
        this.enqueue(key, async () => {
          if (bar.openTime <= (this.watermarks.get(key) ?? -1)) return;
          try {
            await this.recover(key, s, k.i, bar.openTime);
          } catch (err) {
            this.db.gap(key, message(err));
          }
          this.db.tx(() => {
            this.history.save(bar);
            this.db.run("INSERT OR REPLACE INTO stream_closes VALUES (?,?)", key, bar.openTime);
          });
          this.watermarks.set(key, bar.openTime);
          this.hub.emit(key, {
            source: bar.openTime < (this.reconnectCutoff.get(key) ?? 0) ? "backfill" : "live",
            bar,
          });
        });
      }
    }
  }
  bars(s: string, interval: Interval, warmup: number, push: (e: BarEvent) => void): Subscription<BarEvent> {
    const key = streamKey(s, "kline_" + interval),
      step = periods[interval],
      source = this.source,
      history = this.history;
    let cutoff = Math.floor(source.now() / step) * step;
    const missing = new Set<number>();
    const off = this.hub.on(key, push),
      drop = this.acquire(key);
    async function* initial() {
      await source.ready(key);
      // With a warmup, close the historical window only after the upstream subscription is active.
      if (warmup) {
        cutoff = Math.floor(source.now() / step) * step;
        const start = cutoff - warmup * step;
        let next = start;
        for (const bar of await history.bars(s, interval, { start, end: cutoff })) {
          for (; next < bar.openTime; next += step) missing.add(next);
          next = bar.openTime + step;
          yield { source: "history" as const, bar };
        }
        for (; next < cutoff; next += step) missing.add(next);
      }
    }
    return {
      initial: initial(),
      release() {
        off();
        drop();
      },
      select(e) {
        if (e.bar.openTime >= cutoff) return e;
        if (missing.delete(e.bar.openTime)) return { source: "backfill", bar: e.bar };
      },
    };
  }
  watch(kind: string, s: string, push: (v: any) => void): Subscription<any> {
    const suffix = { quotes: "quotes", depth: "depth@100ms", trades: "aggTrade", liquidations: "forceOrder" }[kind];
    requireThat(suffix, "Unknown stream", 404);
    const key = streamKey(s, suffix),
      source = this.source;
    if (kind === "depth" && !this.depths.has(key)) {
      this.depths.set(
        key,
        new DepthRecorder(
          s,
          () => source.depth(s),
          (frame) => {
            this.db.run(
              "INSERT INTO depth_frames(symbol,time,body) VALUES (?,?,?)",
              s,
              frame.time,
              JSON.stringify(frame),
            );
            this.hub.emit(key, frame);
          },
          (r) => this.db.gap(key, r),
        ),
      );
    }
    const snapshot: DepthFrame | undefined = this.depths.get(key)?.snapshot();
    const off = this.hub.on(key, push);
    const keys = kind === "quotes" ? ["bookTicker", "markPrice@1s"].map((x) => streamKey(s, x)) : [key];
    const drops = keys.map((k) => this.acquire(k));
    async function* initial() {
      await Promise.all(keys.map((k) => source.ready(k)));
      if (snapshot) yield snapshot;
    }
    return {
      initial: initial(),
      release() {
        off();
        for (const drop of drops) drop();
      },
    };
  }
  health() {
    return {
      pendingBarRepairs: this.db.one<{ n: number }>("SELECT COUNT(*) AS n FROM bar_repairs")!.n,
      connections: this.source.status(),
      lastEvent: this.lastEvent,
      lastPong: this.source.heartbeats?.(),
      streams: [...this.keys].sort(),
      quotes: Object.fromEntries([...new Set([...this.fixed, ...this.exposure])].map((s) => [s, this.quote(s)])),
    };
  }
  async stop() {
    this.stopped = true;
    this.source.stop();
    for (const d of this.depths.values()) d.stop();
    await this.history.stop();
    await this.repairJob;
    await Promise.all(this.jobs.values()).catch(recordError);
  }
}
