import type { DepthFrame, DepthSnapshot, Levels } from "@quant/tools";
import { Decimal, message } from "../util";
import type { Wire } from "./source";
export class DepthRecorder {
  private generation = 0;
  private loading = false;
  private active = true;
  private buffered: Wire[] = [];
  private last?: bigint;
  private bridged = false;
  private retry?: Timer;
  private bids = new Map<string, string>();
  private asks = new Map<string, string>();
  constructor(
    private symbol: string,
    private fetch: () => Promise<DepthSnapshot>,
    private emit: (f: DepthFrame) => void,
    private gap: (reason: string) => void,
  ) {}
  reset() {
    this.generation++;
    this.last = undefined;
    this.bridged = false;
    this.loading = false;
    this.buffered = [];
    clearTimeout(this.retry);
    this.retry = undefined;
    this.bids.clear();
    this.asks.clear();
  }
  stop() {
    this.active = false;
    this.reset();
  }
  snapshot() {
    if (this.bridged) return this.frame();
  }
  private frame(): DepthFrame {
    return {
      kind: "snapshot",
      symbol: this.symbol,
      first: String(this.last),
      last: String(this.last),
      time: Date.now(),
      bids: [...this.bids],
      asks: [...this.asks],
    };
  }
  private retryLater() {
    this.retry = setTimeout(() => {
      this.retry = undefined;
      void this.load();
    }, 250);
  }
  event(e: Wire) {
    if (this.last !== undefined) {
      const last = this.last,
        first = BigInt(e.U),
        end = BigInt(e.u);
      if (end < last || (this.bridged && end === last)) return;
      if (this.bridged ? BigInt(e.pu) === last : first <= last) {
        if (!this.bridged) this.emit(this.frame());
        this.apply(e);
        this.bridged = true;
        this.last = end;
        return;
      }
      this.gap(`depth sequence break at ${last} -> ${e.U}/${e.pu}`);
      this.reset();
      // A lagging REST replica may return the same snapshot again.
      this.retryLater();
    }
    if (this.buffered.length >= 2048) {
      this.gap("depth buffer overflow");
      this.reset();
    }
    this.buffered.push(e);
    if (!this.loading && !this.retry) void this.load();
  }
  private async load() {
    const version = this.generation;
    this.loading = true;
    try {
      const snap = await this.fetch();
      if (!this.active || version !== this.generation) return;
      this.last = BigInt(snap.lastUpdateId);
      this.bridged = false;
      this.bids = new Map(snap.bids);
      this.asks = new Map(snap.asks);
      const buffered = this.buffered;
      this.buffered = [];
      this.loading = false;
      for (const e of buffered) this.event(e);
    } catch (err) {
      if (!this.active || version !== this.generation) return;
      this.gap(message(err));
      this.loading = false;
      this.retryLater();
    }
  }
  private apply(e: Wire) {
    const change = (book: Map<string, string>, levels: Levels) => {
      for (const [price, qty] of levels) {
        if (new Decimal(qty).isZero()) book.delete(price);
        else book.set(price, qty);
      }
    };
    change(this.bids, e.b);
    change(this.asks, e.a);
    this.emit({
      kind: "delta",
      symbol: this.symbol,
      first: String(e.U),
      last: String(e.u),
      previous: String(e.pu),
      bids: e.b,
      asks: e.a,
      time: e.E,
    });
  }
}
