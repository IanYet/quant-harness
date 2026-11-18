import { periods, type Bar, type Interval, type Range } from "@quant/tools";
import type { Store } from "../db";
import { requireThat } from "../util";
import type { Source } from "./source";

export class History {
  private active = new Set<Promise<Bar[]>>();
  private stopped = false;
  private downloads = new Map<string, Promise<void>>();
  constructor(
    private db: Store,
    private source: Source,
  ) {}
  save(bar: Bar) {
    this.db.run(
      "INSERT OR IGNORE INTO bars VALUES (?,?,?,?)",
      bar.symbol,
      bar.interval,
      bar.openTime,
      JSON.stringify(bar),
    );
  }
  latest(symbol: string, interval: Interval) {
    return this.db.one<{ time: number }>(
      "SELECT time FROM bars WHERE symbol=? AND interval=? ORDER BY time DESC LIMIT 1",
      symbol,
      interval,
    )?.time;
  }
  bars(symbol: string, interval: Interval, range: Range) {
    requireThat(!this.stopped, "History is stopping", 503);
    const task = this.read(symbol, interval, range);
    this.active.add(task);
    return task.finally(() => this.active.delete(task));
  }
  async stop() {
    this.stopped = true;
    await Promise.allSettled(this.active);
  }
  private async read(symbol: string, interval: Interval, range: Range) {
    const step = periods[interval];
    const start = Math.ceil(range.start / step) * step;
    const end = Math.min(Math.ceil(range.end / step) * step, Math.floor(this.source.now() / step) * step);
    requireThat((end - start) / step <= 50000, "At most 50000 bars per request");
    const args = [symbol, interval, start, end] as const;
    const times = this.db.all<{ time: number }>(
      "SELECT time FROM bars WHERE symbol=? AND interval=? AND time>=? AND time<? ORDER BY time",
      ...args,
    );
    let next = start;
    for (const { time } of [...times, { time: end }]) {
      while (next < time) {
        requireThat(!this.stopped, "History is stopping", 503);
        const stop = Math.min(time, next + 1000 * step);
        const key = `${symbol}/${interval}/${next}/${stop}`;
        let task = this.downloads.get(key);
        if (!task) {
          task = this.source
            .bars(symbol, interval, { start: next, end: stop })
            .then((bars) =>
              this.db.tx(() => {
                for (const bar of bars) this.save(bar);
              }),
            )
            .finally(() => this.downloads.delete(key));
          this.downloads.set(key, task);
        }
        await task;
        next = stop;
      }
      next = time + step;
    }
    return this.db
      .all<{
        body: string;
      }>("SELECT body FROM bars WHERE symbol=? AND interval=? AND time>=? AND time<? ORDER BY time", ...args)
      .map((r) => JSON.parse(r.body) as Bar);
  }
}
