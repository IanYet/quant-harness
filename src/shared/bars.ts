import type { Kline, KlineInterval } from "@quant/tools";

export const AGGREGATES = { "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440 } as const;
export const MINUTE = 60_000;

export class MinuteBars {
  private readonly minutes = new Map<string, Map<number, Kline>>();
  private readonly emitted = new Map<string, number>();

  add(bar: Kline) {
    if (!bar.closed || bar.interval !== "1m") return [];
    const bars = this.minutes.get(bar.symbol) ?? new Map<number, Kline>();
    this.minutes.set(bar.symbol, bars);
    bars.set(bar.openTime, bar);
    const results: Kline[] = [];
    for (const [interval, count] of Object.entries(AGGREGATES)) {
      const duration = count * MINUTE;
      const start = Math.floor(bar.openTime / duration) * duration;
      const last = bars.get(start + duration - MINUTE);
      const key = bar.symbol + ":" + interval;
      if (!last || start <= (this.emitted.get(key) ?? -1)) continue;
      let high = -Infinity, low = Infinity, volume = 0, quoteVolume = 0;
      for (let minute = start; minute < start + duration; minute += MINUTE) {
        const constituent = bars.get(minute);
        if (!constituent) break;
        high = Math.max(high, constituent.h);
        low = Math.min(low, constituent.l);
        volume += constituent.v;
        quoteVolume += constituent.quoteVolume;
        if (constituent === last) {
          this.emitted.set(key, start);
          results.push({ symbol: bar.symbol, interval: interval as KlineInterval, openTime: start, closeTime: last.closeTime,
            o: bars.get(start)!.o, c: last.c, h: high, l: low, v: volume, quoteVolume, closed: true });
        }
      }
    }
    if (bars.size > 3000) {
      const cutoff = bar.openTime - 2 * 1440 * MINUTE;
      for (const time of bars.keys()) if (time < cutoff) bars.delete(time);
    }
    return results;
  }

  remove(symbol: string) {
    this.minutes.delete(symbol);
    for (const interval of Object.keys(AGGREGATES)) this.emitted.delete(symbol + ":" + interval);
  }
}
