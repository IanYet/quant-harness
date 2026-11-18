import { expect, test } from "bun:test";
import { type BarEvent, periods } from "@quant/tools";
import { exchange } from "./exchange";
import { sandbox, until } from "./support";
const min = 60000,
  hour = 3600000;
function candle(t: number, i: "1m" | "1h", closed = true) {
  return {
    e: "kline",
    s: "BTCUSDT",
    E: t + periods[i],
    k: { i, t, T: t + periods[i] - 1, o: "60000", h: "60002", l: "59999", c: "60001", v: "1", x: closed },
  };
}
test("routed shared WS, sorted 60-day warmup, live handoff, cached holes, reconnect backfill and sampling", async () => {
  const ex = exchange(),
    box = await sandbox({ ...ex.cfg, healthMs: 60000 });
  try {
    const { ticker, db } = box.host,
      sdk = box.sdk;
    const t = Math.floor(Date.now() / hour) * hour;
    let now = t;
    ticker.source.now = () => now;
    await until(() => ex.watching("btcusdt@kline_1m"));
    expect(ex.subscriptions.find((s) => s.group === "public")!.keys).toEqual(["btcusdt@bookTicker"]);
    expect(ex.subscriptions.find((s) => s.group === "market")!.keys.sort()).toEqual([
      "btcusdt@forceOrder",
      "btcusdt@kline_1m",
      "btcusdt@markPrice@1s",
    ]);
    const start = t - 1440 * hour,
      range = { start, end: t };
    const watch = sdk.market.watchBars("BTCUSDT", "1h", { warmup: 1440 });
    ex.state.barDelay = 35;
    const first = watch.next();
    await until(() => ex.actions.some((a) => a.path.endsWith("klines")));
    // The live close arrives while REST warmup is still downloading.
    now += hour;
    ex.stream("btcusdt@kline_1h", candle(t, "1h"));
    const history: BarEvent[] = [(await first).value!];
    while (history.length < 1440) history.push((await watch.next()).value!);
    expect(history.every((r, i) => r.source === "history" && r.bar.openTime === start + i * hour)).toBe(true);
    expect((await watch.next()).value).toMatchObject({ source: "live", bar: { openTime: t } });
    ex.state.barDelay = 0;
    const requests = () => ex.actions.filter((a) => a.path.endsWith("klines"));
    const n = requests().length;
    expect((await sdk.market.bars("BTCUSDT", "1h", range)).length).toBe(1440);
    expect(requests().length).toBe(n);
    db.run("DELETE FROM bars WHERE symbol='BTCUSDT' AND interval='1h' AND time=?", start + 3 * hour);
    expect((await sdk.market.bars("BTCUSDT", "1h", range)).length).toBe(1440);
    expect(requests().at(-1)!.params).toMatchObject({
      startTime: String(start + 3 * hour),
      endTime: String(start + 4 * hour - 1),
    });
    await watch.return();
    await until(() => !ex.watching("btcusdt@kline_1h"));

    const minuteWatch = sdk.market.watchBars("BTCUSDT", "1m");
    const firstMinute = minuteWatch.next(),
      m = now;
    await until(() => box.host.hub.counts()["btcusdt@kline_1m"] === 1);
    ex.stream("btcusdt@kline_1m", candle(m, "1m", false));
    await until(() => ticker.currentBars.get("btcusdt@kline_1m")?.openTime === m);
    expect(ticker.history.latest("BTCUSDT", "1m")).toBeUndefined();
    now += min;
    ex.stream("btcusdt@kline_1m", candle(m, "1m"));
    ex.stream("btcusdt@kline_1m", candle(m, "1m"));
    expect((await firstMinute).value?.source).toBe("live");
    ex.drop("market");
    await until(() => ticker.health().connections.market === "disconnected");
    now += min;
    expect((await minuteWatch.next()).value).toMatchObject({ source: "backfill", bar: { openTime: m + min } });
    await until(() => ex.watching("btcusdt@forceOrder"));
    now += min;
    ex.stream("btcusdt@kline_1m", candle(m + 2 * min, "1m"));
    expect((await minuteWatch.next()).value?.bar.openTime).toBe(m + 2 * min);
    expect(db.one<{ n: number }>("SELECT COUNT(*) AS n FROM bars WHERE interval='1m'")!.n).toBe(3);
    await minuteWatch.return();
    const E = now;
    ex.stream("btcusdt@bookTicker", { e: "bookTicker", s: "BTCUSDT", E, b: "60000", a: "60001", B: "1", A: "2" });
    ex.stream("btcusdt@markPrice@1s", {
      e: "markPriceUpdate",
      s: "BTCUSDT",
      E: E - 25,
      p: "60000.5",
      i: "60001",
      r: "0.001",
      T: E + 3600000,
    });
    ex.stream("btcusdt@forceOrder", { e: "forceOrder", E, o: { s: "BTCUSDT", q: "1" } });
    await until(() => ticker.quote("BTCUSDT").mark);
    ticker.sample();
    ticker.sample();
    const snap = JSON.parse(db.one<{ body: string }>("SELECT body FROM snapshots WHERE minute=?", E)!.body);
    expect(snap.book.eventTime).toBe(E);
    expect(snap.mark.eventTime).toBe(E - 25);
    expect(snap.bookStale).toBe(false);
    expect(db.one<{ n: number }>("SELECT COUNT(*) AS n FROM liquidations")!.n).toBe(1);
    now += min;
    expect(ticker.quote("BTCUSDT").markStale).toBe(true);
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
}, 15000);

test("depth snapshot bridges buffered diff, duplicate and broken sequence, reference-counted optional feeds", async () => {
  const ex = exchange(),
    box = await sandbox(ex.cfg);
  try {
    const sdk = box.sdk,
      key = "btcusdt@depth@100ms";
    const a = sdk.market.watchDepth("BTCUSDT"),
      b = sdk.market.watchDepth("BTCUSDT"),
      one = a.next(),
      two = b.next();
    await until(() => box.host.hub.counts()[key] === 2 && ex.watching(key));
    expect(ex.subscriptions.filter((s) => s.keys.includes(key))).toHaveLength(1);
    const diff = (U: number, u: number, pu: number) => ({
      e: "depthUpdate",
      s: "BTCUSDT",
      E: Date.now(),
      U,
      u,
      pu,
      b: [["60000", "0"]],
      a: [["60001", "3"]],
    });
    ex.state.depthDelay = 25;
    ex.stream(key, diff(95, 99, 94));
    ex.stream(key, diff(99, 101, 98));
    expect((await one).value?.kind).toBe("snapshot");
    expect((await two).value?.last).toBe("100");
    expect((await a.next()).value?.last).toBe("101");
    await b.return();
    const c = sdk.market.watchDepth("BTCUSDT");
    const book = (await c.next()).value!;
    expect(book.bids).toEqual([]);
    expect(book.asks).toEqual([["60001", "3"]]);
    await c.return();
    ex.stream(key, diff(99, 101, 98));
    ex.stream(key, diff(102, 103, 101));
    expect((await a.next()).value?.last).toBe("103");
    ex.state.depthId = "110";
    ex.stream(key, diff(110, 111, 109));
    expect((await a.next()).value).toMatchObject({ kind: "snapshot", last: "110" });
    expect((await a.next()).value).toMatchObject({ kind: "delta", last: "111" });
    expect(box.host.db.one("SELECT id FROM gaps WHERE reason LIKE 'depth sequence break%'")).toBeTruthy();
    await a.return();
    await until(() => !ex.watching(key));
    const trades = sdk.market.watchTrades("BTCUSDT"),
      next = trades.next();
    await until(() => ex.watching("btcusdt@aggTrade"));
    ex.stream("btcusdt@aggTrade", {
      e: "aggTrade",
      s: "BTCUSDT",
      a: 123,
      T: 1234,
      p: "60000.1",
      q: "0.1",
      m: true,
    });
    expect((await next).value).toEqual({
      id: "123",
      price: "60000.1",
      quantity: "0.1",
      time: 1234,
      buyerMaker: true,
    });
    await trades.return();
    await until(() => !ex.watching("btcusdt@aggTrade"));
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
});
test("on-demand REST and cached daily archives preserve metric and aggregate-trade meaning", async () => {
  const ex = exchange(),
    box = await sandbox(ex.cfg);
  try {
    const market = box.sdk.market,
      d = 86400000,
      old = Math.floor(Date.now() / d) * d - 60 * d;
    const range = { start: old, end: old + d };
    expect((await market.openInterest("BTCUSDT", range))[0]).toEqual({
      time: old + 300000,
      value: "999.1",
      source: "archive",
    });
    expect((await market.longShortRatio("BTCUSDT", range))[0].value).toBe("1.25");
    expect(ex.state.archiveReads).toBe(1);
    expect((await market.trades("BTCUSDT", range))[0].id).toBe("90071992547409931");
    const recent = { start: Date.now() - 600000, end: Date.now() };
    expect((await market.openInterest("BTCUSDT", recent))[0].value).toBe("100");
    expect((await market.longShortRatio("BTCUSDT", recent))[0].value).toBe("1.1");
    expect((await market.funding("BTCUSDT", recent))[0].value).toBe("0.0001");
    expect((await market.trades("BTCUSDT", recent))[0].id).toBe("42");
    expect((await market.openInterest("BTCUSDT"))[0].value).toBe("4728");
    expect((await market.stats("BTCUSDT")).volume).toBe("10000");
    expect((await market.spec("BTCUSDT")).tickSize).toBe("0.1");
    ex.state.archiveMissing = true;
    await expect(market.openInterest("BTCUSDT", { start: old - d, end: old })).rejects.toThrow("404");
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
});

test("failed minute recovery survives restart and fills the recorded gap on retry", async () => {
  const ex = exchange(),
    box = await sandbox(ex.cfg);
  try {
    const t = Math.floor(Date.now() / min) * min;
    let now = t;
    box.host.ticker.source.now = () => now;
    const watch = box.sdk.market.watchBars("BTCUSDT", "1m"),
      first = watch.next();
    await until(() => ex.watching("btcusdt@kline_1m") && box.host.hub.counts()["btcusdt@kline_1m"] === 1);
    now += min;
    ex.stream("btcusdt@kline_1m", candle(t, "1m"));
    await first;
    ex.state.barError = true;
    now += 2 * min;
    ex.stream("btcusdt@kline_1m", candle(t + 2 * min, "1m"));
    expect((await watch.next()).value?.bar.openTime).toBe(t + 2 * min);
    expect(box.host.ticker.health().pendingBarRepairs).toBe(1);
    await watch.return();
    await box.restart();
    box.host.ticker.source.now = () => now;
    expect(box.host.ticker.health().pendingBarRepairs).toBe(1);
    ex.state.barError = false;
    box.host.db.run("UPDATE bar_repairs SET retryAt=0");
    box.host.ticker.retryGap();
    await until(() => box.host.ticker.health().pendingBarRepairs === 0);
    expect(box.host.db.one("SELECT time FROM bars WHERE interval='1m' AND time=?", t + min)).toBeTruthy();
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
});

test("a delayed close omitted by the REST warmup is delivered once as backfill", async () => {
  const ex = exchange(),
    box = await sandbox(ex.cfg);
  try {
    const t = Math.floor(Date.now() / hour) * hour;
    box.host.ticker.source.now = () => t;
    ex.state.omitBar = t - hour;
    const watch = box.sdk.market.watchBars("BTCUSDT", "1h", { warmup: 2 });
    expect((await watch.next()).value).toMatchObject({ source: "history", bar: { openTime: t - 2 * hour } });
    ex.stream("btcusdt@kline_1h", candle(t - hour, "1h"));
    ex.stream("btcusdt@kline_1h", candle(t - hour, "1h"));
    expect((await watch.next()).value).toMatchObject({ source: "backfill", bar: { openTime: t - hour } });
    box.host.ticker.source.now = () => t + hour;
    ex.stream("btcusdt@kline_1h", candle(t, "1h"));
    expect((await watch.next()).value).toMatchObject({ source: "live", bar: { openTime: t } });
    await watch.return();
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
});
