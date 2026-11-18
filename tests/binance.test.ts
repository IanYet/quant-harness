import { expect, test } from "bun:test";
import type { Place } from "@quant/tools";
import { exchange } from "./exchange";
import { sandbox, until } from "./support";
const symbol = "BTCUSDT";
test("signed place accepted before timeout, duplicate key, restart lookup by client ID and unique fills", async () => {
  const ex = exchange(),
    box = await sandbox({ ...ex.cfg, binanceTrading: true, timeoutMs: 200, reconcileMs: 60000 });
  const postOrders = () => ex.actions.filter((a) => a.path === "/fapi/v1/order" && a.method === "POST");
  async function tick() {
    await until(() => ex.watching("btcusdt@bookTicker"));
    ex.stream("btcusdt@bookTicker", {
      e: "bookTicker",
      s: symbol,
      E: Date.now(),
      b: "60000.0",
      a: "60000.1",
      B: "1",
      A: "1",
    });
    await until(() => box.host.ticker.quote(symbol).book);
  }
  try {
    await tick();
    const { client: c } = await box.actor("swing", "binance");
    const p: Place = {
      key: "first-entry",
      symbol,
      quantity: "0.03",
      price: "55000.0",
      type: "LIMIT",
      side: "BUY",
      quote: await c.market.quote(symbol),
    };
    ex.state.delayPlace = 450;
    ex.state.holdOrders = true;
    const a = await c.trade.place(p);
    expect(a.state).toBe("unknown");
    expect((await c.trade.order(a.clientId!)).status).toBe("UNKNOWN");
    expect((await c.trade.place(p)).id).toBe(a.id);
    expect(postOrders()).toHaveLength(1);
    await box.host.trade.live!.reconcile();
    expect(
      ex.actions.some(
        (r) => r.method === "GET" && r.path.endsWith("/order") && r.params.origClientOrderId === a.clientId,
      ),
    ).toBe(true);
    expect((await c.trade.instruction(a.id)).state).toBe("unknown");
    await until(
      async () => ((await (await fetch(box.host.url + "/health")).json()) as any).trade.unresolved.length === 1,
    );
    const remote = ex.orders.get(a.clientId!)!;
    remote.executedQty = "0.01";
    remote.status = "PARTIALLY_FILLED";
    remote.updateTime = Date.now();
    ex.fills.push({
      id: "90071992547409930",
      orderId: remote.orderId,
      price: "54999.9",
      qty: "0.01",
      time: remote.updateTime,
    });
    ex.state.holdOrders = false;
    ex.state.delayPlace = 0;
    await box.restart();
    await tick();
    const next = await box.actor("swing", "binance");
    const client = next.client;
    expect((await client.trade.place(p)).id).toBe(a.id);
    expect(postOrders()).toHaveLength(1);
    expect((await client.trade.order(a.clientId!)).status).toBe("PARTIALLY_FILLED");
    expect((await client.trade.fills(a.clientId!))[0].tradeId).toBe("90071992547409930");
    const T = Date.now() + 10;
    const event = {
      e: "ORDER_TRADE_UPDATE",
      E: T,
      o: {
        s: symbol,
        c: a.clientId,
        i: remote.orderId,
        S: "BUY",
        ps: "BOTH",
        o: "LIMIT",
        q: "0.03",
        p: "55000.0",
        X: "FILLED",
        x: "TRADE",
        l: "0.02",
        L: "55000.0",
        z: "0.03",
        R: false,
        T,
        t: "90071992547409931",
      },
    };
    await until(() => box.host.trade.live!.connection === "connected");
    ex.private(event);
    ex.private(event);
    ex.private({ ...event, o: { ...event.o, x: "NEW", X: "NEW", z: "0", T: T - 10 } });
    await until(async () => (await client.trade.order(a.clientId!)).status === "FILLED");
    expect((await client.trade.fills(a.clientId!)).map((f) => f.quantity)).toEqual(["0.01", "0.02"]);
    const confirmed = await client.trade.instruction(a.id);
    expect(confirmed.state).toBe("accepted");
    expect(confirmed.runId).toBe(a.runId);
    ex.state.reject = true;
    const rejected = await client.trade.place({ ...p, key: "rejected" });
    expect(rejected.state).toBe("rejected");
    expect((await client.trade.order(rejected.clientId!)).status).toBe("REJECTED");
    ex.state.reject = false;
    const offline = await client.trade.place({ ...p, key: "filled-while-offline" });
    const completed = ex.orders.get(offline.clientId!)!;
    Object.assign(completed, { status: "FILLED", executedQty: p.quantity, updateTime: Date.now() });
    ex.fills.push({
      orderId: completed.orderId,
      id: "90071992547409932",
      price: p.price,
      qty: p.quantity,
      time: completed.updateTime,
    });
    const since = ex.actions.length;
    await box.restart();
    expect((await box.sdk.trade.order(offline.clientId!, "binance")).status).toBe("FILLED");
    expect((await box.sdk.trade.fills(offline.clientId!, "binance"))[0].quantity).toBe(p.quantity);
    expect(
      ex.actions
        .slice(since)
        .some(
          (r) =>
            r.method === "GET" && r.path === "/fapi/v1/order" && r.params.origClientOrderId === offline.clientId,
        ),
    ).toBe(true);
    await until(() => box.host.trade.live?.lastPong);
    expect(ex.state.invalidSignatures).toBe(0);
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
}, 15000);
test("Binance cancellation, actual one-way / hedge close, leverage, and private reconnect account reconciliation", async () => {
  const ex = exchange(),
    box = await sandbox({ ...ex.cfg, binanceTrading: true });
  try {
    await until(() => ex.watching("btcusdt@bookTicker"));
    const quoteTick = (s: string) =>
      ex.stream(s.toLowerCase() + "@bookTicker", {
        e: "bookTicker",
        s,
        E: Date.now(),
        b: "60000",
        a: "60000.1",
        B: "1",
        A: "1",
      });
    quoteTick(symbol);
    await until(() => box.host.ticker.quote(symbol).book);
    const { client: c } = await box.actor("swing", "binance");
    const p: Place = {
      key: "rest",
      symbol,
      side: "BUY",
      type: "LIMIT",
      quantity: "0.01",
      price: "55555",
      quote: await c.market.quote(symbol),
    };
    const a = await c.trade.place(p);
    expect((await c.trade.openOrders())[0].clientId).toBe(a.clientId!);
    expect((await c.trade.cancel({ symbol, key: "cancel", clientId: a.clientId! })).state).toBe("accepted");
    expect((await c.trade.order(a.clientId!)).status).toBe("CANCELED");
    await c.trade.setLeverage({ key: "lev", symbol, leverage: 7 });
    expect(ex.state.leverage).toBe(7);
    ex.positions.push({ symbol, positionSide: "BOTH", positionAmt: "-0.037" });
    const close = await c.trade.close({ key: "flatten", symbol });
    expect(ex.orders.get(close.clientId!)).toMatchObject({ side: "BUY", origQty: "0.037", reduceOnly: true });
    ex.state.hedge = true;
    ex.positions.splice(0, 1, { symbol, positionSide: "LONG", positionAmt: "0.023" });
    await expect(c.trade.close({ key: "need-side", symbol })).rejects.toThrow("LONG or SHORT");
    const hedge = await c.trade.close({ key: "flatten-long", symbol, positionSide: "LONG" });
    const request = ex.actions.find((a) => a.params.newClientOrderId === hedge.clientId)!;
    expect(request.params).toMatchObject({ side: "SELL", quantity: "0.023", positionSide: "LONG", type: "MARKET" });
    expect(request.params.reduceOnly).toBeUndefined();
    ex.private({ e: "ACCOUNT_UPDATE", T: Date.now(), a: { P: [{ s: "ETHUSDT", pa: "1", ps: "LONG" }] } });
    await until(() => ex.watching("ethusdt@kline_1m"));
    ex.positions.push({ symbol: "ETHUSDT", positionSide: "LONG", positionAmt: "2.03" });
    ex.drop("private");
    await until(() => box.host.trade.live!.connection === "disconnected");
    await until(async () =>
      (await c.trade.positions()).some((p) => p.symbol === "ETHUSDT" && p.quantity === "2.03"),
    );
    expect(
      ex.actions.filter((r) => r.path === "/fapi/v1/listenKey" && r.method === "POST").length,
    ).toBeGreaterThanOrEqual(2);
    expect(ex.state.invalidSignatures).toBe(0);
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
});

test("a close waiting on the exchange loses its execution right at promotion", async () => {
  const ex = exchange(),
    box = await sandbox({ ...ex.cfg, binanceTrading: true, reconcileMs: 60000 });
  try {
    await until(() => ex.watching("btcusdt@bookTicker"));
    ex.stream("btcusdt@bookTicker", {
      e: "bookTicker",
      s: symbol,
      E: Date.now(),
      b: "60000",
      a: "60000.1",
      B: "1",
      A: "1",
    });
    const old = await box.actor("swing", "binance"),
      candidate = await box.actor("swing", "binance", true);
    ex.positions.push({ symbol, positionSide: "BOTH", positionAmt: "1" });
    ex.state.positionDelay = 300;
    const count = () => ex.actions.filter((a) => a.path.endsWith("positionRisk")).length;
    const before = count();
    const result = old.client.trade.close({ key: "late-close", symbol }).then(
      () => "",
      (err) => String(err),
    );
    await until(() => count() > before);
    expect((await box.host.runs.replace("swing", candidate.file)).id).toBe(candidate.run.id);
    expect(await result).toContain("403");
    expect(ex.orders.size).toBe(0);
    expect(box.host.db.one("SELECT id FROM instructions WHERE key='late-close'")).toBeNull();
  } finally {
    await box.close();
    await ex.server.stop(true);
  }
});
