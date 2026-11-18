import { expect, test } from "bun:test";
import { type Place } from "@quant/tools";
import { SimSource } from "../src/market/sim";
import { sandbox } from "./support";

test("decimal paper execution, partial fills, idempotency, owner fencing, cancellation and close", async () => {
  const box = await sandbox();
  try {
    const { client: c } = await box.actor("swing"),
      outsider = await box.actor("other");
    const quote = await c.market.quote("BTCUSDT");
    const p: Place = { key: "buy", symbol: "BTCUSDT", side: "BUY", type: "MARKET", quantity: "2.003", quote };
    const [a, b] = await Promise.all([c.trade.place(p), c.trade.place(p)]);
    expect(a.id).toEqual(b.id);
    await expect(c.trade.place({ ...p, quantity: "2.001" })).rejects.toThrow("409");
    expect((await c.trade.order(a.clientId!)).status).toBe("PARTIALLY_FILLED");
    expect((await c.trade.positions())[0].quantity).toBe("2");
    (box.host.ticker.source as SimSource).step();
    expect((await c.trade.order(a.clientId!)).status).toBe("FILLED");
    expect((await c.trade.fills(a.clientId!)).map((x) => x.quantity)).toEqual(["2", "0.003"]);
    await expect(outsider.client.trade.setLeverage({ symbol: p.symbol, key: "lev", leverage: 3 })).rejects.toThrow(
      "409",
    );
    await expect(c.trade.place({ ...p, key: "bad", quantity: "0.0001" })).rejects.toThrow("step");
    await expect(c.trade.place({ ...p, key: "tick", type: "LIMIT", price: "59000.001" })).rejects.toThrow("tick");
    const rest = await c.trade.place({ ...p, key: "rest", type: "LIMIT", price: "50000", quantity: "0.01" });
    expect((await c.trade.openOrders())[0].clientId).toBe(rest.clientId!);
    expect((await c.trade.cancel({ key: "cancel", symbol: p.symbol, clientId: rest.clientId! })).state).toBe(
      "accepted",
    );
    expect((await c.trade.order(rest.clientId!)).status).toBe("CANCELED");
    expect((await c.trade.setLeverage({ key: "lev", symbol: p.symbol, leverage: 3 })).state).toBe("accepted");
    const close = await c.trade.close({ key: "flatten", symbol: p.symbol });
    (box.host.ticker.source as SimSource).step();
    expect((await c.trade.positions()).length).toBe(0);
    expect((await c.trade.order(close.clientId!)).reduceOnly).toBe(true);
    expect((await c.trade.instruction(close.id)).state).toBe("accepted");
    expect(
      JSON.parse(
        box.host.db.one<{ decision: string }>("SELECT decision FROM instructions WHERE id=?", a.id)!.decision,
      ),
    ).toEqual(quote);
    expect((await outsider.client.trade.setLeverage({ symbol: p.symbol, key: "lev", leverage: 2 })).state).toBe(
      "accepted",
    );
    await box.restart();
    expect((await box.sdk.trade.order(a.clientId!)).executed).toBe("2.003");
    expect((await box.sdk.trade.fills(a.clientId!)).length).toBe(2);
  } finally {
    await box.close();
  }
});
