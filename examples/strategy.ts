import { currentRun, market, trade } from "@quant/tools";
import { Decimal } from "decimal.js";
const symbol = "BTCUSDT";
const account = (await currentRun()).account;
if (account !== "paper") throw new Error("This example requires the paper account");
const [positions, orders] = await Promise.all([trade.positions(), trade.openOrders()]);
console.log("startup", JSON.stringify({ positions, orders }));
const closes = new Map<number, Decimal>();
let oldest = 0;
for await (const { source, bar } of market.watchBars(symbol, "1h", { warmup: 1440 })) {
  oldest = Math.max(oldest, bar.openTime - 1439 * 3600000);
  if (bar.openTime < oldest) continue;
  closes.set(bar.openTime, new Decimal(bar.close));
  for (const time of closes.keys()) if (time < oldest) closes.delete(time);
  if (source === "history" && closes.size === 1440) console.log("warmup", closes.size, bar.closeTime);
  if (source !== "live" || closes.size < 1440) continue;
  const run = await currentRun();
  if (run.mode === "observe") {
    console.log("observe", bar.openTime);
    continue;
  }
  if ((await trade.openOrders()).some((o) => o.symbol === symbol)) continue;
  const quote = await market.quote(symbol);
  if (quote.bookStale || !quote.book) continue;
  const mean = Decimal.sum(...closes.values()).div(closes.size);
  const tick = new Decimal((await market.spec(symbol)).tickSize);
  const price = mean.mul("0.90").div(tick).floor().mul(tick).toFixed();
  const order = await trade.place({
    key: "hour-" + bar.openTime,
    symbol,
    side: "BUY",
    type: "LIMIT",
    price,
    quantity: "0.01",
    quote,
  });
  console.log("limit", order.clientId, order.state);
}
