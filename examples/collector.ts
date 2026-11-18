import { market, data } from "@quant/tools";
const symbol = "BTCUSDT";
const indicator =
  process.env.SCRIPT_INDICATOR_URL ??
  new URL(
    "/market/oi?symbol=BTCUSDT",
    process.env.HARNESS_URL ?? `http://127.0.0.1:${process.env.HARNESS_PORT ?? 8787}`,
  ).href;
for await (const { source, bar } of market.watchBars(symbol, "1h")) {
  if (source !== "live") continue;
  try {
    const response = await fetch(indicator, { signal: AbortSignal.timeout(5000) });
    if (!response.ok) throw new Error("Indicator HTTP " + response.status);
    const row = await data.append("btc-hourly", {
      openTime: bar.openTime,
      close: bar.close,
      indicator: await response.json(),
    });
    console.log("sample", row.cursor, bar.openTime);
  } catch (err) {
    console.error("Indicator sample skipped", err);
  }
}
