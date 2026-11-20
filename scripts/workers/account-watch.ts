import { accounts, agents, harness, market, notify, state } from "@quant/tools";

const checkEveryMs = 30_000;
const minLiquidationDistance = 0.03;
const maxMarginUse = 0.85;

type Status = { lastScan: number; observedSymbols: string[]; accountCount: number };
const status: Status = (await state.load<Status>())?.data ?? { lastScan: 0, observedSymbols: [], accountCount: 0 };
const summaries = await accounts.list();
console.log("Watching account risk for " + summaries.map(account => account.accountId).join(", "));
harness.onHandover(async () => { await state.save(status); });

for await (const event of market.watch(["mark_price"])) {
  const now = harness.now();
  if (event.topic !== "market.mark_price") continue;
  if (!status.observedSymbols.includes(event.payload.symbol)) status.observedSymbols.push(event.payload.symbol);
  if (now - status.lastScan < checkEveryMs) continue;
  status.lastScan = now;
  const current = await accounts.list();
  status.accountCount = current.length;
  for (const summary of current) {
    const account = await accounts.state(summary.accountId);
    const equity = account.walletBalance + account.unrealizedPnl;
    const used = equity > 0 ? (equity - account.availableBalance) / equity : 0;
    const concerns: string[] = [];
    if (used >= maxMarginUse) concerns.push("Margin use " + (used * 100).toFixed(1) + "%");
    for (const pair of Object.values(account.positions)) {
      for (const leg of [pair.long, pair.short]) {
        if (leg.qty > 0 && leg.markPrice > 0 && leg.liquidationPrice > 0) {
          const gap = Math.abs(leg.markPrice - leg.liquidationPrice) / leg.markPrice;
          if (gap < minLiquidationDistance) concerns.push(leg.symbol + " " + leg.side + " liquidation gap " + (gap * 100).toFixed(2) + "%");
        }
      }
    }
    if (!concerns.length) continue;
    const title = account.accountId + " margin risk";
    const body = concerns.join("\n");
    try { await notify.send({ level: "critical", title, body, dedupKey: title, cooldownSec: 300 }); }
    catch (error) { console.error("Could not queue risk alert", error); }
    try {
      await agents.startTask({ title, prompt: "Investigate the margin risk for account " + account.accountId + ". Use read-only account and mainnet market data, and report the cause and evidence.",
        visibility: "visible", dedupKey: title, cooldownSec: 600, context: { concerns } });
    } catch (error) { console.error("Could not queue risk investigation", error); }
  }
  await state.save(status);
}
