import { Database } from "bun:sqlite";
import { join } from "node:path";
import { localDataDirectory } from "@quant/tools/paths";

const symbol = process.argv[2] ?? "BTCUSDT";
const days = Number(process.argv[3] ?? 7);
if (!/^[A-Z0-9]{2,32}$/.test(symbol) || !Number.isInteger(days) || days < 1 || days > 3650) {
  throw new Error("Usage: bun run funding-summary.ts SYMBOL DAYS");
}
const root = localDataDirectory();
const db = new Database(join(root, "db", "timeseries.db"), { readonly: true });
try {
  db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=5000");
  const from = Date.now() - days * 86_400_000;
  const result = db.query("SELECT symbol,COUNT(*) AS settlements,AVG(funding_rate) AS meanRate,SUM(funding_rate) AS cumulativeRate," +
    "AVG(funding_rate>0) AS positiveShare,MIN(funding_rate) AS minRate,MAX(funding_rate) AS maxRate " +
    "FROM funding_rates WHERE symbol=? AND funding_time>=? GROUP BY symbol").get(symbol, from);
  console.log(JSON.stringify({ symbol, days, statistics: result }, null, 2));
} finally { db.close(); }
