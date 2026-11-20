import { Backtests } from "./backtest";
import { HistoryBackfill } from "./history-backfill";
import { mkdir, symlink } from "node:fs/promises";
import { join, resolve } from "node:path";
import { serviceAuth } from "../shared/auth";
import { config, initLayout } from "../shared/config";
import { openStorage } from "./db";
import { ingest } from "./ingestor";
import { startStorage } from "./server";
import { StrategyRegistry } from "./strategy-registry";

const settings = config();
await initLayout(settings.dataDir);
const researchPackages = join(settings.dataDir, "node_modules", "@quant");
await mkdir(researchPackages, { recursive: true, mode: 0o750 });
try { await symlink(resolve(import.meta.dir, "../../packages/tools"), join(researchPackages, "tools"), "dir"); }
catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
const auth = await serviceAuth(settings.dataDir);
const storage = openStorage(settings.dataDir);
const registry = new StrategyRegistry(settings.dataDir, storage.ledger);
await registry.recover();
const history = new HistoryBackfill(settings, storage);
const backtests = new Backtests(settings, storage, history, registry);
const server = await startStorage(settings, storage, auth, registry, {
  klines: history.klines.bind(history), funding: history.funding.bind(history), backtest: backtests.run.bind(backtests),
});
const abort = new AbortController();
for (const signal of ["SIGTERM", "SIGINT"] as const) process.once(signal, () => abort.abort());
console.log("quant-storage listening on " + server.url);
await Promise.all([ingest(settings.dataDir, storage, auth, abort.signal), history.run(abort.signal)]);
await server.stop(true);
storage.close();
