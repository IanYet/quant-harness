import { join } from "node:path";
import coreSchema from "./core.sql" with { type: "text" };
import { serviceAuth } from "../shared/auth";
import { config, initLayout } from "../shared/config";
import { openDb } from "../shared/db";
import { canonical } from "../shared/fs";
import { bad } from "../shared/http";
import { startAdmin, Control } from "./admin-server";
import { AccountPool } from "./binance/pool";
import { PublicMarket } from "./binance/public-ws";
import { WeightBucket } from "./binance/signer";
import { Checkpoints } from "./checkpoint";
import { EventBus } from "./event-bus";
import { Orders } from "./orders";
import { startRuntime } from "./runtime-server";
import { Supervisor } from "./supervisor";

const shutdown = Promise.withResolvers<void>();
globalThis.addEventListener("message", (event: MessageEvent) => {
  if (event.data === "shutdown") shutdown.resolve();
});
const settings = config();
await initLayout(settings.dataDir);
const auth = await serviceAuth(settings.dataDir);
const db = openDb(join(settings.dataDir, "state/core.db"), coreSchema, 0o600);
const bus = new EventBus(db);
const bucket = new WeightBucket(settings.binance.ipWeightLimitPerMin);
const market = new PublicMarket(settings, bus, bucket);
const orders = new Orders(bus, new Map(), Number(settings.watchdog.maxPublicWsStaleSec ?? 30) * 1000);
const pool = new AccountPool(settings, orders, market, bucket);
const checkpoints = new Checkpoints(settings.dataDir, bus);
const supervisor = new Supervisor(settings, bus, orders, checkpoints, auth);
const control = new Control(supervisor);
await supervisor.cleanOrphans();
control.recover();
market.configure(settings);
pool.start();

let expected = performance.now() + 500;
let lag = 0;
const loop = setInterval(() => {
  const now = performance.now();
  lag = Math.max(0, now - expected);
  expected = now + 500;
}, 500);
const health = () => ({
  ok: true, pid: process.pid, eventLoopLagMs: lag, seq: bus.watermark, pendingStorageEvents: bus.pending("storage"),
  symbols: settings.symbols,
  ...market.health(), accounts: [...orders.accounts.values()].map(account => ({
    mode: account.mode, accountId: account.id, allowedSymbols: account.config.allowedSymbols, ...account.healthy(),
  })), runs: supervisor.health(),
});
const reload = async () => {
  const next = config();
  if (next.dataDir !== settings.dataDir || next.defaultSymbol !== settings.defaultSymbol || canonical(next.binance) !== canonical(settings.binance)) {
    bad("Data directory, default symbol and Binance endpoints require a service restart");
  }
  pool.validateReload(next);
  market.configure(next);
  pool.reload(next);
  const before = settings.symbols;
  settings.symbols = next.symbols;
  settings.liveAccounts = next.liveAccounts;
  settings.shadowAccounts = next.shadowAccounts;
  return { reloaded: true, symbols: next.symbols, added: next.symbols.filter(symbol => !before.includes(symbol)),
    removed: before.filter(symbol => !next.symbols.includes(symbol)) };
};
const [admin, runtime] = await Promise.all([startAdmin(control, auth, market, health, reload), startRuntime(supervisor, market, auth)]);
const recover = setInterval(() => supervisor.recover(), 2000);
const prune = setInterval(() => bus.prune(), 10_000);
supervisor.recover();
console.log("quant-core listening on " + admin.url + " and " + runtime.url);

await shutdown.promise;
clearInterval(loop);
clearInterval(recover);
clearInterval(prune);
await supervisor.shutdown();
await Promise.all([admin.stop(true), runtime.stop(true)]);
market.close();
await pool.close();
await checkpoints.drain();
db.close();
globalThis.postMessage("stopped");
