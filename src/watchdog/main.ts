import { join } from "node:path";
import { config, initLayout, socketPath } from "../shared/config";
import { openDb } from "../shared/db";
import { handler, integer, missing } from "../shared/http";
import { serveUnix } from "../shared/uds";
import { imSettings } from "../notifier/settings";
import { EmergencyCancel } from "./emergency-cancel";
import { FallbackAlerts } from "./fallback-alert";
import { probe } from "./probes";

const settings = config();
await initLayout(settings.dataDir);
const db = openDb(join(settings.dataDir, "state/watchdog.db"), [
  "CREATE TABLE incidents(key TEXT PRIMARY KEY,message TEXT NOT NULL,next_send INTEGER NOT NULL);",
  "CREATE TABLE emergency_done(key TEXT PRIMARY KEY, ts INTEGER NOT NULL);",
  "CREATE TABLE fallback_outbox(id INTEGER PRIMARY KEY, channel TEXT NOT NULL, note TEXT NOT NULL, due INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);",
].join("\n"), 0o600);
const emergency = new EmergencyCancel(db, settings);
const alerts = new FallbackAlerts(imSettings(settings), db);
let health: Record<string, unknown> = { ok: true, pid: process.pid };
const server = await serveUnix(socketPath(settings.dataDir, "watchdog"), handler((req, url) =>
  req.method === "GET" && url.pathname === "/v1/health" ? health : missing("Unknown watchdog endpoint")));
const abort = new AbortController();
for (const signal of ["SIGTERM", "SIGINT"] as const) process.once(signal, () => abort.abort());
const interval = integer(settings.watchdog.intervalSec ?? 10, "intervalSec", 1, 300) * 1000;
const outbox = alerts.deliver(abort.signal);
console.log("quant-watchdog listening on " + server.url);

while (!abort.signal.aborted) {
  const started = Date.now();
  try {
    const current = await probe(settings);
    for (const [id, symbols] of current.liveSymbols ?? []) {
      if (Object.hasOwn(settings.liveAccounts, id)) settings.liveAccounts[id].allowedSymbols = symbols;
    }
    const cancellationIssues = await emergency.inspect(current.coreAvailable, abort.signal);
    const issues = [...current.issues, ...cancellationIssues];
    alerts.update(issues);
    health = { ...current, issues, ok: true, healthy: !issues.length, pid: process.pid, checkedAt: Date.now(),
      consecutiveCoreFailures: emergency.consecutiveFailures };
  } catch (error) {
    console.warn("Watchdog cycle failed", (error as Error).message);
    health = { ...health, healthy: false, detail: (error as Error).message };
  }
  if (!abort.signal.aborted) {
    const delay = Math.max(100, interval - (Date.now() - started));
    await new Promise<void>(resolve => {
      function stop() { clearTimeout(timer); abort.signal.removeEventListener("abort", stop); resolve(); }
      const timer = setTimeout(stop, delay);
      abort.signal.addEventListener("abort", stop, { once: true });
    });
  }
}
await Promise.all([server.stop(true), outbox]);
db.close();
