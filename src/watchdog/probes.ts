import { statfs } from "node:fs/promises";
import { socketPath, type HarnessConfig } from "../shared/config";
import { integer, num } from "../shared/http";
import { call } from "../shared/uds";

export type Issue = { key: string; message: string; level: "warn" | "critical" };
type Core = {
  ok: boolean; eventLoopLagMs: number; seq: number; pendingStorageEvents: number;
  symbols?: string[];
  freshness: { mainnet?: Record<string, Record<string, number>> };
  accounts: { accountId: string; mode?: string; allowedSymbols?: string[]; ready: boolean; privateWsAt: number; error?: string }[];
  runs: { runId: string; status: string; alive: boolean; startedAt: number; publishDatasets?: string[] }[];
};
type Peripheral = { ok: boolean; lastSeq?: number; availableBytes?: number; failures?: number; backends?: Record<string, { ok: boolean; detail?: string }> };

export function checkCore(core: Core, config: HarnessConfig, now = Date.now()): Issue[] {
  const settings = config.watchdog;
  const threshold = integer(settings.maxPublicWsStaleSec ?? 30, "maxPublicWsStaleSec", 1) * 1000;
  const issues: Issue[] = [];
  if (core.eventLoopLagMs > integer(settings.maxEventLoopLagMs ?? 250, "maxEventLoopLagMs", 1)) {
    issues.push({ key: "core:loop", message: "Core loop lag " + Math.round(core.eventLoopLagMs) + " ms", level: "critical" });
  }
  for (const ticker of core.symbols ?? config.symbols) for (const topic of ["book_ticker", "kline", "mark_price"]) {
    const time = core.freshness.mainnet?.[ticker]?.[topic] ?? 0;
    if (now - time > threshold) issues.push({ key: "market:" + ticker + ":" + topic,
      message: ticker + " " + topic + " is stale", level: "warn" });
  }
  for (const account of core.accounts) {
    if (!account.ready || now - account.privateWsAt > 27 * 60_000) issues.push({ key: "account:" + account.accountId,
      message: account.accountId + " private stream or reconciliation unavailable" + (account.error ? "\n" + account.error : ""), level: "critical" });
  }
  for (const run of core.runs) {
    if ((!run.alive && run.status === "active") || (run.status === "starting" && now - run.startedAt > 75_000)) {
      issues.push({ key: "run:" + run.runId, message: "Run " + run.runId + " remains " + run.status, level: "critical" });
    }
  }
  return issues;
}

export async function probe(config: HarnessConfig) {
  const dir = config.dataDir;
  const timeoutMs = integer(config.watchdog.coreTimeoutMs ?? 2000, "coreTimeoutMs", 100, 30_000);
  const [coreResult, storageResult, notifier, agent, datasets, disk] = await Promise.allSettled([
    call<Core>(socketPath(dir, "admin"), "/v1/health", { timeoutMs }),
    call<Peripheral>(socketPath(dir, "storage"), "/v1/health", { timeoutMs }),
    call<Peripheral>(socketPath(dir, "notifier"), "/v1/health", { timeoutMs }),
    call<Peripheral>(socketPath(dir, "agent"), "/v1/health", { timeoutMs }),
    call<Array<{ dataset: string; expired: number }>>(socketPath(dir, "storage"), "/v1/data/datasets", { timeoutMs }),
    statfs(dir),
  ]);
  const core = coreResult.status === "fulfilled" && coreResult.value.ok ? coreResult.value : undefined;
  const storage = storageResult.status === "fulfilled" && storageResult.value.ok ? storageResult.value : undefined;
  const issues: Issue[] = core ? checkCore(core, config) : [{ key: "service:core", message: "quant-core is unreachable", level: "critical" } as const];
  for (const [name, result] of [["storage", storageResult], ["notifier", notifier], ["agent", agent]] as const) {
    if (result.status === "rejected" || !result.value.ok) issues.push({ key: "service:" + name, message: "quant-" + name + " is unavailable", level: "warn" });
  }
  const pending = core?.pendingStorageEvents ?? 0;
  if (storage && pending > integer(config.watchdog.maxStorageSeqLag ?? 5000, "maxStorageSeqLag", 0)) issues.push({
    key: "storage:lag", message: "Storage has " + pending + " pending events", level: "warn",
  });
  if (disk.status === "fulfilled") {
    const bytes = disk.value.bavail * disk.value.bsize;
    if (bytes < num(config.watchdog.minDiskFreeGb ?? 5, "minDiskFreeGb", 0) * 1_000_000_000) issues.push({
      key: "storage:disk", message: "Storage free space " + (bytes / 1_000_000_000).toFixed(2) + " GB", level: "critical",
    });
  }
  if (datasets.status === "fulfilled") {
    const entries = new Set(datasets.value.map(dataset => dataset.dataset));
    for (const dataset of datasets.value.filter(dataset => dataset.expired)) issues.push({ key: "dataset:" + dataset.dataset,
      message: "Dataset " + dataset.dataset + " exceeded its TTL", level: "warn" });
    for (const run of core?.runs ?? []) if (run.status === "active" && Date.now() - run.startedAt > 300_000) {
      for (const dataset of run.publishDatasets ?? []) if (!entries.has(dataset)) issues.push({ key: "dataset:" + dataset,
        message: "Declared dataset " + dataset + " has no samples", level: "warn" });
    }
  }
  if (notifier.status === "fulfilled" && (notifier.value.failures ?? 0) > 0) issues.push({
    key: "notifier:delivery", message: "IM deliveries are retrying", level: "warn",
  });
  if (agent.status === "fulfilled") for (const [backend, info] of Object.entries(agent.value.backends ?? {})) {
    if (!info.ok) issues.push({ key: "agent:" + backend, message: backend + " backend unavailable\n" + (info.detail ?? ""), level: "warn" });
  }
  const liveSymbols = core?.accounts.filter(account => account.mode === "live" && account.allowedSymbols)
    .map(account => [account.accountId, account.allowedSymbols!] as const);
  return { coreAvailable: !!core, liveSymbols, issues, services: { core: !!core, storage: !!storage, notifier: notifier.status === "fulfilled" && notifier.value.ok,
    agent: agent.status === "fulfilled" && agent.value.ok, watchdog: true }, pendingStorageEvents: pending };
}
