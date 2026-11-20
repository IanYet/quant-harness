import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AccountFullState, OpenOrder } from "@quant/tools";
import { serviceAuth } from "../src/shared/auth";
import type { HarnessConfig } from "../src/shared/config";
import { initLayout, socketPath } from "../src/shared/config";
import { openDb } from "../src/shared/db";
import { call } from "../src/shared/uds";
import { Checkpoints } from "../src/core/checkpoint";
import { Control } from "../src/core/admin-server";
import schema from "../src/core/core.sql" with { type: "text" };
import { EventBus } from "../src/core/event-bus";
import { emptyLeg } from "../src/core/gateway";
import type { Markets } from "../src/core/gateway";
import { Orders } from "../src/core/orders";
import { checkExposure, normalizeOrder } from "../src/core/risk";
import { startRuntime } from "../src/core/runtime-server";
import { planCancel, Supervisor } from "../src/core/supervisor";
import type { Session } from "../src/core/supervisor";
import { openStorage } from "../src/storage/db";
import { writeLedger, writeTimeseries } from "../src/storage/ingestor";

async function workspace() {
  const dir = await mkdtemp(join(tmpdir(), "quant-core-test-"));
  await initLayout(dir);
  return dir;
}

test("critical frames replay after ring eviction and sequence reservation survives a restart", async () => {
  const dir = await workspace();
  const path = join(dir, "state/core.db");
  const storage = openStorage(dir);
  let db = openDb(path, schema);
  try {
    let bus = new EventBus(db, 2, 4);
    const first = bus.publish("sys.operation", "cli", { operationId: "op-1", action: "reload", actor: "local", source: "cli", result: "ok" });
    writeLedger(storage.ledger, [first], first.seq);
    for (let i = 0; i < 7; i++) bus.publish("market.contract_info", "binance.mainnet", {});
    const second = bus.publish("sys.operation", "cli", { operationId: "op-2", action: "reload", actor: "local", source: "cli", result: "ok" });
    db.close();
    db = openDb(path, schema);
    bus = new EventBus(db, 2, 4);
    expect(bus.watermark).toBeGreaterThan(second.seq);
    const { frames, close } = bus.follow({ topics: ["sys.operation", "market.*"], after: storage.cursor().lastSeq });
    try {
      expect((await frames.next()).value).toMatchObject({ type: "gap" });
      const events = [(await frames.next()).value, (await frames.next()).value];
      const boundary = (await frames.next()).value;
      expect(events).toEqual([first, second]);
      if (!boundary || !("throughSeq" in boundary)) throw new Error("Missing watermark");
      writeLedger(storage.ledger, [first, second], boundary.throughSeq);
      writeTimeseries(storage.timeseries, [first, second], boundary.throughSeq);
      expect(storage.ledger.query("SELECT COUNT(*) AS n FROM operation_audit").get()).toEqual({ n: 2 });
      bus.ack("storage", storage.cursor().lastSeq);
      expect(bus.pending("storage")).toBe(0);
    } finally { close(); }
  } finally { storage.close(); db.close(); await rm(dir, { recursive: true, force: true }); }
});

test("shutdown disposition follows the owning intent and current position leg", () => {
  const state: AccountFullState = { accountId: "a", mode: "live", walletBalance: 0, availableBalance: 0, unrealizedPnl: 0,
    leverages: {}, reconciling: false, openOrders: [], positions: {
      BTCUSDT: { long: { ...emptyLeg("BTCUSDT", "LONG"), qty: 0.01 }, short: emptyLeg("BTCUSDT", "SHORT") },
    } };
  const base = { symbol: "BTCUSDT", positionSide: "LONG", side: "SELL", qty: 0.01, status: "NEW", source: "strategy",
    updatedAt: 1, createdAt: 1 } as const;
  const stop: OpenOrder = { ...base, kind: "CONDITIONAL", orderType: "STOP_MARKET", algoId: "1" };
  const emptyShort: OpenOrder = { ...stop, positionSide: "SHORT", side: "BUY", algoId: "2" };
  const entry: OpenOrder = { ...stop, side: "BUY", algoId: "3" };
  const limit: OpenOrder = { ...base, kind: "STANDARD", orderType: "LIMIT", exchangeOrderId: "4" };
  const ordinary: OpenOrder = { ...limit, exchangeOrderId: "5" };
  state.openOrders = [stop, emptyShort, entry, limit, ordinary, { ...ordinary, source: "manual", exchangeOrderId: "6" }];
  const owned = [stop, emptyShort, entry, limit, ordinary].map(order => ({
    order, intent: { request_json: JSON.stringify(order === limit ? { protection: "take_profit" } : {}) },
  }));
  expect(planCancel(owned, state)).toEqual([emptyShort, entry, ordinary]);
  expect(planCancel(owned, state, undefined, { cancel: [{ algoId: "1" }], retain: [{ algoId: "3" }] })).toEqual([stop, emptyShort, ordinary]);
  expect(planCancel(owned, state, { policy: "cancel_open", includeConditional: false })).toEqual([limit, ordinary]);
  expect(planCancel(owned, state, { policy: "cancel_open", includeConditional: true })).toHaveLength(5);
  expect(planCancel(owned, state, { policy: "adopt", includeConditional: true })).toEqual([]);
});

test("runtime-scoped workers, atomic proposal decisions and a single terminal checkpoint", async () => {
  const dir = await workspace();
  const db = openDb(join(dir, "state/core.db"), schema);
  const bus = new EventBus(db);
  const config = { dataDir: dir, defaultSymbol: "BTCUSDT", liveAccounts: {}, shadowAccounts: {} } as HarnessConfig;
  const orders = new Orders(bus, new Map(), 30_000);
  const checkpoints = new Checkpoints(dir, bus);
  const auth = await serviceAuth(dir);
  const supervisor = new Supervisor(config, bus, orders, checkpoints, auth);
  const caps = { accountRead: ["sub"], publishDatasets: ["a", "b"], notify: true };
  const artifact = { artifactPath: "/unused", codeHash: "hash", sourceHash: "source", sdkHash: "sdk" };
  const row = bus.commit(emit => supervisor["insert"]({ mode: "worker", capabilities: caps, persistent: false }, artifact, emit));
  expect(() => bus.commit(emit => supervisor["insert"]({ mode: "worker", capabilities: { publishDatasets: ["b"] },
    persistent: false }, artifact, emit))).toThrow(expect.objectContaining({ code: "DATASET_LOCKED" }));
  const session: Session = { token: "worker-test-token", mode: "worker", runId: row.run_id, namespace: row.namespace, epoch: 0, caps };
  supervisor.sessions.set(session.token, session);
  const server = await startRuntime(supervisor, {} as Markets, auth);
  try {
    const socket = socketPath(dir, "runtime");
    const request = <T = unknown>(path: string, method = "GET", body?: unknown) => call<T>(socket, path,
      { token: session.token, method, body });
    await request("/v1/session/ready", "POST");
    expect(await request("/v1/accounts")).toEqual([]);
    await expect(request("/v1/accounts/unlisted/state")).rejects.toMatchObject({ status: 403 });
    await expect(request("/v1/trade/account")).rejects.toMatchObject({ status: 403 });
    for (const dataset of ["a", "b"]) expect(await request("/v1/data/publish", "POST", { dataset, ts: 1, payload: {} })).toHaveProperty("seq");
    await expect(request("/v1/data/publish", "POST", { dataset: "nope", ts: 1, payload: {} })).rejects.toMatchObject({ status: 403 });
    expect(await request("/v1/notify", "POST", { level: "info", title: "Queue contract", body: "Infrastructure check" })).toHaveProperty("seq");
    expect(bus.replay("sys.notify", 0, bus.watermark)).toHaveLength(1);
    await expect(request("/v1/state", "PUT", { data: "x".repeat(1024 * 1024) })).rejects.toMatchObject({ code: "CHECKPOINT_TOO_LARGE" });

    const control = new Control(supervisor);
    const order = normalizeOrder({ action: "closeShort", params: { type: "MARKET", qty: 0.01, clientOrderId: "manual-1" } }, "BTCUSDT");
    const pending = control.submit({ operationId: "op", action: "manual_trade", target: { accountId: "sub", command: "submit", order } },
      { actor: "cli-test", source: "cli" }, true) as Response;
    const { proposalId } = await pending.json() as { proposalId: string };
    expect(pending.status).toBe(202);
    expect(() => control.decide(proposalId, true, { proposalDigest: "wrong" }, { actor: "im-test", source: "notifier" }))
      .toThrow(expect.objectContaining({ code: "PROPOSAL_CONFLICT" }));
    control.decide(proposalId, false, {}, { actor: "mcp-test", source: "mcp" });
    expect(() => control.decide(proposalId, false, {}, { actor: "im-test", source: "notifier" }))
      .toThrow(expect.objectContaining({ code: "PROPOSAL_CONFLICT" }));
    expect(bus.replay("sys.operation", 0, bus.watermark).map(event => event.source)).toEqual(["cli", "mcp"]);
    expect(supervisor.health()[0].status).toBe("active");

    const final = { handoverId: "stop-id", deadline: Date.now() + 3000, orders: [], runId: row.run_id, action: "stop" };
    db.query("UPDATE active_runs SET status='handover' WHERE run_id=?").run(row.run_id);
    db.query("INSERT INTO worker_handovers VALUES (?,?)").run(row.run_id, JSON.stringify(final));
    const p = (data: string) => request("/v1/state", "PUT", { data, handoverId: "stop-id" });
    const commits = await Promise.all([p("one"), p("two")]);
    expect(commits[0]).toEqual(commits[1]);
    expect(bus.replay("state.checkpoint", 0, bus.watermark)).toHaveLength(1);
    expect(await request("/v1/handover/complete", "POST", { handoverId: "stop-id" })).toEqual({ completed: true });
    expect(await request("/v1/handover/complete", "POST", { handoverId: "stop-id" })).toEqual({ completed: true });
  } finally { await server.stop(true); await checkpoints.drain(); db.close(); await rm(dir, { recursive: true, force: true }); }
});

test("a protective conditional close bypasses opening caps and still validates its Hedge leg", () => {
  const mark = () => undefined;
  const view = () => ({
    accountId: "hedge", mode: "shadow" as const, walletBalance: 1000, availableBalance: 100, unrealizedPnl: 0,
    leverages: { BTCUSDT: 8 }, reconciling: true,
    positions: { BTCUSDT: { long: { ...emptyLeg("BTCUSDT", "LONG", 8), qty: 0.02 }, short: emptyLeg("BTCUSDT", "SHORT", 8) } },
    openOrders: [{ symbol: "BTCUSDT", side: "SELL", orderType: "LIMIT", positionSide: "LONG", status: "NEW",
      kind: "STANDARD", qty: 0.01, source: "strategy", createdAt: 0, updatedAt: 0 }] as OpenOrder[],
  });
  const config = { allowedSymbols: ["BTCUSDT"], requireApproval: false, requireApprovalOnStop: false, requireApprovalOnManualTrade: false,
    marginType: "CROSSED" as const, initialLeverage: 1, risk: { maxLeverage: 3, maxOrderNotionalUsd: 5, maxOrdersPerMin: 1 }, labels: [] };
  const account = { config, view, mark, healthy: () => ({ ready: false, privateWsAt: 0 }) };
  const order = (action: string, type: string, qty: number) => normalizeOrder({
    action, params: { type, clientOrderId: "risk-check", qty, triggerPrice: type === "STOP_MARKET" ? 100 : undefined },
  }, "BTCUSDT");

  expect(() => checkExposure(order("closeLong", "STOP_MARKET", 0.02), account, true, 1000)).not.toThrow();
  expect(() => checkExposure(order("closeLong", "MARKET", 0.02), account, true, 1000)).toThrow(expect.objectContaining({ code: "RISK_REJECTED" }));
  expect(() => checkExposure(order("closeShort", "STOP_MARKET", 0.02), account, true, 1000)).toThrow(expect.objectContaining({ code: "RISK_REJECTED" }));
  expect(() => checkExposure(order("openLong", "MARKET", 0.01), account, true, 1000)).toThrow(expect.objectContaining({ code: "KILL_SWITCH_ACTIVE" }));
});
