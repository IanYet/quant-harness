import { randomBytes, randomUUID } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import { basename, join } from "node:path";
import type { CheckpointEnvelope, OpenOrder, OrderRef, StopDecision, WorkerCapabilities } from "@quant/tools";
import type { ServiceAuth } from "../shared/auth";
import type { AccountMode, HarnessConfig, RunMode } from "../shared/config";
import { socketPath } from "../shared/config";
import { capabilities } from "../shared/capabilities";
import { sha256 } from "../shared/fs";
import { bad, Fault, flag, ID, missing, oneOf, record, str } from "../shared/http";
import { call } from "../shared/uds";
import type { Artifact } from "../storage/strategy-registry";
import { Checkpoints } from "./checkpoint";
import type { Emit } from "./event-bus";
import { EventBus } from "./event-bus";
import { accountKey, legs } from "./gateway";
import type { ExchangeAccount } from "./gateway";
import { Orders, protective, reference, selection } from "./orders";
import type { Intent } from "./orders";
import type { NormalizedOrder } from "./risk";

type RunStatus = "starting" | "active" | "handover" | "stopped" | "failed" | "interrupted";
type Target = { strategyId?: string; entryPath?: string; draftPath?: string };
type RunSpec = Target & { mode: RunMode; capabilities: WorkerCapabilities; stateName?: string; persistent: boolean; parentTaskId?: string };
export interface RunRow {
  run_id: string; pid: number | null; mode: RunMode; account_id: string | null; epoch: number;
  target_json: string; code_hash: string; artifact_path: string; capabilities_json: string; namespace: string;
  state_name: string | null; persistent: number; status: RunStatus; started_at: number; ended_at: number | null;
  parent_task_id: string | null;
}
type Next = { spec: RunSpec; artifact: Artifact; resetState: boolean; includeConditional: boolean; policy: "adopt" | "cancel_open" };
type Handover = {
  handoverId: string; action: "stop" | "replace"; runId: string; orders: OpenOrder[]; deadline: number;
  next?: Next; complete?: boolean; decision?: StopDecision; interrupted?: boolean;
};
type Lease = { mode: AccountMode; account_id: string; run_id: string; epoch: number; status: RunStatus; handover_json: string | null };
type Child = ReturnType<typeof Bun.spawn<"ignore", "pipe", "pipe">>;
export type Session = { token: string; runId: string; mode: RunMode; accountId?: string; epoch: number; caps: WorkerCapabilities; namespace: string };
const running = ["starting", "active", "handover"];

function orderIdentity(order: OpenOrder) {
  return order.symbol + ":" + order.kind + ":" + (order.kind === "STANDARD" ? order.exchangeOrderId : order.algoId);
}

export function planCancel(owned: { order: OpenOrder; intent: Pick<Intent, "request_json"> }[], state: ReturnType<ExchangeAccount["view"]>,
    next?: Pick<Next, "policy" | "includeConditional">, decision?: StopDecision) {
  const orders = owned.map(item => item.order);
  const chosen = (refs?: OrderRef[]) => new Set((refs ?? []).flatMap(ref => selection(orders, ref)).map(orderIdentity));
  const keep = chosen(decision?.retain);
  const cancel = chosen(decision?.cancel);
  return owned.filter(({ order, intent }) => {
    if (next) return next.policy === "cancel_open" && (order.kind === "STANDARD" || next.includeConditional);
    const id = orderIdentity(order);
    if (cancel.has(id)) return true;
    if (keep.has(id)) return false;
    const leg = legs(state.positions, order.symbol)[order.positionSide === "LONG" ? "long" : "short"];
    return leg.qty <= 0 || !protective(order, JSON.parse(intent.request_json) as NormalizedOrder);
  }).map(item => item.order);
}

export class Supervisor {
  readonly sessions = new Map<string, Session>();
  private readonly children = new Map<string, Child>();
  private readonly inflight = new Set<string>();
  private readonly retryAt = new Map<string, number>();
  private stopping = false;
  constructor(readonly config: HarnessConfig, readonly bus: EventBus, readonly orders: Orders,
    readonly checkpoints: Checkpoints, private readonly auth: ServiceAuth) {}

  list() {
    const rows = this.bus.db.query<RunRow, []>(
      "SELECT * FROM active_runs ORDER BY status IN ('active','handover','starting') DESC,started_at DESC LIMIT 500").all();
    return rows.map(row => this.summary(row));
  }

  health() {
    return this.activeRows().map(row => ({ runId: row.run_id, mode: row.mode, accountId: row.account_id, status: row.status,
      pid: row.pid, alive: this.children.has(row.run_id), startedAt: row.started_at, persistent: !!row.persistent,
      publishDatasets: (JSON.parse(row.capabilities_json) as WorkerCapabilities).publishDatasets ?? [] }));
  }

  private activeRows() {
    return this.bus.db.query<RunRow, []>("SELECT * FROM active_runs WHERE status IN ('active','handover','starting')").all();
  }

  get(runId: string) {
    return this.bus.db.query<RunRow, [string]>("SELECT * FROM active_runs WHERE run_id=?").get(runId) ?? missing("Unknown run " + runId);
  }

  lease(mode: AccountMode, accountId: string) {
    return this.bus.db.query<Lease, [string, string]>("SELECT * FROM leases WHERE mode=? AND account_id=?").get(mode, accountId);
  }

  private summary(row: RunRow) {
    return { runId: row.run_id, pid: row.pid, mode: row.mode, accountId: row.account_id, epoch: row.epoch,
      codeHash: row.code_hash, target: JSON.parse(row.target_json) as Target, capabilities: JSON.parse(row.capabilities_json) as WorkerCapabilities,
      stateName: row.state_name, namespace: row.namespace, persistent: !!row.persistent, status: row.status,
      startedAt: row.started_at, endedAt: row.ended_at };
  }

  guard(session: Session, writing = true) {
    const row = this.get(session.runId);
    if (!running.includes(row.status) || this.sessions.get(session.token) !== session) {
      throw new Fault(409, "STALE_EPOCH", "Run is no longer active");
    }
    if (session.accountId && session.mode !== "worker") {
      const lease = this.lease(session.mode, session.accountId);
      if (lease?.run_id !== session.runId || lease.epoch !== session.epoch) throw new Fault(409, "STALE_EPOCH", "Account epoch changed");
    }
    if (writing && row.status !== "active") throw new Fault(409, "STALE_EPOCH", "Run is starting or frozen");
    return row;
  }

  ready(session: Session) {
    const row = this.guard(session, false);
    if (row.status === "starting") this.lifecycle(row, "active");
    return { ready: row.status !== "handover" };
  }

  private lifecycle(row: RunRow, status: RunStatus) {
    this.bus.commit(emit => {
      this.bus.db.query("UPDATE active_runs SET status=?,ended_at=? WHERE run_id=?")
        .run(status, running.includes(status) ? null : Date.now(), row.run_id);
      if (row.account_id) {
        const sql = running.includes(status) ? "UPDATE leases SET status=? WHERE mode=? AND account_id=? AND run_id=?" :
          "DELETE FROM leases WHERE ? IS NOT NULL AND mode=? AND account_id=? AND run_id=?";
        this.bus.db.query(sql).run(status, row.mode, row.account_id, row.run_id);
      }
      emit("sys.run_lifecycle", "core", { ...this.summary(row), status });
    });
  }

  prepare(spec: RunSpec) {
    return call<Artifact>(socketPath(this.config.dataDir, "storage"), "/v1/scripts/prepare", {
      method: "POST", token: this.auth.token("core"), timeoutMs: 60_000,
      body: { mode: spec.mode, strategyId: spec.strategyId, path: spec.entryPath ?? spec.draftPath, capabilities: spec.capabilities },
    });
  }

  async start(input: Record<string, unknown>, parentTaskId?: string) {
    const mode = oneOf(input.mode, "mode", ["worker", "shadow"]);
    const config = { mode, capabilities: capabilities(input.capabilities), parentTaskId, persistent: false };
    const spec: RunSpec = mode === "worker" ? { ...config, entryPath: str(input.entryPath, "entryPath"),
      persistent: flag(input.persistent, "persistent"), stateName: input.stateName === undefined ? undefined : str(input.stateName, "stateName", ID) } :
      { ...config, strategyId: input.strategyId === undefined ? undefined : str(input.strategyId, "strategyId"),
        draftPath: input.draftPath === undefined ? undefined : str(input.draftPath, "draftPath") };
    if (mode === "shadow" && Number(!!spec.strategyId) + Number(!!spec.draftPath) !== 1) bad("Specify one shadow target");
    const artifact = await this.prepare(spec);
    const shadow = mode === "shadow" ? [...this.orders.accounts.values()]
      .find(account => account.mode === "shadow" && account.healthy().ready && !this.lease("shadow", account.id)) : undefined;
    if (mode === "shadow" && !shadow) throw new Fault(409, "SHADOW_LOCKED", "No reconciled shadow account is available");
    const row = this.bus.commit(emit => this.insert(spec, artifact, emit, shadow));
    return this.spawn(row);
  }

  async livePlan(input: Record<string, unknown>, parentTaskId?: string): Promise<Next> {
    const spec: RunSpec = { mode: "live", strategyId: str(input.strategyId, "strategyId"),
      capabilities: capabilities(input.capabilities), persistent: true, parentTaskId };
    const artifact = await this.prepare(spec);
    return { spec, artifact, resetState: flag(input.resetState, "resetState"),
      policy: oneOf(input.handoverOrders ?? "adopt", "handoverOrders", ["adopt", "cancel_open"]),
      includeConditional: flag(input.includeConditional, "includeConditional") };
  }

  async live(id: string, plan: Next, baseRunId: string | null) {
    const account = this.orders.account("live", id);
    const old = this.lease("live", id);
    if ((old?.run_id ?? null) !== baseRunId || old?.status === "handover") throw new Fault(409, "PROPOSAL_CONFLICT", "The account lease changed");
    const sealed = await this.prepare(plan.spec);
    if (sealed.codeHash !== plan.artifact.codeHash || sealed.sourceHash !== plan.artifact.sourceHash) {
      throw new Fault(422, "HASH_MISMATCH", "Strategy no longer matches the approved hashes");
    }
    await this.verifyArtifact(plan.artifact.artifactPath, plan.artifact.codeHash);
    if ((this.lease("live", id)?.run_id ?? null) !== baseRunId) throw new Fault(409, "PROPOSAL_CONFLICT", "The account lease changed");
    if (old) return this.stop(old.run_id, plan);
    if (!account.healthy().ready) throw new Fault(503, "RECONCILING", "Account is not ready");
    let checkpoint: CheckpointEnvelope<unknown> | undefined;
    const row = this.bus.commit(emit => {
      const run = this.insert(plan.spec, plan.artifact, emit, account);
      if (plan.resetState) checkpoint = this.checkpoints.stage(emit, run.namespace, run.epoch, id, {}, 1);
      return run;
    }, plan.resetState ? 2 : 1);
    if (checkpoint) this.checkpoints.remember(checkpoint);
    return this.spawn(row);
  }

  private insert(spec: RunSpec, artifact: Artifact, emit: Emit, account?: ExchangeAccount, previous?: string) {
    const db = this.bus.db;
    if (account && this.lease(account.mode, account.id) && !previous) throw new Fault(409, "ACCOUNT_LOCKED", "Account already has a run");
    const id = randomUUID();
    const namespace = spec.mode === "live" ? "account:" + account!.id : spec.mode === "shadow" ? "shadow:" + id : "worker:" + (spec.stateName ?? id);
    const datasets = spec.capabilities.publishDatasets ?? [];
    for (const run of this.activeRows()) {
      if (run.run_id === previous) continue;
      const caps = JSON.parse(run.capabilities_json) as WorkerCapabilities;
      if (datasets.some(dataset => caps.publishDatasets?.includes(dataset))) throw new Fault(409, "DATASET_LOCKED", "Dataset has a publisher");
      if (namespace === run.namespace) throw new Fault(409, "STATE_LOCKED", "A worker uses this state namespace");
    }
    const epoch = account ? db.query<{ epoch: number }, [string, string]>(
      "SELECT COALESCE(MAX(epoch),0)+1 AS epoch FROM active_runs WHERE mode=? AND account_id=?").get(account.mode, account.id)!.epoch : 0;
    const { strategyId, entryPath, draftPath } = spec;
    const now = Date.now();
    db.query("INSERT INTO active_runs VALUES (?,NULL,?,?,?,?,?,?,?,?,?,?,'starting',?,NULL,?)")
      .run(id, spec.mode, account?.id ?? null, epoch, JSON.stringify({ strategyId, entryPath, draftPath }), artifact.codeHash,
        artifact.artifactPath, JSON.stringify(spec.capabilities), namespace, spec.stateName ?? null, Number(spec.persistent), now, spec.parentTaskId ?? null);
    if (account) db.query("INSERT INTO leases VALUES (?,?,?,?,'starting',NULL) ON CONFLICT(mode,account_id) DO UPDATE SET " +
      "run_id=excluded.run_id,epoch=excluded.epoch,status='starting',handover_json=NULL").run(account.mode, account.id, id, epoch);
    const row = this.get(id);
    emit("sys.run_lifecycle", "core", this.summary(row));
    return row;
  }

  private async verifyArtifact(path: string, hash: string) {
    if (path !== join(this.config.dataDir, "artifacts", hash, "entry.mjs") || !/^[a-f0-9]{64}$/.test(hash) ||
        sha256(await readFile(path)) !== hash) throw new Fault(422, "HASH_MISMATCH", "Prepared artifact has changed");
  }

  private async spawn(row: RunRow) {
    if (this.stopping) throw new Fault(503, "UNAVAILABLE", "Core is stopping");
    if (this.children.has(row.run_id)) return this.summary(this.get(row.run_id));
    await this.verifyArtifact(row.artifact_path, row.code_hash);
    if (this.get(row.run_id).status !== "starting") throw new Fault(409, "STALE_EPOCH", "Run was stopped before spawn");
    const token = randomBytes(32).toString("hex");
    const caps = JSON.parse(row.capabilities_json) as WorkerCapabilities;
    const session: Session = { token, runId: row.run_id, mode: row.mode, accountId: row.account_id ?? undefined, epoch: row.epoch, caps, namespace: row.namespace };
    this.sessions.set(token, session);
    let child: Child;
    try {
      child = Bun.spawn([process.execPath, "--no-env-file", "run", row.artifact_path], {
        cwd: this.config.dataDir, stdin: "ignore", stdout: "pipe", stderr: "pipe", env: {
          QUANT_SOCKET: socketPath(this.config.dataDir, "runtime"), QUANT_STORAGE_SOCKET: socketPath(this.config.dataDir, "storage"),
          QUANT_TOKEN: token, QUANT_MODE: row.mode, QUANT_RUN_ID: row.run_id, QUANT_CAPABILITIES: row.capabilities_json,
          ...(row.account_id ? { QUANT_ACCOUNT: row.account_id, QUANT_EPOCH: String(row.epoch) } : {}),
          ...(row.state_name ? { QUANT_STATE_NAME: row.state_name } : {}),
        },
      });
    } catch (error) {
      this.sessions.delete(token);
      throw error;
    }
    this.children.set(row.run_id, child);
    this.bus.db.query("UPDATE active_runs SET pid=? WHERE run_id=?").run(child.pid, row.run_id);
    const startup = setTimeout(() => {
      if (this.get(row.run_id).status === "starting") child.kill("SIGKILL");
    }, 70_000);
    void this.logs(row, child.stdout, "stdout");
    void this.logs(row, child.stderr, "stderr");
    void child.exited.then(() => {
      clearTimeout(startup);
      this.children.delete(row.run_id);
      this.sessions.delete(token);
      this.bus.db.query("UPDATE active_runs SET pid=NULL WHERE run_id=?").run(row.run_id);
      const current = this.get(row.run_id);
      if (this.stopping || current.status === "handover") return;
      if (current.mode === "live" || (current.mode === "worker" && current.persistent)) {
        this.lifecycle(current, "starting");
        this.retryAt.set(row.run_id, Date.now() + 5000);
      } else if (current.mode === "shadow") {
        void this.stop(row.run_id, undefined, true).catch(error => this.report(row.run_id, error));
      } else this.lifecycle(current, child.exitCode === 0 ? "stopped" : "failed");
    }).catch(error => this.report(row.run_id, error));
    return this.summary(this.get(row.run_id));
  }

  private async logs(row: RunRow, stream: ReadableStream<Uint8Array>, channel: string) {
    try {
      const target = JSON.parse(row.target_json) as Target;
      const name = basename(target.strategyId ?? target.draftPath ?? target.entryPath ?? row.run_id).replace(/[^A-Za-z0-9_-]/g, "-").slice(0, 64);
      const reader = stream.getReader();
      const decoder = new TextDecoder();
      let partial = "";
      for (;;) {
        const chunk = await reader.read();
        partial += decoder.decode(chunk.value, { stream: !chunk.done });
        const last = partial.lastIndexOf("\n");
        if (last >= 0 || partial.length > 64_000 || chunk.done) {
          const split = !chunk.done && last >= 0 ? last + 1 : partial.length;
          const prefix = new Date().toISOString() + " " + channel + " ";
          const text = partial.slice(0, split).split("\n").filter(Boolean).map(line => prefix + line + "\n").join("");
          partial = partial.slice(split);
          if (text) await call(socketPath(this.config.dataDir, "storage"), "/v1/logs/append", {
            token: this.auth.token("core"), method: "POST", timeoutMs: 1500, body: { runId: row.run_id, mode: row.mode, name, text },
          }).catch(() => {});
        }
        if (chunk.done) break;
      }
    } catch (error) { console.error("Run log pipeline failed", row.run_id, error); }
  }

  handover(row: RunRow): Handover | null {
    if (row.status !== "handover") return null;
    const raw = row.account_id && row.mode !== "worker" ? this.lease(row.mode, row.account_id)?.handover_json :
      this.bus.db.query<{ document: string }, [string]>("SELECT document FROM worker_handovers WHERE run_id=?").get(row.run_id)?.document;
    return raw ? JSON.parse(raw) as Handover : null;
  }

  private writeHandover(row: RunRow, handover: Handover) {
    const json = JSON.stringify(handover);
    if (row.account_id) this.bus.db.query("UPDATE leases SET status='handover',handover_json=? WHERE mode=? AND account_id=? AND run_id=?")
      .run(json, row.mode, row.account_id, row.run_id);
    else this.bus.db.query("INSERT INTO worker_handovers VALUES (?,?) ON CONFLICT(run_id) DO UPDATE SET document=excluded.document")
      .run(row.run_id, json);
  }

  save(session: Session, input: Record<string, unknown>) {
    const row = this.guard(session, false);
    const handover = this.handover(row);
    if (row.status !== "active" && (!handover || handover.handoverId !== input.handoverId || Date.now() > handover.deadline || handover.complete)) {
      throw new Fault(409, "STALE_EPOCH", "Checkpoint write is outside the active run or its handover");
    }
    if (row.status === "active" && input.handoverId !== undefined) throw new Fault(409, "STALE_EPOCH", "No handover is active");
    const envelope = this.bus.commit(emit => this.checkpoints.stage(emit, session.namespace, session.epoch, session.accountId,
      input.data, input.schemaVersion, handover?.handoverId));
    return this.checkpoints.remember(envelope);
  }

  complete(session: Session, input: Record<string, unknown>) {
    const row = this.guard(session, false);
    const handover = this.handover(row);
    if (!handover || handover.handoverId !== input.handoverId || Date.now() > handover.deadline) {
      throw new Fault(409, "STALE_EPOCH", "Handover expired");
    }
    if (handover.complete) return { completed: true };
    if (input.decision !== undefined && handover.action === "stop") {
      const decision = record(input.decision);
      const refs = (value: unknown): OrderRef[] => {
        if (value === undefined) return [];
        if (!Array.isArray(value) || value.length > 1000) bad("Invalid stop order list");
        return value.map(item => reference(record(item)));
      };
      const result = { retain: refs(decision.retain), cancel: refs(decision.cancel) };
      const selected = [...result.retain, ...result.cancel].map(ref => {
        const matches = selection(handover.orders, ref);
        if (matches.length !== 1) bad("Stop decision must identify one owned order");
        return orderIdentity(matches[0]);
      });
      if (selected.length !== new Set(selected).size) bad("Stop decision contains duplicate order references");
      handover.decision = result;
    }
    handover.complete = true;
    this.bus.commit(emit => {
      this.writeHandover(row, handover);
      emit("sys.handover_result", "core", { runId: row.run_id, handoverId: handover.handoverId, decision: handover.decision });
    });
    return { completed: true };
  }

  async stop(id: string, next?: Next, interrupted = false) {
    const row = this.get(id);
    if (row.status === "handover") {
      if (next) throw new Fault(409, "ACCOUNT_LOCKED", "Handover is already in progress");
      return { runId: id, status: "handover" };
    }
    if (!running.includes(row.status)) return this.summary(row);
    const account = row.account_id && row.mode !== "worker" ? this.orders.account(row.mode, row.account_id) : undefined;
    const handover: Handover = { handoverId: randomUUID(), action: next ? "replace" : "stop", runId: id,
      orders: account ? this.orders.owned(account, id).map(item => item.order) : [], deadline: Date.now() + 3000, next, interrupted };
    this.bus.commit(emit => {
      this.bus.db.query("UPDATE active_runs SET status='handover' WHERE run_id=?").run(id);
      this.writeHandover(row, handover);
      emit("sys.handover", "core", { handoverId: handover.handoverId, action: handover.action, runId: id, orders: handover.orders });
    });
    return this.finish(this.get(id));
  }

  private async finish(row: RunRow) {
    const id = row.run_id;
    if (this.inflight.has(id)) return { runId: id, status: "handover" };
    this.inflight.add(id);
    try {
      const initial = this.handover(row);
      if (!initial) throw new Error("Missing persistent handover record");
      const child = this.children.get(id);
      if (child) {
        child.kill("SIGTERM");
        await Promise.race([child.exited, Bun.sleep(Math.max(0, initial.deadline - Date.now()))]);
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
        await child.exited;
      }
      const handover = this.handover(row) ?? initial;
      if (!handover.next && row.mode === "worker") {
        this.lifecycle(row, handover.interrupted ? "interrupted" : "stopped");
        this.bus.db.query("DELETE FROM worker_handovers WHERE run_id=?").run(id);
        return this.summary(this.get(id));
      }
      if (!row.account_id || row.mode === "worker") throw new Error("Missing account binding");
      const account = this.orders.account(row.mode, row.account_id);
      return await this.orders.serial.run(accountKey(account.mode, account.id), async () => {
        await account.refresh();
        await this.orders.settle(account, id);
        for (let attempt = 0; attempt < 3; attempt++) {
          const pending = planCancel(this.orders.owned(account, id), account.view(), handover.next, handover.decision);
          if (!pending.length) break;
          for (const order of pending) await account.cancel(order);
          await account.refresh();
        }
        if (planCancel(this.orders.owned(account, id), account.view(), handover.next, handover.decision).length) {
          throw new Fault(503, "RECONCILING", "Open orders still need reconciliation");
        }
        const next = handover.next;
        if (next) {
          await this.verifyArtifact(next.artifact.artifactPath, next.artifact.codeHash);
          let checkpoint: CheckpointEnvelope<unknown> | undefined;
          const replacement = this.bus.commit(emit => {
            this.bus.db.query("UPDATE active_runs SET status='stopped',ended_at=? WHERE run_id=?").run(Date.now(), id);
            emit("sys.run_lifecycle", "core", { ...this.summary(row), status: "stopped" });
            const newRun = this.insert(next.spec, next.artifact, emit, account, id);
            this.orders.reassign(account, id, newRun.run_id);
            if (next.resetState) checkpoint = this.checkpoints.stage(emit, newRun.namespace, newRun.epoch, account.id, {}, 1);
            return newRun;
          }, next.resetState ? 3 : 2);
          if (checkpoint) this.checkpoints.remember(checkpoint);
          return this.spawn(replacement);
        }
        const status = handover.interrupted ? "interrupted" : "stopped";
        this.lifecycle(row, status);
        const view = account.view();
        if (view.openOrders.length || Object.values(view.positions).some(position => position.long.qty > 0 || position.short.qty > 0)) {
          this.bus.publish("sys.alert", "core", { level: "critical", title: "Stopped strategy left exchange exposure",
            runId: id, accountId: account.id, mode: account.mode, orders: view.openOrders, positions: view.positions });
        }
        return this.summary(this.get(id));
      });
    } catch (error) {
      this.report(id, error);
      throw error;
    } finally { this.inflight.delete(id); }
  }

  private report(runId: string, error: unknown) {
    if (this.stopping) return;
    const retry = this.retryAt.get(runId) ?? 0;
    if (Date.now() < retry) return;
    this.retryAt.set(runId, Date.now() + 10_000);
    this.bus.publish("sys.alert", "core", { level: "critical", title: "Managed run requires reconciliation", runId, body: (error as Error).message });
  }

  async cleanOrphans() {
    const active = new Set(this.activeRows().map(row => row.run_id));
    const binding = "QUANT_SOCKET=" + socketPath(this.config.dataDir, "runtime");
    const children: number[] = [];
    for (const item of await readdir("/proc")) {
      if (!/^\d+$/.test(item) || Number(item) === process.pid) continue;
      const name = await readFile("/proc/" + item + "/comm", "utf8").catch(() => "");
      if (name.trim() !== basename(process.execPath)) continue;
      const env = await readFile("/proc/" + item + "/environ", "utf8").catch(() => "");
      const values = env.split("\0");
      if (!values.includes(binding) || !values.some(value => value.startsWith("QUANT_RUN_ID=") && active.has(value.slice(13)))) continue;
      try { process.kill(Number(item), "SIGKILL"); children.push(Number(item)); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      }
    }
    for (const pid of children) {
      for (let attempt = 0; attempt < 20; attempt++) {
        const env = await readFile("/proc/" + pid + "/environ", "utf8").catch(() => "");
        if (!env.split("\0").includes(binding)) break;
        if (attempt === 19) throw new Error("Could not confirm orphan exit for PID " + pid);
        await Bun.sleep(50);
      }
    }
    this.bus.db.query("UPDATE active_runs SET pid=NULL WHERE pid IS NOT NULL").run();
    for (const row of this.activeRows()) {
      if (row.status === "handover") continue;
      if (row.mode === "shadow") void this.stop(row.run_id, undefined, true).catch(error => this.report(row.run_id, error));
      else this.lifecycle(row, row.mode === "live" || row.persistent ? "starting" : "interrupted");
    }
  }

  recover() {
    if (this.stopping) return;
    const rows = this.activeRows().sort((a, b) => Number(b.status === "handover") - Number(a.status === "handover"));
    for (const row of rows) {
      const id = row.run_id;
      if (this.inflight.has(id) || this.children.has(id) || Date.now() < (this.retryAt.get(id) ?? 0)) continue;
      const account = row.account_id && row.mode !== "worker" ? this.orders.accounts.get(accountKey(row.mode, row.account_id)) : undefined;
      if (row.account_id && !account?.healthy().ready) continue;
      if (row.status === "handover") void this.finish(row).catch(() => {});
      else if (row.status === "starting") {
        this.inflight.add(id);
        void this.spawn(row).catch(error => this.report(id, error)).finally(() => this.inflight.delete(id));
      }
    }
  }

  async shutdown() {
    this.stopping = true;
    const children = [...this.children.values()];
    for (const child of children) child.kill("SIGTERM");
    await Promise.race([Promise.all(children.map(child => child.exited)), Bun.sleep(3100)]);
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    await Promise.all(children.map(child => child.exited));
  }
}
