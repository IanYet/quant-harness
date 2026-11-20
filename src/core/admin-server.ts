import { randomBytes, randomUUID } from "node:crypto";
import type { Client, ServiceAuth } from "../shared/auth";
import { socketPath } from "../shared/config";
import type { AccountMode } from "../shared/config";
import { CONSUMER_TOPICS } from "../shared/events";
import { canonical, sha256 } from "../shared/fs";
import { body, Fault, flag, handler, ID, integer, missing, oneOf, queryNumber, str } from "../shared/http";
import { ndjson, serveUnix } from "../shared/uds";
import type { Markets } from "./gateway";
import type { Emit } from "./event-bus";
import { normalizeOrder } from "./risk";
import type { NormalizedOrder } from "./risk";
import { reference } from "./orders";
import { Supervisor } from "./supervisor";

type Source = Extract<Client, "cli" | "notifier" | "mcp">;
type Actor = { source: Source; actor: string; confirmationRef?: string };
type Target = { accountId?: string; mode?: AccountMode; runId?: string; baseRunId?: string | null; plan?: Awaited<ReturnType<Supervisor["livePlan"]>>;
  command?: "submit" | "cancel" | "cancel-all" | "leverage"; order?: NormalizedOrder; args?: Record<string, unknown> };
type Operation = { operationId: string; action: string; reason?: string; target: Target };
type Proposal = { proposal_id: string; digest: string; operation_json: string; expires_at: number; status: string; result_json: string | null };

export class Control {
  constructor(readonly supervisor: Supervisor) {}
  private get bus() { return this.supervisor.bus; }

  audit(emit: Emit, op: Operation, identity: Actor, result: string) {
    emit("sys.operation", identity.source, { ...op, ...identity, result });
  }

  private describe(row: Proposal) {
    const status = row.status === "pending" && row.expires_at <= Date.now() ? "expired" : row.status;
    const op = JSON.parse(row.operation_json) as Operation;
    return { proposalId: row.proposal_id, proposalDigest: row.digest, expiresAt: row.expires_at, status,
      action: op.action, target: op.target, reason: op.reason, result: row.result_json ? JSON.parse(row.result_json) : null };
  }

  proposal(id: string) {
    const row = this.bus.db.query<Proposal, [string]>("SELECT * FROM proposals WHERE proposal_id=?").get(id);
    if (!row) return missing("Unknown proposal");
    return this.describe(row);
  }

  submit(op: Operation, actor: Actor, approval: boolean) {
    if (!approval) return this.execute(op, actor);
    const proposalId = randomUUID();
    const expiresAt = Date.now() + 15 * 60_000;
    const proposalDigest = sha256(canonical({ proposalId, expiresAt, op }));
    this.bus.commit(emit => {
      this.bus.db.query("INSERT INTO proposals VALUES (?,?,?,?,'pending',NULL)").run(proposalId, proposalDigest, JSON.stringify(op), expiresAt);
      this.audit(emit, op, actor, "pending_approval");
      emit("sys.proposal", actor.source, { proposalId, proposalDigest, expiresAt, action: op.action, target: op.target, reason: op.reason });
    }, 2);
    return Response.json({ status: "pending_approval", proposalId, proposalDigest }, { status: 202 });
  }

  decide(id: string, approved: boolean, input: Record<string, unknown>, actor: Actor) {
    const op = this.bus.commit(emit => {
      const row = this.bus.db.query<Proposal, [string]>("SELECT * FROM proposals WHERE proposal_id=?").get(id) ?? missing("Unknown proposal");
      if (row.status !== "pending" || row.expires_at <= Date.now() || (approved && row.digest !== input.proposalDigest)) {
        throw new Fault(409, "PROPOSAL_CONFLICT", "Proposal has expired, was decided, or has a different digest");
      }
      const op = JSON.parse(row.operation_json) as Operation;
      this.bus.db.query("UPDATE proposals SET status=? WHERE proposal_id=?").run(approved ? "executing" : "rejected", id);
      this.audit(emit, { ...op, action: approved ? "approve" : "reject",
        reason: input.reason === undefined ? op.reason : str(input.reason, "reason", /^[\s\S]{1,1000}$/) },
        { ...actor, confirmationRef: actor.confirmationRef ?? id }, approved ? "approved" : "rejected");
      return op;
    });
    return approved ? this.execute(op, { ...actor, confirmationRef: actor.confirmationRef ?? id }, id) : this.proposal(id);
  }

  private async execute(op: Operation, actor: Actor, proposalId?: string) {
    const { supervisor } = this;
    const { orders, config } = supervisor;
    const { accountId, mode = "live", runId, plan, command, order, args, baseRunId } = op.target;
    try {
      let result: unknown;
      if (op.action === "live_start" || op.action === "live_replace") result = await supervisor.live(accountId!, plan!, baseRunId ?? null);
      else if (op.action === "live_stop" || op.action === "run_stop") result = await supervisor.stop(runId!);
      else {
        if (command === "submit") result = await orders.submit(mode, accountId!, order!, { source: "manual" });
        if (command === "cancel") result = await orders.cancel(mode, accountId!, args!);
        if (command === "cancel-all") result = await orders.cancelAll(mode, accountId!, args!);
        if (command === "leverage") result = await orders.setLeverage(mode, accountId!, args!, config.defaultSymbol);
      }
      this.bus.commit(emit => {
        if (proposalId) this.bus.db.query("UPDATE proposals SET status='executed',result_json=? WHERE proposal_id=?")
          .run(JSON.stringify(result ?? null), proposalId);
        this.audit(emit, op, actor, "executed");
        if (op.action === "manual_trade") emit("sys.alert", "manual", {
          level: "warn", title: "Manual trade operation executed", accountId, mode, actor: actor.actor, command, result, reason: op.reason,
        });
      }, op.action === "manual_trade" ? 2 : 1);
      return result;
    } catch (error) {
      const failure = { code: error instanceof Fault ? error.code : "INTERNAL_ERROR", message: (error as Error).message };
      this.bus.commit(emit => {
        if (proposalId) this.bus.db.query("UPDATE proposals SET status='failed',result_json=? WHERE proposal_id=?")
          .run(JSON.stringify(failure), proposalId);
        this.audit(emit, op, actor, failure.code);
      });
      throw error;
    }
  }

  async recorded<T>(op: Operation, actor: Actor, run: () => Promise<T>) {
    try {
      const result = await run();
      this.bus.commit(emit => this.audit(emit, op, actor, "executed"));
      return result;
    } catch (error) {
      this.bus.commit(emit => this.audit(emit, op, actor, error instanceof Fault ? error.code : "INTERNAL_ERROR"));
      throw error;
    }
  }

  recover() {
    for (const proposal of this.bus.db.query<Proposal, []>("SELECT * FROM proposals WHERE status='executing'").all()) {
      this.bus.commit(emit => {
        this.bus.db.query("UPDATE proposals SET status='interrupted',result_json=? WHERE proposal_id=?")
          .run(JSON.stringify({ code: "INTERRUPTED", message: "Core restarted during this operation. Inspect the run or order intent." }), proposal.proposal_id);
        emit("sys.alert", "core", { level: "critical", title: "Approval execution interrupted", proposalId: proposal.proposal_id });
      });
    }
  }
}

export async function startAdmin(control: Control, auth: ServiceAuth, markets: Markets, health: () => Record<string, unknown>,
    reload: () => Promise<Record<string, unknown>>) {
  const supervisor = control.supervisor;
  const { config, bus, orders } = supervisor;
  return serveUnix(socketPath(config.dataDir, "admin"), handler(async (req, url) => {
    const method = req.method;
    const path = url.pathname;
    if (method === "GET" && path === "/v1/health") return health();
    if (method === "GET" && path === "/v1/market/snapshot") return markets.snapshot(url.searchParams.get("symbol") ?? config.defaultSymbol);
    if (method === "GET" && path.startsWith("/v1/market/rest/")) return markets.rest(path.slice(16), url.searchParams);
    if (path === "/v1/stream" || path === "/v1/stream/ack") {
      const consumer = auth.require(req, ["storage", "notifier", "agent"]) as keyof typeof CONSUMER_TOPICS;
      if (path === "/v1/stream" && method === "GET") {
        const subscription = bus.follow({ topics: CONSUMER_TOPICS[consumer], after: queryNumber(url, "after_seq", 0) });
        return ndjson(subscription.frames, subscription.close);
      }
      if (path === "/v1/stream/ack" && method === "POST") {
        bus.ack(consumer, integer((await body(req)).throughSeq, "throughSeq"));
        return { acknowledged: true };
      }
    }
    if (method === "POST" && path === "/v1/agent/task-update") {
      auth.require(req, ["agent"]);
      const data = await body(req);
      return { seq: bus.publish("agent.task_update", "agent", data).seq };
    }

    const source = auth.require(req, ["cli", "notifier", "mcp", "watchdog"]);
    if (method === "GET") {
      if (path === "/v1/accounts") return [...orders.accounts.values()].map(account => ({
        accountId: account.id, mode: account.mode, ...account.config, ...account.healthy(),
        lease: supervisor.lease(account.mode, account.id), killed: orders.killed(account.id),
      }));
      const account = /^\/v1\/accounts\/([A-Za-z0-9_-]+)\/state$/.exec(path);
      if (account) return (orders.accounts.get("live:" + account[1]) ?? orders.account("shadow", account[1])).view();
      if (path.startsWith("/v1/state/")) {
        const namespace = decodeURIComponent(path.slice(10));
        return supervisor.checkpoints.read(namespace) ?? missing("Checkpoint not found");
      }
      if (path === "/v1/runs") return supervisor.list().filter(run =>
        (!url.searchParams.has("mode") || run.mode === url.searchParams.get("mode")) &&
        (!url.searchParams.has("status") || run.status === url.searchParams.get("status")));
      if (path.startsWith("/v1/proposals/")) return control.proposal(str(path.slice(14), "proposalId", ID));
    }
    if (source === "watchdog") throw new Fault(403, "FORBIDDEN", "Watchdog uses its independent emergency policy");
    const input = await body(req);
    const actor: Actor = {
      source,
      actor: str(input.actor ?? req.headers.get("x-quant-actor") ?? source, "actor", /^[\s\S]{1,160}$/),
      confirmationRef: input.confirmationRef === undefined ? undefined : str(input.confirmationRef, "confirmationRef", /^[\s\S]{1,200}$/),
    };
    const reason = input.reason === undefined ? undefined : str(input.reason, "reason", /^[\s\S]{1,1000}$/);
    const parent = req.headers.get("x-quant-parent-task");
    const parentTaskId = parent ? str(parent, "parentTaskId", ID) : undefined;
    const operation = (action: string, target: Target): Operation => ({ action, target, operationId: randomUUID(), reason });

    if (method === "POST" && path === "/v1/runs/start") return control.recorded(operation("run_start", { args: input }), actor,
      () => supervisor.start(input, parentTaskId));
    if (method === "POST" && path === "/v1/live/propose") {
      const id = str(input.accountId, "accountId", ID);
      const account = orders.account("live", id);
      const baseRunId = supervisor.lease("live", id)?.run_id ?? null;
      const plan = await supervisor.livePlan(input, parentTaskId);
      return control.submit(operation(baseRunId ? "live_replace" : "live_start", { accountId: id, baseRunId, plan }),
        actor, account.config.requireApproval);
    }
    const stop = /^\/v1\/runs\/([A-Za-z0-9_-]+)\/stop$/.exec(path);
    if (method === "POST" && stop) {
      const row = supervisor.get(stop[1]);
      const approval = row.mode === "live" && orders.account("live", row.account_id!).config.requireApprovalOnStop;
      return control.submit(operation(row.mode === "live" ? "live_stop" : "run_stop", { runId: row.run_id, accountId: row.account_id ?? undefined }),
        actor, approval && ["starting", "active"].includes(row.status));
    }
    const manual = /^\/v1\/accounts\/([A-Za-z0-9_-]+)\/manual-(order|orders|leverage)$/.exec(path);
    if (manual) {
      const id = manual[1];
      const account = orders.accounts.get("live:" + id) ?? orders.account("shadow", id);
      let target: Target | undefined;
      if (manual[2] === "order" && method === "POST") {
        str(reason, "reason");
        target = { accountId: id, command: "submit", order: normalizeOrder(input, config.defaultSymbol, randomBytes(8).toString("hex")) };
      }
      if (manual[2] === "order" && method === "DELETE") target = { accountId: id, command: "cancel", args: reference(input) };
      if (manual[2] === "orders" && method === "DELETE") target = { accountId: id, command: "cancel-all",
        args: { symbol: input.symbol, includeConditional: flag(input.includeConditional, "includeConditional") } };
      if (manual[2] === "leverage" && method === "POST") target = { accountId: id, command: "leverage",
        args: { symbol: input.symbol, leverage: integer(input.leverage, "leverage", 1, account.config.risk.maxLeverage) } };
      if (target) return control.submit(operation("manual_trade", { ...target, mode: account.mode }), actor, account.config.requireApprovalOnManualTrade);
    }
    const decision = /^\/v1\/proposals\/([A-Za-z0-9_-]+)\/(approve|reject)$/.exec(path);
    if (method === "POST" && decision) return control.decide(decision[1], decision[2] === "approve", input, actor);

    if ((method === "POST" || method === "DELETE") && path === "/v1/risk/kill-switch") {
      const scope = str(input.scope, "scope", ID);
      if (scope !== "global" && ![...orders.accounts.values()].some(account => account.id === scope)) missing("Unknown kill-switch scope");
      const enabled = method === "POST";
      const cancel = enabled && flag(input.cancelOpenOrders, "cancelOpenOrders");
      const includeConditional = flag(input.includeConditional, "includeConditional");
      const op = operation("kill_switch", { args: { scope, enabled, cancel, includeConditional } });
      bus.commit(emit => {
        bus.db.query("INSERT INTO kill_switches VALUES (?,?,?) ON CONFLICT(scope) DO UPDATE SET enabled=excluded.enabled,updated_at=excluded.updated_at")
          .run(scope, Number(enabled), Date.now());
        emit("sys.kill_switch", source, { scope, enabled, cancel, includeConditional, reason, actor: actor.actor });
        control.audit(emit, op, actor, enabled ? "enabled" : "disabled");
      }, 2);
      if (!cancel) return { scope, enabled };
      const cancellations = await Promise.all([...orders.accounts.values()].filter(account => scope === "global" || account.id === scope)
        .map(async account => {
          try { return { accountId: account.id, ...await orders.cancelAll(account.mode, account.id, { includeConditional }) }; }
          catch (error) {
            bus.publish("sys.alert", "core", { level: "critical", title: "Kill switch cancellation incomplete", accountId: account.id, body: (error as Error).message });
            return { accountId: account.id, error: (error as Error).message };
          }
        }));
      bus.commit(emit => control.audit(emit, op, actor, cancellations.some(item => "error" in item) ? "cancel_incomplete" : "cancel_complete"));
      return { scope, enabled, cancellations };
    }
    if (method === "POST" && path === "/v1/reload") return control.recorded(operation("reload", {}), actor, reload);
    if (method === "POST" && path === "/v1/admin/check") return { source: oneOf(source, "source", ["cli", "mcp", "notifier"]) };
    return missing("Unknown admin endpoint");
  }));
}
