import { randomBytes } from "node:crypto";
import type { OpenOrder, OrderIntentStatus, OrderRef } from "@quant/tools";
import type { AccountMode } from "../shared/config";
import { canonical } from "../shared/fs";
import { bad, Fault, flag, integer, missing, str, SYMBOL } from "../shared/http";
import { EventBus } from "./event-bus";
import { accountKey } from "./gateway";
import type { ExchangeAccount, AccountDirectory } from "./gateway";
import type { NormalizedOrder } from "./risk";
import { checkExposure, isClosing, validateFilters } from "./risk";

export class Serial {
  private readonly queues = new Map<string, Promise<unknown>>();
  async run<T>(key: string, task: () => T | Promise<T>): Promise<T> {
    const current = (this.queues.get(key) ?? Promise.resolve()).catch(() => {}).then(task);
    this.queues.set(key, current);
    try { return await current; } finally { if (this.queues.get(key) === current) this.queues.delete(key); }
  }
}

export type Owner = { source: "manual" } | { source: "strategy"; runId: string; epoch: number; strategyId?: string };
export type Intent = {
  mode: AccountMode; account_id: string; symbol: string; client_order_id: string; order_kind: OpenOrder["kind"];
  source: Owner["source"]; run_id: string | null; strategy_id: string | null; epoch: number | null;
  managing_run_id: string | null; exchange_client_id: string; exchange_order_id: string | null; algo_id: string | null;
  actual_order_id: string | null; status: OrderIntentStatus["status"]; request_json: string; result_json: string | null;
  created_at: number; updated_at: number;
};
const terminal = new Set(["FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH", "FINISHED"]);
export function orderTopic(mode: AccountMode, topic: string) {
  return (mode === "shadow" ? "shadow." : "trade.") + topic;
}
export function protective(order: OpenOrder, intent?: NormalizedOrder) {
  return isClosing(order.side, order.positionSide) &&
    (order.kind === "CONDITIONAL" || intent?.protection !== undefined || (intent?.kind === "CONDITIONAL"));
}

export function selection(orders: OpenOrder[], ref: OrderRef) {
  return orders.filter(order => (!ref.symbol || ref.symbol === order.symbol) &&
    ((ref.orderId && ref.orderId === order.exchangeOrderId) || (ref.algoId && ref.algoId === order.algoId) ||
      (ref.clientOrderId && (ref.clientOrderId === order.clientOrderId || ref.clientOrderId === order.exchangeClientId))));
}

export function reference(input: Record<string, unknown>): OrderRef {
  const fields = ["orderId", "algoId", "clientOrderId"] as const;
  if (fields.filter(key => input[key] != null).length !== 1) bad("Specify exactly one order reference");
  const result: OrderRef = {};
  for (const key of fields) if (input[key] != null) result[key] = str(input[key], key, /^[A-Za-z0-9_:/.-]{1,64}$/);
  if (input.symbol !== undefined) result.symbol = str(input.symbol, "symbol", SYMBOL);
  return result;
}

export class Orders {
  readonly serial = new Serial();
  private readonly timestamps = new Map<string, number[]>();
  constructor(readonly bus: EventBus, readonly accounts: AccountDirectory, readonly staleMs: number) {}

  account(mode: AccountMode, id: string) {
    return this.accounts.get(accountKey(mode, id)) ?? missing("Unknown " + mode + " account " + id);
  }

  killed(id: string) {
    return !!this.bus.db.query("SELECT 1 FROM kill_switches WHERE enabled=1 AND scope IN ('global',?)").get(id);
  }

  lookup(mode: AccountMode, id: string, symbol: string, clientId: string) {
    return this.bus.db.query<Intent, string[]>(
      "SELECT * FROM order_intents WHERE mode=? AND account_id=? AND symbol=? AND client_order_id=?").get(mode, id, symbol, clientId);
  }

  private existing(intent: Intent): OrderIntentStatus {
    if (intent.status === "unknown" || intent.status === "sending") {
      throw new Fault(503, "ORDER_UNKNOWN", "Order response is pending exchange verification", { exchangeClientId: intent.exchange_client_id });
    }
    const result = intent.result_json ? JSON.parse(intent.result_json) as { order?: OpenOrder; error?: { code: string; message: string; status: number } } : {};
    if (result.error) throw new Fault(result.error.status, result.error.code, result.error.message);
    return { clientOrderId: intent.client_order_id, exchangeClientId: intent.exchange_client_id, status: intent.status, order: result.order };
  }

  status(mode: AccountMode, id: string, symbol: string, clientId: string): OrderIntentStatus {
    const intent = this.lookup(mode, id, symbol, clientId) ?? missing("Order intent not found");
    return { clientOrderId: intent.client_order_id, exchangeClientId: intent.exchange_client_id, status: intent.status,
      order: intent.result_json ? (JSON.parse(intent.result_json) as { order?: OpenOrder }).order : undefined };
  }

  identify(account: ExchangeAccount, order: Partial<OpenOrder> & { symbol: string }): Intent | null {
    const { exchangeClientId, exchangeOrderId, algoId } = order;
    const db = this.bus.db;
    const direct = exchangeClientId ? db.query<Intent, [string, string, string, string]>(
      "SELECT * FROM order_intents WHERE mode=? AND account_id=? AND symbol=? AND exchange_client_id=?")
      .get(account.mode, account.id, order.symbol, exchangeClientId) : null;
    if (direct) return direct;
    if (algoId) {
      return db.query<Intent, string[]>("SELECT * FROM order_intents WHERE mode=? AND account_id=? AND symbol=? AND algo_id=?")
        .get(account.mode, account.id, order.symbol, algoId);
    }
    if (!exchangeOrderId) return null;
    return db.query<Intent, string[]>("SELECT * FROM order_intents WHERE mode=? AND account_id=? AND symbol=? AND exchange_order_id=?")
      .get(account.mode, account.id, order.symbol, exchangeOrderId) ??
      db.query<Intent, string[]>("SELECT * FROM order_intents WHERE mode=? AND account_id=? AND symbol=? AND actual_order_id=?")
        .get(account.mode, account.id, order.symbol, exchangeOrderId);
  }

  annotate(intent: Intent | null, order: OpenOrder): OpenOrder {
    if (!intent) return order;
    return { ...order, source: intent.source, runId: intent.run_id ?? undefined, strategyId: intent.strategy_id ?? undefined,
      epoch: intent.epoch ?? undefined, clientOrderId: intent.client_order_id };
  }

  updateFromExchange(account: ExchangeAccount, order: OpenOrder) {
    const intent = this.identify(account, order);
    if (!intent) return order;
    const tagged = this.annotate(intent, order);
    const previous = intent.result_json ? (JSON.parse(intent.result_json) as { order?: OpenOrder }).order : undefined;
    const updateResult = order.kind === intent.order_kind && (!previous ||
      (order.updatedAt >= previous.updatedAt && (!terminal.has(previous.status) || previous.status === order.status)));
    const triggeredOrderId = order.actualOrderId ?? (intent.order_kind === "CONDITIONAL" && order.kind === "STANDARD" ? order.exchangeOrderId : undefined);
    this.bus.db.query("UPDATE order_intents SET exchange_order_id=COALESCE(?,exchange_order_id),algo_id=COALESCE(?,algo_id)," +
      "actual_order_id=COALESCE(?,actual_order_id),updated_at=MAX(updated_at,?),status='accepted'," +
      "result_json=COALESCE(?,result_json) WHERE exchange_client_id=?")
      .run(intent.order_kind === "STANDARD" ? order.exchangeOrderId ?? null : null, order.algoId ?? null,
        triggeredOrderId ?? null, order.updatedAt, updateResult ? JSON.stringify({ order: tagged }) : null, intent.exchange_client_id);
    return tagged;
  }

  submit(mode: AccountMode, id: string, order: NormalizedOrder, owner: Owner, guard: () => void = () => {}) {
    return this.serial.run(accountKey(mode, id), async () => {
      const account = this.account(mode, id);
      guard();
      const json = canonical(order);
      let intent = this.lookup(mode, id, order.symbol, order.clientOrderId);
      if (intent) {
        if (intent.request_json !== json || intent.source !== owner.source) throw new Fault(409, "INTENT_CONFLICT", "Order key is bound to a different intent");
        if (intent.status !== "prepared") return this.existing(intent);
        if (owner.source === "strategy" && intent.run_id !== owner.runId) throw new Fault(409, "STALE_EPOCH", "Prepared order belongs to an earlier run");
      }
      try {
        if (!account.config.allowedSymbols.includes(order.symbol)) throw new Fault(422, "SYMBOL_NOT_ALLOWED", "Symbol is not enabled for the account");
        const closing = isClosing(order.side, order.positionSide);
        const rules = await account.rules(order.symbol, closing);
        guard();
        validateFilters(order, rules);
        if (!closing && (rules.status !== "TRADING" || rules.contractType !== "PERPETUAL")) {
          throw new Fault(422, "INVALID_ORDER", "Only trading perpetual contracts are accepted");
        }
        const pending = closing && order.kind === "STANDARD" ? this.unsettled(account).filter(row => row.symbol === order.symbol)
          .map(row => JSON.parse(row.request_json) as NormalizedOrder)
          .filter(prior => prior.kind === "STANDARD" && prior.side === order.side && prior.positionSide === order.positionSide)
          .reduce((total, prior) => total + prior.qty, 0) : 0;
        checkExposure(order, account, this.killed(id), this.staleMs, pending);
        if (!closing) {
          const key = accountKey(mode, id);
          const now = Date.now();
          const recent = (this.timestamps.get(key) ?? []).filter(ts => now - ts < 60_000);
          if (recent.length >= account.config.risk.maxOrdersPerMin) throw new Fault(429, "RATE_LIMITED", "Account order rate exceeded");
          this.timestamps.set(key, [...recent, now]);
        }
      } catch (error) {
        if (error instanceof Fault) this.bus.publish(orderTopic(mode, "risk_reject"), owner.source, {
          accountId: id, ...owner, symbol: order.symbol, clientOrderId: order.clientOrderId, code: error.code, reason: error.message,
        });
        throw error;
      }
      if (!intent) {
        const exchangeId = "qh_" + randomBytes(16).toString("hex");
        const runId = owner.source === "strategy" ? owner.runId : null;
        const now = Date.now();
        this.bus.commit(emit => {
          this.bus.db.query("INSERT INTO order_intents(mode,account_id,symbol,client_order_id,order_kind,source,exchange_client_id," +
            "request_json,run_id,strategy_id,epoch,managing_run_id,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'prepared',?,?)")
            .run(mode, id, order.symbol, order.clientOrderId, order.kind, owner.source, exchangeId, json, runId,
              owner.source === "strategy" ? owner.strategyId ?? null : null, owner.source === "strategy" ? owner.epoch : null, runId, now, now);
          emit(orderTopic(mode, "intent"), owner.source, { accountId: id, order, exchangeClientId: exchangeId, status: "prepared", ...owner });
        });
        intent = this.lookup(mode, id, order.symbol, order.clientOrderId)!;
      }
      const exchangeId = intent.exchange_client_id;
      this.transition(intent, "sending");
      let submitted: OpenOrder;
      try {
        submitted = await account.submit(order, exchangeId);
      } catch (error) {
        if (error instanceof Fault && error.code !== "ORDER_UNKNOWN" && error.code !== "UNAVAILABLE") {
          this.transition(intent, "rejected", { error: { status: error.status, code: error.code, message: error.message } });
          throw error;
        }
        const current = this.lookup(mode, id, order.symbol, order.clientOrderId)!;
        if (current.status === "accepted") return this.existing(current);
        this.transition(intent, "unknown");
        this.bus.publish("sys.alert", "core", { level: "critical", title: "Order result unknown", accountId: id, mode, symbol: order.symbol, exchangeClientId: exchangeId });
        throw new Fault(503, "ORDER_UNKNOWN", "No confirmed order result", { exchangeClientId: exchangeId });
      }
      const tagged = this.annotate(intent, submitted);
      const known = this.lookup(mode, id, order.symbol, order.clientOrderId)!;
      if (known.status !== "accepted") this.transition(intent, "accepted", { order: tagged });
      return this.existing(this.lookup(mode, id, order.symbol, order.clientOrderId)!);
    });
  }

  private transition(intent: Intent, status: Intent["status"], result?: { order?: OpenOrder; error?: Record<string, unknown> }) {
    this.bus.commit(emit => {
      const order = result?.order;
      this.bus.db.query("UPDATE order_intents SET status=?,result_json=?,updated_at=?,exchange_order_id=COALESCE(?,exchange_order_id)," +
        "algo_id=COALESCE(?,algo_id),actual_order_id=COALESCE(?,actual_order_id) WHERE exchange_client_id=?")
        .run(status, result ? JSON.stringify(result) : null, Date.now(), order?.exchangeOrderId ?? null, order?.algoId ?? null,
          order?.actualOrderId ?? null, intent.exchange_client_id);
      emit(orderTopic(intent.mode, "intent"), intent.source, {
        accountId: intent.account_id, symbol: intent.symbol, clientOrderId: intent.client_order_id, source: intent.source,
        status, exchangeClientId: intent.exchange_client_id,
      });
      if (order) emit(orderTopic(intent.mode, order.kind === "CONDITIONAL" ? "algo_order" : "order"), "account:" + intent.account_id,
        { ...order, accountId: intent.account_id, mode: intent.mode });
    }, result?.order ? 2 : 1);
  }

  unsettled(account: ExchangeAccount) {
    return this.bus.db.query<Intent, [string, string]>("SELECT * FROM order_intents WHERE mode=? AND account_id=? AND status IN ('sending','unknown')")
      .all(account.mode, account.id);
  }

  priorOpen(account: ExchangeAccount) {
    return this.bus.db.query<Intent, [string, string, number]>("SELECT * FROM order_intents WHERE mode=? AND account_id=? " +
      "AND status='accepted' AND updated_at>=? AND json_extract(result_json,'$.order.status') IN ('NEW','PARTIALLY_FILLED','TRIGGERING','TRIGGERED')")
      .all(account.mode, account.id, Date.now() - 90 * 86_400_000);
  }

  async reconcileIntents(account: ExchangeAccount) {
    for (const intent of this.unsettled(account)) {
      const order = await account.queryOrder(intent.symbol, intent.order_kind, intent.exchange_client_id);
      if (order) {
        this.transition(intent, "accepted", { order: this.annotate(intent, order) });
        continue;
      }
      const endpoint = intent.order_kind === "CONDITIONAL" ? "allAlgoOrders" : "allOrders";
      const row = this.bus.db.query<{ cursor_json: string }, string[]>("SELECT cursor_json FROM reconciliation_cursors WHERE mode=? AND account_id=? AND symbol=? AND endpoint=?")
        .get(account.mode, account.id, intent.symbol, endpoint);
      const cursor = row && (JSON.parse(row.cursor_json) as { through: number }).through;
      if (cursor && cursor >= intent.updated_at + 60_000 && Date.now() - intent.created_at < 7 * 86_400_000) {
        this.transition(intent, "rejected", { error: { status: 502, code: "ORDER_NOT_FOUND", message: "Order absent after exchange history and direct query reconciliation" } });
      }
    }
  }

  async settle(account: ExchangeAccount, runId: string) {
    const prepared = this.bus.db.query<Intent, [string, string, string]>(
      "SELECT * FROM order_intents WHERE mode=? AND account_id=? AND run_id=? AND status='prepared'").all(account.mode, account.id, runId);
    for (const intent of prepared) this.transition(intent, "rejected", {
      error: { status: 409, code: "STALE_EPOCH", message: "Old run stopped before the order was sent" },
    });
    await this.reconcileIntents(account);
    if (this.unsettled(account).length) throw new Fault(503, "ORDER_UNKNOWN", "Unverified orders block account handover");
  }

  owned(account: ExchangeAccount, runId: string) {
    return account.view().openOrders.flatMap(order => {
      const intent = this.identify(account, order);
      return intent?.source === "strategy" && intent.managing_run_id === runId ? [{ order: this.annotate(intent, order), intent }] : [];
    });
  }

  reassign(account: ExchangeAccount, oldRun: string, newRun: string) {
    for (const { intent } of this.owned(account, oldRun)) {
      this.bus.db.query("UPDATE order_intents SET managing_run_id=? WHERE exchange_client_id=?").run(newRun, intent.exchange_client_id);
    }
  }

  cancel(mode: AccountMode, id: string, input: Record<string, unknown>, guard: () => void = () => {}) {
    const ref = reference(input);
    return this.serial.run(accountKey(mode, id), async () => {
      guard();
      const account = this.account(mode, id);
      let matches = selection(account.view().openOrders, ref);
      if (!matches.length) {
        await account.refresh();
        guard();
        matches = selection(account.view().openOrders, ref);
      }
      if (matches.length !== 1) return matches.length ? bad("Order reference is ambiguous. Specify symbol") : missing("No matching open order");
      return account.cancel(matches[0]);
    });
  }

  cancelAll(mode: AccountMode, id: string, input: Record<string, unknown>, guard: () => void = () => {}) {
    const symbol = input.symbol === undefined ? undefined : str(input.symbol, "symbol", SYMBOL);
    const conditional = flag(input.includeConditional, "includeConditional");
    return this.serial.run(accountKey(mode, id), async () => {
      guard();
      const account = this.account(mode, id);
      const orders = account.view().openOrders;
      const symbols = symbol ? [symbol] : [...new Set([...account.config.allowedSymbols, ...orders.map(order => order.symbol)])];
      for (const ticker of symbols) if (!account.config.allowedSymbols.includes(ticker) && !orders.some(order => order.symbol === ticker)) {
        throw new Fault(422, "SYMBOL_NOT_ALLOWED", "Symbol is not configured");
      }
      for (const ticker of symbols) { guard(); await account.cancelAll(ticker, conditional); }
      const affected = orders.filter(order => symbols.includes(order.symbol));
      return { canceledStandard: affected.filter(order => order.kind === "STANDARD").length,
        canceledConditional: conditional ? affected.filter(order => order.kind === "CONDITIONAL").length : 0 };
    });
  }

  setLeverage(mode: AccountMode, id: string, input: Record<string, unknown>, symbol: string, guard: () => void = () => {}) {
    return this.serial.run(accountKey(mode, id), async () => {
      guard();
      const account = this.account(mode, id);
      const ticker = str(input.symbol ?? symbol, "symbol", SYMBOL);
      if (!account.config.allowedSymbols.includes(ticker)) throw new Fault(422, "SYMBOL_NOT_ALLOWED", "Symbol is not configured");
      const leverage = integer(input.leverage, "leverage", 1, account.config.risk.maxLeverage);
      await account.leverage(ticker, leverage);
      return { symbol: ticker, leverage };
    });
  }
}
