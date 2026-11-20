import { randomUUID } from "node:crypto";
import type { ServiceAuth } from "../shared/auth";
import { socketPath } from "../shared/config";
import type { AccountMode } from "../shared/config";
import { MARKET_TOPICS, TRADING_TOPICS } from "../shared/events";
import type { BusEvent } from "../shared/events";
import { bad, body, Fault, handler, ID, integer, missing, oneOf, queryFlag, queryNumber, record, str } from "../shared/http";
import { notification } from "../shared/notify";
import { call, ndjson, serveUnix, withQuery } from "../shared/uds";
import type { Markets } from "./gateway";
import { legs } from "./gateway";
import { orderTopic } from "./orders";
import { normalizeOrder } from "./risk";
import { Supervisor } from "./supervisor";
import type { Session } from "./supervisor";

const forbidden = (msg: string): never => { throw new Fault(403, "FORBIDDEN", msg); };

export async function fills(supervisor: Supervisor, auth: ServiceAuth, mode: AccountMode, accountId: string, url: URL) {
  const after = queryNumber(url, "after_seq", 0);
  const through = queryNumber(url, "through_seq", supervisor.bus.watermark);
  const symbol = url.searchParams.get("symbol");
  if (after > through) bad("after_seq exceeds through_seq");
  const topic = orderTopic(mode, "fill");
  const group = new Map<string, BusEvent>();
  const stored = await call<Record<string, unknown>[]>(socketPath(supervisor.config.dataDir, "storage"), withQuery("/v1/ledger/query", {
    type: "fills", mode, accountId, symbol, after_seq: after, through_seq: through, limit: 10_000,
  }), { token: auth.token("core") });
  for (const row of stored) {
    const { event_seq, ts, symbol, price, qty, side, fee, source } = row;
    const payload = { accountId, mode, symbol, price, qty, side, fee, source, ts, positionSide: row.position_side,
      feeAsset: row.fee_asset, realizedPnl: row.realized_pnl, tradeId: row.trade_id, exchangeOrderId: row.exchange_order_id,
      clientOrderId: row.client_order_id, runId: row.run_id, strategyId: row.strategy_id };
    group.set(symbol + ":" + row.trade_id, { seq: Number(event_seq), ts: Number(ts), topic, source: "account:" + accountId, payload });
  }
  let cursor = after;
  for (;;) {
    const page = supervisor.bus.replay(topic, cursor, through);
    if (!page.length) break;
    for (const event of page) {
      const p = event.payload;
      if (p.accountId !== accountId || (symbol && p.symbol !== symbol)) continue;
      const key = p.symbol + ":" + p.tradeId;
      if (!group.has(key)) group.set(key, event);
    }
    cursor = page[page.length - 1].seq;
    if (page.length < 1000) break;
    await Bun.sleep(0);
  }
  return [...group.values()].sort((a, b) => a.seq - b.seq).slice(0, 10_000);
}

export async function startRuntime(supervisor: Supervisor, markets: Markets, auth: ServiceAuth) {
  const { config, bus, orders } = supervisor;
  const usage = new Map<string, { minuteAt: number; notices: number; secondAt: number; samples: number }>();
  function use(runId: string, kind: "notices" | "samples") {
    const now = Date.now();
    const item = usage.get(runId) ?? { minuteAt: now, secondAt: now, notices: 0, samples: 0 };
    if (now - item.minuteAt >= 60_000) { item.minuteAt = now; item.notices = 0; }
    if (now - item.secondAt >= 1000) { item.secondAt = now; item.samples = 0; }
    usage.set(runId, item);
    if (item[kind] >= (kind === "notices" ? 10 : 50)) throw new Fault(429, "RATE_LIMITED", "Run exceeds its " + kind + " rate");
    item[kind]++;
  }
  const cleanup = setInterval(() => {
    const active = new Set([...supervisor.sessions.values()].map(session => session.runId));
    for (const runId of usage.keys()) if (!active.has(runId)) usage.delete(runId);
  }, 60_000);
  cleanup.unref();
  const accountAllowed = (session: Session, id: string) =>
    session.mode === "worker" && (session.caps.accountRead === "*" || session.caps.accountRead?.includes(id));

  return serveUnix(socketPath(config.dataDir, "runtime"), handler(async (req, url) => {
    const { pathname: path } = url;
    const method = req.method;
    const header = req.headers.get("authorization");
    const session = header?.startsWith("Bearer ") ? supervisor.sessions.get(header.slice(7)) : undefined;
    if (header && !session) throw new Fault(401, "UNAUTHORIZED", "Invalid run token");
    if (method === "GET" && path === "/v1/market/snapshot") {
      const env = oneOf(url.searchParams.get("env") ?? "mainnet", "environment", ["mainnet", "testnet"]);
      if (env === "testnet" && session?.mode !== "shadow") forbidden("Only shadow runs may inspect Demo marks");
      return markets.snapshot(url.searchParams.get("symbol") ?? config.defaultSymbol, env);
    }
    if (method === "GET" && path.startsWith("/v1/market/rest/")) return markets.rest(path.slice(16), url.searchParams);
    if (method === "GET" && path === "/v1/market/fast-stream") {
      let specs: unknown;
      try { specs = JSON.parse(url.searchParams.get("streams") ?? "null"); } catch { bad("Invalid streams"); }
      return markets.fast(specs, req.signal);
    }
    if (!session) throw new Fault(401, "UNAUTHORIZED", "A run token is required");
    const row = supervisor.guard(session, false);

    if (method === "POST" && path === "/v1/session/ready") return supervisor.ready(session);
    if (method === "GET" && path === "/v1/handover") {
      const h = supervisor.handover(row);
      return h ? { handoverId: h.handoverId, action: h.action, orders: h.orders } : null;
    }
    if (method === "POST" && path === "/v1/handover/complete") return supervisor.complete(session, await body(req));
    if (method === "GET" && path === "/v1/state") return supervisor.checkpoints.read(session.namespace);
    if (method === "PUT" && path === "/v1/state") return supervisor.save(session, await body(req));

    if (method === "GET" && path === "/v1/stream") {
      const topics = (url.searchParams.get("topics") ?? "").split(",").filter(Boolean);
      if (!topics.length || topics.length > 32) bad("Specify event topics");
      const accounts = url.searchParams.get("accounts")?.split(",");
      if (accounts && session.mode === "worker" && accounts.some(id => id !== "*" && !accountAllowed(session, id))) {
        forbidden("Account is outside the worker's read scope");
      }
      for (const topic of topics) {
        if (MARKET_TOPICS.includes(topic) || topic === "data.sample" || topic === "sys.handover") continue;
        const [group, event] = topic.split(".");
        if (!TRADING_TOPICS.includes(event) || topic !== group + "." + event) forbidden("Topic is not available");
        const accountRead = session.caps.accountRead;
        if (session.mode === "worker") {
          if (group !== "trade" || !(accountRead === "*" || accountRead?.length)) forbidden("Worker has no account topic access");
        } else if (group !== (session.mode === "shadow" ? "shadow" : "trade")) forbidden("Cannot observe another account mode");
      }
      const symbols = url.searchParams.get("symbols")?.split(",");
      const intervals = url.searchParams.get("intervals")?.split(",");
      const dataset = url.searchParams.get("dataset");
      const subscription = bus.follow({ topics, after: url.searchParams.has("after_seq") ? queryNumber(url, "after_seq", 0) : undefined,
        filter(event) {
          const p = event.payload;
          if (event.topic === "sys.handover") return p.runId === session.runId;
          if (!supervisor.sessions.has(session.token)) return false;
          if (event.topic.startsWith("market.") && event.source !== "binance.mainnet") return false;
          if (event.topic.startsWith("trade.") || event.topic.startsWith("shadow.")) {
            const id = String(p.accountId);
            if (session.mode === "worker" ? !accountAllowed(session, id) : id !== session.accountId) return false;
            if (accounts && !accounts.includes("*") && !accounts.includes(id)) return false;
          }
          return (!symbols || p.symbol === undefined || symbols.includes(String(p.symbol))) &&
            (!intervals || p.interval === undefined || intervals.includes(String(p.interval))) &&
            (!dataset || p.dataset === dataset);
        },
      });
      return ndjson(subscription.frames, subscription.close);
    }

    if (method === "GET" && path === "/v1/accounts") {
      if (session.mode !== "worker" || !session.caps.accountRead ||
          (Array.isArray(session.caps.accountRead) && !session.caps.accountRead.length)) forbidden("Account observation requires accountRead");
      return [...orders.accounts.values()].filter(account => account.mode === "live" && accountAllowed(session, account.id))
        .map(account => ({ accountId: account.id, mode: account.mode, allowedSymbols: account.config.allowedSymbols,
          ...account.healthy(), lease: supervisor.lease(account.mode, account.id) }));
    }
    const accountPath = /^\/v1\/accounts\/([A-Za-z0-9_-]+)\/(state|fills)$/.exec(path);
    if (method === "GET" && accountPath) {
      const [, id, operation] = accountPath;
      if (!accountAllowed(session, id)) forbidden("Account is outside the worker's read scope");
      const account = orders.account("live", id);
      return operation === "state" ? account.view() : fills(supervisor, auth, "live", id, url);
    }

    if (path.startsWith("/v1/trade/")) {
      if (session.mode === "worker" || !session.accountId) return forbidden("Workers cannot access trade endpoints");
      const { mode, accountId } = session;
      const account = orders.account(mode, accountId);
      const symbol = url.searchParams.get("symbol") ?? config.defaultSymbol;
      const guard = () => supervisor.guard(session);
      if (method === "GET") {
        if (path === "/v1/trade/order") return orders.status(mode, accountId, symbol, str(url.searchParams.get("clientOrderId"), "clientOrderId"));
        if (path === "/v1/trade/fills") return fills(supervisor, auth, mode, accountId, url);
        const view = account.view();
        if (path === "/v1/trade/account") {
          const { walletBalance, availableBalance, unrealizedPnl, marginType, leverages } = view;
          return { walletBalance, availableBalance, unrealizedPnl, marginType, leverages };
        }
        if (path === "/v1/trade/positions") return symbol === "*" ? view.positions : legs(view.positions, symbol, view.leverages[symbol]);
        if (path === "/v1/trade/open-orders") {
          const conditional = queryFlag(url, "includeConditional", true);
          return view.openOrders.filter(order => (conditional || order.kind === "STANDARD") &&
            (!url.searchParams.has("symbol") || order.symbol === symbol));
        }
      }
      if (method === "POST" && path === "/v1/trade/order") return orders.submit(mode, accountId,
        normalizeOrder(await body(req), config.defaultSymbol),
        { source: "strategy", runId: session.runId, epoch: session.epoch, strategyId: (JSON.parse(row.target_json) as { strategyId?: string }).strategyId }, guard);
      if (method === "DELETE" && path === "/v1/trade/order") return orders.cancel(mode, accountId, await body(req), guard);
      if (method === "DELETE" && path === "/v1/trade/orders") return orders.cancelAll(mode, accountId, await body(req), guard);
      if (method === "POST" && path === "/v1/trade/leverage") return orders.setLeverage(mode, accountId, await body(req), config.defaultSymbol, guard);
    }

    if (method === "POST" && path === "/v1/data/publish") {
      const input = await body(req);
      supervisor.guard(session);
      const dataset = str(input.dataset, "dataset", ID);
      if (!session.caps.publishDatasets?.includes(dataset)) forbidden("Dataset is not declared for this run");
      if (Buffer.byteLength(JSON.stringify(input)) > 64 * 1024) throw new Fault(422, "SAMPLE_TOO_LARGE", "Dataset sample exceeds 64 KiB");
      const payload = record(input.payload);
      const ts = integer(input.ts, "ts");
      const key = input.key === undefined ? undefined : str(input.key, "key", /^[\s\S]{1,256}$/);
      const ttlSec = integer(input.ttlSec ?? 300, "ttlSec", 1, 31_536_000);
      use(session.runId, "samples");
      const event = bus.publish("data.sample", "run:" + session.runId, { dataset, payload, ts, key, ttlSec, ingestedTs: Date.now(), runId: session.runId });
      return { seq: event.seq };
    }

    if (method === "POST" && path === "/v1/notify") {
      const note = notification(await body(req));
      supervisor.guard(session);
      if (session.caps.notify === false) forbidden("Run cannot send notifications");
      use(session.runId, "notices");
      const event = bus.publish("sys.notify", "run:" + session.runId, { ...note, runId: session.runId, accountId: session.accountId });
      return Response.json({ queued: true, seq: event.seq }, { status: 202 });
    }

    if (path.startsWith("/v1/agents/tasks")) {
      if (session.caps.agentTask === false) forbidden("Run cannot start agent tasks");
      if (method === "POST" && path === "/v1/agents/tasks") {
        const input = await body(req);
        supervisor.guard(session);
        const taskId = randomUUID();
        const prompt = str(input.prompt, "prompt", /^[\s\S]{1,20000}$/);
        const dedupKey = str(input.dedupKey, "dedupKey", /^[\s\S]{1,160}$/);
        const title = input.title === undefined ? undefined : str(input.title, "title", /^[\s\S]{1,160}$/);
        const backend = input.backend === undefined ? undefined : str(input.backend, "backend", ID);
        const visibility = input.visibility === undefined ? undefined : oneOf(input.visibility, "visibility", ["visible", "hidden", "ephemeral"]);
        const cooldownSec = input.cooldownSec === undefined ? undefined : integer(input.cooldownSec, "cooldownSec", 0, 86_400);
        const context = input.context === undefined ? undefined : record(input.context);
        if (context && Buffer.byteLength(JSON.stringify(context)) > 32_000) bad("Agent context exceeds 32 KB");
        bus.commit(emit => {
          const e = emit("agent.request", "run:" + session.runId, { taskId, runId: session.runId,
            sourceScope: session.namespace, parentTaskId: row.parent_task_id, prompt, dedupKey, title, backend, visibility, cooldownSec, context });
          bus.db.query("INSERT INTO task_requests VALUES (?,?,?)").run(taskId, session.runId, e.seq);
        });
        return Response.json({ taskId }, { status: 202 });
      }
      if (method === "GET" && path.startsWith("/v1/agents/tasks/")) {
        const taskId = str(path.slice(17), "taskId", ID);
        if (!bus.db.query("SELECT 1 FROM task_requests WHERE task_id=? AND run_id=?").get(taskId, session.runId)) forbidden("Task is outside this run");
        try {
          return await call(socketPath(config.dataDir, "agent"), "/v1/tasks/" + taskId, { token: auth.token("core") });
        } catch (error) {
          if (error instanceof Fault && [404, 503].includes(error.status)) return { taskId, status: "queued" };
          throw error;
        }
      }
    }
    return missing("Unknown runtime endpoint");
  }));
}
