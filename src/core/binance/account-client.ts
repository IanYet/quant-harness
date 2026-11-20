import type { AccountFullState, OpenOrder, PositionLeg, Positions } from "@quant/tools";
import type { AccountConfig, AccountMode, BinanceConfig } from "../../shared/config";
import { Fault } from "../../shared/http";
import { canonical, sha256 } from "../../shared/fs";
import { legs } from "../gateway";
import type { ExchangeAccount } from "../gateway";
import { orderTopic, Orders } from "../orders";
import type { Emit } from "../event-bus";
import { isClosing } from "../risk";
import type { NormalizedOrder } from "../risk";
import { PublicMarket } from "./public-ws";
import { BinanceError, BinanceRest, exchangeJson, WeightBucket } from "./signer";

type Row = Record<string, unknown>;
type Snapshot = Omit<AccountFullState, "openOrders" | "reconciling">;
const DAY = 86_400_000;
const terminal = new Set(["FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH", "FINISHED"]);
const keyFor = (order: OpenOrder) => order.symbol + ":" + (order.kind === "STANDARD" ? "order:" + order.exchangeOrderId : "algo:" + order.algoId);
const n = (value: unknown) => Number(value ?? 0);
const optional = (value: unknown) => value === undefined || value === null || value === "" ? undefined : Number(value);
const id = (value: unknown) => value === undefined || value === null || value === "" || value === 0 || value === "0" ? undefined : String(value);

function decodeOrder(row: Row, kind: OpenOrder["kind"], clock = Date.now()): OpenOrder {
  const standard = kind === "STANDARD";
  const current = row.updateTime ?? row.T ?? row.time ?? clock;
  const clientAlgoId = id(row.clientAlgoId ?? row.caid);
  const status = String(row.algoStatus ?? row.status ?? row.X ?? "NEW");
  return {
    kind, symbol: String(row.symbol ?? row.s), orderType: String(row.orderType ?? row.type ?? row.o) as OpenOrder["orderType"],
    side: String(row.side ?? row.S) as OpenOrder["side"], positionSide: String(row.positionSide ?? row.ps) as OpenOrder["positionSide"],
    status, source: "external", qty: n(row.origQty ?? row.quantity ?? row.q), price: optional(row.price ?? row.p),
    triggerPrice: optional(row.triggerPrice ?? row.tp), filledQty: n(row.executedQty ?? row.z),
    exchangeOrderId: standard ? id(row.orderId ?? row.i) : undefined,
    algoId: standard ? undefined : id(row.algoId ?? row.aid),
    actualOrderId: standard ? undefined : id(row.actualOrderId ?? row.ai),
    exchangeClientId: standard ? id(row.clientOrderId ?? row.c) : clientAlgoId,
    clientAlgoId, createdAt: n(row.createTime ?? row.time ?? current), updatedAt: n(current),
  };
}

export class BinanceAccount implements ExchangeAccount {
  readonly client: BinanceRest;
  readonly env: "mainnet" | "testnet";
  private readonly wsBase: string;
  private socket?: WebSocket;
  private state: Snapshot;
  private positionTimes = new Map<string, number>();
  private ordersCache = new Map<string, OpenOrder>();
  private privateWsAt = 0;
  private balanceAt = 0;
  private streamReady = false;
  private configured = false;
  private hedge = false;
  private oneWayExposure = false;
  private shadowed?: Row[];
  private refreshing?: Promise<void>;
  private tuning = new Set<string>();
  private error?: string;
  private readonly abort = new AbortController();

  constructor(readonly mode: AccountMode, readonly id: string, public config: AccountConfig, endpoints: BinanceConfig,
    keys: { apiKey: string; apiSecret: string }, bucket: WeightBucket, private readonly market: PublicMarket, private readonly orders: Orders) {
    this.env = mode === "live" ? "mainnet" : "testnet";
    this.client = new BinanceRest(mode === "live" ? endpoints.mainnetRest : endpoints.testnetRest, bucket, keys);
    this.wsBase = mode === "live" ? endpoints.mainnetWs : endpoints.testnetWs;
    this.state = { accountId: id, mode, walletBalance: 0, availableBalance: 0, unrealizedPnl: 0, positions: {}, leverages: {} };
    this.tuning = new Set(config.allowedSymbols);
  }

  private get db() { return this.orders.bus.db; }
  private signed<T>(path: string, params: Record<string, string | number | boolean | undefined> = {},
      method = "GET", weight = 1, priority = false, uncertainWrite = false) {
    return this.client.request<T>("/fapi/" + path, params, { method, signed: true, weight, priority, uncertainWrite, signal: this.abort.signal });
  }

  rules(symbol: string, priority = false) {
    if (!priority && this.tuning.has(symbol)) throw new Fault(503, "RECONCILING", "New symbol configuration is not verified");
    return this.market.restProxy.rules(symbol, this.env, priority);
  }
  mark(symbol: string) { return this.market.mark(symbol, this.env); }

  healthy() {
    return { ready: this.streamReady && this.configured && this.hedge, privateWsAt: this.privateWsAt, error: this.error };
  }

  view(): AccountFullState {
    return { ...this.state, positions: { ...this.state.positions }, leverages: { ...this.state.leverages },
      openOrders: [...this.ordersCache.values()].filter(order => !terminal.has(order.status))
        .map(order => this.orders.annotate(this.orders.identify(this, order), order)),
      reconciling: !this.healthy().ready,
    };
  }

  private info(topic: string, payload: Record<string, unknown>, emit: Emit, ts = Date.now()) {
    emit(orderTopic(this.mode, topic), "account:" + this.id, structuredClone({ ...payload, accountId: this.id, mode: this.mode }), ts);
  }

  private acceptKeys(key: string, ts: number) {
    return this.db.query("INSERT INTO private_event_keys VALUES (?,?) ON CONFLICT DO NOTHING")
      .run(this.mode + ":" + this.id + ":" + key, ts).changes > 0;
  }

  private recordOrder(order: OpenOrder, raw: Row) {
    const key = keyFor(order);
    if (key.endsWith(":undefined")) throw new Error("Binance order has no ID");
    const previous = this.ordersCache.get(key);
    const future = this.orders.bus.commit(emit => this.persistOrder(order, raw, emit));
    if (!previous || (order.updatedAt >= previous.updatedAt && (!terminal.has(previous.status) || terminal.has(order.status)))) {
      this.ordersCache.set(key, future);
      if (this.ordersCache.size > 2500) {
        for (const [id, old] of this.ordersCache) {
          if (terminal.has(old.status)) this.ordersCache.delete(id);
          if (this.ordersCache.size < 2000) break;
        }
      }
    }
    return future;
  }

  private persistOrder(order: OpenOrder, raw: Row, emit: Emit) {
    const key = keyFor(order);
    if (key.endsWith(":undefined")) throw new Error("Binance order has no ID");
    const tagged = this.orders.updateFromExchange(this, order);
    const owner = tagged.runId ?? tagged.source;
    const eventKey = ["order", key, order.updatedAt, order.status, order.filledQty, order.actualOrderId, owner].join(":");
    if (this.acceptKeys(eventKey, order.updatedAt)) this.info(order.kind === "STANDARD" ? "order" : "algo_order", {
      ...tagged, avgPrice: optional(raw.avgPrice ?? raw.ap), callbackRate: optional(raw.callbackRate ?? raw.cr),
      workingType: raw.workingType ?? raw.wt,
    }, emit, order.updatedAt);
    return tagged;
  }

  private persistFill(row: Row, ts: number, emit: Emit) {
    const symbol = String(row.symbol ?? row.s);
    const tradeId = id(row.id ?? row.t);
    const exchangeOrderId = id(row.orderId ?? row.i);
    const rawClientId = id(row.c);
    const qty = n(row.qty ?? row.l);
    if (!tradeId || !exchangeOrderId || !(qty > 0)) return;
    const intent = this.orders.identify(this, { symbol, exchangeOrderId, exchangeClientId: rawClientId });
    const payload = {
      symbol, tradeId, exchangeOrderId, qty, ts, side: row.side ?? row.S, positionSide: row.positionSide ?? row.ps,
      source: intent?.source ?? "external", clientOrderId: intent?.client_order_id, runId: intent?.run_id, strategyId: intent?.strategy_id,
      price: n(row.price ?? row.L), fee: n(row.commission ?? row.n), feeAsset: row.commissionAsset ?? row.N ?? "USDT",
      realizedPnl: n(row.realizedPnl ?? row.rp),
    };
    const key = ["fill", symbol, tradeId, intent?.exchange_client_id ?? "external"].join(":");
    if (this.acceptKeys(key, ts)) this.info("fill", payload, emit, ts);
  }

  private income(row: Row, emit: Emit) {
    const ts = n(row.time);
    const key = ["income", row.incomeType, row.tranId].join(":");
    if (!this.acceptKeys(key, ts)) return;
    this.info("income", { incomeType: row.incomeType, tranId: String(row.tranId), symbol: row.symbol || null,
      amount: row.income, asset: row.asset, ts }, emit, ts);
  }

  private frame(message: Row) {
    const ts = n(message.T ?? message.E ?? Date.now());
    if (message.e === "ORDER_TRADE_UPDATE") {
      const o = message.o as Row;
      this.recordOrder(decodeOrder(o, "STANDARD", ts), o);
      if (o.x === "TRADE" || Number(o.l) > 0) this.orders.bus.commit(emit => this.persistFill(o, n(o.T ?? ts), emit));
      return;
    }
    if (message.e === "ALGO_UPDATE") {
      const o = message.o as Row;
      this.recordOrder(decodeOrder(o, "CONDITIONAL", ts), o);
      return;
    }
    if (message.e !== "ACCOUNT_UPDATE") return;
    const update = message.a as { B?: Row[]; P?: Row[] };
    for (const value of update.P ?? []) this.applyPosition(value, ts);
    const balances = update.B?.find(balance => balance.a === "USDT");
    if (balances && ts >= this.balanceAt) {
      this.balanceAt = ts;
      this.state.walletBalance = n(balances.wb);
    }
    this.state.unrealizedPnl = Object.values(this.state.positions).reduce((total, p) => total + p.long.unrealizedPnl + p.short.unrealizedPnl, 0);
    this.orders.bus.commit(emit => {
      if (!this.acceptKeys("account:" + sha256(canonical(message)), ts)) return;
      if (update.P?.length) this.info("position", { positions: this.state.positions }, emit, ts);
      this.info("account", { ...this.state }, emit, ts);
    }, 2);
  }

  private applyPosition(raw: Row, ts: number, positions: Positions = this.state.positions, times = this.positionTimes, leverages = this.state.leverages) {
    const symbol = String(raw.symbol ?? raw.s);
    const side = raw.positionSide ?? raw.ps;
    if (side !== "LONG" && side !== "SHORT") return;
    const key = symbol + ":" + side;
    const when = n(raw.updateTime ?? ts) || ts;
    if (when < (times.get(key) ?? 0)) return;
    const pair = legs(positions, symbol, leverages[symbol]);
    const old = pair[side === "LONG" ? "long" : "short"];
    const position: PositionLeg = {
      symbol, side, qty: Math.abs(n(raw.positionAmt ?? raw.pa)), leverage: n(raw.leverage ?? leverages[symbol] ?? old.leverage),
      entryPrice: n(raw.entryPrice ?? raw.ep), markPrice: n(raw.markPrice ?? this.mark(symbol)?.price ?? old.markPrice),
      unrealizedPnl: n(raw.unRealizedProfit ?? raw.up), liquidationPrice: n(raw.liquidationPrice ?? old.liquidationPrice),
    };
    pair[side === "LONG" ? "long" : "short"] = position;
    positions[symbol] = pair;
    times.set(key, when);
  }

  refresh() {
    if (!this.refreshing) this.refreshing = this.fetchState().finally(() => { this.refreshing = undefined; });
    return this.refreshing;
  }

  private async fetchState() {
    const before = Date.now();
    const pending: Row[] = [];
    const wasBuffering = this.shadowed;
    if (!wasBuffering) this.shadowed = pending;
    try {
      const [account, positions, open, algos] = await Promise.all([
        this.signed<Row>("v2/account", {}, "GET", 5),
        this.signed<Row[]>("v2/positionRisk", {}, "GET", 5),
        this.signed<Row[]>("v1/openOrders", {}, "GET", 40),
        this.signed<Row[]>("v1/openAlgoOrders", {}, "GET", 40),
      ]);
      if (!Array.isArray(positions) || !Array.isArray(open) || !Array.isArray(algos)) throw new Error("Incomplete exchange snapshot");
      const leverages = { ...this.state.leverages };
      const allPositions: Positions = Object.create(null);
      this.positionTimes = new Map();
      this.oneWayExposure = positions.some(raw => raw.positionSide === "BOTH" && n(raw.positionAmt) !== 0);
      for (const raw of positions) {
        if (Number(raw.leverage) > 0) leverages[String(raw.symbol)] = Number(raw.leverage);
        this.applyPosition(raw, before, allPositions, this.positionTimes, leverages);
      }
      this.state = { ...this.state, positions: allPositions, leverages, walletBalance: n(account.totalWalletBalance),
        availableBalance: n(account.availableBalance), unrealizedPnl: n(account.totalUnrealizedProfit), marginType: this.config.marginType };
      this.balanceAt = before;
      const alive = new Map<string, OpenOrder>();
      for (const [kind, rows] of [["CONDITIONAL", algos], ["STANDARD", open]] as const) {
        for (const row of rows) {
          const order = this.recordOrder(decodeOrder(row, kind, before), row);
          alive.set(keyFor(order), order);
        }
      }
      this.ordersCache = alive;
      this.orders.bus.commit(emit => this.info("account", { ...this.state }, emit));
    } finally {
      if (!wasBuffering) {
        this.shadowed = undefined;
        for (const row of pending) this.frame(row);
      }
    }
  }

  private async initialize() {
    const mode = await this.signed<{ dualSidePosition: boolean }>("v1/positionSide/dual", {}, "GET", 30);
    this.hedge = mode.dualSidePosition === true;
    await this.refresh();
    if (!this.hedge) {
      if (this.oneWayExposure || this.view().openOrders.length || Object.values(this.state.positions).some(pair => pair.long.qty || pair.short.qty)) {
        throw new Fault(422, "ACCOUNT_INCOMPATIBLE", "Hedge mode is disabled while the account has exchange exposure");
      }
      await this.signed("v1/positionSide/dual", { dualSidePosition: true }, "POST");
      this.hedge = true;
    }
    await this.tuneSymbols();
  }

  private async tuneSymbols() {
    for (const ticker of [...this.tuning]) {
      if (!this.config.allowedSymbols.includes(ticker)) { this.tuning.delete(ticker); continue; }
      const rules = await this.market.restProxy.rules(ticker, this.env);
      if (rules.contractType !== "PERPETUAL" || rules.status !== "TRADING") {
        throw new Fault(422, "ACCOUNT_INCOMPATIBLE", "Symbol is not a trading perpetual: " + ticker);
      }
      const target = this.config;
      const configs = await this.signed<Row[]>("v1/symbolConfig", { symbol: ticker }, "GET", 5);
      const row = configs.find(item => item.symbol === ticker);
      if (!row) throw new Fault(503, "RECONCILING", "Missing symbol configuration for " + ticker);
      const pair = legs(this.state.positions, ticker);
      const busy = !!(pair.long.qty || pair.short.qty || this.view().openOrders.some(order => order.symbol === ticker));
      if (row.marginType !== target.marginType) {
        if (busy) throw new Fault(422, "ACCOUNT_INCOMPATIBLE", "Unexpected margin mode for " + ticker + " with open risk");
        try { await this.signed("v1/marginType", { symbol: ticker, marginType: target.marginType }, "POST"); }
        catch (error) { if (!(error instanceof BinanceError && error.binanceCode === -4046)) throw error; }
      }
      const desired = Math.min(target.initialLeverage, target.risk.maxLeverage);
      this.setLeverage(ticker, n(row.leverage));
      if (!busy && n(row.leverage) !== desired) await this.leverage(ticker, desired);
      this.tuning.delete(ticker);
    }
    this.configured = true;
  }

  reload(next: AccountConfig) {
    for (const ticker of next.allowedSymbols) if (!this.config.allowedSymbols.includes(ticker)) this.tuning.add(ticker);
    for (const ticker of this.tuning) if (!next.allowedSymbols.includes(ticker)) this.tuning.delete(ticker);
    this.config = next;
    if (this.streamReady && this.tuning.size) {
      void this.orders.serial.run(this.mode + ":" + this.id, async () => {
        await this.refresh();
        await this.tuneSymbols();
      }).catch(error => this.report(error));
    }
  }

  private window(symbol: string, endpoint: string) {
    return this.db.query<{ cursor_json: string }, string[]>("SELECT cursor_json FROM reconciliation_cursors WHERE mode=? AND account_id=? AND symbol=? AND endpoint=?")
      .get(this.mode, this.id, symbol, endpoint);
  }

  private cursor(symbol: string, endpoint: string, through: number) {
    this.db.query("INSERT INTO reconciliation_cursors VALUES (?,?,?,?,?) ON CONFLICT(mode,account_id,symbol,endpoint) " +
      "DO UPDATE SET cursor_json=excluded.cursor_json WHERE json_extract(excluded.cursor_json,'$.through')>=json_extract(reconciliation_cursors.cursor_json,'$.through')")
      .run(this.mode, this.id, symbol, endpoint, JSON.stringify({ through }));
  }

  private async history(endpoint: "allAlgoOrders" | "allOrders" | "userTrades", symbol: string) {
    const end = Date.now();
    const previous = this.window(symbol, endpoint);
    const through = previous ? (JSON.parse(previous.cursor_json) as { through: number }).through : end - DAY;
    let start = Math.max(end - 89 * DAY, through - 60_000);
    const load = async (from: number, to: number): Promise<void> => {
      const rows = await this.signed<Row[]>("v1/" + endpoint, { symbol, startTime: from, endTime: to, limit: 1000 }, "GET", 5);
      if (rows.length >= 1000) {
        if (from === to) throw new Fault(503, "RECONCILING", "History exceeded pagination capacity at one exchange timestamp");
        const middle = Math.floor((from + to) / 2);
        await load(from, middle);
        await load(middle + 1, to);
        return;
      }
      this.orders.bus.commit(emit => {
        for (const row of rows) {
          if (endpoint === "userTrades") this.persistFill(row, n(row.time), emit);
          else this.persistOrder(decodeOrder(row, endpoint === "allOrders" ? "STANDARD" : "CONDITIONAL"), row, emit);
        }
        this.cursor(symbol, endpoint, to);
      }, Math.max(1, rows.length));
    };
    while (start < end) {
      const stop = Math.min(end, start + 7 * DAY - 1);
      await load(start, stop);
      start = stop + 1;
    }
  }

  private async incomes() {
    const endpoint = "income";
    const end = Date.now();
    const old = this.window("*", endpoint);
    let start = Math.max(end - 89 * DAY, (old ? (JSON.parse(old.cursor_json) as { through: number }).through : end - DAY) - 60_000);
    while (start < end) {
      const to = Math.min(end, start + 7 * DAY - 1);
      for (let page = 1; ; page++) {
        const limit = 1000;
        const rows = await this.signed<Row[]>("v1/income", { startTime: start, endTime: to, page, limit }, "GET", 30);
        this.orders.bus.commit(emit => {
          for (const row of rows) this.income(row, emit);
          if (rows.length < limit) this.cursor("*", endpoint, to);
        }, Math.max(1, rows.length));
        if (rows.length < limit) break;
      }
      start = to + 1;
    }
  }

  private userStream<T>(method: string, listenKey?: string) {
    return this.client.request<T>("/fapi/v1/listenKey", { listenKey }, { method, apiKeyOnly: true, priority: true, signal: this.abort.signal });
  }

  private async connect() {
    const { listenKey } = await this.userStream<{ listenKey: string }>("POST");
    if (!listenKey || this.abort.signal.aborted) return;
    const url = this.wsBase + "/private/ws?listenKey=" + encodeURIComponent(listenKey) +
      "&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/ALGO_UPDATE";
    const socket = this.socket = new WebSocket(url);
    this.shadowed = [];
    let opened = false;
    let markOpen!: () => void;
    const open = new Promise<void>(resolve => { markOpen = resolve; });
    let markClosed!: () => void;
    const closed = new Promise<void>(resolve => { markClosed = resolve; });
    socket.onopen = () => { opened = true; this.privateWsAt = Date.now(); markOpen(); };
    socket.onerror = () => socket.close();
    socket.onclose = () => { this.streamReady = false; markClosed(); };
    socket.onmessage = event => {
      this.privateWsAt = Date.now();
      try {
        const raw = exchangeJson(String(event.data), true) as Row;
        const row = (raw.data ?? raw) as Row;
        if (row.e === "listenKeyExpired" || raw.code !== undefined) return socket.close();
        if (this.shadowed) {
          if (this.shadowed.length >= 20_000) return socket.close();
          this.shadowed.push(row);
        } else this.frame(row);
      } catch (error) { this.report(error); socket.close(); }
    };
    const expire = setTimeout(() => socket.close(), 23 * 3600_000 + 50 * 60_000);
    const keep = setInterval(() => {
      void this.userStream("PUT", listenKey).then(() => { this.privateWsAt = Date.now(); }).catch(() => socket.close());
    }, 25 * 60_000);
    let maintenance: ReturnType<typeof setInterval> | undefined;
    try {
      await Promise.race([open, closed, Bun.sleep(10_000)]);
      if (!opened || socket.readyState !== WebSocket.OPEN) throw new Fault(503, "UNAVAILABLE", "Binance private stream did not connect");
      await this.initialize();
      const captured = [...new Set([...this.config.allowedSymbols, ...this.view().openOrders.map(order => order.symbol),
        ...this.orders.unsettled(this).map(order => order.symbol)])];
      for (const symbol of captured) {
        for (const endpoint of ["allAlgoOrders", "allOrders", "userTrades"] as const) await this.history(endpoint, symbol);
        if (socket.readyState !== WebSocket.OPEN) throw new Error("Private stream disconnected during history replay");
      }
      await this.incomes();
      await this.orders.reconcileIntents(this);
      for (const intent of this.orders.priorOpen(this)) {
        const checked = await this.queryOrder(intent.symbol, intent.order_kind, intent.exchange_client_id);
        if (checked) this.recordOrder(checked, {});
      }
      await this.refresh();
      const pending = this.shadowed ?? [];
      this.shadowed = undefined;
      for (const frame of pending) this.frame(frame);
      if (socket.readyState !== WebSocket.OPEN) throw new Error("Private stream disconnected during reconciliation");
      this.streamReady = true;
      this.error = undefined;
      let running = false;
      maintenance = setInterval(() => {
        if (running || !this.streamReady) return;
        running = true;
        void this.orders.serial.run(this.mode + ":" + this.id, async () => {
          if (this.tuning.size) await this.tuneSymbols();
          await this.incomes();
          for (const symbol of new Set(this.orders.unsettled(this).map(intent => intent.symbol))) {
            await this.history("allAlgoOrders", symbol);
            await this.history("allOrders", symbol);
            await this.history("userTrades", symbol);
          }
          await this.orders.reconcileIntents(this);
          await this.refresh();
        }).catch(error => this.report(error)).finally(() => { running = false; });
      }, 120_000);
      await closed;
      if (!this.abort.signal.aborted) throw new Error("Binance private stream disconnected");
    } finally {
      clearTimeout(expire);
      clearInterval(keep);
      clearInterval(maintenance);
      this.streamReady = false;
      this.shadowed = undefined;
      socket.close();
      this.socket = undefined;
    }
  }

  private report(error: unknown) {
    if (this.abort.signal.aborted) return;
    const message = (error as Error).message;
    if (message === this.error) return;
    this.error = message;
    this.orders.bus.publish("sys.alert", "account:" + this.id, { level: "warn", title: "Binance account needs reconciliation",
      mode: this.mode, accountId: this.id, body: message });
  }

  async run() {
    let attempt = 0;
    while (!this.abort.signal.aborted) {
      const before = Date.now();
      try { await this.connect(); } catch (error) { this.report(error); }
      if (Date.now() - before > 60_000) attempt = 0;
      if (!this.abort.signal.aborted) await Bun.sleep([1000, 2000, 5000, 15_000][Math.min(attempt++, 3)]);
    }
  }

  close() { this.abort.abort(); this.socket?.close(); }

  async submit(order: NormalizedOrder, exchangeId: string) {
    if (!this.hedge) throw new Fault(422, "ACCOUNT_INCOMPATIBLE", "Hedge mode has not been verified");
    const common = { symbol: order.symbol, side: order.side, positionSide: order.positionSide, type: order.type,
      quantity: order.qty, price: "price" in order ? order.price : undefined,
      timeInForce: "timeInForce" in order ? order.timeInForce : undefined };
    const priority = isClosing(order.side, order.positionSide);
    let raw: Row;
    if (order.kind === "CONDITIONAL") {
      raw = await this.signed<Row>("v1/algoOrder", { ...common, algoType: "CONDITIONAL", clientAlgoId: exchangeId,
        workingType: "workingType" in order ? order.workingType : undefined, triggerPrice: "triggerPrice" in order ? order.triggerPrice : undefined,
        callbackRate: "callbackRate" in order ? order.callbackRate : undefined, activatePrice: "activationPrice" in order ? order.activationPrice : undefined,
        priceProtect: ["STOP_MARKET", "TAKE_PROFIT_MARKET"].includes(order.type) && "priceProtect" in order ? order.priceProtect : undefined,
      }, "POST", 1, priority, true);
    } else raw = await this.signed<Row>("v1/order", { ...common, newClientOrderId: exchangeId }, "POST", 1, priority, true);
    return this.recordOrder(decodeOrder(raw, order.kind), raw);
  }

  async queryOrder(symbol: string, kind: OpenOrder["kind"], exchangeId: string) {
    try {
      const raw = kind === "STANDARD" ? await this.signed<Row>("v1/order", { symbol, origClientOrderId: exchangeId }, "GET", 1, true) :
        await this.signed<Row>("v1/algoOrder", { clientAlgoId: exchangeId }, "GET", 1, true);
      return decodeOrder(raw, kind);
    } catch (error) {
      if (error instanceof BinanceError && [-2011, -2013].includes(error.binanceCode ?? 0)) return null;
      throw error;
    }
  }

  async cancel(order: OpenOrder) {
    const conditional = order.kind === "CONDITIONAL";
    try {
      const raw = conditional ? await this.signed<Row>("v1/algoOrder", { algoId: order.algoId }, "DELETE", 1, true, true) :
        await this.signed<Row>("v1/order", { symbol: order.symbol, orderId: order.exchangeOrderId }, "DELETE", 1, true, true);
      const next = conditional ? { ...order, status: "CANCELED", updatedAt: Date.now() } : decodeOrder(raw, order.kind);
      return this.recordOrder(next, raw);
    } catch (error) {
      if (!(error instanceof BinanceError && [-2011, -2013].includes(error.binanceCode ?? 0))) throw error;
      await this.refresh();
      if (this.view().openOrders.some(item => keyFor(item) === keyFor(order))) throw error;
      return { ...order, status: "NOT_OPEN" };
    }
  }

  async cancelAll(symbol: string, conditional: boolean) {
    await this.signed("v1/allOpenOrders", { symbol }, "DELETE", 1, true, true);
    if (conditional) await this.signed("v1/algoOpenOrders", { symbol }, "DELETE", 1, true, true);
    await this.refresh();
  }

  private setLeverage(symbol: string, value: number) {
    this.state.leverages[symbol] = value;
    const existing = this.state.positions[symbol];
    if (existing) this.state.positions[symbol] = { long: { ...existing.long, leverage: value }, short: { ...existing.short, leverage: value } };
  }

  async leverage(symbol: string, value: number) {
    const raw = await this.signed<{ leverage: number }>("v1/leverage", { symbol, leverage: value }, "POST", 1);
    this.setLeverage(symbol, raw.leverage);
  }
}
