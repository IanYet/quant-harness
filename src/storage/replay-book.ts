import type { FundingRate, Kline, OpenOrder, OrderIntentStatus, Positions } from "@quant/tools";
import { Fault, missing } from "../shared/http";
import { canonical } from "../shared/fs";
import { legs } from "../core/gateway";
import { isClosing } from "../core/risk";
import type { NormalizedOrder } from "../core/risk";

type State = { params: NormalizedOrder; public: OpenOrder; submitted: number; activation?: number; extreme?: number; triggered?: boolean };
type Settings = { initialBalance: number; makerFee: number; takerFee: number; slippageBps: number; triggerProtect: Record<string, number> };
type Emit = (topic: string, payload: Record<string, unknown>) => void;

export class ReplayBook {
  readonly positions: Positions = Object.create(null);
  readonly leverages: Record<string, number> = Object.create(null);
  private readonly orders = new Map<string, State>();
  private count = 0;
  private tradeId = 0;
  private cash: number;
  private fees = 0;
  private realized = 0;
  private funding = 0;
  private trades = 0;
  private wins = 0;
  private losses = 0;
  private profit = 0;
  private loss = 0;
  private peak: number;
  private drawdown = 0;
  private reference = new Map<string, { trade: number; mark?: number }>();

  constructor(readonly settings: Settings, readonly emit: Emit) { this.cash = this.peak = settings.initialBalance; }

  private open(order: State) { return ["NEW", "TRIGGERED"].includes(order.public.status); }
  openOrders() { return [...this.orders.values()].filter(order => this.open(order)).map(order => order.public); }

  private leg(order: NormalizedOrder) { return legs(this.positions, order.symbol, this.leverages[order.symbol])[order.positionSide === "LONG" ? "long" : "short"]; }
  private key(symbol: string, clientId: string) { return symbol + ":" + clientId; }
  private status(o: State): OrderIntentStatus {
    return { clientOrderId: o.params.clientOrderId, exchangeClientId: o.public.exchangeClientId!, status: "accepted", order: o.public };
  }

  get(symbol: string, clientOrderId: string) { return this.status(this.orders.get(this.key(symbol, clientOrderId)) ?? missing("Order intent not found")); }

  submit(p: NormalizedOrder, now: number) {
    const key = this.key(p.symbol, p.clientOrderId);
    const prior = this.orders.get(key);
    if (prior) {
      if (canonical(prior.params) !== canonical(p)) throw new Fault(409, "INTENT_CONFLICT", "Order key is bound to another intent");
      return this.status(prior);
    }
    if (isClosing(p.side, p.positionSide)) {
      const reserved = p.kind === "CONDITIONAL" ? 0 : [...this.orders.values()].filter(order =>
        this.open(order) && (order.params.kind === "STANDARD" || order.triggered) && order.params.symbol === p.symbol &&
        order.params.positionSide === p.positionSide && order.params.side === p.side).reduce((sum, order) => sum + order.params.qty, 0);
      if (p.qty > this.leg(p).qty - reserved + 1e-12) throw new Fault(422, "RISK_REJECTED", "Requested close exceeds the unreserved leg");
    }
    const conditional = p.kind === "CONDITIONAL";
    const price = "price" in p ? p.price : undefined;
    const ref = this.reference.get(p.symbol);
    if ("timeInForce" in p && p.timeInForce === "GTX" && p.kind === "STANDARD" && price && ref &&
      (p.side === "BUY" ? price >= ref.trade : price <= ref.trade)) {
      throw new Fault(422, "INVALID_ORDER", "Post-only limit crosses the last known price");
    }
    if ("priceProtect" in p && p.priceProtect && this.settings.triggerProtect[p.symbol] === undefined) {
      throw new Fault(422, "UNSUPPORTED_BACKTEST", "priceProtect requires a historical triggerProtect parameter for the symbol");
    }
    const exchangeId = String(++this.count);
    const order: State = {
      params: p, submitted: now,
      public: { kind: p.kind, symbol: p.symbol, side: p.side, positionSide: p.positionSide, orderType: p.type, status: "NEW", qty: p.qty,
        price, triggerPrice: "triggerPrice" in p ? p.triggerPrice : undefined, source: "strategy", clientOrderId: p.clientOrderId,
        exchangeOrderId: conditional ? undefined : exchangeId, algoId: conditional ? exchangeId : undefined,
        exchangeClientId: exchangeId, createdAt: now, updatedAt: now },
    };
    if (p.type === "TRAILING_STOP_MARKET") {
      const known = p.workingType === "MARK_PRICE" ? ref?.mark : ref?.trade;
      if (p.activationPrice && known && (p.side === "BUY" ? p.activationPrice >= known : p.activationPrice <= known)) {
        throw new Fault(422, "INVALID_ORDER", "Trailing stop activation would immediately trigger");
      }
      order.activation = p.activationPrice ?? known;
    }
    this.orders.set(key, order);
    return this.status(order);
  }

  cancel(order: OpenOrder, now: number) {
    const o = this.orders.get(this.key(order.symbol, order.clientOrderId!))!;
    o.public = { ...o.public, status: "CANCELED", updatedAt: now };
    return o.public;
  }

  private trigger(o: State, signal: Kline, trade: Kline) {
    const p = o.params;
    const buy = p.side === "BUY";
    if ("priceProtect" in p && p.priceProtect && Math.abs(signal.c - trade.c) / trade.c > this.settings.triggerProtect[p.symbol]) return null;
    if (p.type === "TRAILING_STOP_MARKET") {
      o.activation ??= signal.o;
      if (o.extreme === undefined && (buy ? signal.l > o.activation : signal.h < o.activation)) return null;
      const old = o.extreme;
      const move = p.callbackRate / 100;
      const previous = old === undefined ? undefined : old * (buy ? 1 + move : 1 - move);
      if (previous !== undefined && (buy ? signal.h >= previous : signal.l <= previous)) {
        return { atOpen: buy ? signal.o >= previous : signal.o <= previous, price: previous };
      }
      o.extreme = buy ? Math.min(signal.l, old ?? Infinity) : Math.max(signal.h, old ?? -Infinity);
      const trigger = o.extreme * (buy ? 1 + move : 1 - move);
      return (buy ? signal.c >= trigger : signal.c <= trigger) ? { atOpen: false, price: trigger } : null;
    }
    if (!("triggerPrice" in p)) return null;
    const rising = p.type.startsWith("STOP") === buy;
    if (rising ? signal.h < p.triggerPrice : signal.l > p.triggerPrice) return null;
    return { atOpen: rising ? signal.o >= p.triggerPrice : signal.o <= p.triggerPrice, price: p.triggerPrice };
  }

  advance(bar: Kline, mark?: Kline) {
    const eligible = [...this.orders.values()].filter(order => this.open(order) && order.params.symbol === bar.symbol && order.submitted < bar.openTime);
    for (const state of eligible) {
      const p = state.params;
      const buy = p.side === "BUY";
      if (p.kind === "CONDITIONAL" && !state.triggered) {
        const signal = "workingType" in p && p.workingType === "MARK_PRICE" ? mark : bar;
        if (!signal || ("priceProtect" in p && p.priceProtect && !mark)) throw new Fault(422, "HISTORY_MISSING", "Historical mark bar is required");
        const checked = "priceProtect" in p && p.priceProtect && mark && Math.abs(mark.c - bar.c) / bar.c > this.settings.triggerProtect[p.symbol] ? null :
          this.trigger(state, signal, bar);
        if (!checked) continue;
        state.triggered = true;
        state.public = { ...state.public, status: "TRIGGERED", updatedAt: bar.closeTime, actualOrderId: "trigger-" + state.public.algoId };
        this.emit("trade.algo_order", { ...state.public });
        if ("price" in p) continue;
        const marketPrice = checked.atOpen ? bar.o : signal === bar ? checked.price : bar.c;
        this.match(state, marketPrice, false, bar.closeTime);
        continue;
      }
      if (p.type === "MARKET") this.match(state, bar.o, false, bar.openTime);
      else if ("price" in p) {
        const throughOpen = buy ? bar.o <= p.price : bar.o >= p.price;
        const touched = buy ? bar.l <= p.price : bar.h >= p.price;
        const tif = p.timeInForce ?? "GTC";
        if (throughOpen && tif === "GTX") this.cancel(state.public, bar.openTime);
        else if (throughOpen || (touched && !["IOC", "FOK"].includes(tif))) this.match(state, throughOpen ? bar.o : p.price, !throughOpen, bar.closeTime);
        else if (["IOC", "FOK"].includes(tif)) this.cancel(state.public, bar.openTime);
      }
    }
    this.reference.set(bar.symbol, { trade: bar.c, mark: mark?.c });
    const pair = legs(this.positions, bar.symbol, this.leverages[bar.symbol]);
    for (const leg of [pair.long, pair.short]) {
      leg.markPrice = mark?.c ?? bar.c;
      leg.unrealizedPnl = (leg.markPrice - leg.entryPrice) * leg.qty * (leg.side === "LONG" ? 1 : -1);
    }
    this.positions[bar.symbol] = pair;
    this.drawdownStep();
  }

  private match(state: State, basePrice: number, maker: boolean, ts: number) {
    const p = state.params;
    const pair = legs(this.positions, p.symbol, this.leverages[p.symbol]);
    const leg = pair[p.positionSide === "LONG" ? "long" : "short"];
    const closing = isClosing(p.side, p.positionSide);
    const qty = closing ? Math.min(leg.qty, p.qty) : p.qty;
    if (qty <= 1e-12) { this.cancel(state.public, ts); return; }
    const price = "price" in p ? basePrice : basePrice * (1 + (p.side === "BUY" ? 1 : -1) * this.settings.slippageBps / 10_000);
    const fee = qty * price * (maker ? this.settings.makerFee : this.settings.takerFee);
    const realized = closing ? (price - leg.entryPrice) * qty * (leg.side === "LONG" ? 1 : -1) : 0;
    if (!closing && qty * price / (this.leverages[p.symbol] ?? 1) + fee > this.account().availableBalance) {
      state.public = { ...state.public, status: "REJECTED", updatedAt: ts };
      this.emit(state.params.kind === "CONDITIONAL" ? "trade.algo_order" : "trade.order", { ...state.public });
      return;
    }
    if (closing) {
      leg.qty = Math.max(0, leg.qty - qty);
      if (leg.qty <= 1e-12) { leg.qty = 0; leg.entryPrice = 0; }
      if (realized - fee > 0) { this.wins++; this.profit += realized - fee; }
      else if (realized - fee < 0) { this.losses++; this.loss += fee - realized; }
    } else {
      leg.entryPrice = (leg.qty * leg.entryPrice + qty * price) / (leg.qty + qty);
      leg.qty += qty;
    }
    this.positions[p.symbol] = pair;
    this.trades++;
    this.fees += fee;
    this.realized += realized;
    this.cash += realized - fee;
    state.public = { ...state.public, status: p.kind === "CONDITIONAL" ? "FINISHED" : "FILLED", updatedAt: ts, filledQty: qty };
    this.emit("trade.fill", { accountId: "backtest", symbol: p.symbol, tradeId: String(++this.tradeId),
      exchangeOrderId: state.public.exchangeOrderId ?? state.public.actualOrderId, clientOrderId: p.clientOrderId,
      qty, price, fee, feeAsset: "USDT", realizedPnl: realized, side: p.side, positionSide: p.positionSide, ts, source: "strategy" });
    this.emit(p.kind === "CONDITIONAL" ? "trade.algo_order" : "trade.order", { ...state.public });
  }

  settleFunding(rate: FundingRate) {
    const pair = this.positions[rate.symbol];
    if (!pair) return;
    const mark = rate.markPrice ?? this.reference.get(rate.symbol)?.mark;
    if (!mark && (pair.long.qty || pair.short.qty)) throw new Fault(422, "HISTORY_MISSING", "Historical funding mark is required");
    const amount = (pair.short.qty - pair.long.qty) * (mark ?? 0) * rate.fundingRate;
    this.funding += amount;
    this.cash += amount;
    this.drawdownStep();
  }

  account() {
    let unrealizedPnl = 0, reserved = 0;
    for (const [symbol, pair] of Object.entries(this.positions)) for (const leg of [pair.long, pair.short]) {
      unrealizedPnl += leg.unrealizedPnl;
      reserved += leg.qty * (leg.markPrice || leg.entryPrice) / (this.leverages[symbol] ?? 1);
    }
    return { walletBalance: this.cash, availableBalance: this.cash + unrealizedPnl - reserved, unrealizedPnl, leverages: this.leverages };
  }

  private drawdownStep() {
    const equity = this.cash + this.account().unrealizedPnl;
    this.peak = Math.max(this.peak, equity);
    this.drawdown = Math.max(this.drawdown, this.peak - equity);
  }

  metrics() {
    const { unrealizedPnl } = this.account();
    return { realizedPnl: this.realized, fees: this.fees, funding: this.funding, unrealizedPnl, netPnl: this.cash + unrealizedPnl - this.settings.initialBalance,
      fills: this.trades, wins: this.wins, losses: this.losses, winRate: this.wins + this.losses ? this.wins / (this.wins + this.losses) : null,
      profitFactor: this.loss ? this.profit / this.loss : null, maxDrawdown: this.drawdown };
  }
}
