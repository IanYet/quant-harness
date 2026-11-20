import type { ExchangeSymbolInfo, OpenOrder, OrderParams, TradeAction } from "@quant/tools";
import type { ExchangeAccount } from "./gateway";
import { legs } from "./gateway";
import { bad, Fault, flag, num, oneOf, record, str, SYMBOL } from "../shared/http";

const actions = ["openLong", "closeLong", "openShort", "closeShort"] as const;
const types = ["MARKET", "LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT", "TRAILING_STOP_MARKET"] as const;
const intentKey = /^[A-Za-z0-9_-]{1,16}$/;

export type NormalizedOrder = OrderParams & {
  symbol: string; action: TradeAction; side: OpenOrder["side"]; positionSide: OpenOrder["positionSide"]; kind: OpenOrder["kind"];
};

const directions = {
  openLong: ["BUY", "LONG"], closeLong: ["SELL", "LONG"], openShort: ["SELL", "SHORT"], closeShort: ["BUY", "SHORT"],
} as const;

export const isClosing = (side: OpenOrder["side"], leg: OpenOrder["positionSide"]) =>
  (leg === "LONG" && side === "SELL") || (leg === "SHORT" && side === "BUY");

export function normalizeOrder(input: Record<string, unknown>, defaultSymbol: string, defaultClientOrderId?: string): NormalizedOrder {
  const action = oneOf(input.action, "action", actions);
  const raw = record(input.params);
  const symbol = str(raw.symbol ?? input.symbol ?? defaultSymbol, "symbol", SYMBOL);
  if (input.symbol !== undefined && symbol !== input.symbol) bad("Order symbols disagree");
  const type = oneOf(raw.type, "order type", types);
  const [side, positionSide] = directions[action];
  const limit = type === "LIMIT" || type === "STOP" || type === "TAKE_PROFIT";
  const trailing = type === "TRAILING_STOP_MARKET";
  const kind = type === "LIMIT" || type === "MARKET" ? "STANDARD" : "CONDITIONAL";
  const protection = raw.protection === undefined ? undefined : oneOf(raw.protection, "protection", ["take_profit", "stop_loss"]);
  if (protection && (!isClosing(side, positionSide) || (!limit && kind === "STANDARD"))) bad("Protection requires a closing limit or conditional order");
  const fields: Record<string, unknown> = { action, symbol, type, side, positionSide, kind, protection,
    qty: num(raw.qty, "qty", Number.MIN_VALUE), clientOrderId: str(raw.clientOrderId ?? defaultClientOrderId, "clientOrderId", intentKey) };
  if (limit) {
    fields.price = num(raw.price, "price", Number.MIN_VALUE);
    fields.timeInForce = oneOf(raw.timeInForce ?? "GTC", "timeInForce", ["GTC", "IOC", "FOK", "GTX"]);
  }
  if (kind === "CONDITIONAL") {
    fields.workingType = oneOf(raw.workingType ?? "CONTRACT_PRICE", "workingType", ["MARK_PRICE", "CONTRACT_PRICE"]);
    fields.priceProtect = flag(raw.priceProtect, "priceProtect");
    if (trailing) {
      fields.callbackRate = num(raw.callbackRate, "callbackRate", 0.1, 10);
      if (raw.activationPrice !== undefined) fields.activationPrice = num(raw.activationPrice, "activationPrice", Number.MIN_VALUE);
    } else fields.triggerPrice = num(raw.triggerPrice, "triggerPrice", Number.MIN_VALUE);
  }
  for (const key of ["price", "timeInForce", "triggerPrice", "workingType", "priceProtect", "callbackRate", "activationPrice"]) {
    if (raw[key] !== undefined && fields[key] === undefined) bad(key + " is not accepted for " + type);
  }
  return fields as NormalizedOrder;
}

function within(value: number, filter: Record<string, string | number>, minKey: string, maxKey: string, stepKey: string) {
  const [min, max, step] = [Number(filter[minKey]), Number(filter[maxKey]), Number(filter[stepKey])];
  const units = value / step;
  const epsilon = Math.min(1e-6, Math.abs(units) * Number.EPSILON * 4 + 1e-9);
  return (!min || value >= min) && (!max || value <= max) && (!step || Math.abs(units - Math.round(units)) <= epsilon);
}

export function validateFilters(order: NormalizedOrder, rules: ExchangeSymbolInfo) {
  const qtyType = order.type === "MARKET" || order.type.endsWith("_MARKET") ? "MARKET_LOT_SIZE" : "LOT_SIZE";
  const qty = rules.filters.find(f => f.filterType === qtyType);
  const price = rules.filters.find(f => f.filterType === "PRICE_FILTER");
  if (!qty || !price) throw new Fault(503, "RECONCILING", "Exchange price or lot-size filters unavailable");
  if (!within(order.qty, qty, "minQty", "maxQty", "stepSize")) throw new Fault(422, "INVALID_ORDER", "Quantity violates exchange lot size");
  const prices = [("price" in order ? order.price : undefined), ("triggerPrice" in order ? order.triggerPrice : undefined),
    ("activationPrice" in order ? order.activationPrice : undefined)];
  if (prices.some(value => value !== undefined && !within(value, price, "minPrice", "maxPrice", "tickSize"))) {
    throw new Fault(422, "INVALID_ORDER", "Price violates exchange tick or range");
  }
}

export function checkExposure(order: NormalizedOrder, account: Pick<ExchangeAccount, "view" | "config" | "healthy" | "mark">,
    killed: boolean, staleMs: number, reservedCloseQty = 0) {
  const view = account.view();
  const leg = legs(view.positions, order.symbol, view.leverages[order.symbol])[order.positionSide === "LONG" ? "long" : "short"];
  if (isClosing(order.side, order.positionSide)) {
    const pending = order.kind === "CONDITIONAL" ? 0 : view.openOrders.filter(other =>
      other.symbol === order.symbol && other.kind === "STANDARD" &&
      other.side === order.side && other.positionSide === leg.side)
      .reduce((sum, other) => sum + Math.max(0, other.qty - (other.filledQty ?? 0)), reservedCloseQty);
    if (order.qty > Math.max(0, leg.qty - pending) + Math.max(1e-12, leg.qty * 1e-10)) {
      throw new Fault(422, "RISK_REJECTED", "Requested close exceeds the unreserved position leg");
    }
    return;
  }
  if (killed) throw new Fault(422, "KILL_SWITCH_ACTIVE", "Opening orders are disabled");
  if (view.reconciling || !account.healthy().ready) throw new Fault(503, "RECONCILING", "Account is verifying exchange state");
  if (leg.leverage > account.config.risk.maxLeverage) throw new Fault(422, "RISK_REJECTED", "Position leverage exceeds the current cap");
  const mark = account.mark(order.symbol);
  if (!mark || Date.now() - mark.ts > staleMs || !(mark.price > 0)) throw new Fault(503, "MARKET_STALE", "No fresh mark for " + order.symbol);
  const estimates = [mark.price, "triggerPrice" in order ? order.triggerPrice : 0, "price" in order ? order.price : 0];
  if (order.qty * Math.max(...estimates) > account.config.risk.maxOrderNotionalUsd) throw new Fault(422, "RISK_REJECTED", "Order notional exceeds the account limit");
}
