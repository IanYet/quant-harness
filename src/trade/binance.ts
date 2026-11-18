import { monitorSocket } from "../socket-health";
import { createHmac } from "node:crypto";
import type { Order, OrderStatus, Position } from "@quant/tools";
import type { Config } from "../config";
import { Decimal, message, wireJSON } from "../util";
import type { Wire } from "../market/source";
import { Ledger } from "./ledger";

export class ExchangeError extends Error {
  constructor(
    public code: number,
    public status: number,
    msg: string,
  ) {
    super(`Binance ${code}: ${msg}`);
  }
  get ambiguous() {
    return (
      (this.status < 300 && this.code === 0) ||
      this.status >= 500 ||
      this.status === 408 ||
      [-1001, -1006, -1007, -4116].includes(this.code) ||
      /duplicate/i.test(this.message)
    );
  }
}
function order(r: Wire, stream = false): Order {
  const raw = stream
    ? {
        symbol: r.s,
        clientOrderId: r.c,
        orderId: r.i,
        side: r.S,
        positionSide: r.ps,
        type: r.o,
        origQty: r.q,
        price: r.p,
        executedQty: r.z,
        status: r.X,
        reduceOnly: r.R,
        updateTime: r.T,
      }
    : r;
  const status = raw.status === "EXPIRED_IN_MATCH" ? "EXPIRED" : raw.status;
  return {
    account: "binance",
    symbol: raw.symbol,
    clientId: raw.clientOrderId,
    exchangeId: String(raw.orderId),
    side: raw.side,
    positionSide: raw.positionSide ?? "BOTH",
    type: raw.type,
    quantity: raw.origQty,
    price: raw.type === "LIMIT" ? raw.price : null,
    executed: raw.executedQty,
    status: status as OrderStatus,
    reduceOnly: raw.reduceOnly === true,
    updated: raw.updateTime ?? Date.now(),
  };
}
export class BinanceAccount {
  connection = "disconnected";
  lastPong: number | null = null;
  lastError: string | null = null;
  private ws?: WebSocket;
  private stopped = false;
  private retry?: Timer;
  private keepalive?: Timer;
  private synced?: Promise<void>;
  private connecting?: Promise<void>;
  private positionLoad?: Promise<Position[]>;
  private duringSnapshot?: Set<string>;
  private positionTimes = new Map<string, number>();
  constructor(
    private cfg: Config,
    private ledger: Ledger,
    private changed: () => void,
  ) {}
  async request(
    method: string,
    path: string,
    params: Record<string, string | number | undefined> = {},
    signed = true,
  ): Promise<any> {
    const payload = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v !== undefined) payload.set(k, String(v));
    if (signed) {
      payload.set("timestamp", String(Date.now()));
      payload.set("signature", createHmac("sha256", this.cfg.secret).update(payload.toString()).digest("hex"));
    }
    const res = await fetch(new URL(path + (payload.size ? "?" + payload : ""), this.cfg.rest), {
      method,
      headers: { "X-MBX-APIKEY": this.cfg.apiKey },
      signal: AbortSignal.timeout(this.cfg.timeoutMs),
    });
    const body = await res.text();
    let r;
    try {
      r = wireJSON(body);
    } catch {
      throw new ExchangeError(0, res.status, "Non-JSON response");
    }
    if (!res.ok || r.code < 0) throw new ExchangeError(r.code ?? 0, res.status, r.msg ?? res.statusText);
    return r;
  }
  async place(o: Order) {
    const r = await this.request("POST", "/fapi/v1/order", {
      symbol: o.symbol,
      side: o.side,
      positionSide: o.positionSide,
      type: o.type,
      newClientOrderId: o.clientId,
      quantity: o.quantity,
      price: o.price ?? undefined,
      timeInForce: o.type === "LIMIT" ? "GTC" : undefined,
      reduceOnly: o.reduceOnly && o.positionSide === "BOTH" ? "true" : undefined,
    });
    this.ledger.report(order(r));
  }
  async cancel(o: Order) {
    this.ledger.report(
      order(await this.request("DELETE", "/fapi/v1/order", { symbol: o.symbol, origClientOrderId: o.clientId })),
    );
  }
  positions() {
    if (!this.positionLoad) {
      this.duringSnapshot = new Set();
      this.positionLoad = this.loadPositions(this.duringSnapshot).finally(() => {
        this.positionLoad = undefined;
        this.duringSnapshot = undefined;
      });
    }
    return this.positionLoad;
  }
  private async loadPositions(changed: Set<string>) {
    const rows: Wire[] = await this.request("GET", "/fapi/v3/positionRisk");
    const seen = new Set<string>();
    this.ledger.db.tx(() => {
      for (const p of rows) {
        const k = p.symbol + "/" + p.positionSide;
        seen.add(k);
        if (changed.has(k) || (p.updateTime > 0 && p.updateTime < (this.positionTimes.get(k) ?? 0))) continue;
        this.ledger.position("binance", p.symbol, p.positionSide, p.positionAmt);
        if (p.updateTime > 0) this.positionTimes.set(k, p.updateTime);
      }
      for (const p of this.ledger.positions("binance")) {
        const k = p.symbol + "/" + p.positionSide;
        if (!seen.has(k) && !changed.has(k)) this.ledger.position("binance", p.symbol, p.positionSide, "0");
      }
    });
    return this.ledger.positions("binance");
  }
  event(frame: Wire) {
    if (frame.e === "listenKeyExpired") {
      this.ws?.close();
      return;
    }
    if (frame.e === "ORDER_TRADE_UPDATE") {
      const r = frame.o;
      this.ledger.db.tx(() => {
        this.ledger.report(order(r, true));
        if (r.x === "TRADE" && new Decimal(r.l).gt(0))
          this.ledger.fill("binance", r.s, r.c, String(r.t), r.L, r.l, r.T);
      });
    }
    if (frame.e === "ACCOUNT_UPDATE") {
      for (const p of frame.a.P ?? []) {
        const k = p.s + "/" + p.ps;
        if (frame.T < (this.positionTimes.get(k) ?? 0)) continue;
        this.duringSnapshot?.add(k);
        this.positionTimes.set(k, frame.T);
        this.ledger.position("binance", p.s, p.ps, p.pa);
      }
    }
    this.changed();
  }
  start() {
    this.connecting = this.connect().catch((e) => {
      if (!this.stopped) {
        this.lastError = message(e);
        this.schedule();
      }
    });
  }
  private schedule() {
    this.connection = "disconnected";
    clearInterval(this.keepalive);
    clearTimeout(this.retry);
    if (!this.stopped) this.retry = setTimeout(() => this.start(), this.cfg.reconnectMs);
  }
  private async connect() {
    this.connection = "connecting";
    const { listenKey } = await this.request("POST", "/fapi/v1/listenKey", {}, false);
    if (this.stopped) return;
    const ws = (this.ws = new WebSocket(this.cfg.ws + "/private/ws/" + encodeURIComponent(listenKey)));
    ws.onopen = () => {
      this.connection = "connected";
      monitorSocket(ws, (t) => {
        this.lastPong = t;
      });
      void (this.synced ?? Promise.resolve()).then(() => this.reconcile());
      this.keepalive = setInterval(() => {
        void this.request("PUT", "/fapi/v1/listenKey", {}, false).catch(() => ws.close());
      }, 30 * 60000);
    };
    ws.onmessage = (e) => {
      try {
        this.event(wireJSON(String(e.data)));
      } catch (err) {
        this.lastError = message(err);
        ws.close();
      }
    };
    ws.onerror = () => ws.close();
    ws.onclose = () => {
      if (!this.stopped) this.ledger.db.gap("private", "websocket disconnected");
      this.schedule();
    };
  }
  reconcile() {
    if (this.stopped) return Promise.resolve();
    return (this.synced ??= this.sync()
      .then(() => {
        this.lastError = null;
      })
      .catch((err) => {
        this.lastError = message(err);
      })
      .finally(() => {
        this.synced = undefined;
      }));
  }
  private async sync() {
    const db = this.ledger.db;
    await this.positions();
    const open: Wire[] = await this.request("GET", "/fapi/v1/openOrders");
    const visible = new Set<string>();
    for (const r of open) {
      this.ledger.report(order(r));
      visible.add(r.clientOrderId);
    }
    for (const o of this.ledger.open("binance")) {
      if (visible.has(o.clientId)) continue;
      try {
        this.ledger.report(
          order(await this.request("GET", "/fapi/v1/order", { symbol: o.symbol, origClientOrderId: o.clientId })),
        );
      } catch (err) {
        if (!(err instanceof ExchangeError && err.code === -2013)) throw err;
      }
    }
    for (const { id, params } of db.all<{ id: string; params: string }>(
      "SELECT id,params FROM instructions WHERE account='binance' AND action='setLeverage' AND state='unknown'",
    )) {
      const p = JSON.parse(params);
      const rows = await this.request("GET", "/fapi/v1/symbolConfig", { symbol: p.symbol });
      if (Number(rows.find((r: Wire) => r.symbol === p.symbol)?.leverage) === p.leverage)
        this.ledger.state(id, "accepted");
    }
    for (const { clientId } of db.all<{ clientId: string }>("SELECT clientId FROM fill_sync")) {
      const o = this.ledger.order("binance", clientId)!;
      const covered = () =>
        this.ledger
          .fills("binance", clientId)
          .reduce((sum, f) => sum.plus(f.quantity), new Decimal(0))
          .gte(this.ledger.order("binance", clientId)!.executed);
      let fromId: string | undefined;
      while (!covered()) {
        const rows: Wire[] = await this.request("GET", "/fapi/v1/userTrades", {
          symbol: o.symbol,
          orderId: o.exchangeId!,
          limit: 1000,
          fromId,
        });
        for (const r of rows)
          if (String(r.orderId) === o.exchangeId)
            this.ledger.fill("binance", o.symbol, clientId, String(r.id), r.price, r.qty, r.time);
        if (rows.length < 1000) break;
        const next = String(BigInt(rows.at(-1)!.id) + 1n);
        if (next === fromId) break;
        fromId = next;
      }
      if (covered()) db.run("DELETE FROM fill_sync WHERE clientId=?", clientId);
    }
    this.changed();
  }
  async stop() {
    this.stopped = true;
    clearInterval(this.keepalive);
    clearTimeout(this.retry);
    this.ws?.close();
    await this.connecting;
    await this.synced;
  }
}
