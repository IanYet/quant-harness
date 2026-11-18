import { createHmac } from "node:crypto";
import { zipSync, strToU8 } from "fflate";
import type { ServerWebSocket } from "bun";
import { periods, type Interval } from "@quant/tools";
import { simBar } from "../src/market/sim";
import type { Wire } from "../src/market/source";
type Peer = ServerWebSocket<{ group: string; keys: Set<string> }>;
export function exchange() {
  const peers = new Set<Peer>(),
    actions: { method: string; path: string; params: Record<string, string> }[] = [];
  const orders = new Map<string, Wire>();
  const fills: Wire[] = [],
    positions: Wire[] = [];
  const subscriptions: { group: string; keys: string[]; method: string }[] = [];
  const spec = {
    symbol: "BTCUSDT",
    status: "TRADING",
    contractType: "PERPETUAL",
    filters: [
      { filterType: "PRICE_FILTER", tickSize: "0.1" },
      { filterType: "LOT_SIZE", minQty: "0.001", maxQty: "1000", stepSize: "0.001" },
      { filterType: "MARKET_LOT_SIZE", minQty: "0.001", maxQty: "1000", stepSize: "0.001" },
      { filterType: "MIN_NOTIONAL", notional: "5" },
    ],
  };
  const state = {
    delayPlace: 0,
    reject: false,
    holdOrders: false,
    hedge: false,
    leverage: 1,
    depthId: "100",
    depthDelay: 0,
    barDelay: 0,
    barError: false,
    omitBar: -1,
    invalidSignatures: 0,
    positionDelay: 0,
    oldDay: "",
    archiveReads: 0,
    archiveMissing: false,
  };
  const secret = "test-only-secret",
    apiKey = "test-only-key";
  const server = Bun.serve<{ group: string; keys: Set<string> }>({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req, server) {
      const u = new URL(req.url),
        path = u.pathname,
        p = Object.fromEntries(u.searchParams),
        method = req.method;
      if (req.headers.get("upgrade") === "websocket") {
        if (server.upgrade(req, { data: { group: path.slice(1).split("/")[0], keys: new Set() } })) return;
        return new Response(null, { status: 400 });
      }
      actions.push({ method, path, params: p });
      if (path === "/fapi/v1/exchangeInfo")
        return Response.json({ symbols: [spec, { ...spec, symbol: "ETHUSDT" }] });
      if (path === "/fapi/v1/klines") {
        if (state.barDelay) await Bun.sleep(state.barDelay);
        if (state.barError) return new Response(null, { status: 503 });
        const i = p.interval as Interval,
          rows = [];
        for (let t = Number(p.startTime); t <= Number(p.endTime); t += periods[i]) {
          if (t === state.omitBar) continue;
          const b = simBar(p.symbol, i, t);
          rows.push([t, b.open, b.high, b.low, b.close, b.volume, b.closeTime]);
        }
        return Response.json(rows);
      }
      if (path === "/fapi/v1/depth") {
        const lastUpdateId = state.depthId;
        if (state.depthDelay) await Bun.sleep(state.depthDelay);
        return Response.json({ lastUpdateId, bids: [["60000", "1"]], asks: [["60001", "1"]] });
      }
      if (path.includes(".zip")) {
        state.archiveReads++;
        if (state.archiveMissing) return new Response(null, { status: 404 });
        const date = path.slice(-14, -4);
        const csv = path.includes("/metrics/")
          ? "create_time,symbol,sum_open_interest,count_long_short_ratio\n" +
            date +
            " 00:05:00,BTCUSDT,999.1,1.25\n"
          : "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n90071992547409931,62000.1,0.001,12,13," +
            Date.parse(date + "T00:05:00Z") +
            ",false\n";
        return new Response(zipSync({ "day.csv": strToU8(csv) }));
      }
      if (path === "/fapi/v1/aggTrades")
        return Response.json([{ a: 42, p: "60000.1", q: "0.1", T: Number(p.startTime ?? Date.now()), m: true }]);
      if (path === "/fapi/v1/fundingRate")
        return Response.json([{ fundingTime: Number(p.startTime), fundingRate: "0.0001" }]);
      if (path === "/fapi/v1/openInterest") return Response.json({ time: Date.now(), openInterest: "4728" });
      if (path.startsWith("/futures/data/"))
        return Response.json([{ timestamp: Number(p.startTime), sumOpenInterest: "100", longShortRatio: "1.1" }]);
      if (path === "/fapi/v1/ticker/24hr") return Response.json({ symbol: p.symbol, volume: "10000" });
      if (req.headers.get("x-mbx-apikey") !== apiKey)
        return Response.json({ code: -2015, msg: "Bad API key" }, { status: 401 });
      if (path === "/fapi/v1/listenKey") return Response.json({ listenKey: "local-session" });
      const payload = new URLSearchParams(u.search);
      payload.delete("signature");
      if (
        p.signature !== createHmac("sha256", secret).update(payload.toString()).digest("hex") ||
        Math.abs(Date.now() - Number(p.timestamp)) > 5000
      ) {
        state.invalidSignatures++;
        return Response.json({ code: -1022, msg: "Invalid signature" }, { status: 400 });
      }
      if (path === "/fapi/v3/positionRisk") {
        if (state.positionDelay) await Bun.sleep(state.positionDelay);
        return Response.json(positions);
      }
      if (path === "/fapi/v1/positionSide/dual") return Response.json({ dualSidePosition: state.hedge });
      if (path === "/fapi/v1/openOrders")
        return Response.json(
          state.holdOrders
            ? []
            : [...orders.values()].filter((o) => ["NEW", "PARTIALLY_FILLED"].includes(o.status)),
        );
      if (path === "/fapi/v1/userTrades")
        return Response.json(fills.filter((f) => String(f.orderId) === p.orderId));
      if (path === "/fapi/v1/leverage") {
        state.leverage = Number(p.leverage);
        return Response.json({ leverage: state.leverage, symbol: p.symbol });
      }
      if (path === "/fapi/v1/symbolConfig") return Response.json([{ symbol: p.symbol, leverage: state.leverage }]);
      if (path === "/fapi/v1/order" && method === "POST") {
        if (state.reject) return Response.json({ code: -2010, msg: "Order rejected" }, { status: 400 });
        const row = {
          symbol: p.symbol,
          side: p.side,
          type: p.type,
          origQty: p.quantity,
          price: p.price ?? "0",
          positionSide: p.positionSide,
          reduceOnly: p.reduceOnly === "true",
          clientOrderId: p.newClientOrderId,
          orderId: orders.size + 1,
          status: "NEW",
          executedQty: "0",
          updateTime: Date.now(),
        };
        orders.set(row.clientOrderId, row);
        if (state.delayPlace) await Bun.sleep(state.delayPlace);
        return Response.json(row);
      }
      if (path === "/fapi/v1/order") {
        const o = orders.get(p.origClientOrderId);
        if (!o || state.holdOrders)
          return Response.json({ code: -2013, msg: "Order does not exist" }, { status: 400 });
        if (method === "DELETE") {
          o.status = "CANCELED";
          o.updateTime = Date.now();
        }
        return Response.json(o);
      }
      return new Response(path, { status: 404 });
    },
    websocket: {
      open(ws) {
        peers.add(ws);
      },
      close(ws) {
        peers.delete(ws);
      },
      message(ws, raw) {
        const { method, params, id } = JSON.parse(String(raw));
        subscriptions.push({ group: ws.data.group, keys: params, method });
        for (const key of params) method === "SUBSCRIBE" ? ws.data.keys.add(key) : ws.data.keys.delete(key);
        ws.send(JSON.stringify({ id, result: null }));
      },
    },
  });
  return {
    state,
    actions,
    orders,
    fills,
    positions,
    subscriptions,
    server,
    peers,
    cfg: {
      market: "binance" as const,
      rest: server.url.origin,
      ws: server.url.origin.replace("http:", "ws:"),
      archive: server.url.origin + "/daily",
      apiKey,
      secret,
    },
    stream(key: string, e: Wire) {
      for (const ws of peers) if (ws.data.keys.has(key)) ws.send(JSON.stringify({ stream: key, data: e }));
    },
    private(e: Wire) {
      for (const ws of peers) if (ws.data.group === "private") ws.send(JSON.stringify(e));
    },
    drop(group: string) {
      for (const ws of peers) if (ws.data.group === group) ws.close();
    },
    watching(key: string) {
      return [...peers].some((ws) => ws.data.keys.has(key));
    },
  };
}
