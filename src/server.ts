import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { periods, type Account, type BarEvent, type Interval, type Sample } from "@quant/tools";
import type { Config } from "./config";
import { Connectors } from "./connectors";
import { Store } from "./db";
import { BinanceSource } from "./market/binance";
import { SimSource } from "./market/sim";
import { Ticker } from "./market/ticker";
import { Runs } from "./runs/manager";
import { Hub, ndjson } from "./streams";
import { Trading } from "./trade/engine";
import { HttpError, integer, message, nameOf, recordError, requireThat, symbolOf, uid } from "./util";

export async function startHost(cfg: Config) {
  const db = new Store(cfg.dir);
  db.run("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT)");
  db.run("INSERT OR IGNORE INTO settings VALUES ('market',?)", cfg.market);
  const mode = db.one<{ value: string }>("SELECT value FROM settings WHERE key='market'")!.value;
  if (mode !== cfg.market) {
    db.db.close();
    throw new Error("Use a separate data directory for each market source");
  }
  const hub = new Hub();
  const resume = db.one<{ t: number | null }>("SELECT MAX(minute) AS t FROM snapshots")?.t ?? 0;
  const source = cfg.market === "sim" ? new SimSource(cfg.tickMs, cfg.symbols, resume) : new BinanceSource(cfg);
  const ticker = new Ticker(source, db, hub, cfg.symbols);
  const trade = new Trading(db, ticker, cfg);
  const connectors = new Connectors(db, cfg);
  const tokenFile = join(cfg.dir, "operator.token");
  if (!existsSync(tokenFile)) writeFileSync(tokenFile, uid() + uid(), { mode: 0o600, flag: "wx" });
  const operator = readFileSync(tokenFile, "utf8").trim();
  let server: Bun.Server<undefined>;
  const runs = new Runs(db, cfg, () => server.url.origin);
  function inspect() {
    return {
      checkedAt: Date.now(),
      marketTime: source.now(),
      market: cfg.market,
      ticker: ticker.health(),
      trade: trade.health(),
      listeners: hub.counts(),
      runs: runs.list(),
      gaps: db.all("SELECT * FROM gaps ORDER BY id DESC LIMIT 15"),
    };
  }
  let health = inspect();
  function samples(name: string, after: number, limit: number, until = Number.MAX_SAFE_INTEGER): Sample[] {
    return db
      .all<
        Sample<string>
      >("SELECT * FROM samples WHERE name=? AND cursor>? AND cursor<=? ORDER BY cursor LIMIT ?", name, after, until, limit)
      .map((r) => ({ ...r, record: JSON.parse(r.record) }));
  }
  async function handle(req: Request) {
    const url = new URL(req.url),
      path = url.pathname;
    const token = req.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
    const q = Object.fromEntries(url.searchParams);
    const symbol = () => symbolOf(q.symbol);
    const interval = () => {
      requireThat(Object.hasOwn(periods, q.interval), "Unsupported interval");
      return q.interval as Interval;
    };
    const range = () => {
      const start = integer(q.start, 0),
        end = integer(q.end, start + 1);
      requireThat(end - start <= 366 * 86400000, "At most one year per range");
      return { start, end };
    };
    const account = (): Account => {
      const a = q.account ?? (token && token !== operator ? runs.identity(token).account : "paper");
      requireThat(a === "paper" || (a === "binance" && cfg.binanceTrading), "Account unavailable");
      return a;
    };
    if (path.startsWith("/admin/")) requireThat(token === operator, "Operator credential required", 403);
    if (req.method === "GET") {
      if (path === "/health") return Response.json(health);
      if (path === "/admin/status") return Response.json(inspect());
      if (path === "/admin/logs") return Response.json(runs.logs(nameOf(q.ref)));
      if (path === "/run") return Response.json(runs.identity(token));
      if (path === "/market/quote") return Response.json(await ticker.latest(symbol()));
      if (path === "/market/bars") return Response.json(await ticker.history.bars(symbol(), interval(), range()));
      if (path === "/market/spec") return Response.json(ticker.spec(symbol()));
      if (path === "/market/stats") return Response.json(await source.stats(symbol()));
      if (path === "/market/depth") return Response.json(await source.depth(symbol()));
      if (path === "/market/trades")
        return Response.json(
          await source.trades(symbol(), q.start || q.end ? range() : undefined, integer(q.limit ?? 500, 1, 1000)),
        );
      if (["/market/oi", "/market/ratio", "/market/funding"].includes(path)) {
        const kind = path.slice(8) as "oi" | "ratio" | "funding";
        return Response.json(
          await source.metric(kind, symbol(), kind === "oi" && !q.start && !q.end ? undefined : range()),
        );
      }
      if (path === "/trade/positions") return Response.json(trade.ledger.positions(account()));
      if (path === "/trade/openOrders") return Response.json(trade.ledger.open(account()));
      if (path === "/trade/order") {
        const o = trade.ledger.order(account(), nameOf(q.clientId));
        requireThat(o, "Order not found", 404);
        return Response.json(o);
      }
      if (path === "/trade/fills") return Response.json(trade.ledger.fills(account(), nameOf(q.clientId)));
      if (path === "/trade/instruction") return Response.json(trade.ledger.instruction(nameOf(q.id)));
      if (path === "/agents/status") return Response.json(await connectors.agent("status", q));
      if (path === "/data/read")
        return Response.json(samples(nameOf(q.name), integer(q.after ?? 0, 0), integer(q.limit ?? 500, 1, 5000)));
      if (path.startsWith("/stream/")) {
        server.timeout(req, 0);
        if (path === "/stream/data") {
          const name = nameOf(q.name),
            after = integer(q.after ?? 0, 0);
          return ndjson<Sample>(req, (push) => {
            const release = hub.on("data/" + name, push);
            const end = db.one<{ n: number }>(
              "SELECT COALESCE(MAX(cursor),0) AS n FROM samples WHERE name=?",
              name,
            )!.n;
            async function* initial() {
              let cursor = after;
              while (cursor < end) {
                const page = samples(name, cursor, 128, end);
                if (!page.length) break;
                for (const row of page) {
                  cursor = row.cursor;
                  yield row;
                }
              }
            }
            return {
              release,
              initial: initial(),
              select: (r) => (r.cursor > Math.max(after, end) ? r : undefined),
            };
          });
        }
        const s = symbol();
        ticker.spec(s);
        if (path === "/stream/bars")
          return ndjson<BarEvent>(req, (push) =>
            ticker.bars(s, interval(), integer(q.warmup ?? 0, 0, 50000), push),
          );
        return ndjson(req, (push) => ticker.watch(path.slice(8), s, push));
      }
    }
    if (req.method === "POST") {
      const p = (await req.json()) as Record<string, any>;
      requireThat(p && typeof p === "object" && !Array.isArray(p), "Expected JSON object");
      if (path === "/admin/run") return Response.json(runs.start(p.file, p.name, p.observe, p.account));
      if (path === "/admin/stop") return Response.json(await runs.stop(nameOf(p.ref)));
      if (path === "/admin/replace") return Response.json(await runs.replace(nameOf(p.name), p.file, p.from));
      if (path === "/admin/refresh") {
        await ticker.refreshSpecs();
        return Response.json({ symbols: ticker.specs.size });
      }
      if (["/trade/place", "/trade/cancel", "/trade/close", "/trade/setLeverage"].includes(path)) {
        return Response.json(await trade.command(() => runs.identity(token, true), path.slice(7), p));
      }
      if (path === "/data/append") {
        const name = nameOf(p.name),
          time = Date.now();
        requireThat(p.record !== undefined, "Missing record");
        const r = db.run(
          "INSERT INTO samples(name,time,record) VALUES (?,?,?)",
          name,
          time,
          JSON.stringify(p.record),
        );
        const sample = { cursor: Number(r.lastInsertRowid), name, time, record: p.record };
        hub.emit("data/" + name, sample);
        return Response.json(sample);
      }
      if (path === "/notify/send") return Response.json(await connectors.notify(p.type, p.content));
      if (path === "/agents/startTask" || path === "/agents/sendMessage")
        return Response.json(await connectors.agent(path.slice(8), p));
    }
    throw new HttpError(404, "Endpoint not found");
  }
  const requests = new Set<Promise<Response>>();
  try {
    await ticker.start();
    server = Bun.serve({
      hostname: "127.0.0.1",
      port: cfg.port,
      idleTimeout: 120,
      maxRequestBodySize: 1_048_576,
      fetch(req) {
        const task = handle(req).catch((e) =>
          Response.json({ error: message(e) }, { status: e instanceof HttpError ? e.status : 500 }),
        );
        requests.add(task);
        void task.then(() => requests.delete(task));
        return task;
      },
    });
  } catch (err) {
    await ticker.stop();
    db.db.close();
    throw err;
  }
  const live = trade.live;
  live?.start();
  if (live) await live.reconcile();
  health = inspect();
  const healthTimer = setInterval(() => {
    ticker.sample();
    trade.track();
    health = inspect();
  }, cfg.healthMs);
  const syncTimer = setInterval(() => {
    ticker.retryGap();
    void live?.reconcile();
  }, cfg.reconcileMs);
  const specTimer = setInterval(() => {
    void ticker.refreshSpecs().catch(recordError);
  }, 3600000);
  return {
    url: server.url.origin,
    db,
    hub,
    ticker,
    trade,
    runs,
    async stop() {
      clearInterval(healthTimer);
      clearInterval(syncTimer);
      clearInterval(specTimer);
      await runs.shutdown();
      await server.stop(true);
      await Promise.allSettled(requests);
      await trade.stop();
      await ticker.stop();
      db.db.close();
    },
  };
}
