import type { Account, Instruction, Order, PositionSide, Quote, Run } from "@quant/tools";
import type { Config } from "../config";
import type { Store } from "../db";
import type { Ticker } from "../market/ticker";
import { Decimal, integer, message, nameOf, positive, requireThat, stable, symbolOf, uid } from "../util";
import { BinanceAccount, ExchangeError } from "./binance";
import { active, Ledger } from "./ledger";
import { Paper } from "./paper";
type Input = Record<string, any>;
export class Trading {
  ledger: Ledger;
  paper: Paper;
  live?: BinanceAccount;
  private lanes = new Map<string, Promise<unknown>>();
  constructor(
    private db: Store,
    private ticker: Ticker,
    cfg: Config,
  ) {
    this.ledger = new Ledger(db);
    this.paper = new Paper(this.ledger);
    if (cfg.binanceTrading) this.live = new BinanceAccount(cfg, this.ledger, () => this.track());
    db.run("UPDATE instructions SET state='unknown' WHERE state='pending'");
    ticker.onBook = (s, b) => {
      if (ticker.quote(s).bookStale) return;
      this.paper.tick(s, b);
    };
    this.track();
  }
  track() {
    this.ticker.track(
      this.db
        .all<{ symbol: string }>(
          `SELECT symbol FROM positions WHERE quantity!='0'
      UNION SELECT symbol FROM orders WHERE status IN ('UNKNOWN','NEW','PARTIALLY_FILLED')`,
        )
        .map((r) => r.symbol),
    );
  }
  health() {
    return {
      privateLastPong: this.live?.lastPong ?? null,
      privateStream: this.live?.connection ?? "disabled",
      reconciliationError: this.live?.lastError ?? null,
      unresolved: this.db.all("SELECT id,clientId,state FROM instructions WHERE state IN ('unknown','pending')"),
      outstandingFillRepairs: this.db.all("SELECT clientId FROM fill_sync"),
    };
  }
  private existing(run: Run, action: string, key: string, params: string) {
    const existing = this.db.one<{ id: string; params: string; action: string }>(
      "SELECT id,params,action FROM instructions WHERE account=? AND strategy=? AND key=?",
      run.account,
      run.name,
      key,
    );
    if (!existing) return;
    requireThat(
      existing.action === action && existing.params === params,
      "Idempotency key has different parameters",
      409,
    );
    return this.ledger.instruction(existing.id);
  }
  command(authorize: () => Run, action: string, p: Input): Promise<Instruction> {
    const run = authorize(),
      symbol = symbolOf(p.symbol),
      key = nameOf(p.key);
    const params = stable(p);
    const existing = this.existing(run, action, key, params);
    if (existing) return Promise.resolve(existing);
    const lane = run.account + "/" + symbol;
    const result = (this.lanes.get(lane) ?? Promise.resolve())
      .catch(() => {})
      .then(() => this.execute(authorize, action, p, params));
    this.lanes.set(lane, result);
    return result.finally(() => {
      if (this.lanes.get(lane) === result) this.lanes.delete(lane);
    });
  }
  private checkOwner(account: Account, symbol: string, strategy: string) {
    const owner = this.db.one<{ strategy: string }>(
      "SELECT strategy FROM owners WHERE account=? AND symbol=?",
      account,
      symbol,
    );
    if (owner && owner.strategy !== strategy) {
      const pending = this.db.one(
        "SELECT 1 FROM instructions WHERE account=? AND json_extract(params,'$.symbol')=? AND state IN ('pending','unknown') LIMIT 1",
        account,
        symbol,
      );
      requireThat(
        !pending &&
          !this.ledger.open(account, symbol).length &&
          !this.ledger.positions(account).some((p) => p.symbol === symbol),
        "Contract belongs to " + owner.strategy,
        409,
      );
    }
  }
  private async execute(authorize: () => Run, action: string, p: Input, params: string) {
    const run = authorize(),
      { account } = run,
      symbol = p.symbol;
    const earlier = this.existing(run, action, p.key, params);
    if (earlier) return earlier;
    requireThat(account === "paper" || this.live, "Binance execution disabled", 403);
    const owner = this.db.one<{ strategy: string }>(
      "SELECT strategy FROM owners WHERE account=? AND symbol=?",
      account,
      symbol,
    );
    if (account === "binance" && owner && owner.strategy !== run.name) {
      await this.live!.reconcile();
      requireThat(!this.live!.lastError, "Cannot transfer contract while reconciliation is incomplete", 409);
      await this.live!.positions();
    }
    this.checkOwner(account, symbol, run.name);
    const positionSide: PositionSide = p.positionSide ?? "BOTH";
    let side = p.side,
      qty = p.quantity,
      type = p.type,
      price: string | null = null;
    let target: Order | null = null,
      proposed: Order | undefined;
    const decision: Quote | undefined = p.quote ?? (action === "close" ? this.ticker.quote(symbol) : undefined);
    const id = uid();
    if (action === "place" || action === "close") {
      requireThat(["BOTH", "LONG", "SHORT"].includes(positionSide), "Invalid position side");
      if (action === "close" && account === "binance") {
        const mode = await this.live!.request("GET", "/fapi/v1/positionSide/dual");
        if (!mode.dualSidePosition) requireThat(positionSide === "BOTH", "One-way account uses BOTH");
        else requireThat(positionSide !== "BOTH", "Specify LONG or SHORT for hedge mode");
      }
      if (account === "paper") requireThat(positionSide === "BOTH", "Paper account uses one-way mode");
      if (action === "close") {
        const positions = account === "paper" ? this.ledger.positions(account) : await this.live!.positions();
        const pos = positions.find((v) => v.symbol === symbol && v.positionSide === positionSide);
        requireThat(pos && !new Decimal(pos.quantity).isZero(), "No matching open position", 409);
        const amount = new Decimal(pos.quantity);
        side = amount.gt(0) ? "SELL" : "BUY";
        qty = amount.abs().toFixed();
        type = "MARKET";
      }
      requireThat(
        (side === "BUY" || side === "SELL") && (type === "MARKET" || type === "LIMIT"),
        "Unsupported order side/type",
      );
      requireThat(
        decision &&
          decision.symbol === symbol &&
          decision.book &&
          Number.isSafeInteger(decision.book.eventTime) &&
          Number.isSafeInteger(decision.book.receivedAt),
        "Supply the decision quote for this symbol",
      );
      positive(decision.book.bid);
      positive(decision.book.ask);
      const book = this.ticker.quote(symbol);
      requireThat(book.book && !book.bookStale, "Fresh execution quote unavailable", 409);
      const spec = this.ticker.spec(symbol),
        quantity = new Decimal(positive(qty)),
        market = type === "MARKET";
      const step = new Decimal(market ? spec.marketStep : spec.stepSize);
      requireThat(
        (step.isZero() || quantity.mod(step).isZero()) &&
          quantity.gte(market ? spec.marketMin : spec.minQty) &&
          quantity.lte(market ? spec.marketMax : spec.maxQty),
        "Invalid quantity step or limits",
      );
      if (!market) {
        price = positive(p.price);
        requireThat(new Decimal(price).mod(spec.tickSize).isZero(), "Invalid price tick");
      } else requireThat(p.price === undefined, "MARKET order has no limit price");
      requireThat(
        action === "close" ||
          quantity.mul(price ?? (side === "BUY" ? book.book.ask : book.book.bid)).gte(spec.minNotional),
        "Below minimum notional",
      );
      proposed = {
        account,
        clientId: "qh" + id.replaceAll("-", ""),
        exchangeId: null,
        symbol,
        side,
        type,
        positionSide,
        reduceOnly: action === "close",
        quantity: quantity.toFixed(),
        executed: "0",
        price,
        status: "UNKNOWN",
        updated: Date.now(),
      };
    } else if (action === "cancel") {
      target = this.ledger.order(account, nameOf(p.clientId));
      requireThat(target && target.symbol === symbol && active(target), "Open order not found", 404);
    } else if (action === "setLeverage") integer(p.leverage, 1, 125);
    else throw new Error("Unknown trade action");

    // Preflight may await position mode and size. Recheck the run immediately before the durable intent and send.
    authorize();
    const duplicate = this.existing(run, action, p.key, params);
    if (duplicate) return duplicate;
    this.db.tx(() => {
      this.checkOwner(account, symbol, run.name);
      this.db.run("INSERT OR REPLACE INTO owners VALUES (?,?,?)", account, symbol, run.name);
      this.db.run(
        "INSERT INTO instructions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        id,
        account,
        run.name,
        run.id,
        p.key,
        action,
        params,
        decision ? JSON.stringify(decision) : null,
        "pending",
        proposed?.clientId ?? target?.clientId ?? null,
        null,
        Date.now(),
      );
      if (proposed) this.ledger.save(proposed);
      if (account === "paper") {
        if (proposed) this.paper.place(proposed);
        if (target) this.ledger.report({ ...target, status: "CANCELED", updated: Date.now() });
        if (action === "setLeverage")
          this.db.run("INSERT OR REPLACE INTO leverage VALUES (?,?,?)", account, symbol, p.leverage);
        this.ledger.state(id, "accepted");
      }
    });
    if (account === "binance") {
      try {
        if (proposed) await this.live!.place(proposed);
        else if (target) await this.live!.cancel(target);
        else await this.live!.request("POST", "/fapi/v1/leverage", { symbol, leverage: p.leverage });
        this.ledger.state(id, "accepted");
      } catch (err) {
        const rejected = err instanceof ExchangeError && !err.ambiguous;
        this.db.tx(() => {
          this.ledger.state(id, rejected ? "rejected" : "unknown", message(err));
          if (rejected && proposed && this.ledger.order(account, proposed.clientId)?.status === "UNKNOWN") {
            this.ledger.save({ ...proposed, status: "REJECTED" });
          }
        });
      }
    }
    this.track();
    return this.ledger.instruction(id);
  }
  async stop() {
    await Promise.allSettled(this.lanes.values());
    await this.live?.stop();
  }
}
