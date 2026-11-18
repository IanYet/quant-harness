import type { Book, Order } from "@quant/tools";
import { Decimal, uid } from "../util";
import type { Ledger } from "./ledger";
export class Paper {
  private liquidity = new Map<string, { book: Book; BUY: Decimal; SELL: Decimal }>();
  private resting = new Set<string>();
  constructor(private ledger: Ledger) {
    this.refresh();
  }
  refresh() {
    this.resting = new Set(this.ledger.open("paper").map((o) => o.symbol));
  }
  tick(symbol: string, book: Book) {
    this.liquidity.set(symbol, { book, BUY: new Decimal(book.askQty), SELL: new Decimal(book.bidQty) });
    if (this.resting.has(symbol)) this.match(symbol);
  }
  match(symbol: string) {
    const levels = this.liquidity.get(symbol);
    if (!levels) return;
    this.ledger.db.tx(() => {
      for (const o of this.ledger.open("paper", symbol)) {
        const buy = o.side === "BUY",
          price = new Decimal(buy ? levels.book.ask : levels.book.bid);
        if (o.price && (buy ? price.gt(o.price) : price.lt(o.price))) continue;
        const pos = this.ledger.positions("paper").find((p) => p.symbol === symbol);
        const balance = new Decimal(pos?.quantity ?? 0);
        let qty = Decimal.min(new Decimal(o.quantity).minus(o.executed), levels[o.side]);
        if (o.reduceOnly) {
          if (buy ? balance.gte(0) : balance.lte(0)) {
            this.ledger.report({ ...o, status: "EXPIRED", updated: Date.now() });
            continue;
          }
          qty = Decimal.min(qty, balance.abs());
        }
        if (qty.isZero()) continue;
        levels[o.side] = levels[o.side].minus(qty);
        const executed = new Decimal(o.executed).plus(qty);
        this.ledger.fill("paper", symbol, o.clientId, uid(), price.toFixed(), qty.toFixed(), levels.book.eventTime);
        this.ledger.position("paper", symbol, "BOTH", balance.plus(buy ? qty : qty.negated()).toFixed());
        this.ledger.report({
          ...o,
          executed: executed.toFixed(),
          status: executed.eq(o.quantity) ? "FILLED" : "PARTIALLY_FILLED",
          updated: Date.now(),
        });
      }
    });
    this.refresh();
  }
  place(o: Order) {
    this.ledger.report({ ...o, status: "NEW", exchangeId: o.clientId });
    this.resting.add(o.symbol);
    this.match(o.symbol);
  }
}
