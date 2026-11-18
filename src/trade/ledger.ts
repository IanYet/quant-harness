import type { Account, Instruction, Order, Position, PositionSide } from "@quant/tools";
import type { Store } from "../db";
import { Decimal, requireThat } from "../util";

export const active = (o: Order) => ["UNKNOWN", "NEW", "PARTIALLY_FILLED"].includes(o.status);
export class Ledger {
  constructor(readonly db: Store) {
    db.run("CREATE TABLE IF NOT EXISTS fill_sync(clientId TEXT PRIMARY KEY)");
  }
  order(account: Account, clientId: string): Order | null {
    const r = this.db.one<{ body: string }>(
      "SELECT body FROM orders WHERE account=? AND clientId=?",
      account,
      clientId,
    );
    return r ? JSON.parse(r.body) : null;
  }
  open(account: Account, symbol?: string): Order[] {
    return this.db
      .all<{
        body: string;
      }>(`SELECT body FROM orders WHERE account=? AND status IN ('UNKNOWN','NEW','PARTIALLY_FILLED')${symbol ? " AND symbol=?" : ""} ORDER BY rowid`, account, ...(symbol ? [symbol] : []))
      .map((r) => JSON.parse(r.body));
  }
  save(o: Order) {
    this.db.run(
      "INSERT INTO orders VALUES (?,?,?,?,?) ON CONFLICT(account,clientId) DO UPDATE SET status=excluded.status,body=excluded.body",
      o.account,
      o.clientId,
      o.symbol,
      o.status,
      JSON.stringify(o),
    );
  }
  report(o: Order) {
    this.db.tx(() => this.applyReport(o));
  }
  private applyReport(o: Order) {
    const prev = this.order(o.account, o.clientId);
    if (
      prev &&
      prev.status !== "UNKNOWN" &&
      (new Decimal(o.executed).lt(prev.executed) ||
        (!active(prev) && active(o)) ||
        (o.updated < prev.updated && new Decimal(o.executed).eq(prev.executed)))
    )
      return;
    this.save(o);
    if (o.account === "binance" && new Decimal(o.executed).gt(prev?.executed ?? 0)) {
      this.db.run("INSERT OR IGNORE INTO fill_sync VALUES (?)", o.clientId);
    }
    this.db.run(
      "UPDATE instructions SET state=?, detail=NULL WHERE account=? AND clientId=? AND action IN ('place','close') AND state IN ('unknown','pending')",
      o.status === "REJECTED" ? "rejected" : "accepted",
      o.account,
      o.clientId,
    );
    if (!active(o))
      this.db.run(
        "UPDATE instructions SET state=?,detail=? WHERE account=? AND clientId=? AND action='cancel' AND state IN ('unknown','pending')",
        o.status === "CANCELED" ? "accepted" : "rejected",
        o.status,
        o.account,
        o.clientId,
      );
  }
  fill(
    account: Account,
    symbol: string,
    clientId: string,
    tradeId: string,
    price: string,
    quantity: string,
    time: number,
  ) {
    return this.db.run(
      "INSERT OR IGNORE INTO fills VALUES (?,?,?,?,?,?,?)",
      account,
      symbol,
      tradeId,
      clientId,
      price,
      quantity,
      time,
    ).changes;
  }
  fills(account: Account, id: string) {
    return this.db.all<{ tradeId: string; price: string; quantity: string; time: number }>(
      "SELECT tradeId,price,quantity,time FROM fills WHERE account=? AND clientId=? ORDER BY time,rowid",
      account,
      id,
    );
  }
  position(account: Account, symbol: string, side: PositionSide, qty: string) {
    this.db.run(
      "INSERT OR REPLACE INTO positions VALUES (?,?,?,?)",
      account,
      symbol,
      side,
      new Decimal(qty).toFixed(),
    );
  }
  positions(account: Account) {
    return this.db.all<Position>(
      "SELECT symbol,positionSide,quantity FROM positions WHERE account=? AND quantity!='0'",
      account,
    );
  }
  instruction(id: string) {
    const item = this.db.one<
      Omit<Instruction, "params" | "decision"> & { params: string; decision: string | null }
    >("SELECT * FROM instructions WHERE id=?", id);
    requireThat(item, "Instruction not found", 404);
    return {
      ...item,
      params: JSON.parse(item.params),
      decision: item.decision ? JSON.parse(item.decision) : null,
    } as Instruction;
  }
  state(id: string, state: Instruction["state"], detail: string | null = null) {
    this.db.run(
      "UPDATE instructions SET state=?,detail=? WHERE id=? AND state IN ('pending','unknown')",
      state,
      detail,
      id,
    );
  }
}
