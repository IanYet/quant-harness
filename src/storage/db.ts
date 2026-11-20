import { join } from "node:path";
import { openDb } from "../shared/db";
import ledgerSchema from "./ledger.sql" with { type: "text" };
import timeseriesSchema from "./timeseries.sql" with { type: "text" };

export function openStorage(dir: string) {
  const ledger = openDb(join(dir, "db", "ledger.db"), ledgerSchema);
  try {
    const timeseries = openDb(join(dir, "db", "timeseries.db"), timeseriesSchema);
    const cursor = () => {
      const sql = "SELECT last_seq FROM sync_cursor WHERE id=1";
      const ledgerSeq = ledger.query<{ last_seq: number }, []>(sql).get()!.last_seq;
      const timeseriesSeq = timeseries.query<{ last_seq: number }, []>(sql).get()!.last_seq;
      return { ledgerSeq, timeseriesSeq, lastSeq: Math.min(ledgerSeq, timeseriesSeq) };
    };
    return { ledger, timeseries, cursor, close() { ledger.close(); timeseries.close(); } };
  } catch (error) {
    ledger.close();
    throw error;
  }
}
export type Storage = ReturnType<typeof openStorage>;
