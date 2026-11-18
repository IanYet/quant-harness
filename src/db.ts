import { Database, type SQLQueryBindings } from "bun:sqlite";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
export class Store {
  db: Database;
  constructor(dir: string) {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    const ignore = join(dir, ".gitignore");
    if (!existsSync(ignore)) writeFileSync(ignore, "*\n");
    this.db = new Database(join(dir, "harness.sqlite"), { create: true, strict: true });
    this.db.exec(`
      PRAGMA locking_mode=EXCLUSIVE;
      PRAGMA journal_mode=WAL;
      PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS bars(symbol TEXT, interval TEXT, time INTEGER, body TEXT NOT NULL, PRIMARY KEY(symbol,interval,time));
      CREATE TABLE IF NOT EXISTS bar_repairs(symbol TEXT,interval TEXT,start INTEGER,end INTEGER,retryAt INTEGER,
        PRIMARY KEY(symbol,interval,start,end));
      CREATE TABLE IF NOT EXISTS stream_closes(stream TEXT PRIMARY KEY,time INTEGER);
      CREATE TABLE IF NOT EXISTS snapshots(symbol TEXT, minute INTEGER, body TEXT NOT NULL, PRIMARY KEY(symbol,minute));
      CREATE TABLE IF NOT EXISTS liquidations(id INTEGER PRIMARY KEY, symbol TEXT, time INTEGER, body TEXT);
      CREATE INDEX IF NOT EXISTS liquidation_time ON liquidations(symbol,time);
      CREATE TABLE IF NOT EXISTS depth_frames(id INTEGER PRIMARY KEY, symbol TEXT, time INTEGER, body TEXT);
      CREATE INDEX IF NOT EXISTS depth_time ON depth_frames(symbol,time);
      CREATE TABLE IF NOT EXISTS gaps(id INTEGER PRIMARY KEY, stream TEXT, time INTEGER, reason TEXT);
      CREATE TABLE IF NOT EXISTS samples(cursor INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, time INTEGER, record TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS sample_cursor ON samples(name,cursor);
      CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY, name TEXT, file TEXT, hash TEXT, source TEXT, account TEXT, mode TEXT,
        state TEXT, ready INTEGER DEFAULT 0, started INTEGER, ended INTEGER, exitCode INTEGER);
      CREATE INDEX IF NOT EXISTS run_name ON runs(name,started);
      CREATE TABLE IF NOT EXISTS leases(name TEXT PRIMARY KEY, runId TEXT UNIQUE);
      CREATE TABLE IF NOT EXISTS owners(account TEXT, symbol TEXT, strategy TEXT NOT NULL, PRIMARY KEY(account,symbol));
      CREATE TABLE IF NOT EXISTS instructions(id TEXT PRIMARY KEY, account TEXT, strategy TEXT, runId TEXT, key TEXT,
        action TEXT, params TEXT, decision TEXT, state TEXT, clientId TEXT, detail TEXT, created INTEGER,
        UNIQUE(account,strategy,key));
      CREATE INDEX IF NOT EXISTS instruction_client ON instructions(account,clientId);
      CREATE TABLE IF NOT EXISTS orders(account TEXT, clientId TEXT, symbol TEXT, status TEXT, body TEXT NOT NULL, PRIMARY KEY(account,clientId));
      CREATE INDEX IF NOT EXISTS order_status ON orders(account,symbol,status);
      CREATE TABLE IF NOT EXISTS fills(account TEXT, symbol TEXT, tradeId TEXT, clientId TEXT, price TEXT, quantity TEXT, time INTEGER,
        PRIMARY KEY(account,symbol,tradeId));
      CREATE INDEX IF NOT EXISTS fill_client ON fills(account,clientId);
      CREATE TABLE IF NOT EXISTS positions(account TEXT, symbol TEXT, positionSide TEXT, quantity TEXT, PRIMARY KEY(account,symbol,positionSide));
      CREATE TABLE IF NOT EXISTS leverage(account TEXT, symbol TEXT, value INTEGER, PRIMARY KEY(account,symbol));
      CREATE TABLE IF NOT EXISTS notifications(id TEXT PRIMARY KEY, type TEXT, content TEXT, state TEXT, detail TEXT, time INTEGER);
      CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, backend TEXT, sessionId TEXT, state TEXT, result TEXT, time INTEGER);
    `);
  }
  run(sql: string, ...args: SQLQueryBindings[]) {
    return this.db.query(sql).run(...args);
  }
  all<T>(sql: string, ...args: SQLQueryBindings[]) {
    return this.db.query<T, SQLQueryBindings[]>(sql).all(...args);
  }
  one<T>(sql: string, ...args: SQLQueryBindings[]) {
    return this.db.query<T, SQLQueryBindings[]>(sql).get(...args);
  }
  tx<T>(fn: () => T) {
    return this.db.transaction(fn)();
  }
  gap(stream: string, reason: string) {
    this.run("INSERT INTO gaps(stream,time,reason) VALUES (?,?,?)", stream, Date.now(), reason);
  }
}
