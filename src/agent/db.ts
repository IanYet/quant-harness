import { join } from "node:path";
import { openDb } from "../shared/db";

const schema = [
  "CREATE TABLE sessions(session_id TEXT PRIMARY KEY, backend TEXT NOT NULL, backend_session_id TEXT, profile TEXT NOT NULL,",
  "  visibility TEXT NOT NULL, title TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);",
  "CREATE TABLE tasks(task_id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(session_id),",
  "  profile TEXT NOT NULL, backend TEXT NOT NULL, source TEXT NOT NULL, depth INTEGER NOT NULL, parent_task_id TEXT,",
  "  status TEXT NOT NULL, request_json TEXT NOT NULL, summary TEXT, code TEXT, cost_usd REAL, created_at INTEGER NOT NULL,",
  "  started_at INTEGER, finished_at INTEGER, reported INTEGER NOT NULL DEFAULT 0, usage_day TEXT NOT NULL);",
  "CREATE INDEX tasks_queue ON tasks(status,created_at);",
  "CREATE TABLE task_aliases(alias_id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(task_id));",
  "CREATE TABLE dedup_windows(source_scope TEXT NOT NULL, dedup_key TEXT NOT NULL, task_id TEXT NOT NULL REFERENCES tasks(task_id),",
  "  expires_at INTEGER NOT NULL, PRIMARY KEY(source_scope,dedup_key));",
  "CREATE TABLE daily_usage(day TEXT PRIMARY KEY, task_count INTEGER NOT NULL DEFAULT 0, cost_usd REAL NOT NULL DEFAULT 0,",
  "  soft_warned INTEGER NOT NULL DEFAULT 0, unmetered INTEGER NOT NULL DEFAULT 0);",
  "CREATE TABLE consumer_cursor(id INTEGER PRIMARY KEY CHECK(id=1), seq INTEGER NOT NULL); INSERT INTO consumer_cursor VALUES(1,0);",
  "CREATE TABLE rejected_requests(task_id TEXT PRIMARY KEY, code TEXT NOT NULL, summary TEXT NOT NULL, created_at INTEGER NOT NULL);",
  "CREATE TABLE cron_runs(name TEXT NOT NULL, slot INTEGER NOT NULL, PRIMARY KEY(name,slot));",
].join("\n");

export function openAgentDb(dir: string) { return openDb(join(dir, "db", "agent.db"), schema, 0o600); }
