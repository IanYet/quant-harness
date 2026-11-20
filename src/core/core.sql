CREATE TABLE sequence_state (id INTEGER PRIMARY KEY CHECK(id=1), reserved_until INTEGER NOT NULL);
INSERT INTO sequence_state VALUES (1,0);

CREATE TABLE critical_events (
  seq INTEGER PRIMARY KEY, topic TEXT NOT NULL, envelope TEXT NOT NULL
);
CREATE INDEX critical_topic_seq ON critical_events(topic,seq);
CREATE TABLE consumer_ack (consumer TEXT PRIMARY KEY, through_seq INTEGER NOT NULL);
INSERT INTO consumer_ack VALUES ('storage',0),('notifier',0),('agent',0);

CREATE TABLE leases (
  mode TEXT NOT NULL, account_id TEXT NOT NULL, run_id TEXT NOT NULL,
  epoch INTEGER NOT NULL, status TEXT NOT NULL, handover_json TEXT,
  PRIMARY KEY(mode,account_id)
);
CREATE TABLE active_runs (
  run_id TEXT PRIMARY KEY, pid INTEGER, mode TEXT NOT NULL, account_id TEXT, epoch INTEGER NOT NULL,
  target_json TEXT NOT NULL, code_hash TEXT NOT NULL, artifact_path TEXT NOT NULL, capabilities_json TEXT NOT NULL,
  namespace TEXT NOT NULL, state_name TEXT, persistent INTEGER NOT NULL, status TEXT NOT NULL,
  started_at INTEGER NOT NULL, ended_at INTEGER, parent_task_id TEXT
);
CREATE INDEX runs_account ON active_runs(mode,account_id,epoch);
CREATE TABLE proposals (
  proposal_id TEXT PRIMARY KEY, digest TEXT NOT NULL, operation_json TEXT NOT NULL,
  expires_at INTEGER NOT NULL, status TEXT NOT NULL, result_json TEXT
);
CREATE TABLE order_intents (
  mode TEXT NOT NULL, account_id TEXT NOT NULL, symbol TEXT NOT NULL, client_order_id TEXT NOT NULL,
  order_kind TEXT NOT NULL, source TEXT NOT NULL, exchange_client_id TEXT NOT NULL UNIQUE,
  request_json TEXT NOT NULL, run_id TEXT, strategy_id TEXT, epoch INTEGER,
  managing_run_id TEXT, exchange_order_id TEXT, algo_id TEXT, actual_order_id TEXT,
  status TEXT NOT NULL, result_json TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  PRIMARY KEY(mode,account_id,symbol,client_order_id)
);
CREATE INDEX order_owner ON order_intents(mode,account_id,managing_run_id,status);
CREATE INDEX intent_exchange_order ON order_intents(mode,account_id,symbol,exchange_order_id);
CREATE INDEX intent_algo ON order_intents(mode,account_id,symbol,algo_id);
CREATE INDEX intent_actual_order ON order_intents(mode,account_id,symbol,actual_order_id);
CREATE TABLE reconciliation_cursors (
  mode TEXT NOT NULL, account_id TEXT NOT NULL, symbol TEXT NOT NULL, endpoint TEXT NOT NULL, cursor_json TEXT NOT NULL,
  PRIMARY KEY(mode,account_id,symbol,endpoint)
);
CREATE TABLE private_event_keys (key TEXT PRIMARY KEY, ts INTEGER NOT NULL);
CREATE INDEX private_event_ts ON private_event_keys(ts);
CREATE TABLE kill_switches(scope TEXT PRIMARY KEY, enabled INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE checkpoint_latest(namespace TEXT PRIMARY KEY, version INTEGER NOT NULL, handover_id TEXT, checkpoint_json TEXT NOT NULL);
CREATE TABLE task_requests(task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, seq INTEGER NOT NULL);
CREATE TABLE worker_handovers (run_id TEXT PRIMARY KEY, document TEXT NOT NULL);
