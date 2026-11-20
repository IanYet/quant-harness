CREATE TABLE sync_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_seq INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
INSERT INTO sync_cursor VALUES (1, 0, 0);

CREATE TABLE orders (
  exchange_order_id TEXT NOT NULL,
  client_order_id TEXT,
  mode TEXT NOT NULL,            -- live | shadow
  account_id TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'strategy', -- strategy | manual | external
  run_id TEXT,
  strategy_id TEXT,
  epoch INTEGER,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,            -- BUY | SELL
  position_side TEXT NOT NULL,   -- LONG | SHORT
  order_type TEXT NOT NULL,      -- MARKET | LIMIT
  qty REAL NOT NULL,
  price REAL,
  filled_qty REAL NOT NULL DEFAULT 0,
  avg_price REAL,
  status TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (mode, account_id, symbol, exchange_order_id)
);
CREATE INDEX idx_orders_acct_time ON orders(account_id, created_at DESC);

CREATE TABLE algo_orders (
  algo_id TEXT NOT NULL,         -- Binance Algo Order ID
  client_algo_id TEXT,           -- qh_ 开头
  mode TEXT NOT NULL,            -- live | shadow
  account_id TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'strategy', -- strategy | manual | external
  run_id TEXT,
  strategy_id TEXT,
  epoch INTEGER,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,            -- BUY | SELL
  position_side TEXT NOT NULL,   -- LONG | SHORT
  order_type TEXT NOT NULL,      -- STOP_MARKET | TAKE_PROFIT_MARKET | STOP | TAKE_PROFIT | TRAILING_STOP_MARKET
  qty REAL NOT NULL,
  trigger_price REAL,
  price REAL,
  callback_rate REAL,
  working_type TEXT,             -- MARK_PRICE | CONTRACT_PRICE
  algo_status TEXT NOT NULL,     -- NEW | TRIGGERING | TRIGGERED | FINISHED | CANCELED | REJECTED
  actual_order_id TEXT,          -- 触发后对应的普通单 ID
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (mode, account_id, symbol, algo_id)
);
CREATE INDEX idx_algo_orders_acct_time ON algo_orders(account_id, created_at DESC);

CREATE TABLE fills (
  trade_id TEXT NOT NULL,
  exchange_order_id TEXT NOT NULL,
  client_order_id TEXT,
  mode TEXT NOT NULL,
  account_id TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'strategy', -- strategy | manual | external
  symbol TEXT NOT NULL,
  event_seq INTEGER NOT NULL UNIQUE,
  run_id TEXT,
  strategy_id TEXT,
  side TEXT NOT NULL,
  position_side TEXT NOT NULL,
  price REAL NOT NULL,
  qty REAL NOT NULL,
  fee REAL NOT NULL,
  fee_asset TEXT NOT NULL,
  realized_pnl REAL NOT NULL DEFAULT 0,
  ts INTEGER NOT NULL,
  PRIMARY KEY (mode, account_id, symbol, trade_id)
);
CREATE INDEX idx_fills_acct_ts ON fills(account_id, ts DESC);

CREATE TABLE account_income (
  mode TEXT NOT NULL,
  account_id TEXT NOT NULL,
  income_type TEXT NOT NULL,
  tran_id TEXT NOT NULL,
  symbol TEXT,
  asset TEXT NOT NULL,
  amount TEXT NOT NULL,
  ts INTEGER NOT NULL,
  PRIMARY KEY (mode, account_id, income_type, tran_id)
);

CREATE TABLE operation_audit (
  seq INTEGER PRIMARY KEY,
  operation_id TEXT NOT NULL,
  action TEXT NOT NULL,
  actor TEXT NOT NULL,
  source TEXT NOT NULL,          -- mcp | cli | notifier
  target_json TEXT NOT NULL,
  confirmation_ref TEXT,
  reason TEXT,
  result TEXT NOT NULL,
  ts INTEGER NOT NULL
);

CREATE TABLE risk_rejects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seq INTEGER NOT NULL UNIQUE,
  mode TEXT NOT NULL,            -- live | shadow
  account_id TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'strategy',
  run_id TEXT,
  code TEXT NOT NULL,
  reason TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  ts INTEGER NOT NULL
);

CREATE TABLE account_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seq INTEGER NOT NULL UNIQUE,
  mode TEXT NOT NULL,            -- live | shadow
  account_id TEXT NOT NULL,
  wallet_balance REAL NOT NULL,
  available_balance REAL NOT NULL,
  unrealized_pnl REAL NOT NULL,
  positions_json TEXT NOT NULL,
  ts INTEGER NOT NULL
);
CREATE INDEX idx_acct_snap_ts ON account_snapshots(account_id, ts DESC);

CREATE TABLE checkpoints (
  namespace TEXT NOT NULL,
  version INTEGER NOT NULL,
  account_id TEXT,
  epoch INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  data_json TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (namespace, version)
);

CREATE TABLE strategies (
  strategy_id TEXT PRIMARY KEY,  -- "<family>@<version>"
  family TEXT NOT NULL,
  version TEXT NOT NULL,
  file_path TEXT NOT NULL UNIQUE,
  source_hash TEXT NOT NULL,
  sdk_hash TEXT NOT NULL,
  code_hash TEXT NOT NULL,
  parent_id TEXT REFERENCES strategies(strategy_id),
  lifecycle TEXT NOT NULL,       -- backtested | shadow | live | shelved | deprecated
  diff_summary TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (family, version)
);

CREATE TABLE strategy_tags (
  strategy_id TEXT NOT NULL REFERENCES strategies(strategy_id),
  dimension TEXT NOT NULL,       -- regime | style | account_fit | factor
  value TEXT NOT NULL,
  PRIMARY KEY (strategy_id, dimension, value)
);
CREATE INDEX idx_strategy_tags_lookup ON strategy_tags(dimension, value);

CREATE TABLE strategy_notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_id TEXT NOT NULL REFERENCES strategies(strategy_id),
  author TEXT NOT NULL,          -- user | cron_review | script_triggered | human
  note TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE strategy_drafts (
  draft_path TEXT PRIMARY KEY,
  parent_id TEXT REFERENCES strategies(strategy_id),
  inherited_tags_json TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE backtest_runs (
  run_id TEXT PRIMARY KEY,
  target TEXT NOT NULL,          -- draft_path 或 strategy_id
  code_hash TEXT NOT NULL,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER NOT NULL,
  params_json TEXT NOT NULL,
  metrics_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX idx_fills_watermark ON fills(mode, account_id, event_seq);
CREATE INDEX idx_income_account ON account_income(mode, account_id, ts);
CREATE INDEX idx_fills_strategy ON fills(strategy_id, ts);

CREATE TABLE pending_seals (
  strategy_id TEXT PRIMARY KEY,
  metadata_json TEXT NOT NULL
);

CREATE TABLE script_logs (
  run_id TEXT PRIMARY KEY,
  path TEXT NOT NULL UNIQUE
);
