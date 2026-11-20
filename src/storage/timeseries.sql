CREATE TABLE sync_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_seq INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
INSERT INTO sync_cursor VALUES (1, 0, 0);

CREATE TABLE klines (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,        -- 主存 1m，其余周期按需查询聚合或缓存
  open_time INTEGER NOT NULL,
  close_time INTEGER NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  volume REAL NOT NULL,
  quote_volume REAL NOT NULL,
  PRIMARY KEY (symbol, interval, open_time)
);

CREATE TABLE market_metrics (
  symbol TEXT NOT NULL,
  ts INTEGER NOT NULL,           -- 每分钟聚合快照（markPrice / indexPrice / fundingRate / bid / ask）
  mark_price REAL NOT NULL,
  index_price REAL NOT NULL,
  funding_rate REAL NOT NULL,
  next_funding_time INTEGER NOT NULL,
  bid REAL,
  ask REAL,
  PRIMARY KEY (symbol, ts)
);

CREATE TABLE funding_rates (
  symbol TEXT NOT NULL,
  funding_time INTEGER NOT NULL,
  funding_rate REAL NOT NULL,
  mark_price REAL,
  PRIMARY KEY (symbol, funding_time)
);

CREATE TABLE liquidations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seq INTEGER NOT NULL UNIQUE,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,            -- BUY | SELL
  order_type TEXT NOT NULL,
  price REAL NOT NULL,
  avg_price REAL NOT NULL,
  qty REAL NOT NULL,
  filled_qty REAL NOT NULL,
  trade_time INTEGER NOT NULL
);
CREATE INDEX idx_liquidations_sym_ts ON liquidations(symbol, trade_time DESC);

CREATE TABLE datasets (
  dataset TEXT PRIMARY KEY,
  schema_hint_json TEXT,
  ttl_sec INTEGER NOT NULL,
  last_sample_ts INTEGER,
  last_seq INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);

CREATE TABLE data_samples (
  dataset TEXT NOT NULL,
  seq INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  ingested_ts INTEGER,
  sample_key TEXT,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (dataset, seq)
);
CREATE INDEX idx_data_samples_ts ON data_samples(dataset, ts DESC);
CREATE INDEX idx_data_samples_visible ON data_samples(dataset, ingested_ts, seq);

CREATE TABLE latest_books (
  symbol TEXT PRIMARY KEY,
  ts INTEGER NOT NULL,
  bid REAL NOT NULL,
  ask REAL NOT NULL
);
CREATE TABLE market_gaps (
  through_seq INTEGER PRIMARY KEY,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER NOT NULL
);
CREATE TABLE history_coverage (
  kind TEXT NOT NULL,
  symbol TEXT NOT NULL,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER NOT NULL,
  PRIMARY KEY(kind, symbol, start_ts, end_ts)
);
CREATE TABLE mark_klines (
  symbol TEXT NOT NULL,
  open_time INTEGER NOT NULL,
  close_time INTEGER NOT NULL,
  open REAL NOT NULL,
  high REAL NOT NULL,
  low REAL NOT NULL,
  close REAL NOT NULL,
  PRIMARY KEY (symbol, open_time)
);
