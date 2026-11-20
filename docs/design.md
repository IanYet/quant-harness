# 量化交易 Harness 详细设计文档

本文档将[架构设计](./architecture.md)落实为编码与验收规格。运维权限、Binance 全量行情与条件单接入、双轨脚本执行、持久化与恢复流程以本文为准。

---

## 1. 设计约束与最小实现原则

1. **按功能直写，拒绝过度抽象**：
   - **交易所**。直接实现 **Binance USDⓈ-M Futures** 客户端，支持多标的观测、按账户配置交易标的和 Demo/Testnet 联调。网关对接官方 WebSocket 行情流、Market Data REST API 与 Algo Order 条件单服务。
   - **IM 通道**：本机（腾讯云东京区）具备公网 HTTPS 入口，**仅实现 Webhook 模式**（`Bun.serve` 本地监听，挂在本机 Nginx/Caddy HTTPS 反代后）；不做 Telegram Long Polling，也不接飞书 WebSocket 长连接；直接用原生 `fetch()` 调用 Telegram Bot API 与飞书 Open API，不引入重型 Bot SDK。
   - **运行时与存储**：全面使用 Bun 内置能力（`Bun.serve` UDS/HTTP、`bun:sqlite`、`Bun.spawn`、原生 `WebSocket`/`fetch`、`node:crypto`），不引入 ORM、Web 框架、队列库或复杂构建链。
2. **仅在明确要求处保留抽象**：
   - **`quant-agent` 底层 Harness 适配层**：抽象 `AgentHarnessAdapter` 接口以支持多后端配置，首期实现 **Codex**（`codex app-server`）与 **Antigravity**（`agy` / `agy acp`），并统一支持控制会话在对应 Harness App 侧边栏的可见性（`visible` / `hidden` / `ephemeral`）。
3. **禁止模拟测试策略脚本**：
   - 仓库内**不编写**任何示例策略、假策略、模拟行情生成器（`sim`）或本地模拟撮合账户（`paper`）。
   - `shadow` 通道直连真实 **Binance Futures Demo / Testnet API**（支持草稿直跑）；`backtest_run` 是 `quant-storage` 内置的**标准历史回放引擎**（消费真实历史 K 线、资金费率与采集数据集），不附带任何模拟测试策略脚本。
4. **最小外部依赖与托管脚本纯粹性**：
   - 仅引入 `typescript`（类型检查与 AST 预检）与 `@modelcontextprotocol/sdk`（精简 MCP stdio Server）。
   - 托管与封存脚本维持**纯 Bun + TS、单文件 `.ts`、唯一运行时导入 `@quant/tools`、静态预检与 `Bun.build` 单文件打包**；终端直跑的一次性研究脚本（`scripts/research/`）同样采用纯 Bun + TS（可直接使用 Bun 内置能力与只读 `bun:sqlite`），不引入 Python 或外部依赖蔓延。
5. **可信运维、三层交互与受控手动干预**：
   - 人工操作员和已接入的可信 Agent 共享控制面契约（精简 11 核心 MCP + 统一本机 CLI `quant` + 本地只读 SQLite WAL）。
   - 盘中自动化交易由持有账户独占租约的托管策略执行。交易员授权的单次手动干预（开平仓、挂条件止损、撤单、调杠杆）同样经 `quant-core` 风控、审批和审计。策略继续持有原租约，网关负责交易签名。

---

## 2. 仓库结构、运行态布局与配置

### 2.1 仓库目录结构（单仓 + 仅 1 个 SDK Workspace）

```text
quant-harness/
├── package.json                  # 根项目，workspaces: ["packages/tools"]
├── tsconfig.json
├── config/
│   ├── harness.example.json      # 主配置模板（多标的、子账户风控、IM Webhook、Agent 后端等）
│   ├── secrets.exchange.example.json
│   └── secrets.im.example.json
├── deploy/
│   ├── systemd/                  # 5 个常驻进程 unit 文件
│   │   ├── quant-core.service
│   │   ├── quant-storage.service
│   │   ├── quant-notifier.service
│   │   ├── quant-agent.service
│   │   └── quant-watchdog.service
│   └── nginx/
│       └── quant-webhooks.conf.example
├── packages/
│   └── tools/                    # @quant/tools（供托管脚本与本地研究脚本 import）
│       ├── package.json
│       ├── index.ts
│       ├── types.ts
│       ├── transport-uds.ts      # 托管运行传输层（live / shadow / worker）
│       ├── transport-backtest.ts # 离线回测传输层（QUANT_MODE=backtest）
│       └── transport-local.ts    # 本地只读传输层（终端 bun run 无 QUANT_TOKEN 时自动启用）
├── src/
│   ├── shared/                   # 跨进程公共类型、协议常量、UDS client、配置加载
│   │   ├── types.ts
│   │   ├── events.ts
│   │   ├── config.ts
│   │   ├── uds.ts
│   │   └── logger.ts
│   ├── core/                     # quant-core 主进程
│   │   ├── main.ts
│   │   ├── event-bus.ts          # 主事件总线（预留 seq、关键事件日志及实时环形缓冲）
│   │   ├── binance/
│   │   │   ├── signer.ts         # HMAC-SHA256 + 统一出口 IP 权重限频桶
│   │   │   ├── public-ws.ts      # 基础常驻流（/public bookTicker + /market kline_1m/markPrice/forceOrder/contractInfo）
│   │   │   ├── fast-stream.ts    # 高频行情按需动态订阅（SUBSCRIBE/UNSUBSCRIBE + 无 seq 有损旁路）
│   │   │   ├── market-rest.ts    # 官方 Market Data REST 统一代理与短 TTL 缓存
│   │   │   ├── account-client.ts # 单子账户 REST（含 Algo 条件单）+ User Data Stream（含 ALGO_UPDATE）+ 对账
│   │   │   └── pool.ts           # 多子账户池 + Shadow Demo/Testnet 通道
│   │   ├── risk.ts               # 账户独占锁 + 防呆风控（含减仓/条件单绿灯）+ 受控手动交易 + Kill Switch
│   │   ├── checkpoint.ts         # 托管 Checkpoint（上限 1 MB，内存热份 + 原子热备 + 事件归档）
│   │   ├── supervisor.ts         # Bun.spawn 子进程管理、能力 Token 会话表、租约交接与提案
│   │   ├── admin-server.ts       # admin.sock HTTP Server
│   │   └── runtime-server.ts     # runtime.sock HTTP + NDJSON 主流与高频旁路 Server
│   ├── storage/                  # quant-storage 外围进程
│   │   ├── main.ts
│   │   ├── db.ts                 # ledger.db & timeseries.db 建表与迁移（WAL 并发只读友好）
│   │   ├── ingestor.ts           # 从两库最小游标重放，各库独立提交
│   │   ├── history-backfill.ts   # Binance 公共 REST 历史 K 线与资金费率回补
│   │   ├── strategy-registry.ts  # 草稿预检、SDK 打包、不可变源码与执行产物
│   │   ├── validator.ts          # script_validate 静态预检（tsc + AST 规则）
│   │   ├── backtest.ts           # 标准同码离线回测引擎（多标的 + 普通单 & Algo 条件单撮合）
│   │   ├── log-rotator.ts        # 脚本滚动日志写入、切分与查询
│   │   └── server.ts             # storage.sock HTTP Server
│   ├── notifier/                 # quant-notifier 外围进程
│   │   ├── main.ts
│   │   ├── dedup-queue.ts        # 告警聚合降噪（含脚本 sys.notify）、冷却与指数退避重试
│   │   ├── telegram.ts           # Telegram Bot API 发送 + Webhook 回调校验
│   │   ├── feishu.ts             # 飞书 Open API 发送 + 卡片回调校验
│   │   └── webhook-server.ts     # 审批回调（实盘/手动交易）& 白名单管理员指令 -> admin.sock
│   ├── agent/                    # quant-agent 外围进程
│   │   ├── main.ts
│   │   ├── db.ts                 # agent.db（会话索引、任务表、预算计数、去重冷却）
│   │   ├── guardrails.ts         # 宽松预算型护栏（去重/冷却、并发、预算、Profile、反递归 maxDepth=3）
│   │   ├── scheduler.ts          # 定时全局复盘 Cron 调度器
│   │   ├── server.ts             # agent.sock HTTP Server
│   │   └── adapters/
│   │       ├── interface.ts      # AgentHarnessAdapter 统一接口与 sidebarVisibility 契约
│   │       ├── registry.ts       # 多后端配置注册表
│   │       ├── codex.ts          # Codex App Server JSON-RPC 适配器
│   │       └── antigravity.ts    # Antigravity (agy / agy acp) 适配器
│   ├── watchdog/                 # quant-watchdog 外围进程
│   │   ├── main.ts
│   │   ├── probes.ts             # 探活 5 进程、多标的行情新鲜度、磁盘、存储滞后
│   │   ├── fallback-alert.ts     # 直连 Telegram/飞书兜底告警
│   │   └── emergency-cancel.ts   # 可选极端失联 Binance 应急撤单（可保留保护性条件单）
│   ├── mcp/                      # 精简核心 MCP Server（11 个有状态/强契约工具）
│   │   └── server.ts
│   └── cli/                      # 统一本机 CLI `quant`（覆盖全部查询、运维与控制操作，支持 --json）
│       └── main.ts
└── tests/                        # 仅底座基础设施单测（严禁任何模拟策略脚本）
```

### 2.2 运行态数据与脚本工作区布局（`QUANT_DATA_DIR`，默认 `/var/lib/quant-harness`）

```text
/var/lib/quant-harness/
├── run/                          # 0750，UDS socket 文件
│   ├── admin.sock                # 0660（core 控制面、CLI、MCP、notifier 及内部订阅）
│   ├── runtime.sock              # 0660（托管脚本按 Token 访问；亦承接本地只读公共行情）
│   ├── storage.sock              # 0660（storage 历史查询、策略库、回测与落盘管理）
│   ├── agent.sock                # 0660（agent 托管；cli / mcp / watchdog 访问）
│   ├── notifier.sock             # 0660（notifier 托管；cli / mcp notify_send / watchdog 访问）
│   └── watchdog.sock             # 0660（watchdog 托管；cli / health 聚合读取）
├── db/
│   ├── ledger.db                 # SQLite WAL（storage 独写；Agent 与本地脚本开放只读直查）
│   ├── timeseries.db             # SQLite WAL（storage 独写；Agent 与本地脚本开放只读直查）
│   └── agent.db                  # SQLite WAL：会话索引/任务/去重/预算
├── state/
│   ├── core.db                   # core 独写，序号、关键事件、订单意图及控制状态
│   └── checkpoints/              # 当前 Checkpoint 的原子热备文件
├── strategies/
│   ├── _drafts/                  # 策略草稿自由编辑区（支持直接回测与 shadow 联调）
│   └── <family>/<version>.ts     # 0444 只读不可变业务源码，供查看、fork 与 live 上线
├── scripts/
│   ├── workers/                  # 常驻托管非交易脚本自由编辑区（监听、账户盯盘、外部采集）
│   └── research/                 # 一次性研究脚本自由编辑区（纯 Bun + TS，终端直接 bun run）
├── artifacts/
│   └── <codeHash>/entry.mjs      # 含固定 SDK 的只读执行产物（供托管运行与标准回测使用）
├── logs/
│   └── <role>/<name>/<run_id>.log
├── agent-hidden/                 # Antigravity hidden/ephemeral 隔离 app-data-dir
└── secrets/                      # 0700
    └── control.key               # 0600，本运行目录自动生成的本机控制面密钥
```

### 2.3 配置结构与热重载边界

主配置 `config/harness.json`（由 `QUANT_CONFIG_PATH` 指定）：

本地从 `.env_template` 创建仓库根目录 `.env`，由 Bun 自动加载。`QUANT_CONFIG_PATH` 指向主配置，`QUANT_DATA_DIR` 指向运行目录，`QUANT_SECRETS_DIR` 指向站外服务的凭据目录，默认 `~/.screets`。Git 跟踪 `.env_template`，本机 `.env` 被忽略。

```jsonc
{
  "dataDir": "/var/lib/quant-harness",
  "defaultSymbol": "BTCUSDT",
  // 基础常驻行情观测标的列表（支持 admin.sock POST /v1/reload 热重载增减）
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "binance": {
    "mainnetRest": "https://fapi.binance.com",
    "mainnetWs": "wss://fstream.binance.com",
    "testnetRest": "https://demo-fapi.binance.com",
    "testnetWs": "wss://demo-fstream.binance.com",
    "ipWeightLimitPerMin": 1800,
    "publicRestCacheTtlMs": {
      "exchangeInfo": 300000,
      "fundingInfo": 300000,
      "openInterest": 5000,
      "futuresData": 30000,
      "depth": 1000
    }
  },
  // 以下 accounts 与 risk 支持 admin.sock POST /v1/reload 热重载
  "accounts": {
    "sub_small": {
      "allowedSymbols": ["BTCUSDT"],
      "requireApproval": false,
      "requireApprovalOnStop": false,
      "requireApprovalOnManualTrade": false,
      "marginType": "CROSSED",          // 启动校验项：CROSSED | ISOLATED
      "initialLeverage": 3,             // 启动时设置，须 <= risk.maxLeverage
      "risk": { "maxOrderNotionalUsd": 100, "maxLeverage": 5, "maxOrdersPerMin": 20 },
      "labels": ["canary"]
    },
    "sub_main": {
      "allowedSymbols": ["BTCUSDT"],
      "requireApproval": true,
      "requireApprovalOnStop": false,
      "requireApprovalOnManualTrade": true,
      "marginType": "CROSSED",
      "initialLeverage": 3,
      "risk": { "maxOrderNotionalUsd": 5000, "maxLeverage": 10, "maxOrdersPerMin": 30 },
      "labels": ["prod"]
    }
  },
  "shadow": {
    // Shadow Testnet 账户池：每个账户同一时间仅允许 1 个 shadow 运行（与 live 同一套独占租约）
    "accounts": {
      "shadow_1": { "allowedSymbols": ["BTCUSDT", "ETHUSDT"], "marginType": "CROSSED", "initialLeverage": 3 }
    },
    "risk": { "maxOrderNotionalUsd": 1000, "maxLeverage": 10, "maxOrdersPerMin": 30 }
  },
  "im": {
    "notifierListen": { "host": "127.0.0.1", "port": 18101 },
    "telegram": {
      "enabled": true,
      "alertChatId": "-100...",
      "allowedAdminUserIds": ["123456789"]
    },
    "feishu": {
      "enabled": true,
      "alertChatId": "oc_...",
      "allowedAdminOpenIds": ["ou_..."]
    }
  },
  "agent": {
    "defaultBackend": "codex",
    "backends": {
      "codex": {
        "type": "codex",
        "command": "codex",
        "model": "gpt-5.4",
        "cwd": "/home/ubuntu/projects/quant-harness"
      },
      "antigravity": {
        "type": "antigravity",
        "command": "agy",
        "project": "quant-harness",
        "visibleAppDataDir": "/home/ubuntu/.gemini/antigravity",
        "hiddenAppDataDir": "/var/lib/quant-harness/agent-hidden/agy"
      }
    },
    "profiles": {
      "user":             { "defaultVisibility": "visible", "maxTurns": 80, "timeoutSec": 1800 },
      "cron_review":      { "defaultVisibility": "visible", "maxTurns": 40, "timeoutSec": 1200 },
      "script_triggered": { "defaultVisibility": "visible", "maxTurns": 30, "timeoutSec": 900 }
    },
    "guardrails": {
      "dedupCooldownSec": 300,
      "maxConcurrentTasks": 4,
      "dailyMaxTasks": 200,
      "dailyMaxBudgetUsd": 50,
      "maxDepth": 3
    },
    "cron": [
      { "name": "daily_review",  "expr": "5 0 * * *", "backend": "codex", "visibility": "visible" },
      { "name": "weekly_review", "expr": "15 0 * * 1", "backend": "codex", "visibility": "visible" }
    ]
  },
  "watchdog": {
    "intervalSec": 10,
    "coreTimeoutMs": 2000,
    "maxEventLoopLagMs": 250,
    "maxPublicWsStaleSec": 30,       // 按 symbols 逐标的逐基础主题检查有效帧
    "maxStorageSeqLag": 5000,        // 实际待处理事件数
    "minDiskFreeGb": 5,
    "emergencyCancel": {
      "enabled": false,
      "consecutiveFailures": 6,
      "includeConditional": false    // 默认仅撤普通挂单，保留交易所侧保护性止损条件单
    }
  }
}
```

- **热重载（`reload`）范围**：支持热重载全局 `symbols`（动态增减基础常驻流订阅，并同步初始化新标的 K 线聚合器）、各子账户的 `allowedSymbols`（新增标的即刻触发该标的的保证金/杠杆校验）、`requireApproval`、`requireApprovalOnStop`、`requireApprovalOnManualTrade`、`risk`、`labels` 与 `shadow.risk`；`marginType` / `initialLeverage` 仅在账户/标的初始化时校验与设置；新增或移除交易所 API 凭证需重启对应连接。
- **凭据文件**。`$QUANT_SECRETS_DIR/exchange.json` 结构为 `{ accounts: { [accountId]: { apiKey, apiSecret } }, shadow: { [shadowAccountId]: { apiKey, apiSecret } } }`。各账户也可通过 `credentialFile` 指定目录中的 JSON 文件名，文件直接包含 `{ apiKey, apiSecret }`。通知凭据放在 `$QUANT_SECRETS_DIR/im.json`，飞书和 Telegram 可各自通过 `credentialFile` 指定独立 JSON。文件权限设为 `0600`。`quant-core` 与启用应急撤单的 `quant-watchdog` 按需读取交易凭据。托管脚本通过当次 `QUANT_TOKEN` 调用服务。凭据目录或账户凭据文件的变更需要重启服务。
- **Webhook 路由映射（Nginx/Caddy HTTPS 反代示例，仅通知与审批）**：
  - `https://<domain>/webhooks/telegram/notifier` → `http://127.0.0.1:18101/webhooks/telegram`
  - `https://<domain>/webhooks/feishu/notifier` → `http://127.0.0.1:18101/webhooks/feishu`

---

## 3. 通信协议与事件总线契约

### 3.1 统一错误格式与状态码

所有 UDS HTTP JSON 接口失败时统一返回：

```json
{ "error": { "code": "STALE_EPOCH", "message": "Account sub_main active epoch is 4, got 3", "details": {} } }
```

| HTTP | Error Code | 语义 |
| :--- | :--- | :--- |
| `400` | `BAD_REQUEST` | 参数缺失或格式非法 |
| `401` | `UNAUTHORIZED` | 脚本令牌、服务凭据或 Webhook 来源校验失败 |
| `403` | `FORBIDDEN` | 脚本角色、能力集合（`capabilities`）或账户范围不允许当前操作 |
| `404` | `NOT_FOUND` | 账户、标的、策略、Run、提案或任务不存在 |
| `409` | `ACCOUNT_LOCKED` / `SHADOW_LOCKED` / `DATASET_LOCKED` / `STALE_EPOCH` / `PROPOSAL_CONFLICT` / `INTENT_CONFLICT` | 租约、数据集独占、提案或订单意图参数冲突 |
| `422` | `INVALID_ORDER` / `SYMBOL_NOT_ALLOWED` / `RISK_REJECTED` / `KILL_SWITCH_ACTIVE` / `HASH_MISMATCH` / `STRATEGY_NOT_SEALED` / `ACCOUNT_INCOMPATIBLE` | 业务或风控拦截；标的不在 `allowedSymbols`；账户持仓/保证金模式不符 |
| `429` | `RATE_LIMITED` | 账户下单频控、数据发布限流、脚本通知限流、公共 REST 权重节流或 Agent 冷却/预算超限 |
| `502` | `EXCHANGE_ERROR` | 已确认失败的上游请求 |
| `503` | `ORDER_UNKNOWN` / `RECONCILING` / `MARKET_STALE` | 执行结果待核实、账户对账中或目标标的开仓所需行情过期 |

### 3.2 主事件总线与高频行情旁路

#### (1) 主事件总线（Event Bus，严格有序 `seq`）
- **统一信封**：
  ```ts
  export interface BusEvent<T = unknown> {
    seq: number;        // 全局严格单调递增整数
    ts: number;         // Unix 毫秒时间戳
    topic: EventTopic;
    source: string;     // 如 "binance.mainnet" | "binance.testnet" | "account:sub_main" | "run:<runId>" | "manual" | "core"
    payload: T;
  }
  ```
- **分配序号**：首次部署从 0 建立序号状态。`quant-core` 是唯一写者，在 `state/core.db` 的持久事务中先将 `reserved_until` 提高一个区间（默认 4096），提交成功后才分配该区间的 `seq`。重启从已预留上界之后重新分配，未用完号码作废；`storage` 离线也能恢复。
- **关键事件先落盘**：`trade.*`、`shadow.*`、`state.*`、`agent.*`、有序号的 `sys.*`（含 `sys.notify`）与 `data.sample` 保存到 `critical_events(seq PRIMARY KEY, topic, envelope)`，持久提交后才能广播或确认成功。关联的订单意图、Checkpoint、提案或 Kill Switch 转移与对应事件在同一个 `core.db` 事务内提交。
- **实时与重放**：内存环形缓冲保留最近 20,000 帧。行情慢消费者队列上限 1,000，允许合并过期盘口与标记价。重连时先固定已发布水位 `W` 并缓冲新帧，将 `(after_seq, W]` 的日志与环缓冲按 `seq` 去重合并，再接入实时流；覆盖不到的行情区间返回 `gap{topics,fromSeq,toSeq}`，区间处理完发送无序号控制帧 `watermark{throughSeq}`。
- **确认与保留**：`storage`、`notifier`、`agent` 各有持久订阅游标，收到水位且本地已持久接收相关事件后确认。`core` 按所需消费者的最小确认水位清理日志。
- **主总线完整 Topic 清单**：
  - 基础行情（按 `symbols` 订阅，均含 `symbol` 字段）：
    - `market.kline`（`symbol`, `interval: "1m" | "5m" | "15m" | "1h" | "4h" | "1d"`, `openTime`, `closeTime`, `o/h/l/c/v`, `quoteVolume`, `closed`；`1m` 推送实时更新与闭合，其余周期由 `quant-core` 基于 `1m` 闭合 K 线按标的内存聚合、仅推送闭合帧）
    - `market.book_ticker`（`symbol`, `bid`, `bidQty`, `ask`, `askQty`, `last`, `eventTs`）
    - `market.mark_price`（`symbol`, `markPrice`, `indexPrice`, `fundingRate`, `nextFundingTime`, `eventTs`）
    - `market.liquidation`（来自 `{symbol}@forceOrder`：`symbol`, `side`, `orderType`, `price`, `avgPrice`, `qty`, `filledQty`, `status`, `tradeTime`）
    - `market.contract_info`（来自 `!contractInfo`：合约上市/下架/保证金阶梯变更通知）
  - 账户事件使用 `trade.*`（实盘）或 `shadow.*`（测试账户）前缀，后缀为 `order`、`algo_order`（来自 `ALGO_UPDATE`）、`fill`、`position`、`account`、`income` 或 `risk_reject`。
  - 数据与状态：`data.sample`（`dataset`, `ts`, `ingestedTs`, `key?`, `payload`, `runId`）、`state.checkpoint`（`namespace`, `accountId?`, `version`, `epoch`, `schemaVersion`, `updatedAt`, `data`）
  - 通知、Agent 与系统：`sys.notify`（托管脚本 `notify.send()` 写入：`runId`, `level`, `title`, `body`, `dedupKey?`, `cooldownSec?`）、`agent.request`、`agent.task_update`、`sys.handover`、`sys.proposal`、`sys.run_lifecycle`、`sys.kill_switch`、`sys.operation`、`sys.alert`、`sys.heartbeat`（无 `seq`）

#### (2) 高频行情旁路（Fast Stream Bypass，无 `seq`、有损、按需订阅）
- **信封格式**：`{ stream: string, type: FastStreamType, symbol?: string, eventTs: number, ingestedTs: number, payload: T }`（不分配 `seq`、不写 `critical_events`、不进主环形缓冲、默认不落盘 `timeseries.db`）。
- **流控与隔离**：每个消费连接独立维护有界发送队列（默认上限 512 帧），队列满时按 `drop_oldest` 丢弃旧帧并在帧元数据中标注 `droppedCount`；高频流量峰值完全隔离在 `fast-stream.ts` 内，绝不挤占主事件总线。
- **支持流清单（完整覆盖 Binance USDⓈ-M 官方高频/全市场 WS 流）**：
  - `/public` 路由：`depth`（`{symbol}@depth5|10|20[@100ms|@250ms|@500ms]`）、`depth_diff`（`{symbol}@depth[@100ms|@250ms|@500ms]`）、`rpi_depth`（`{symbol}@rpiDepth@500ms`）、`all_book_tickers`（`!bookTicker`）
  - `/market` 路由：`agg_trade`（`{symbol}@aggTrade`）、`continuous_kline`（`{pair}_{contractType}@continuousKline_{interval}`）、`ticker_24h`（`{symbol}@ticker`）、`all_tickers_24h`（`!ticker@arr`）、`mini_ticker`（`{symbol}@miniTicker`）、`all_mini_tickers`（`!miniTicker@arr`）、`all_mark_prices`（`!markPrice@arr[@1s]`）、`all_liquidations`（`!forceOrder@arr`）、`composite_index`（`{symbol}@compositeIndex`）、`asset_index`（`{asset}@assetIndex`）、`all_asset_indexes`（`!assetIndex@arr`）

### 3.3 本机 UDS 接口

#### (1) `admin.sock`（`quant-core` 控制面）

控制面以服务凭据识别调用方，MCP、`quant` CLI 与 `quant-notifier` 共用同一套运维操作契约。每次变更将 `actor`、`source`（`mcp` | `cli` | `notifier`）、目标、原因、结果及可选 `confirmationRef` 写入 `sys.operation`。

| 方法与路径 | 用途与调用方 |
| :--- | :--- |
| `GET /v1/health` | 返回 PID、Event Loop 延迟、按 `symbols` 逐标的各基础主题最后有效时间、高频按需订阅计数、私有 WS 心跳、发布水位、待确认事件数、账户对账与运行状态 |
| `GET /v1/market/snapshot?symbol=` | 返回指定标的（默认 `defaultSymbol`）最新盘口、标记价、指数价与资金费率；传 `symbol=*` 返回全部观测标的快照 |
| `GET /v1/market/rest/*` | 复用核心公共 Market Data REST 代理（§4.1），供 `quant` CLI 查询实时深度、OI、多空比、基差等 |
| `GET /v1/accounts` | 列出所有子账户配置（含 `allowedSymbols`）、占用 `strategyId`/`runId`/`epoch`、风控阈值、Kill Switch 与连接状态 |
| `GET /v1/accounts/:accountId/state` | 查询指定子账户实时余额、可用保证金、强平价、按标的 `{ long, short }` 持仓、活跃普通挂单与 Algo 条件单 |
| `GET /v1/state/:namespace` | 读取热内存 Checkpoint |
| `GET /v1/runs?mode=&status=` | 列出运行中及近期托管脚本（含能力标签与 `codeHash`） |
| `POST /v1/runs/start` | 启动免审批托管脚本：`worker`（指定 `entryPath`、`persistent?`、`stateName?` 与可组合 `capabilities: { accountRead?, publishDatasets?, notify?, agentTask? }`）或 `shadow`（接受 `strategyId` 或 `draftPath`，占用 Shadow 池空闲账户，池满返回 `409 SHADOW_LOCKED`；数据集冲突返回 `409 DATASET_LOCKED`） |
| `POST /v1/runs/:runId/stop` | 接受 `{ reason?: string }`。实盘开启 `requireApprovalOnStop` 时返回 `202 { proposalId, status: "pending_approval" }`，确认后执行。策略按 §4.3 的默认规则和可选 `onStop` 收尾。停止 `worker` 时回收终态并释放其数据集 |
| `POST /v1/live/propose` | 按不可变 `strategyId` 发起实盘上线或替换（含 `handoverOrders: "adopt" | "cancel_open"`、`includeConditional?`、`resetState?`）。免审批直接执行；需审批返回 `202 { proposalId, status: "pending_approval" }` |
| `POST /v1/accounts/:accountId/manual-order` | 受控单次手动下单（支持普通单与 Algo 条件单，传 `action`、`symbol?`、`params`、`reason`）。走同一风控与减仓绿灯；账户开启 `requireApprovalOnManualTrade` 时返回 `202 { proposalId }`，否则直接执行，不抢占策略租约 |
| `DELETE /v1/accounts/:accountId/manual-order` | 受控单次手动撤单（按 `orderId`/`algoId`/`clientOrderId`），可按配置免审批或走提案 |
| `DELETE /v1/accounts/:accountId/manual-orders` | 受控一键撤全部挂单（支持 `symbol?` 与 `includeConditional?`） |
| `POST /v1/accounts/:accountId/manual-leverage` | 受控单次调杠杆（校验 `allowedSymbols` 与 `<= risk.maxLeverage`） |
| `GET /v1/proposals/:proposalId` | 返回动作（`live_start` / `live_replace` / `live_stop` / `manual_trade`）、目标、完整参数、`proposalDigest`、过期时间、状态及执行结果 |
| `POST /v1/proposals/:proposalId/approve` | MCP / CLI / notifier 提交 `{ proposalDigest, confirmationRef? }`，原子消费提案并执行绑定操作 |
| `POST /v1/proposals/:proposalId/reject` | MCP / CLI / notifier 驳回待处理提案并记录原因 |
| `POST /v1/risk/kill-switch` | 按全局或账户启用 Kill Switch，支持 `cancelOpenOrders`、`includeConditional`（默认 `false` 保留保护止损）与原因 |
| `DELETE /v1/risk/kill-switch` | 解除指定范围的 Kill Switch，审计实际执行者 |
| `POST /v1/reload` | 热重载 §2.3 配置（含 `symbols` 列表与子账户风控/标的/审批配置），并记录配置差异 |
| `GET /v1/stream?topics=&after_seq=` | 内部模块订阅主事件流、缺口与水位控制帧 |
| `POST /v1/stream/ack` | 持久消费者确认 `{ throughSeq }` |

#### (2) `runtime.sock`（`quant-core` 脚本运行时与本地公共只读行情）

托管脚本调用必须携带 `Authorization: Bearer <QUANT_TOKEN>`，服务端按 `Session.mode` 与 `Session.capabilities` 强校验权限；同时对本机无 Token 的 `local` 研究脚本开放仅限公共行情快照与公共 Market Data REST 代理的只读接口（标注 `local-ro`），严禁无 Token 访问任何账户、交易、状态或发布端点。

| 方法与路径 | 允许角色 / 能力 | 语义 |
| :--- | :--- | :--- |
| `GET /v1/stream?topics=&after_seq=&symbols=&accounts=` | 全部托管（`live`/`shadow`/`worker`） | 订阅主事件流：基础行情、外部数据集；`live`/`shadow` 可订本账户私有回报；声明 `accountRead` 的 `worker` 可按白名单订阅指定账户的 `trade.*` 只读事件流；`sys.handover` 仅投递目标 `runId` |
| `GET /v1/market/fast-stream?streams=...` | 全部托管 + `local-ro` | 订阅高频行情旁路（按需动态触发上游 `SUBSCRIBE`，零订阅自动退订，无 `seq`、`drop_oldest`） |
| `POST /v1/session/ready` | 全部托管 | SDK 启动握手，校验运行有效后激活会话，重复幂等 |
| `GET /v1/market/snapshot?symbol=` | 全部托管 + `local-ro` | 获取内存最新行情快照（支持单标的或 `symbol=*`） |
| `GET /v1/market/rest/*` | 全部托管 + `local-ro` | 调用 `quant-core` 统一代理缓存的 Binance USDⓈ-M 全部官方 Market Data REST 接口（§4.1） |
| `GET /v1/accounts` | `worker`（声明 `accountRead`） | 列出本运行被授权只读的账户摘要与运行状态 |
| `GET /v1/accounts/:accountId/state` | `worker`（授权该账户 `accountRead`） | 只读查询目标账户实时余额、双腿持仓、普通挂单与 Algo 条件单 |
| `GET /v1/accounts/:accountId/fills?after_seq=&through_seq=` | `worker`（授权该账户 `accountRead`） | 只读查询目标账户历史成交及补齐水位 |
| `POST /v1/trade/order` | `live`, `shadow` | 按 `symbol`（默认 `defaultSymbol`）与稳定 `clientOrderId` 下普通单或 Algo 条件单，按 §4.1 提交 |
| `GET /v1/trade/order?clientOrderId=&symbol=` | `live`, `shadow` | 按 SDK 业务键查询原意图状态、普通单或条件单结果（可跨 epoch） |
| `GET /v1/trade/fills?after_seq=&through_seq=&symbol=` | `live`, `shadow` | 查询本账户持久成交及原始 `seq` |
| `DELETE /v1/trade/order` | `live`, `shadow` | 按 `orderId` / `algoId` / `clientOrderId` 撤普通单或 Algo 条件单 |
| `DELETE /v1/trade/orders` | `live`, `shadow` | 撤销当前账户挂单（支持 `symbol?` 与 `includeConditional?: boolean`，默认 `false`） |
| `GET /v1/trade/positions?symbol=` | `live`, `shadow` | 返回指定标的 `{ long, short }` 双腿持仓（或传 `symbol=*` 返回全部） |
| `GET /v1/trade/account` | `live`, `shadow` | 返回 `{ walletBalance, availableBalance, unrealizedPnl, marginType, leverages }` |
| `POST /v1/trade/leverage` | `live`, `shadow` | 调整指定 `symbol` 杠杆（校验 `allowedSymbols` 且 `<= risk.maxLeverage`） |
| `GET /v1/trade/open-orders?symbol=&includeConditional=` | `live`, `shadow` | 合并返回当前活跃普通挂单与（默认包含的）Algo 条件单 |
| `GET /v1/state` | 全部托管 | 读取当前命名空间最新 Checkpoint |
| `PUT /v1/state` | 全部托管 | 写入当前命名空间 Checkpoint（上限 **1 MB**）。交接期间仅接受本次 `handoverId` 的终态 Checkpoint |
| `GET /v1/handover` | 全部托管 | 查询本运行待处理交接的 `handoverId` 和动作（`stop` 或 `replace`） |
| `POST /v1/data/publish` | 声明 `publishDatasets` 的托管脚本 | 发布本运行独占声明的任意 `dataset` 样本（单条 ≤ 64 KB，单运行限频 50 条/秒），`core` 补 `ingestedTs` 后广播 |
| `POST /v1/notify` | 具有 `notify` 能力的托管脚本 | 校验权限与每运行限流（默认每分钟 10 条），将 `sys.notify` 写入主事件总线立即返回 `202 { queued: true, seq }`，零阻塞 `core` |
| `POST /v1/agents/tasks` | 具有 `agentTask` 能力的托管脚本 | 预分配 `taskId`，持久记录来源及 `agent.request`（支持可选 `cooldownSec` / `title` / `backend` / `visibility`）后返回 `202 { taskId }` |
| `GET /v1/agents/tasks/:taskId` | 具有 `agentTask` 能力的托管脚本 | 查询本运行发起的任务状态（含合并别名返回） |

#### (3) `storage.sock`（`quant-storage` 持久层与策略库）

统一通过 `storage.sock` 提供历史查询、回补、策略库管理、标准同码回测与日志服务；同时 `ledger.db` 与 `timeseries.db` 明确开放给 Agent 及 `scripts/research/` 一次性研究脚本以 SQLite WAL 只读模式直查。

| 方法与路径 | 用途 |
| :--- | :--- |
| `GET /v1/health` | 返回两库独立游标、共同水位 `lastSeq`、SQLite 状态与磁盘剩余空间 |
| `GET /v1/meta/cursor` | 返回 `{ ledgerSeq, timeseriesSeq, lastSeq }` |
| `GET /v1/market/klines?symbol=&interval=&start=&end=&backfill=` | 查询指定标的历史 K 线（缺口按需经 Binance REST 自动回补） |
| `GET /v1/market/funding-rates?symbol=&start=&end=&backfill=` | 查询历史资金费率（缺口按需经 `/fapi/v1/fundingRate` 自动回补） |
| `GET /v1/market/liquidations?symbol=&start=&end=&limit=` | 查询历史强平/爆仓事件记录 |
| `GET /v1/data/datasets` | 列出所有外部采集数据集、最新写入时间、样本数与 TTL 状态 |
| `GET /v1/data/query?dataset=&start=&end=&limit=&asOf=&asOfSeq=` | 按业务时间选样本；回测按接收时间与序号防未来函数；预热同事务返回 `{ items, maxSeq }` |
| `GET /v1/ledger/query?accountId=&mode=&symbol=&type=&start=&end=&limit=&after_seq=&through_seq=` | 查询普通单、条件单、成交、Shadow 与风控拒单记录 |
| `GET /v1/ledger/pnl?target=&window=&symbol=&mode=` | 按 `live`、`shadow` 分别统计净收益、胜率、盈亏比、最大回撤、手续费与资金费率，可指定其中一种模式 |
| `GET /v1/checkpoints?namespace=&version=&limit=` | 查询 Checkpoint 历史版本 |
| `POST /v1/logs/append` | 接收 `quant-core` 转发的托管脚本 `stdout/stderr` 并写入滚动日志 |
| `GET /v1/logs/:runId?tail=&since=&grep=` | 查询托管脚本滚动日志 |
| `GET /v1/strategies/search?...` | 按五维标签与家族检索不可变策略库与历史绩效 |
| `GET /v1/strategies/:strategyId` | 获取指定不可变策略源码、`code_hash`、血缘谱系、标签与归因备注 |
| `POST /v1/strategies/fork` | 从不可变策略复制到 `strategies/_drafts/` 并记录草稿谱系 |
| `POST /v1/strategies/seal` | 预检源码快照、打包固定 SDK，登记只读源码及按 `codeHash` 寻址的执行文件 |
| `PATCH /v1/strategies/:strategyId/tags` | 更新五维标签、追加归因备注或切换 `lifecycle` |
| `POST /v1/scripts/validate` | 对托管脚本源码快照执行静态预检（按 `mode` 与 `capabilities`） |
| `POST /v1/scripts/prepare` | 供 `core` / `storage` 准备托管执行产物：已封存 `strategyId` 校验登记产物；`shadow` 草稿或 `worker` 路径执行预检打包并记录哈希（不写入 `strategies` 不可变表），返回 `{ artifactPath, sourceHash, codeHash }` |
| `POST /v1/backtest/run` | 对草稿路径或 `strategyId` 执行标准同码历史事件回放（支持多标的与 Algo 条件单撮合） |

#### (4) `agent.sock`（`quant-agent` 任务与会话网关）

| 方法与路径 | 用途 |
| :--- | :--- |
| `GET /v1/health` | 返回活跃任务数、各后端状态、当日预算消耗 |
| `POST /v1/tasks` | 接受 `{ prompt, profile, backend?, sidebarVisibility?, dedupKey?, cooldownSec?, title? }`，返回 `{ taskId, sessionId, status }` |
| `POST /v1/sessions/:sessionId/messages` | 向已有会话续聊：`{ message, sidebarVisibility? }` → `{ taskId, status }` |
| `GET /v1/tasks/:taskId` | 查询任务状态、耗时、输出摘要、关联 `sessionId` |
| `POST /v1/tasks/:taskId/cancel` | 取消运行中任务（Codex `turn/interrupt` / Antigravity `SIGINT`） |

---

## 4. 核心进程 `quant-core` 详细设计

### 4.1 Binance USDⓈ-M Futures 全量直连网关

#### (1) 分路 WebSocket 行情体系（`public-ws.ts` + `fast-stream.ts`）

严格遵循 Binance USDⓈ-M 2026 路由拆分（高并发盘口/深度走 `/public`，常规行情走 `/market`，私有回报走 `/private`），按流量特征分为两层以保护 `quant-core` 稳定与 SQLite 吞吐：[Binance 路由说明](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice) · [Demo 地址](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)

1. **基础常驻流（`public-ws.ts`）**按 `config.symbols` 订阅。网关按连接标记行情来源。研究、回测和价格监听订阅主网数据。联调需要时，Shadow 会话可读取内存中的测试网行情，例如核对测试单的触发价。
   - `/public` 组合流：`{symbol}@bookTicker`（最优买卖盘口与数量）；
   - `/market` 组合流：`{symbol}@kline_1m`（实时与闭合 `1m` K 线）、`{symbol}@markPrice@1s`（标记价、指数价、资金费率、下次结算时间）、`{symbol}@forceOrder`（强平/爆仓单推送）、`!contractInfo`（全局合约规则/保证金阶梯变更通知，收到变更自动触发 `exchangeInfo` 缓存刷新）；
   - **多周期 K 线内存聚合**。按环境和标的分别用闭合的 `1m` K 线聚合 `5m / 15m / 1h / 4h / 1d`（UTC 对齐）。主网启动、重连或新增标的时，从主网 REST 补当前未闭合窗口。若 Shadow 联调需要测试网预热，只将当前窗口补入其内存聚合器。
   - 按 `(env, symbol, topic)` 独立记录最后有效帧时间；任意连接按 1s、2s、5s、15s 退避重连；某标的缺少新鲜标记价时仅拦截该标的开仓（返回 `503 MARKET_STALE`），撤单与减仓保持可用。

2. **高频与全市场按需动态流（`fast-stream.ts`，独立连接 + 引用计数 + 无 `seq` 有损旁路，默认不落盘）**：
   - 维护两个按需 WS 连接池（`/public/stream` 与 `/market/stream`）及订阅引用计数 `Map<binanceStreamName, Set<Subscriber>>`；
   - 当首个脚本订阅某流时，向对应通道发送 `{"method":"SUBSCRIBE","params":[streamName],"id":...}`；最后一个订阅者断开时发送 `UNSUBSCRIBE`；单连接遵守 Binance 24 小时生命周期前主动平滑重建并重放当前引用计数 > 0 的流清单；
   - **完整支持的 Binance USDⓈ-M 官方高频/全市场流**：

| 通道 | 逻辑类型（`FastStreamType`） | Binance 官方流名 | 说明 |
| :--- | :--- | :--- | :--- |
| `/public` | `depth` | `{symbol}@depth{5\|10\|20}[@100ms\|@250ms\|@500ms]` | 有限档完整盘口快照（开箱即用，无需本地维护 diff 序列） |
| `/public` | `depth_diff` | `{symbol}@depth[@100ms\|@250ms\|@500ms]` | 增量盘口深度变更（可配合 `market.depth()` REST 快照建本地全量簿） |
| `/public` | `rpi_depth` | `{symbol}@rpiDepth@500ms` | Retail Price Improvement 深度流 |
| `/public` | `all_book_tickers` | `!bookTicker` | 全市场最优买卖盘口实时更新 |
| `/market` | `agg_trade` | `{symbol}@aggTrade` | 归集市场成交流（100ms 聚合，含主动买/卖方向，可用于计算 CVD） |
| `/market` | `continuous_kline` | `{pair}_{contractType}@continuousKline_{interval}` | 连续合约 K 线（`perpetual` / `current_quarter` / `next_quarter`） |
| `/market` | `ticker_24h` / `all_tickers_24h` | `{symbol}@ticker` / `!ticker@arr` | 单标的或全市场 24h 滚动窗口完整 Ticker 统计 |
| `/market` | `mini_ticker` / `all_mini_tickers` | `{symbol}@miniTicker` / `!miniTicker@arr` | 单标的或全市场 24h 精简 Ticker |
| `/market` | `all_mark_prices` | `!markPrice@arr[@1s]` | 全市场标记价与资金费率快照 |
| `/market` | `all_liquidations` | `!forceOrder@arr` | 全市场强平/爆仓流 |
| `/market` | `composite_index` | `{symbol}@compositeIndex` | 综合指数标的成分与价格信息 |
| `/market` | `asset_index` / `all_asset_indexes` | `{asset}@assetIndex` / `!assetIndex@arr` | 多资产模式保证金资产汇率指数 |

#### (2) 官方 Market Data REST 统一代理与缓存（`market-rest.ts`）

托管脚本、本地研究脚本（经 `local-ro`）与 `quant` CLI 通过 `quant-core` 代理查询 Binance 公共 REST 数据。代理复用出口 IP 限频桶，按上游环境、接口和请求参数缓存结果。各接口的缓存期限如下。

| 分组 | 代理路径（`/v1/market/rest/...`） | 对应 Binance 官方接口 | 缓存 TTL |
| :--- | :--- | :--- | :--- |
| 规则与盘口 | `exchange-info`、`depth`、`trades`、`agg-trades` | `GET /fapi/v1/exchangeInfo`、`/fapi/v1/depth`、`/fapi/v1/trades`、`/fapi/v1/aggTrades` | `exchangeInfo` 5m（`!contractInfo` 实时失效）；`depth` 1s；成交不缓存 |
| 全系列 K 线与价格 | `klines`、`continuous-klines`、`index-klines`、`mark-klines`、`premium-klines`、`premium-index`、`ticker-24h`、`ticker-price`、`book-ticker` | `GET /fapi/v1/klines`、`/fapi/v1/continuousKlines`、`/fapi/v1/indexPriceKlines`、`/fapi/v1/markPriceKlines`、`/fapi/v1/premiumIndexKlines`、`/fapi/v1/premiumIndex`、`/fapi/v1/ticker/24hr`、`/fapi/v1/ticker/price`、`/fapi/v1/ticker/bookTicker` | K 线 2s；实时价/Ticker 1s |
| 资金费、OI 与情绪指标 | `funding-rates`、`funding-info`、`open-interest`、`open-interest-hist`、`top-ls-accounts`、`top-ls-positions`、`global-ls-accounts`、`taker-volume`、`basis`、`index-info`、`constituents`、`asset-index` | `GET /fapi/v1/fundingRate`、`/fapi/v1/fundingInfo`、`/fapi/v1/openInterest`、`/futures/data/openInterestHist`、`/futures/data/topLongShortAccountRatio`、`/futures/data/topLongShortPositionRatio`、`/futures/data/globalLongShortAccountRatio`、`/futures/data/takerlongshortRatio`、`/futures/data/basis`、`/fapi/v1/indexInfo`、`/fapi/v1/constituents`、`/fapi/v1/assetIndex` | `fundingInfo` 5m；`openInterest` 5s；`/futures/data/*` 30s |

#### (3) 多子账户池、Algo 条件单与私有流对账（`pool.ts` + `account-client.ts`）

1. **多子账户同构**：所有实盘子账户与 Shadow 池账户共用 `BinanceAccountClient`，仅注入不同 `{ restBaseUrl, wsBaseUrl, apiKey, apiSecret, allowedSymbols }`。
2. **账户启动校验（按 `allowedSymbols` 遍历）**：
   - **Hedge Mode**：调 `GET /fapi/v1/positionSide/dual`；非双向且全账户空仓无普通/条件挂单时自动切换，否则标记不兼容并告警；
   - **保证金模式与初始杠杆**：遍历 `allowedSymbols`，对每个标的核验 `marginType`（空仓无挂单时自动切换）与 `initialLeverage <= risk.maxLeverage`；已有仓位恢复时若热重载令杠杆超限则该标的进入仅减仓状态。
3. **私有流与断线补账（完整含 Algo 条件单）**：
   - 初始化或私有流中断时置账户为 `reconciling` 暂停新增开仓。建立 `/private/ws?listenKey=<key>&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/ALGO_UPDATE` 并缓冲增量（每 25 分钟续期 `listenKey`）；
   - 拉取余额、持仓、`GET /fapi/v1/openOrders` 与 **`GET /fapi/v1/openAlgoOrders`** 全量快照；另从持久对账水位按账户与 `allowedSymbols` 分页补取 `/fapi/v1/allOrders`、`/fapi/v1/userTrades` 与 `/fapi/v1/income`；对之前未终结的普通单、条件单和结果未知意图逐笔查单（普通单查 `/fapi/v1/order`，条件单查 `GET /fapi/v1/algoOrder`）；
   - 合并去重并同事务持久化成交、条件单状态变更、资金费与对账水位，核对无矛盾后发布账户就绪。
4. **Hedge Mode 四原语、Algo 条件单路由与稳定意图键**：
   - 方向映射：`openLong` → `BUY / LONG`、`closeLong` → `SELL / LONG`、`openShort` → `SELL / SHORT`、`closeShort` → `BUY / SHORT`（Hedge 模式天然不反手）；
   - **按订单种类分流（对齐 Binance USDⓈ-M 新版 Algo Order Service）**：
     - **`STANDARD`（`MARKET` / `LIMIT`）**：走 `POST /fapi/v1/order`、`DELETE /fapi/v1/order`、`DELETE /fapi/v1/allOpenOrders`、`GET /fapi/v1/order`、`GET /fapi/v1/openOrders`；
     - **`CONDITIONAL`（`STOP_MARKET` / `TAKE_PROFIT_MARKET` / `STOP` / `TAKE_PROFIT` / `TRAILING_STOP_MARKET`）**：走 **`POST /fapi/v1/algoOrder`**（必传 `algoType=CONDITIONAL`、`clientAlgoId`、`triggerPrice` / `callbackRate`、`workingType` 等）、**`DELETE /fapi/v1/algoOrder`**、**`DELETE /fapi/v1/algoOpenOrders`**、**`GET /fapi/v1/algoOrder`**、**`GET /fapi/v1/openAlgoOrders`**；条件单触发后生成的实际成交仍经 `ORDER_TRADE_UPDATE` 回报并关联到账户账本。
   - **稳定意图键与先记意图再发单**：脚本或手动请求为业务意图提供 `clientOrderId`（1–16 位 `[A-Za-z0-9_-]`）；`core` 以 `(mode, accountId, symbol, clientOrderId)` 唯一索引保存 `order_kind`、规范化参数、来源（策略 `run/epoch` 或 `manual`）、状态与结果，首次受理生成固定交易所号 `qh_` + 32 位 hex（普通单传 `newClientOrderId`，条件单传 `clientAlgoId`）。沿用 `prepared → sending → accepted/rejected/unknown` 状态机；`unknown` 意图在核实前阻止换代并告警。
5. **统一出口 IP 限频桶（`signer.ts`）**：所有私有交易 REST 与公共 Market Data REST 共享按 IP 的权重桶，对照 `X-MBX-USED-WEIGHT-1M` 实时校准；为撤单、减仓与未决订单查询预留高优配额（公共行情代理限流绝不饿死实盘交易与撤单）。

### 4.2 账户独占锁、受控手动交易、防呆风控、Kill Switch 与 Checkpoint

1. **账户独占租约（实盘与 Shadow 统一）**：
   - 每个实盘 `accountId` 同一时刻仅允许 1 个活跃 `live` 租约；已有租约时拒绝普通启动（`409 ACCOUNT_LOCKED`），仅允许通过 `replace` 递增 `epoch` 交接；
   - `shadow` 启动时从 Shadow 池分配空闲账户，支持运行已封存 `strategyId` 或 `_drafts/` 草稿。停机时完成 §4.3 的订单对账后释放租约。
   - `live` / `shadow` 交易写入校验绑定账户、`runId`、`epoch` 与运行状态，过期返回 `409 STALE_EPOCH`。
2. **受控单次手动交易干预**：
   - 经 `admin.sock /v1/accounts/:accountId/manual-*` 提交，进入目标账户的**同一个串行风控与意图状态机入口**；
   - **不抢占、不中断当前活跃实盘策略租约**（意图与事件标记 `source: "manual"`，`runId: null`）；成交与条件单变更照常推送至主事件总线，活跃策略可通过 `watchFills()` / `positions()` / `openOrders()` 实时感知仓位变化；
   - 读取账户配置 `requireApprovalOnManualTrade`（若为 `true` 则先落盘 `pending` 提案经审批确认后执行；若为 `false` 直接执行），全程写入 `operation_audit` 并发高优通知。
3. **数据集独占写入者（支持单运行发布多 `dataset`）**：
   - `Map<Dataset, runId>`：同一 `dataset` 同一时刻仅允许 1 个活跃写入者，冲突返回 `409 DATASET_LOCKED`；单个 `worker`（或可选声明的策略）可在启动时声明多个 `publishDatasets: string[]`，释放时原子释放其占用的全部数据集。
4. **按动作与订单种类检查风险**：
   - 校验标的在 `account.allowedSymbols` 内（否则 `422 SYMBOL_NOT_ALLOWED`），校验有限正数、价格/数量步长精度（取自 `exchangeInfo` 缓存）与条件单必填触发参数；
   - **开仓（含开仓类条件单）**：Kill Switch、目标标的行情过期、未完成对账或杠杆超限时拒绝；市价单与市价条件单按新鲜标记价（或 `triggerPrice`）估算名义金额，检查单笔限额与频控；
   - **减仓与撤单（含 `closeLong`/`closeShort` 挂出的条件止损/止盈单）**：按 Hedge 持仓腿及可平数量（扣除本地未决普通平仓单量；保护性条件止损/止盈单允许按该腿持仓总量校验，防止止损+止盈双边保护单互相误拒）放行；**开仓限额、Kill Switch、杠杆收紧均不阻断合法减仓、挂保护性止损/止盈条件单或撤单**。
5. **Kill Switch 与撤单范围**：
   - `core.db` 同事务提交开关状态与 `sys.kill_switch`；开启 `cancelOpenOrders` 时默认仅撤普通挂单，仅当显式传入 `includeConditional: true` 时才同步调用 `DELETE /fapi/v1/algoOpenOrders` 撤销条件单，避免误撤救命止损。
6. **托管 Checkpoint（上限放宽至 1 MB）**：
   - 命名空间：`live` → `account:<accountId>`，`shadow` → `shadow:<runId>`，`worker` → `worker:<stateName || runId>`；
   - 结构体：`{ namespace, accountId?, version, epoch, schemaVersion, updatedAt, data }`，JSON 序列化硬上限 **1 MB**（超限返回 `422`）；
   - 按命名空间串行检查租约、递增版本，`core.db` 事务落盘 + `state.checkpoint` 事件广播 + 原子热备文件（`tmp + fsync + rename + dir fsync`）。

### 4.3 脚本调度、提案与账户交接

1. **托管脚本启动与环境注入（`Bun.spawn`）**：`core` 与 `storage`（标准回测）通过 `Bun.spawn(["bun", "run", artifactPath], { env })` 拉起经校验的只读产物 `entry.mjs`，显式构造最小环境变量集合：
   - `QUANT_SOCKET`（`runtime.sock`；回测模式替换为 `QUANT_BACKTEST_SOCKET`）、`QUANT_STORAGE_SOCKET`（`storage.sock`；回测不注入）、`QUANT_TOKEN`（32 字节随机 hex）、`QUANT_MODE`（`live` | `shadow` | `worker` | `backtest`）、`QUANT_RUN_ID`；
   - 按角色附加 `QUANT_ACCOUNT` / `QUANT_EPOCH`（`live`、`shadow`）、`QUANT_CAPABILITIES`（JSON 序列化能力清单：`accountRead`、`publishDatasets`、`notify`、`agentTask`）、`QUANT_STATE_NAME`（常驻 `worker` 可选稳定状态名）；
   - 严禁向脚本注入交易所密钥、IM 凭据或 `admin.sock` 路径。
2. **进程、日志与孤儿回收**：活跃运行 `runId`、`pid`、`mode`、`capabilities` 与 `startedAt` 持久记录在 `core.db`；管道收集 `stdout/stderr` 异步批量 POST 至 `storage.sock /v1/logs/append`；停止或交接发 `SIGTERM`，超时 `SIGKILL` 并撤销令牌；`core` 重启先扫描清理孤儿 PID 再执行自动恢复；SDK 在 `runtime.sock` 连续断连超 60 秒主动退出自保。
3. **提案状态机**覆盖 `live_start`、`live_replace`、`live_stop`、`manual_trade`。账户配置决定直接执行或登记 `pending` 提案（15 分钟过期，绑定完整参数与 `proposalDigest`）。可信 Agent 可通过 MCP 或 `quant` CLI 确认或驳回提案，包括自己提交的提案。交易员也可通过通知卡片确认。core 原子消费提案，记录实际操作者和来源，有人工确认记录时可附 `confirmationRef`。
4. **`replace` 与 `stop` 五阶段交接**：
   1. **冻结**：绑定旧 `run/epoch`，置 `handover` 关闭旧运行交易与普通状态写权；
   2. core 发送带动作及 `handoverId` 的 `sys.handover` 和 `SIGTERM`。热替换调用 `onHandover`。停策略时优先调用可选的 `onStop`，未注册时沿用 `onHandover` 保存终态。SDK 对同一次交接只调用一个回调，收尾限时 3 秒。
   3. **收敛在途订单**：确认旧运行退出、作废未发送意图，等已送出的普通单与条件单请求取得确定结果（`unknown` 阻止放行并告警）；
   4. **处置挂单**。
      - **默认撤掉本策略创建或接管的挂单**，包括普通单和条件单。只保留保护同一标的、同一方向现有仓位的止盈止损，例如追踪止损和下单意图标为止盈的平仓限价单。开仓条件单、对应仓位已清零的保护单都要撤。手动单和外部订单留给原操作者管理。
      - **策略可用 `harness.onStop` 自定义收尾**，保存状态并指定自己哪些订单要撤、哪些要留。未点名的订单按默认规则处理。未定义回调，或脚本退出、报错、超时而未提交决定时，core 执行默认规则。
      - **core 负责执行撤单和对账**，按 `handoverId` 记录脚本交回的决定或采用的默认规则。遇到部分成交、条件单触发或重启，继续核对实际仓位、挂单及触发产生的子订单。确认完成才释放租约，有剩余仓位或挂单时发送高优通知。
      - **热替换**。`adopt` 把原策略管理的挂单交给新运行。`cancel_open` 撤普通挂单，`includeConditional` 决定是否同步撤条件单。交接后继续由策略管理的挂单登记到新运行，保留原下单归属。
   5. **启动新版**：`replace` 同事务写入新 `epoch`、目标哈希、`starting` 及可选空 Checkpoint，`Bun.spawn` 启动新版并完成 SDK 握手激活。
5. **`core` 恢复**。`state/core.db` 由单写 Worker 管理（SQLite WAL，`synchronous=FULL`）。启动先清孤儿 PID，恢复控制状态与订阅，按 `allowedSymbols` 核对各账户持仓、普通挂单与 Algo 条件单。优先续办未完成的交接，再恢复活跃 `live` 和标记 `persistent: true` 的 `worker`。其余一次性 `worker` 与 `shadow` 标为 `interrupted`，中断的 Shadow 账户按停机规则收尾后回池。

**`core.db` 持久键**：

| 记录 | 主键与恢复所需字段 |
| :--- | :--- |
| `sequence_state`、`critical_events`、`consumer_ack` | 单例 `reserved_until`；按 `seq` 的事件信封；按持久消费者的 `through_seq` |
| `leases`、`proposals` | `(mode, accountId)` 对应 `runId`/`epoch`、目标（`strategyId` 或 `draftPath`）、`codeHash`、从上一运行接管的活动订单、操作 ID、阶段及已采纳的停机决定。提案按 `proposalId` 存动作类型、摘要、参数、到期时间与结果 |
| `order_intents` | `(mode, accountId, symbol, clientOrderId)` 唯一，存 `order_kind`（`STANDARD` \| `CONDITIONAL`）、`source`（`strategy` \| `manual`）、交易所号（`exchange_client_id` 唯一）、规范化请求、原 `run/epoch`、状态与已知 `exchange_order_id` / `algo_id` |
| `reconciliation_cursors` | 按环境、账户、交易对和历史接口记录已处理窗口、分页边界与业务 ID |
| `kill_switches`、`checkpoint_latest` | 按作用范围存熔断状态；按命名空间存完整 Checkpoint（≤ 1 MB）、终态 `handoverId` 和版本 |
| `active_runs`、`task_requests` | 活跃托管运行 PID、`mode`、`persistent`、`capabilities_json` 与目标哈希；`task_requests` 按 `taskId` 记录来源 `runId` 与请求 `seq` |

---

## 5. `@quant/tools` SDK、静态预检与双模式离线回测

### 5.1 `@quant/tools` 核心 API 契约

托管脚本维持单文件 `.ts` 且唯一运行时导入 `@quant/tools`，由 `storage` 打包为固定 SDK 的单文件 `entry.mjs` 执行；本地研究脚本（`scripts/research/*.ts`）在终端直接 `bun run` 时同样可 `import ... from "@quant/tools"`——SDK 检测到无 `QUANT_TOKEN` 时自动加载 `transport-local.ts` 进入 `local` 只读模式（访问 `storage.sock` 与 `runtime.sock` 公共只读行情接口，调用任何写接口直接报错）。

```ts
export type WorkingType = "MARK_PRICE" | "CONTRACT_PRICE";

export type OrderParams = {
  symbol?: string;                  // 省略时取 config.defaultSymbol
  qty: number;                      // 合约数量，有限正数
  clientOrderId: string;            // 稳定业务意图键，1–16 位 [A-Za-z0-9_-]
} & (
  // 普通单（走 /fapi/v1/order）
  | { type: "MARKET"; price?: never; timeInForce?: never }
  | { type: "LIMIT"; price: number; timeInForce?: "GTC" | "IOC" | "FOK" | "GTX" }
  // 交易所原生条件单（走 Binance Algo Order Service /fapi/v1/algoOrder）
  | { type: "STOP_MARKET" | "TAKE_PROFIT_MARKET"; triggerPrice: number; workingType?: WorkingType; priceProtect?: boolean }
  | { type: "STOP" | "TAKE_PROFIT"; price: number; triggerPrice: number; timeInForce?: "GTC" | "IOC" | "FOK" | "GTX"; workingType?: WorkingType; priceProtect?: boolean }
  | { type: "TRAILING_STOP_MARKET"; callbackRate: number; activationPrice?: number; workingType?: WorkingType; priceProtect?: boolean }
);

export interface PositionLeg {
  symbol: string;
  side: "LONG" | "SHORT";
  qty: number;
  entryPrice: number;
  markPrice: number;
  unrealizedPnl: number;
  leverage: number;
  liquidationPrice: number;
}

export interface CheckpointEnvelope<T = Record<string, unknown>> {
  version: number;
  epoch: number;
  schemaVersion: number;
  updatedAt: number;
  data: T;
}

export type KlineInterval = "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "8h" | "12h" | "1d" | "3d" | "1w" | "1M";
export type FuturesPeriod = "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "12h" | "1d";

export type FastStreamSpec =
  | { type: "depth"; symbol?: string; levels: 5 | 10 | 20; speed?: "100ms" | "250ms" | "500ms" }
  | { type: "depth_diff"; symbol?: string; speed?: "100ms" | "250ms" | "500ms" }
  | { type: "rpi_depth"; symbol?: string }
  | { type: "all_book_tickers" }
  | { type: "agg_trade"; symbol?: string }
  | { type: "continuous_kline"; pair?: string; contractType: "perpetual" | "current_quarter" | "next_quarter"; interval: KlineInterval }
  | { type: "ticker_24h"; symbol?: string }
  | { type: "all_tickers_24h" }
  | { type: "mini_ticker"; symbol?: string }
  | { type: "all_mini_tickers" }
  | { type: "all_mark_prices"; speed?: "1s" | "3s" }
  | { type: "all_liquidations" }
  | { type: "composite_index"; symbol?: string }
  | { type: "asset_index"; asset: string }
  | { type: "all_asset_indexes" };

// 1. 全量行情（基础流 + 按需高频旁路 + 官方 Market Data REST 代理）
export declare const market: {
  snapshot(symbol?: string): Promise<MarketSnapshot>;
  // 可靠主总线基础流（按 afterSeq 断线续传）
  watch(
    topics: Array<"kline" | "book_ticker" | "mark_price" | "liquidation" | "contract_info">,
    opts?: { symbols?: string[]; intervals?: Array<"1m" | "5m" | "15m" | "1h" | "4h" | "1d">; afterSeq?: number }
  ): AsyncIterable<MarketEvent>;
  // 高频与全市场按需流（无 seq、有损 drop_oldest、零订阅自动退订）
  watchFast(streams: FastStreamSpec[]): AsyncIterable<FastMarketEvent>;
  // 历史库查询（经 storage.sock，缺口自动回补）
  klines(interval: KlineInterval, opts: { symbol?: string; start: number; end: number; backfill?: boolean }): Promise<Kline[]>;
  fundingRates(opts: { symbol?: string; start?: number; end?: number; limit?: number; backfill?: boolean }): Promise<FundingRate[]>;
  liquidations(opts: { symbol?: string; start?: number; end?: number; limit?: number }): Promise<LiquidationEvent[]>;
  // Binance USDⓈ-M 官方 Market Data REST 代理（经 core 限频缓存）
  exchangeInfo(symbol?: string): Promise<ExchangeSymbolInfo[]>;
  depth(opts?: { symbol?: string; limit?: 5 | 10 | 20 | 50 | 100 | 500 | 1000 }): Promise<OrderBookSnapshot>;
  trades(opts?: { symbol?: string; limit?: number }): Promise<MarketTrade[]>;
  aggTrades(opts?: { symbol?: string; startTime?: number; endTime?: number; fromId?: number; limit?: number }): Promise<AggTrade[]>;
  continuousKlines(opts: { pair: string; contractType: "PERPETUAL" | "CURRENT_QUARTER" | "NEXT_QUARTER"; interval: KlineInterval; start?: number; end?: number; limit?: number }): Promise<Kline[]>;
  indexPriceKlines(opts: { pair: string; interval: KlineInterval; start?: number; end?: number; limit?: number }): Promise<Kline[]>;
  markPriceKlines(opts: { symbol?: string; interval: KlineInterval; start?: number; end?: number; limit?: number }): Promise<Kline[]>;
  premiumIndexKlines(opts: { symbol?: string; interval: KlineInterval; start?: number; end?: number; limit?: number }): Promise<Kline[]>;
  ticker24h(symbol?: string): Promise<Ticker24h | Ticker24h[]>;
  fundingInfo(symbol?: string): Promise<FundingInfo[]>;
  openInterest(symbol?: string): Promise<OpenInterestSnapshot>;
  openInterestHist(opts: { symbol?: string; period: FuturesPeriod; start?: number; end?: number; limit?: number }): Promise<OpenInterestPoint[]>;
  longShortRatio(
    kind: "top_accounts" | "top_positions" | "global_accounts",
    opts: { symbol?: string; period: FuturesPeriod; start?: number; end?: number; limit?: number }
  ): Promise<LongShortRatioPoint[]>;
  takerVolume(opts: { symbol?: string; period: FuturesPeriod; start?: number; end?: number; limit?: number }): Promise<TakerVolumePoint[]>;
  basis(opts: { pair: string; contractType: "PERPETUAL" | "CURRENT_QUARTER" | "NEXT_QUARTER"; period: FuturesPeriod; start?: number; end?: number; limit?: number }): Promise<BasisPoint[]>;
  indexInfo(symbol?: string): Promise<CompositeIndexInfo[]>;
  constituents(symbol?: string): Promise<IndexConstituents>;
  assetIndex(symbol?: string): Promise<AssetIndexInfo[]>;
};

// 2. 外部自定义数据集（支持单运行发布多个 dataset）
export declare const data: {
  publish(dataset: string, sample: { ts: number; key?: string; payload: Record<string, unknown> }): Promise<{ seq: number }>;
  query(dataset: string, opts: { start?: number; end?: number; limit?: number }): Promise<{ items: DataSample[]; maxSeq: number }>;
  watch(dataset: string, opts?: { afterSeq?: number }): AsyncIterable<DataSampleEvent>;
};

// 3. 只读账户观测（供声明 accountRead 的 worker 监控持仓、浮亏、强平距离与异常成交）
export declare const accounts: {
  list(): Promise<AccountSummary[]>;
  state(accountId: string): Promise<AccountFullState>;
  fills(accountId: string, opts?: { afterSeq?: number; throughSeq?: number; symbol?: string }): Promise<TradeFillEvent[]>;
  watch(
    accountIds: string[] | "*",
    topics: Array<"order" | "algo_order" | "fill" | "position" | "account" | "risk_reject">,
    opts?: { afterSeq?: number }
  ): AsyncIterable<AccountWatchEvent>;
};

// 4. 交易写原语（仅 live / shadow，统一 Hedge Mode + 普通单与 Algo 条件单）
export declare const trade: {
  openLong(params: OrderParams): Promise<OrderAck>;
  closeLong(params: OrderParams): Promise<OrderAck>;
  openShort(params: OrderParams): Promise<OrderAck>;
  closeShort(params: OrderParams): Promise<OrderAck>;
  cancel(ref: { orderId?: string; algoId?: string; clientOrderId?: string; symbol?: string }): Promise<CancelAck>;
  cancelAll(opts?: { symbol?: string; includeConditional?: boolean }): Promise<{ canceledStandard: number; canceledConditional: number }>;
  positions(symbol?: string): Promise<{ long: PositionLeg; short: PositionLeg }>;
  allPositions(): Promise<Record<string, { long: PositionLeg; short: PositionLeg }>>;
  account(): Promise<AccountSnapshot>;
  setLeverage(leverage: number, symbol?: string): Promise<{ symbol: string; leverage: number }>;
  getOrder(clientOrderId: string, symbol?: string): Promise<OrderIntentStatus>;
  openOrders(opts?: { symbol?: string; includeConditional?: boolean }): Promise<OpenOrder[]>;
  watchFills(opts?: { afterSeq?: number; symbol?: string }): AsyncIterable<TradeFillEvent>;
  watchAlgoOrders(opts?: { afterSeq?: number; symbol?: string }): AsyncIterable<AlgoOrderEvent>;
};

// 5. 脚本直接发通知（经 sys.notify 入总线，零阻塞 core，支持冷却降噪）
export declare const notify: {
  send(req: {
    level: "info" | "warn" | "critical";
    title: string;
    body: string;
    dedupKey?: string;
    cooldownSec?: number;
  }): Promise<{ queued: true; seq: number }>;
};

// 6. 托管状态（单份上限 1 MB）
export declare const state: {
  load<T = Record<string, unknown>>(): Promise<CheckpointEnvelope<T> | null>;
  save<T = Record<string, unknown>>(data: T, opts?: { schemaVersion?: number }): Promise<CheckpointEnvelope<T>>;
};

// 7. 异步唤起 Agent
export declare const agents: {
  startTask(req: {
    dedupKey: string;
    prompt: string;
    title?: string;
    backend?: string;
    visibility?: "visible" | "hidden" | "ephemeral";
    cooldownSec?: number;                         // 可选覆盖默认去重冷却秒数
    context?: Record<string, unknown>;
  }): Promise<{ taskId: string }>;
  getTask(taskId: string): Promise<AgentTaskStatus>;
};

// 8. 运行时上下文
export declare const harness: {
  readonly mode: "live" | "shadow" | "worker" | "backtest" | "local";
  readonly runId: string;
  readonly accountId?: string;
  readonly epoch?: number;
  readonly capabilities?: WorkerCapabilities;
  now(): number; // 实盘/Shadow/Worker/Local 返回 Date.now()，回测返回虚拟时钟
  onHandover(cb: () => Promise<void> | void): void;
};
```

策略还可注册 `harness.onStop`，按 §4.3 指定停机时要保留或撤销的自有订单。回调参数和结果的传递方式在实现时确定。

- **续传、断连自保与交接**。主事件流按 `afterSeq` 重连补齐，高频旁路重发当前订阅。托管模式下 `runtime.sock` 连续断连超 60 秒时脚本主动退出。SDK 按 §4.3 分派交接事件，先收到 `SIGTERM` 时从 `GET /v1/handover` 取得动作与交接 ID。

### 5.2 双模式离线研究与回测体系

1. **模式 A：标准同码事件回放精测（`quant-storage/backtest.ts` / `backtest_run`）**
   - 策略代码在回测、`shadow` 与实盘下**完全零改动**；`storage` 在临时 `QUANT_BACKTEST_SOCKET` 启动 `BacktestDriver` 并 `Bun.spawn` 拉起 `QUANT_MODE=backtest` 子进程。
   - 严格防未来函数：`market.klines()` 仅返回 `closeTime <= virtualNow` 的闭合 K 线；多标的与多周期聚合器复用与 `quant-core` 一致的逻辑；`data.query()` 与 `data.watch()` 按 `(ingested_ts, seq) <= (virtualNow, visibleSeq)` 截断；`market.fundingRates()` 与 `market.liquidations()` 同样按虚拟时钟截断；未落盘的高频流（`market.watchFast()`）在回测中明确抛出不支持错误，避免伪造盘口。
   - **完整 Hedge Mode 撮合账本（含 Algo 条件单）**：
     - `MARKET` 单按下一个可成交 bar 开盘价 + 滑点（默认 `2 bps`）成交；`LIMIT` 单在后续 bar `[low, high]` 触及委托价时成交（`GTX` 穿透拒单）；
     - **条件单撮合**：`STOP_MARKET` / `TAKE_PROFIT_MARKET` / `STOP` / `TAKE_PROFIT` / `TRAILING_STOP_MARKET` 维护在独立条件单队列中，每根 bar 校验 `[low, high]`（或对应标记价）是否触及 `triggerPrice`（追踪止损按极值与 `callbackRate` 动态更新触发价），触发后转为市价（+ 滑点）或限价挂单撮合；
     - 按 Binance U 本位永续口径独立计算多标的 `long` / `short` 双腿均价、已实现盈亏、Maker/Taker 手续费（默认 `0.02% / 0.05%`）与实际历史资金费率（读取 `funding_rates` 表结算）。
2. **模式 B：终端自由直跑研究与向量化快测（`scripts/research/*.ts`）**
   - Agent 直接在终端 `bun run scripts/research/xxx.ts`（纯 Bun + TS），使用 `bun:sqlite` 以 `{ readonly: true }` 直查 `timeseries.db`（K 线、资金费率、强平、采集因子）与 `ledger.db`（历史成交/绩效），或经 `@quant/tools` 本地只读模式拉取行情快照/REST 指标；
   - 适用于大规模参数网格搜索、Walk-Forward 验证、因子相关性回归、滑点/费率归因与统计分析，毫秒至秒级直出 stdout，无需经过托管进程与 UDS 逐 bar 回放。

### 5.3 `script_validate(path, mode, capabilities?)` 静态预检

仅对托管脚本（`live` / `shadow` / `worker`）与封存（`seal`）执行；`scripts/research/` 终端自由直跑脚本不经过此预检。基于 `typescript` Compiler API（`ts.createProgram` + AST 检查），不执行脚本代码：
1. **单文件与依赖纯净性（保持严格约束）**：托管与封存业务源码必须为单个 `.ts` 文件，**唯一运行时导入为 `@quant/tools`**，拒绝其他运行时导入、动态加载代码与依赖可变外部模块；由 `Bun.build({ target: "bun", format: "esm" })` 绑定部署版本固定 SDK 打成单文件 `entry.mjs`。
2. **运行时误用检查**：AST 报告直接访问控制面、生产数据库、凭据文件及绕开网关的调用。
3. **按 `mode` 与 `capabilities` 能力预检**：
   - `worker`：禁止引用 `trade.*` 写方法（允许 `accounts.*` 只读观测、全量 `market.*`、`notify.send`、`agents.*`）；若未声明 `publishDatasets`，引用 `data.publish` 报错；若声明了 `publishDatasets` 但未调用 `data.publish` 给出警告；
   - `live` / `shadow`：必须存在 `harness.onHandover(...)` 注册调用；使用 `Date.now()` 时给出回测不安全警告（推荐 `harness.now()`）；调用 `market.watchFast()` 时提示该流不参与离线回测。

策略编写提示词应让 Agent 明确采用默认停机行为，或编写 `onStop` 说明特殊处理。沿用现有 AST 预检，例如缺少回调时提示将按归属撤单，有回调时检查是否误用了全账户撤单。具体提示词与检查项留到实现阶段。

---

## 6. 四个外围常驻进程详细设计

### 6.1 `quant-storage`（分库 SQLite WAL + 不可变策略库）

`ledger.db` 与 `timeseries.db` 分别使用独立 SQLite 连接、WAL、`synchronous=FULL`、`busy_timeout=5000` 和外键检查，由 `quant-storage` 独占写权；文件权限设为同组可读，明确支持 Agent 与 `scripts/research/` 本地脚本以 `new Database(path, { readonly: true })` + `PRAGMA query_only = ON` 并发只读直查。

#### (1) `ledger.db` 核心表结构

实盘账户事件记为 `mode = 'live'`，测试账户事件记为 `mode = 'shadow'`。实时收到和断线补回的委托、成交、仓位快照与资金流水均沿用账户所属模式。

```sql
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
```

#### (2) `timeseries.db` 核心表结构

交易所历史行情表（`klines`、`market_metrics`、`funding_rates`、`liquidations`）只接收主网行情。实时写入按事件来源筛选，历史缺口只从 `binance.mainnetRest` 回补。测试网行情留在内存，存储消费游标照常推进。外部采集样本按各自的 `dataset` 保存。

```sql
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
```

- **双游标与缺口处理**：每 200 ms 或 200 条一批，两库各自事务内写入本库表并推进 `sync_cursor`，均提交后向 `core` 确认 `min(ledgerSeq, timeseriesSeq)`。易失行情缺口按标的回补闭合 K 线与历史资金费率。
- **不可变封存 vs 草稿/Worker 准备**：
  - `shadow` 草稿与 `worker` 启动调用 `POST /v1/scripts/prepare`：对源码快照执行预检、绑定固定 SDK 打包为 `artifacts/<codeHash>/entry.mjs` 并返回 `{ artifactPath, sourceHash, codeHash }`，**不写入 `strategies` 不可变资产表**；
  - `strategies_seal`：除执行上述预检打包外，按族串行分配版本，排他创建只读 `strategies/<family>/<version>.ts`（`0444`），持久化后单事务登记元数据、标签与谱系，同名绝不覆盖。`live` 上线强制读取已封存版本的登记哈希产物。

### 6.2 `quant-notifier`（告警与审批网关，纯 Webhook）

- **通知投递**：订阅主事件总线上的成交、条件单触发、拒单、提案、熔断、告警、运行事件与托管脚本发起的 **`sys.notify`** 事件。落盘本地持久待投递队列后再确认订阅水位；支持按 `dedupKey` 与自定义 `cooldownSec`（默认 60 秒）聚合降噪与指数退避重试。
- **入站校验与审批**：Telegram 校验 `X-Telegram-Bot-Api-Secret-Token`、`chat.id`、`from.id` 并按 `update_id` 去重；飞书用 Encrypt Key 验签并校验时间、应用身份、会话与 `open_id`。审批卡片统一覆盖实盘上线/替换/停机与需审批的手动交易提案（短 ID 反查完整 `proposalId + proposalDigest` 回调 `admin.sock`）。
- **本地 `notifier.sock`**：暴露 `GET /v1/health` 与 `POST /v1/notify`（供 MCP `notify_send` 与 `quant notify` 使用）。

### 6.3 `quant-agent`（多后端 Agent 网关 + 侧边栏可见性控制 + 宽松护栏）

#### (1) `agent.db` 表结构
沿用 `sessions`、`tasks`、`task_aliases`、`dedup_windows`、`daily_usage` 五表（SQLite WAL，`synchronous=FULL`），记录每次任务的 `profile`、`backend`、`source`、`depth`、`cost_usd` 与去重别名映射。

#### (2) `AgentHarnessAdapter` 多后端抽象与侧边栏可见性
```ts
export type SidebarVisibility = "visible" | "hidden" | "ephemeral";
export type AgentProfile = "user" | "cron_review" | "script_triggered";

export interface AdapterStartOptions {
  prompt: string;
  profile: AgentProfile;
  visibility: SidebarVisibility;
  title?: string;
  maxTurns: number;
  timeoutSec: number;
  mcpEnv: Record<string, string>;
}

export interface AgentHarnessAdapter {
  readonly name: string;
  startSession(opts: AdapterStartOptions): Promise<{ backendSessionId: string; summary: string; costUsd?: number }>;
  resumeSession(backendSessionId: string, message: string, opts: Omit<AdapterStartOptions, "prompt">): Promise<{ summary: string; costUsd?: number }>;
  setVisibility(backendSessionId: string, visibility: "visible" | "hidden"): Promise<void>;
  cancel(backendSessionId: string): Promise<void>;
  health(): Promise<{ ok: boolean; detail?: string }>;
}
```
- **`CodexAdapter`（`codex app-server --listen stdio://`）**：常驻托管并自动重启；`visible` 使用 `thread/start({ ephemeral: false })` + `thread/name/set`；`hidden` 首轮后调用 `thread/archive`；`ephemeral` 使用 `ephemeral: true`；取消调用 `turn/interrupt`。挂载精简 11 核心 MCP，同时放开项目工作区（含 `strategies/_drafts/`、`scripts/workers/`、`scripts/research/`）与 `quant` CLI / 只读 SQLite 访问。
- **`AntigravityAdapter`（`agy` / `agy acp`）**：`visible` 使用配置的 `visibleAppDataDir` + `--project <project>`；`hidden` 使用 `agy acp --app-data-dir <hiddenAppDataDir> --stdio`；`ephemeral` 使用一次性临时目录并在任务结束后清理。编码期首个实测项为验证 `visibleAppDataDir` 在远端 App 侧边栏的实时刷新。

#### (3) 任务报告推送与宽松预算型护栏
- `cron_review` 与 `script_triggered` 默认 `visible` 启动，完成后经 `notifier.sock` 推送摘要通知，交易员可从远端 App 侧边栏打开续聊；
- **四项轻量护栏（`guardrails.ts`）**：
  1. **去重与灵活冷却**：沿用 `core` 预分配 `taskId`，按 `(source_scope, dedupKey)` 在冷却窗口（默认 300 秒，支持脚本按请求传入 `cooldownSec` 覆盖）内合并重复请求并登记别名；
  2. **并行与预算保护（无全局代码仓库单写锁）**：允许最多 `maxConcurrentTasks`（默认 4）个任务并行执行，不以全局写锁互相阻塞；按 UTC 日任务数（默认 200）与日费用预算（默认 50 USD）做软告警 + 硬上限保护；单任务轮数与超时上限充裕（`user`: 80 轮 / 30m、`cron_review`: 40 轮 / 20m、`script_triggered`: 30 轮 / 15m）；
  3. **Profile 提示词**区分 `user`、`cron_review`、`script_triggered` 的默认职责，Agent 按交易员的授权处理实盘变更。
  4. **防失控反递归（`maxDepth = 3`）**：根据父任务链计算深度，允许脚本触发的深度研究任务按需再派发专项排查，同时阻断无限自激循环。

### 6.4 `quant-watchdog`（独立外部看门狗）

- **10 秒周期探测**：探测 `quant-core` Event Loop 延迟、按 `config.symbols` 逐标的核查 `bookTicker`/`kline_1m`/`markPrice` 新鲜度、私有 WS 心跳与账户对账状态、活跃托管脚本存活与数据集 TTL；探测 `quant-storage` 两库水位滞后与磁盘余量；探测 `quant-notifier` 与 `quant-agent` 存活。
- **直连兜底告警**：从 `QUANT_SECRETS_DIR` 按渠道配置独立读取凭据并直连 Telegram / 飞书，5 分钟防刷屏抑制与恢复通知。
- **极端失联应急撤单（默认关闭）**：仅当 `emergencyCancel.enabled === true` 且 `quant-core` 连续失联超阈值时，从 `QUANT_SECRETS_DIR` 按账户配置读取凭据，为实盘子账户按 `allowedSymbols` 撤销普通挂单（`DELETE /fapi/v1/allOpenOrders`）；**仅当 `includeConditional === true` 时才追加调用 `DELETE /fapi/v1/algoOpenOrders`**，默认保留交易所侧保护性止损条件单，且绝不擅自市价平仓。
- **本地 `watchdog.sock`**：暴露 `GET /v1/health` 供 `quant health` 聚合读取。

---

## 7. 精简核心 MCP（11 工具）与统一本机 CLI（`quant`）

### 7.1 精简核心 MCP 工具集（11 个）

仅将高价值、有状态变更与审批契约的 **11 个核心工具** 暴露为 MCP Tool，控制对话上下文开销；底层与 `quant` CLI 共用同一套控制面 API 与审计日志：

| 分类 | 工具（11） | 底层路由 | 核心语义 |
| :--- | :--- | :--- | :--- |
| 实盘与手动交易（3） | `live_propose`、`trade_manual`、`proposals_decide` | `admin.sock` | 实盘上线/热替换提案；受控单次手动下单/挂条件单/撤单/调杠杆；批准或驳回提案 |
| 托管运行（2） | `runs_start`、`runs_stop` | `admin.sock` | 启动 `worker`（声明能力组合）或 `shadow`（草稿或 `strategyId`）。停止指定运行时，策略按 §4.3 收尾 |
| 策略封存与回测（3） | `strategies_seal`、`strategies_tag`、`backtest_run` | `storage.sock` | 预检打包封存只读不可变版本；更新五维标签/笔记/生命周期；标准同码事件回放精测 |
| 风控、Agent 与通知（3） | `risk_kill_switch`、`agents_start_task`、`notify_send` | `admin.sock` / `agent.sock` / `notifier.sock` | 开关 Kill Switch；发起异步 Agent 任务；向飞书/Telegram 推送摘要告警 |

### 7.2 统一本机 CLI（`quant`）与只读 SQLite 直查

`src/cli/main.ts`（注册为 `bun run quant` / 全局 `quant` 命令）提供全部查询、开发与运维操作，所有子命令均支持 `--json` 机器可读输出，供 Agent 终端、脚本与人工 SSH 共用：

```bash
# 1. 系统健康与运维
quant health [--json]
quant reload [--json]
quant kill <global|accountId> [--cancel-open] [--include-conditional] [--reason "..."]
quant unkill <global|accountId> [--reason "..."]

# 2. 全量行情与外部数据（直连 core 缓存代理 / storage）
quant market snapshot [symbol|*]
quant market klines <interval> --symbol BTCUSDT --start ... --end ... [--backfill]
quant market depth [--symbol BTCUSDT] [--limit 20]
quant market funding [--symbol BTCUSDT] [--start ...] [--end ...]
quant market oi [--symbol BTCUSDT] [--period 5m]
quant market ls-ratio <top_accounts|top_positions|global_accounts> [--symbol BTCUSDT] [--period 5m]
quant market taker-volume [--symbol BTCUSDT] [--period 5m]
quant market basis [--pair BTCUSDT] [--period 5m]
quant data datasets
quant data query <dataset> [--start ...] [--end ...] [--limit 100]

# 3. 账户、账本与状态
quant accounts list
quant accounts state <accountId>
quant ledger query [--account ...] [--mode live|shadow] [--symbol ...] [--limit 50]
quant ledger pnl <target> [--mode live|shadow] [--window 7d]
quant state inspect <namespace> [--version N]

# 4. 策略库、预检与标准回测
quant strategy search [--regime ...] [--style ...] [--lifecycle ...]
quant strategy get <strategyId>
quant strategy fork <fromStrategyId> <draftName> --reason "..."
quant strategy validate <path> --mode <live|shadow|worker>
quant strategy seal <draftPath> --family <f> --tags ... --summary "..."
quant strategy tag <strategyId> [--lifecycle shelved] [--note "..."]
quant backtest run <target> --start ... --end ...

# 5. 托管运行、实盘提案、受控手动交易与 Agent 任务
quant run list [--mode ...] [--status ...]
quant run logs <runId> [--tail 200] [--grep "..."]
quant run start worker <path> [--persistent] [--state-name ...] [--account-read sub_main] [--publish-datasets ds1,ds2]
quant run start shadow <strategyId|draftPath>
quant run stop <runId> [--reason "..."]
quant live propose <strategyId> --account <accountId> --handover <adopt|cancel_open> [--include-conditional] --reason "..."
quant trade order <accountId> <openLong|closeLong|openShort|closeShort> --symbol BTCUSDT --type <MARKET|LIMIT|STOP_MARKET|...> --qty ... [--price ...] [--trigger-price ...] --reason "..."
quant trade cancel <accountId> (--order-id ... | --algo-id ... | --client-order-id ...)
quant trade cancel-all <accountId> [--symbol BTCUSDT] [--include-conditional]
quant trade leverage <accountId> <leverage> [--symbol BTCUSDT]
quant proposal status <proposalId>
quant proposal approve <proposalId> <proposalDigest>
quant proposal reject <proposalId> [reason]
quant agent start --prompt "..." [--visibility visible] [--dedup-key ...]
quant agent get <taskId>
quant agent cancel <taskId>
quant notify send --level info --title "..." --body "..."
```

**只读 SQLite 直查约定**：Agent 或位于 `scripts/research/` 的一次性 Bun 研究脚本可直接打开 `${QUANT_DATA_DIR}/db/timeseries.db` 与 `${QUANT_DATA_DIR}/db/ledger.db`（`new Database(dbPath, { readonly: true })`）进行任意 SQL 聚合、跨表关联与向量化快测，绝不对生产数据库执行写操作。

---

## 8. 分阶段实施顺序与验收标准

严格自底向上实现，每阶段可独立验证；**全程不写任何模拟测试策略脚本**，测试代码仅验证底座自身正确性。

| 阶段 | 交付模块 | 验收标准（基础设施单测 / 真实联调） |
| :--- | :--- | :--- |
| **P1** | `quant-storage` 双库（含 `algo_orders`、`funding_rates`、`liquidations`）、WAL 只读直查、策略库、预检与打包 | 验证双库交错提交中断后从较小游标恢复且无重复，写入期间 WAL 并发只读查询不阻塞。验证 `prepare`（Shadow 草稿和 worker 不入不可变表）与 `seal`（并发封存不覆盖、SDK 版本变化隔离）。两网同一时刻对同一标的报价不同时，历史查询只返回主网值，测试成交仍可按 `shadow` 查到 |
| **P2** | `quant-core` 主事件总线、订单意图（普通单 + 条件单）、风控（含受控手动交易）、1 MB Checkpoint 与能力调度 | 验证 `storage` 离线越块重启恢复。覆盖 worker 的 `accountRead` 只读放行、`trade.*` 403、多数据集独占发布和 `sys.notify`。验证手动交易经过风控与审批且不抢占租约。验证热替换的 `includeConditional` 与双信号单次终态提交。停机覆盖默认撤单、自定义 `onStop` 和异常回退，分别核对现有仓位保护单、已空仓保护单及手动单 |
| **P3** | Binance 多标的网关、全量 WS（基础流 + 按需高频旁路）、官方 Market Data REST 缓存代理、Algo 条件单与双模式回测 | 验证热重载动态增减 `symbols` 与 `allowedSymbols`；验证高频 `depth`/`aggTrade` 零订阅时退订、高压下丢旧帧不挤占主环形缓冲；验证全部公共 REST 代理走统一权重桶且不饿死撤单；使用 Demo 账户实测普通单 + Algo 条件单（`/fapi/v1/algoOrder` + `ALGO_UPDATE`）全流程、断线补账与杠杆收紧后保护止损绿灯；验证标准回测的条件单撮合与防未来函数 |
| **P4** | 飞书/Telegram 通知审批 Webhook、`quant` 统一 CLI 及审计 | 覆盖 Webhook 验签/白名单/去重；验证脚本 `sys.notify` 经 `notifier` 冷却降噪送达；覆盖实盘与手动交易提案的 notifier / CLI / MCP 审批互斥、审计来源记录、停机持仓/条件单告警及 Kill Switch（含 `includeConditional`） |
| **P5** | 精简 11 核心 MCP、两个 Agent 后端、Cron 复盘与宽松护栏 | 验证精简 11 核心 MCP 与 `quant` CLI 契约一致；验证无全局写锁并发执行、自定义 `cooldownSec` 去重别名、预算软/硬限制与 `maxDepth=3` 反递归；实测 Codex 与 Antigravity 侧边栏 `visible / hidden / ephemeral` 及重启后续聊 |
| **P6** | `quant-watchdog`、5 个常驻 `systemd` unit、反代与整机联调 | 验证孤儿 PID 清理与断连自保退出；验证常驻 `worker` 监听脚本（同时读行情 + 读账户持仓 + 直接发飞书告警 + 唤起 `visible` Agent 会话）与 `scripts/research/` 终端直跑脚本双轨并存；验证多标的行情探活、存储滞后、直连兜底告警与保留保护止损的应急撤单 |
