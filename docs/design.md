# 量化交易 Harness 详细设计文档

本文档将[架构设计](./architecture.md)落实为编码与验收规格。运维权限、交易所接入、持久化与恢复流程以本文为准。

---

## 1. 设计约束与最小实现原则

1. **按功能直写，拒绝过度抽象**：
   - **交易所**：直接实现 **Binance USDⓈ-M Futures** 客户端（`BTCUSDT` 永续 + Demo/Testnet），不设计多交易所插件框架或抽象工厂。
   - **IM 通道**：本机（腾讯云东京区）具备公网 HTTPS 入口，**仅实现 Webhook 模式**（`Bun.serve` 本地监听，挂在本机 Nginx/Caddy HTTPS 反代后）；不做 Telegram Long Polling，也不接飞书 WebSocket 长连接；直接用原生 `fetch()` 调用 Telegram Bot API 与飞书 Open API，不引入重型 Bot SDK。
   - **运行时与存储**：全面使用 Bun 内置能力（`Bun.serve` UDS/HTTP、`bun:sqlite`、`Bun.spawn`、原生 `WebSocket`/`fetch`、`node:crypto`），不引入 ORM、Web 框架、队列库或复杂构建链。
2. **仅在明确要求处保留抽象**：
   - **`quant-agent` 底层 Harness 适配层**：抽象 `AgentHarnessAdapter` 接口以支持多后端配置，首期实现 **Codex**（`codex app-server`）与 **Antigravity**（`agy` / `agy acp`），并统一支持控制会话在对应 Harness App 侧边栏的可见性（`visible` / `hidden` / `ephemeral`）。
3. **禁止模拟测试策略脚本**：
   - 仓库内**不编写**任何示例策略、假策略、模拟行情生成器（`sim`）或本地模拟撮合账户（`paper`）。
   - `shadow` 通道直连真实 **Binance Futures Demo / Testnet API**；`backtest_run` 是 `quant-storage` 内置的**历史回放引擎**（消费真实历史 K 线与采集数据集），不附带任何模拟测试策略脚本。
4. **最小外部依赖**：
   - 仅引入 `typescript`（类型检查与 AST 预检）与 `@modelcontextprotocol/sdk`（MCP stdio Server）。脚本打包使用 Bun 内置 bundler。
5. **可信运维与脚本执行**。人工操作员和已接入的可信 Agent 共享运维工具。Profile 提示词约定职责、何时先询问交易员及报告方式。服务端记录实际执行者、触发来源和已有的确认。盘中下单统一经过绑定账户的脚本运行时，交易凭据由网关持有。

---

## 2. 仓库结构、运行态布局与配置

### 2.1 仓库目录结构（单仓 + 仅 1 个 SDK Workspace）

```text
quant-harness/
├── package.json                  # 根项目，workspaces: ["packages/tools"]
├── tsconfig.json
├── config/
│   ├── harness.example.json      # 主配置模板（子账户风控、IM Webhook、Agent 后端等）
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
│   └── tools/                    # @quant/tools（供所有独立脚本 import）
│       ├── package.json
│       ├── index.ts
│       ├── types.ts
│       ├── transport-uds.ts      # 托管运行传输层（live/shadow/collector/readonly）
│       └── transport-backtest.ts # 离线回测传输层（QUANT_MODE=backtest）
├── src/
│   ├── shared/                   # 跨进程公共类型、协议常量、UDS client、配置加载
│   │   ├── types.ts
│   │   ├── events.ts
│   │   ├── config.ts
│   │   ├── uds.ts
│   │   └── logger.ts
│   ├── core/                     # quant-core 主进程
│   │   ├── main.ts
│   │   ├── event-bus.ts          # 预留 seq、关键事件日志及实时环形缓冲
│   │   ├── binance/
│   │   │   ├── signer.ts         # HMAC-SHA256 + 统一出口 IP 权重限频桶
│   │   │   ├── public-ws.ts      # /public 盘口流与 /market K线、标记价流
│   │   │   ├── account-client.ts # 单子账户 REST + User Data Stream + Hedge 校验与对账
│   │   │   └── pool.ts           # 多子账户池 + Shadow Demo/Testnet 通道
│   │   ├── risk.ts               # 账户独占锁 + 防呆风控 + 幂等键 + Kill Switch
│   │   ├── checkpoint.ts         # 托管 Checkpoint（内存热份 + 原子热备 + 事件归档）
│   │   ├── supervisor.ts         # Bun.spawn 子进程管理、Token 会话表、租约交接与提案
│   │   ├── admin-server.ts       # admin.sock HTTP Server
│   │   └── runtime-server.ts     # runtime.sock HTTP + NDJSON Stream Server
│   ├── storage/                  # quant-storage 外围进程
│   │   ├── main.ts
│   │   ├── db.ts                 # ledger.db & timeseries.db 建表与迁移
│   │   ├── ingestor.ts           # 从两库最小游标重放，各库独立提交
│   │   ├── kline-backfill.ts     # Binance 公共 REST 历史 K 线回补
│   │   ├── strategy-registry.ts  # 草稿预检、SDK 打包、不可变源码与执行产物
│   │   ├── validator.ts          # script_validate 静态预检（tsc + AST 规则）
│   │   ├── backtest.ts           # 离线回测引擎（基于真实历史数据的虚拟时钟驱动）
│   │   ├── log-rotator.ts        # 脚本滚动日志写入、切分与查询
│   │   └── server.ts             # storage.sock HTTP Server
│   ├── notifier/                 # quant-notifier 外围进程
│   │   ├── main.ts
│   │   ├── dedup-queue.ts        # 告警聚合降噪、冷却与指数退避重试
│   │   ├── telegram.ts           # Telegram Bot API 发送 + Webhook 回调校验
│   │   ├── feishu.ts             # 飞书 Open API 发送 + 卡片回调校验
│   │   └── webhook-server.ts     # 审批回调 & 白名单管理员指令 -> admin.sock
│   ├── agent/                    # quant-agent 外围进程
│   │   ├── main.ts
│   │   ├── db.ts                 # agent.db（会话映射、任务表、预算计数、去重冷却）
│   │   ├── guardrails.ts         # 四道护栏（去重/冷却、并发/写锁/预算、Profile、反递归）
│   │   ├── scheduler.ts          # 定时全局复盘 Cron 调度器
│   │   ├── im-bridge.ts          # 飞书/Telegram 独立对话 Bot Webhook（话题 <-> session_id）
│   │   ├── server.ts             # agent.sock HTTP Server
│   │   └── adapters/
│   │       ├── interface.ts      # AgentHarnessAdapter 统一接口与 sidebarVisibility 契约
│   │       ├── registry.ts       # 多后端配置注册表
│   │       ├── codex.ts          # Codex App Server JSON-RPC 适配器
│   │       └── antigravity.ts    # Antigravity (agy / agy acp) 适配器
│   ├── watchdog/                 # quant-watchdog 外围进程
│   │   ├── main.ts
│   │   ├── probes.ts             # 探活 core/storage/notifier/agent、WS 新鲜度、磁盘、滞后
│   │   ├── fallback-alert.ts     # 直连 Telegram/飞书兜底告警
│   │   └── emergency-cancel.ts   # 可选极端失联 Binance 应急撤单
│   ├── mcp/                      # 可信 Agent 共享运维工具
│   │   └── server.ts
│   └── ops/                      # 人工 SSH 运维入口，复用控制面操作
│       └── main.ts
└── tests/                        # 仅底座基础设施单测（严禁任何模拟策略脚本）
```

### 2.2 运行态数据目录（`QUANT_DATA_DIR`，默认 `/var/lib/quant-harness`）

```text
/var/lib/quant-harness/
├── run/                          # 0750，UDS socket 文件
│   ├── admin.sock                # 0660（core 控制面及内部订阅）
│   ├── runtime.sock              # 0660（托管脚本按 Token 访问）
│   ├── storage.sock              # 0660（storage 历史查询、策略库、回测与落盘管理）
│   ├── agent.sock                # 0660（agent 托管；mcp / watchdog 访问）
│   ├── notifier.sock             # 0660（notifier 托管；mcp notify_send / watchdog 访问）
│   └── watchdog.sock             # 0660（watchdog 托管；mcp system_health 聚合读取）
├── db/
│   ├── ledger.db                 # SQLite WAL：订单/成交/Shadow/Checkpoint/策略索引
│   ├── timeseries.db             # SQLite WAL：BTCUSDT K线/标记价费率/外部采集样本
│   └── agent.db                  # SQLite WAL：IM 话题映射/任务/去重/预算
├── state/
│   ├── core.db                   # core 独写，序号、关键事件、订单意图及控制状态
│   └── checkpoints/              # 当前 Checkpoint 的原子热备文件
├── strategies/
│   ├── _drafts/                  # 可编辑策略源码
│   └── <family>/<version>.ts     # 只读业务源码，供查看和 fork
├── artifacts/
│   └── <codeHash>/entry.mjs      # 含固定 SDK 的只读执行产物
├── logs/
│   └── <role>/<name>/<run_id>.log
├── agent-hidden/                 # Antigravity hidden/ephemeral 隔离 app-data-dir
└── secrets/                      # 0700，0600 配置文件
    ├── exchange.json             # 实盘与 Demo 凭据（core 及启用应急撤单的 watchdog 读取）
    └── im.json                   # 告警审批 Bot、Agent 对话 Bot 与 watchdog 兜底凭据
```

### 2.3 配置结构与热重载边界

主配置 `config/harness.json`（由 `QUANT_CONFIG_PATH` 指定）：

```jsonc
{
  "dataDir": "/var/lib/quant-harness",
  "symbol": "BTCUSDT",
  "binance": {
    "mainnetRest": "https://fapi.binance.com",
    "mainnetWs": "wss://fstream.binance.com",
    "testnetRest": "https://demo-fapi.binance.com",
    "testnetWs": "wss://demo-fstream.binance.com",
    "ipWeightLimitPerMin": 1800
  },
  // 以下 accounts 与 risk 支持 admin.sock POST /v1/reload 热重载
  "accounts": {
    "sub_small": {
      "requireApproval": false,
      "requireApprovalOnStop": false,
      "marginType": "CROSSED",          // 启动校验项：CROSSED | ISOLATED
      "initialLeverage": 3,             // 启动时设置，须 <= risk.maxLeverage
      "risk": { "maxOrderNotionalUsd": 100, "maxLeverage": 5, "maxOrdersPerMin": 20 },
      "labels": ["canary"]
    },
    "sub_main": {
      "requireApproval": true,
      "requireApprovalOnStop": false,
      "marginType": "CROSSED",
      "initialLeverage": 3,
      "risk": { "maxOrderNotionalUsd": 5000, "maxLeverage": 10, "maxOrdersPerMin": 30 },
      "labels": ["prod"]
    }
  },
  "shadow": {
    // Shadow Testnet 账户池：每个账户同一时间仅允许 1 个 shadow 运行（与 live 同一套独占租约）
    "accounts": {
      "shadow_1": { "marginType": "CROSSED", "initialLeverage": 3 }
    },
    "risk": { "maxOrderNotionalUsd": 1000, "maxLeverage": 10, "maxOrdersPerMin": 30 }
  },
  "im": {
    "notifierListen": { "host": "127.0.0.1", "port": 18101 },
    "agentListen": { "host": "127.0.0.1", "port": 18102 },
    "telegram": {
      "enabled": true,
      "alertChatId": "-100...",
      "agentChatId": "-100...",
      "allowedAdminUserIds": ["123456789"]
    },
    "feishu": {
      "enabled": true,
      "alertChatId": "oc_...",
      "agentChatId": "oc_...",
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
      "im_user":          { "defaultVisibility": "visible", "maxTurns": 40, "timeoutSec": 900 },
      "cron_review":      { "defaultVisibility": "visible", "maxTurns": 20, "timeoutSec": 600 },
      "script_triggered": { "defaultVisibility": "visible", "maxTurns": 10, "timeoutSec": 300 }
    },
    "guardrails": {
      "dedupCooldownSec": 1800,
      "maxConcurrentTasks": 2,
      "writeLockTimeoutSec": 900,
      "dailyMaxTasks": 50,
      "dailyMaxBudgetUsd": 20
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
    "maxPublicWsStaleSec": 30,       // 逐主题检查有效帧
    "maxStorageSeqLag": 5000,        // 实际待处理事件数
    "minDiskFreeGb": 5,
    "emergencyCancel": {
      "enabled": false,
      "consecutiveFailures": 6
    }
  }
}
```

- **热重载（`reload`）范围**：仅重载各子账户的 `requireApproval`、`requireApprovalOnStop`、`risk`、`labels` 与 `shadow.risk`；`marginType` / `initialLeverage` 仅在账户连接初始化时校验与设置；新增或移除交易所 API 凭证（含 Shadow 池账户）需重启对应连接。
- **凭据文件**。`secrets/exchange.json` 结构为 `{ accounts: { [accountId]: { apiKey, apiSecret } }, shadow: { [shadowAccountId]: { apiKey, apiSecret } } }`，权限 `0600`，由 `quant-core`（及开启应急撤单时的 `quant-watchdog`）直接读取；`secrets/im.json` 拆分告警审批 Bot、对话 Bot 与 watchdog 兜底凭据；托管脚本通过环境变量仅获取本次运行的 `QUANT_TOKEN`，不注入任何密钥。
- **Webhook 路由映射（Nginx/Caddy HTTPS 反代示例）**：
  - `https://<domain>/webhooks/telegram/notifier` → `http://127.0.0.1:18101/webhooks/telegram`
  - `https://<domain>/webhooks/feishu/notifier` → `http://127.0.0.1:18101/webhooks/feishu`
  - `https://<domain>/webhooks/telegram/agent` → `http://127.0.0.1:18102/webhooks/telegram`
  - `https://<domain>/webhooks/feishu/agent` → `http://127.0.0.1:18102/webhooks/feishu`

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
| `403` | `FORBIDDEN` | 脚本模式、账户范围或服务入口不允许当前操作 |
| `404` | `NOT_FOUND` | 账户、策略、Run、提案或任务不存在 |
| `409` | `ACCOUNT_LOCKED` / `SHADOW_LOCKED` / `DATASET_LOCKED` / `STALE_EPOCH` / `PROPOSAL_CONFLICT` / `WRITE_LOCKED` / `INTENT_CONFLICT` | 租约、提案、写锁或订单意图参数冲突 |
| `422` | `INVALID_ORDER` / `RISK_REJECTED` / `KILL_SWITCH_ACTIVE` / `HASH_MISMATCH` / `STRATEGY_NOT_SEALED` / `ACCOUNT_INCOMPATIBLE` | 业务或风控拦截；账户持仓模式 / 保证金模式与配置不符 |
| `429` | `RATE_LIMITED` | 账户下单频控、数据发布限流或 Agent 护栏冷却/预算超限 |
| `502` | `EXCHANGE_ERROR` | 已确认失败的上游请求 |
| `503` | `ORDER_UNKNOWN` / `RECONCILING` / `MARKET_STALE` | 执行结果待核实、账户对账中或开仓所需行情过期，响应携带相关意图键或缺失主题 |

### 3.2 内存事件总线（Event Bus）

- **统一信封**：
  ```ts
  export interface BusEvent<T = unknown> {
    seq: number;        // 全局严格单调递增整数
    ts: number;         // Unix 毫秒时间戳
    topic: EventTopic;
    source: string;     // 如 "binance.public" | "account:sub_main" | "run:<runId>" | "core"
    payload: T;
  }
  ```
- **分配序号**。首次部署从 0 建立序号状态。已有运行目录缺少或无法校验序号状态时，停止分配并告警，恢复可信上界后才能继续。core 是唯一写者，在 `state/core.db` 的持久事务中先将 `reserved_until` 提高一个区间（默认 4096），提交成功后才分配该区间的 `seq`。下个区间同理。重启从已预留上界之后重新分配，未用完的号码作废。storage 离线也能恢复。区间跳号本身不代表丢事件。
- **关键事件先落盘**。`trade.*`、`shadow.*`、`state.*`、`agent.*`、有序号的 `sys.*` 与 `data.sample` 保存到 `critical_events(seq PRIMARY KEY, topic, envelope)`，持久提交后才能广播或确认成功。关联的订单意图、Checkpoint、提案或 Kill Switch 转移与对应事件在同一个 core 事务内提交。通过单写队列异步处理本机短事务，按序发布。磁盘写失败即告警、暂停受影响的写入和新开仓。
- **实时与重放**。内存环形缓冲保留最近 20,000 帧。行情慢消费者队列上限 1,000，允许合并过期盘口与标记价，丢弃时在后续水位前标记受影响的主题。重连时先固定已发布水位 `W` 并缓冲新帧，将 `(after_seq, W]` 的日志与环缓冲按 `seq` 去重合并，再接入实时流。覆盖不到的行情区间先返回 `gap{topics,fromSeq,toSeq}`，消费者回补 K 线或重取快照。区间处理完发送无序号控制帧 `watermark{throughSeq}`。过滤主题和预留跳号均可推进该水位。
- **确认与保留**。storage、notifier、agent 各有持久订阅游标，收到水位且本地已持久接收相关事件后确认。core 按所需消费者的最小确认水位清理日志。告警与积压以待处理事件数、最老待处理时间计算。临时脚本游标超过保留范围时明确返回缺口，成交先从本账户历史补齐，外部因子从 storage 重新预热。
- **完整 Topic 清单**：
  - 行情：`market.kline`（`interval: "1m" | "5m" | "15m" | "1h" | "4h" | "1d"`, `openTime`, `closeTime`, `o/h/l/c/v`, `closed`；`1m` 推送实时更新与闭合，其余周期由 `quant-core` 基于 `1m` 闭合 K 线内存聚合、仅推送闭合帧）、`market.ticker`（`bid`, `bidQty`, `ask`, `askQty`, `last`, `eventTs`）、`market.mark_price`（`markPrice`, `indexPrice`, `fundingRate`, `nextFundingTime`, `eventTs`）
  - 交易：`trade.order`、`trade.fill`、`trade.position`、`trade.account`（余额、可用保证金及未实现盈亏）、`trade.income`（含资金费）、`trade.risk_reject` 和 `shadow.order`、`shadow.fill`、`shadow.position`、`shadow.account`、`shadow.income`
  - 数据与状态：`data.sample`（`dataset`, `ts`（业务时间）, `ingestedTs`（`quant-core` 接收时间，用于回测防未来函数）, `key?`, `payload`, `runId`）、`state.checkpoint`（`namespace`, `accountId?`, `version`, `epoch`, `schemaVersion`, `updatedAt`, `data`）
  - Agent 与系统：`agent.request`（含 core 预分配的 `taskId` 与来源 `runId`）、`agent.task_update`、`sys.handover`、`sys.proposal`、`sys.run_lifecycle`、`sys.kill_switch`、`sys.operation`（运维审计）、`sys.alert`、`sys.heartbeat`（空闲 15 秒发送，无 `seq`）

### 3.3 本机 UDS 接口

#### (1) `admin.sock`（`quant-core` 控制面）

控制面以服务凭据识别调用方。MCP、notifier 和本机 ops 共用运维操作契约。内部订阅者使用自己的服务凭据读取、确认所属订阅。每次变更将 `actor`（实际执行者）、`source`、`accountId/runId/proposalId`、原因、结果及可选 `confirmationRef` 写入 `sys.operation`。MCP 调用记为 Agent。IM 确认引用关联已校验的消息，缺省留空。Profile 和请求正文中的身份字段均不参与授权。

| 方法与路径 | 用途与调用方 |
| :--- | :--- |
| `GET /v1/health` | 返回 PID、Event Loop 延迟、各行情主题的最后有效数据时间、私有 WS 心跳、发布水位、待确认事件数、账户对账与运行状态 |
| `GET /v1/market/snapshot` | 返回 `BTCUSDT` 最新 Ticker、盘口最优价、标记价与资金费率（`mcp`） |
| `GET /v1/accounts` | 列出所有子账户配置、占用 `strategyId`/`runId`/`epoch`、风控阈值、Kill Switch 与连接状态（`mcp` / `ops`） |
| `GET /v1/accounts/:accountId/state` | 查询指定子账户实时余额、可用保证金、强平价、`{ long, short }` 持仓与活跃挂单（`mcp`） |
| `GET /v1/state/:namespace` | 读取热内存 Checkpoint（`mcp`） |
| `GET /v1/runs?mode=&status=` | 列出运行中及近期托管脚本（`mcp` / `ops`） |
| `POST /v1/runs/start` | 启动免审批脚本（`readonly` / `collector` / `shadow`）：`readonly` 支持并行运行多个（研究分析或 `persistent: true` 常驻行情监听）；`shadow` 占用 Shadow 池中空闲账户，池满返回 `409 SHADOW_LOCKED`；`collector` 独占声明的 `dataset`，已有活跃写入者返回 `409 DATASET_LOCKED`（`mcp`） |
| `POST /v1/runs/:runId/stop` | 接受 `{ cancelOpenOrders?: boolean, reason? }`。实盘账户开启 `requireApprovalOnStop` 时返回 `202 { proposalId, status: "pending_approval" }`，旧策略继续运行。其余情况直接停止。实盘与 Shadow 按 §4.3 交接并处理挂单，collector 回收终态后释放数据集，`readonly` 回收可选终态并退出 |
| `POST /v1/live/propose` | 发起上线或替换。免审批账户直接执行；启用 `requireApproval` 的账户返回 `202 { proposalId, status: "pending_approval" }` |
| `GET /v1/proposals/:proposalId` | 返回动作、目标、完整参数、`proposalDigest`、过期时间、状态及执行结果 |
| `POST /v1/proposals/:proposalId/approve` | MCP / notifier / ops 提交 `{ proposalDigest, confirmationRef? }`，原子消费提案并执行其绑定的 start、replace 或 stop |
| `POST /v1/proposals/:proposalId/reject` | MCP / notifier / ops 驳回待处理提案并记录原因 |
| `POST /v1/risk/kill-switch` | 按全局或账户启用 Kill Switch，可指定 `cancelOpenOrders` 并记录原因 |
| `DELETE /v1/risk/kill-switch` | MCP / notifier / ops 解除指定范围的 Kill Switch，审计实际执行者 |
| `POST /v1/reload` | MCP / notifier / ops 热重载 §2.3 的配置，并记录配置差异 |
| `GET /v1/stream?topics=&after_seq=` | 内部模块订阅事件、缺口与水位控制帧 |
| `POST /v1/stream/ack` | 持久消费者确认 `{ throughSeq }`，服务凭据绑定消费者名，重复确认幂等 |

#### (2) `runtime.sock`（`quant-core` 脚本运行时，强校验 `Authorization: Bearer <QUANT_TOKEN>`）

| 方法与路径 | 允许的 `mode` | 语义 |
| :--- | :--- | :--- |
| `GET /v1/stream?topics=&after_seq=` | 全部 | 行情、数据集以及绑定账户/运行的私有事件。成交按实盘或 Shadow 路由，交接只推送给目标 `runId` |
| `POST /v1/session/ready` | 全部 | SDK 使用本次预发 Token 完成启动握手。确认对应运行仍有效且满足就绪条件后激活，重复握手幂等 |
| `GET /v1/market/snapshot` | 全部 | 获取当前内存最新快照 |
| `POST /v1/trade/order` | `live`, `shadow` | 下单动作及 `OrderParams` 绑定稳定意图键，按 §4.1 提交 |
| `GET /v1/trade/order?clientOrderId=` | `live`, `shadow` | 按 SDK 业务键查询本账户原意图的状态与已知订单结果，可跨 epoch |
| `GET /v1/trade/fills?after_seq=&through_seq=` | `live`, `shadow` | runtime 向 storage 查询本账户的持久成交及原始 seq。账本游标到达指定水位后才确认补齐，按水位衔接实时流 |
| `DELETE /v1/trade/order` | `live`, `shadow` | 使用交易所 `orderId` 或 SDK 业务键撤单。core 将业务键解析成持久保存的交易所客户端订单号 |
| `DELETE /v1/trade/orders` | `live`, `shadow` | 撤销当前账户/Shadow 全部挂单 |
| `GET /v1/trade/positions` | `live`, `shadow` | 返回 `{ long, short }` 双腿持仓 |
| `GET /v1/trade/account` | `live`, `shadow` | 返回 `{ walletBalance, availableBalance, unrealizedPnl, marginType, leverage }` |
| `POST /v1/trade/leverage` | `live`, `shadow` | 调整本账户 `BTCUSDT` 杠杆（`leverage <= risk.maxLeverage`，否则 `422 RISK_REJECTED`；计入频控） |
| `GET /v1/trade/open-orders` | `live`, `shadow` | 返回当前活跃挂单列表 |
| `GET /v1/state` | 全部 | 读取当前绑定命名空间的最新 Checkpoint |
| `PUT /v1/state` | 全部 | 常态写检查当前运行和租约（`readonly` 写入自身独立命名空间，供常驻行情监听记录去重/触发状态）。交接期间仅接受本次 `handoverId` 的终态提交，重试返回已提交版本 |
| `GET /v1/handover` | 全部 | 查询本运行的待处理交接，供先收到 SIGTERM 的 SDK 取得 `handoverId` |
| `POST /v1/data/publish` | `collector` | 发布本运行独占的 `dataset` 样本（单条 ≤ 32 KB，限频 20 条/秒），`quant-core` 打上 `ingestedTs` 后入总线并广播 |
| `POST /v1/agents/tasks` | 全部 | core 预分配 `taskId`，持久记录来源及 `agent.request`（支持可选 `title` / `backend` / `visibility`）后返回 `202 { taskId }`，后台异步执行 |
| `GET /v1/agents/tasks/:taskId` | 全部 | 仅查本运行发起的 ID。agent 尚未确认该请求序号且尚未建任务时返回 `queued`。合并任务返回原 ID、`canonicalTaskId` 与实际状态。未知 ID 返回 `404`，状态服务不可达返回 `503` |

#### (3) `storage.sock`（`quant-storage` 持久层与策略库）

统一通过 `storage.sock` 提供历史查询、策略库管理、回测与日志服务。托管脚本经 `@quant/tools` 调用只读行情与因子查询端点（`GET /v1/market/klines`、`GET /v1/data/datasets`、`GET /v1/data/query`）；订单、账户、Checkpoint 等私有读取仍经 `runtime.sock` 按会话限定。

| 方法与路径 | 用途 |
| :--- | :--- |
| `GET /v1/health` | 返回两库独立游标、共同水位 `lastSeq`、SQLite 状态与磁盘剩余空间 |
| `GET /v1/meta/cursor` | 返回 `{ ledgerSeq, timeseriesSeq, lastSeq: min(ledgerSeq, timeseriesSeq) }` |
| `GET /v1/market/klines?interval=&start=&end=&backfill=` | 查询历史 K 线（缺口按需经 Binance REST 自动回补） |
| `GET /v1/data/datasets` | 列出所有外部采集数据集、最新写入时间、样本数与 TTL 状态 |
| `GET /v1/data/query?dataset=&start=&end=&limit=&asOf=&asOfSeq=` | 按业务时间选样本。回测同时按接收时间和同毫秒序号限定可见性。实时预热从同一读事务返回 `{ items, maxSeq }`，`maxSeq` 是本次读取的时序库消费水位 |
| `GET /v1/ledger/query?accountId=&mode=&type=&start=&end=&limit=&after_seq=&through_seq=` | 查询委托、成交、Shadow 与风控记录。按 seq 回补成交时返回页尾和账本已覆盖的水位 |
| `GET /v1/ledger/pnl?target=&window=` | 返回预聚合绩效摘要（净收益、胜率、盈亏比、最大回撤、手续费、资金费率） |
| `GET /v1/checkpoints?namespace=&version=&limit=` | 查询 Checkpoint 历史版本 |
| `POST /v1/logs/append` | 接收 `quant-core` 转发的脚本 `stdout/stderr` 行并写入滚动日志 |
| `GET /v1/logs/:runId?tail=&since=&grep=` | 查询脚本滚动日志 |
| `GET /v1/strategies/search?...` | 按 `family` / `lifecycle` / `regime` / `style` / `account_fit` / `factor` 检索策略与绩效 |
| `GET /v1/strategies/:strategyId` | 获取指定不可变策略源码、`code_hash`、血缘谱系、标签与归因备注 |
| `POST /v1/strategies/fork` | 从不可变策略复制到 `strategies/_drafts/` 并记录草稿谱系 |
| `POST /v1/strategies/seal` | 预检源码快照、打包固定 SDK，登记只读源码及按 `codeHash` 寻址的执行文件 |
| `PATCH /v1/strategies/:strategyId/tags` | 更新五维标签、追加归因备注或切换 `lifecycle`（`shelved` / `deprecated` 等） |
| `POST /v1/scripts/validate` | 对源码快照执行静态预检 |
| `POST /v1/scripts/prepare` | 供 core / storage 准备执行产物。按路径启动时预检并打包，已封存 ID 则校验登记产物。返回受控目录内的 `{ artifactPath, codeHash }` |
| `POST /v1/backtest/run` | 对草稿路径或 `strategyId` 执行历史 K 线与外部数据集离线回放，返回结构化绩效与关键交易明细 |

#### (4) `agent.sock`（`quant-agent` 任务与会话网关）

| 方法与路径 | 用途 |
| :--- | :--- |
| `GET /v1/health` | 返回活跃任务数、各后端状态、当日预算消耗 |
| `POST /v1/tasks` | 接受 `{ prompt, profile, backend?, sidebarVisibility?, dedupKey? }`，返回 `{ taskId, sessionId, status }`。脚本事件沿用 core 的预分配 ID，来源与深度由网关从触发记录确定 |
| `POST /v1/sessions/:sessionId/messages` | 向已有会话续聊：`{ message, sidebarVisibility? }` → `{ taskId, status }` |
| `GET /v1/tasks/:taskId` | 查询任务状态、耗时、输出摘要、关联 `sessionId` |
| `POST /v1/tasks/:taskId/cancel` | 取消运行中任务（Codex `turn/interrupt` / Antigravity `SIGINT`） |

---

## 4. 核心进程 `quant-core` 详细设计

### 4.1 Binance USDⓈ-M Futures 直连网关

1. **分路行情连接（`public-ws.ts`）**。同一环境内的策略共享两条连接。
   - `/public/ws/btcusdt@bookTicker` 接收最优买卖盘，`/market/stream?streams=btcusdt@kline_1m/btcusdt@markPrice@1s` 接收 K 线、标记价和费率。路径拼接到配置的主网或 Demo WS 根地址。每个快照字段保留来源与时间，`last` 取最新 K 线更新中的成交价。 [Binance 路由说明](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice) · [Demo 地址](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)
   - 分别更新 `market.ticker`、`market.kline`、`market.mark_price` 的最后有效帧时间。网关按账户所在环境读取行情，某主题的来包只能刷新该主题。
   - **多周期 K 线内存聚合**：基于闭合的 `1m` K 线滚动聚合 `5m / 15m / 1h / 4h / 1d`（UTC 对齐），每个周期闭合时推送一帧 `market.kline { interval, closed: true }`；启动或断线重连后，先经 Binance 公共 REST 拉取各周期当前未闭合窗口内的 `1m` K 线回填聚合器，避免首根大周期 K 线残缺。
   - 各连接独立按 1s、2s、5s、15s 退避重连。缺少新鲜标记价时开仓返回 `503 MARKET_STALE`，撤单仍可用。
2. **多子账户池（`pool.ts` + `account-client.ts`）**：
   - 所有实盘子账户与 Shadow 池账户（Demo/Testnet）共用同一个 `BinanceAccountClient` 类，仅注入不同 `{ restBaseUrl, wsBaseUrl, apiKey, apiSecret }`。
   - **账户启动校验**。首次激活配置时校验持仓模式、保证金模式及初始杠杆。不兼容账户标记原因并发高优 `sys.alert`。恢复已有运行时区别处理持仓模式冲突与风控上限变更。
     1. **Hedge Mode**：调 `GET /fapi/v1/positionSide/dual`；若 `dualSidePosition === false`，查询 `positionRisk` 与 `openOrders`，仅当空仓且零挂单时调用 `POST /fapi/v1/positionSide/dual`（`dualSidePosition=true`），否则标记不兼容；
     2. **保证金模式**：读取 `positionRisk` 中 `BTCUSDT` 的 `marginType`，与配置 `marginType` 不一致时，仅在空仓且零挂单时调用 `POST /fapi/v1/marginType` 切换，否则标记不兼容；
     3. **初始杠杆**。首次启用时验证 `initialLeverage <= risk.maxLeverage` 并调用 `POST /fapi/v1/leverage`。已有仓位的恢复先读取真实杠杆，若热重载令其超限则进入仅减仓状态，按 §4.2 处理。
   - **私有流与断线补账**。初始化或私有流中断时将该账户置为 `reconciling`，暂停新增风险。先建立 `/private/ws?listenKey=<key>&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE` 并缓冲增量。用 `POST /fapi/v1/listenKey` 取 key，每 25 分钟续期，过期时重建。
     1. 获取余额、持仓和挂单快照。另从持久对账水位按账户与 symbol 分页补取 `/fapi/v1/allOrders`、`/fapi/v1/userTrades` 和 `/fapi/v1/income`（含资金费）。对之前未终结的订单和结果未知的意图逐笔查单。窗口边界重叠，成交按交易所成交 ID 去重，资金流水按类型与流水 ID 去重。[Binance 账户成交及订单接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade) · [账户收入接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account)
     2. 将回补与缓冲事件按订单或持仓腿的更新时间合并，保留新状态与累计成交量，时间相同而内容冲突则重新查询。成交、手续费、余额及资金费事件与相应对账水位同事务持久化。核对挂单、双腿和所有未决意图后发布账户就绪；接口保留期不足或数据矛盾时保留缺口、告警并继续核对。
   - **统一出口 IP 限频桶（`signer.ts`）**：
     - 所有子账户 REST 共享按 IP 的权重桶，对照交易所当前限额并读取 `X-MBX-USED-WEIGHT-1M` 校准。为撤单、减仓与未决订单查询预留容量，同时遵守交易所账户级订单频控。
3. **Hedge Mode 四原语映射与幂等键**：
   - `openLong` → `side=BUY, positionSide=LONG`
   - `closeLong` → `side=SELL, positionSide=LONG`（不传 `reduceOnly`，Binance Hedge 模式下天然最多平至 0 不反手）
   - `openShort` → `side=SELL, positionSide=SHORT`
   - `closeShort` → `side=BUY, positionSide=SHORT`
   - **稳定意图键**。脚本为每个业务意图生成账户内唯一的 `clientOrderId`（1–16 位 `[A-Za-z0-9_-]`）。策略重试或恢复同一笔决策时复用原键。core 以 `(mode, accountId, symbol, clientOrderId)` 唯一索引持久保存规范化参数、原 run/epoch、状态与结果。首次受理生成固定交易所订单号 `qh_` 加 32 位 UUID 十六进制（35 字符），换代或重启沿用已保存的值。
   - **先记意图再发单**。在账户串行化入口校验租约与风控，事务插入 `prepared` 记录。唯一的发送者以条件更新提交 `sending` 后才发 REST。并发重试返回原意图，参数不同返回 `409 INTENT_CONFLICT`。首次发送的条件更新同时检查 `prepared` 和原租约仍活跃。交接或恢复递增 epoch 时取消尚未发送的旧意图。
   - **结果未知**。超时、连接中断或重启时遗留的 `sending` 一律转 `unknown`。通过固定交易所订单号、私有流及成交历史核对，已受理或明确拒绝的结果持久化后返回。单次查无此单仍返回 `503 ORDER_UNKNOWN`；重复 POST 或 GET 只查询既有意图。未取得确定结果前保留记录、阻止换代放行并告警，明确未执行后才允许用新业务键重新决策。

### 4.2 账户独占锁、防呆风控、Kill Switch 与 Checkpoint

1. **账户独占租约（实盘与 Shadow 统一）**：
   - 内存按 `(mode, accountId)` 缓存 `{ strategyId, runId, epoch, status }`。持久租约记录运行、交接、启动和终态，账户连接另记录对账就绪状态。
   - 任意时刻每个实盘 `accountId` 仅允许 1 个活跃 `live` 租约；已有租约时拒绝普通启动（`409 ACCOUNT_LOCKED`），仅允许通过 `replace` 递增 `epoch` 交接。
   - `shadow` 启动时从 Shadow 池分配空闲账户并占用租约，无空闲账户返回 `409 SHADOW_LOCKED`；脚本退出即释放。Shadow 账户启动前若存在残留持仓或挂单，先撤单并发出提示事件（Testnet 资金不做强制平仓）。
   - 实盘和 Shadow 的交易写入均检查绑定的账户、runId、epoch 和运行状态。`handover` 关闭旧运行的交易写入口；终态 Checkpoint 走 §4.3 规定的单独条件更新。被替换或已退出的运行返回 `409 STALE_EPOCH`。
2. **数据集独占写入者**：
   - `Map<Dataset, runId>`：同一 `dataset` 同一时刻仅允许 1 个活跃 `collector` 写入，重复启动返回 `409 DATASET_LOCKED`；`data.publish` 仅接受本运行声明的 `dataset`。
3. **按动作检查风险**。先在账户串行化入口校验会话、动作、类型、有限正数以及交易所数量/价格精度。纯内存风控与随后的意图持久化分别计时。
   - **开仓**。Kill Switch、行情过期、未完成对账或当前杠杆超过热重载后的上限，均拒绝新开仓。市价单按新鲜标记价估算名义金额，只允许限价单携带 `price`。检查单笔限额、账户开仓频控及交易所限频。
   - **减仓与撤单**。按 Hedge 持仓腿及最新可平数量核验方向、数量和已有平仓挂单，可平数量须扣除本地未决平仓量。开仓限额、已触发的 Kill Switch、超限的现有杠杆均不阻断合法减仓或撤单。采用保留配额，服从交易所硬限额；不确定可平数量时先刷新该腿。对账期间允许撤单与经核实的减仓。
   - **调杠杆**。`trade.setLeverage(n)` 只接受上限内的正整数，计入该账户频控。风控拒绝通过 `trade.risk_reject` 留痕。
4. **Kill Switch 持久化与解除**：
   - core 在 `core.db` 中持久提交开关状态与 `sys.kill_switch` 后确认。若 `cancelOpenOrders` 为真，跟踪已送出的订单，按该账户撤单、核对并记录结果。
   - 解除调用 `DELETE /v1/risk/kill-switch`，MCP、notifier 和 ops 共用审计契约。提示词约定可信 Agent 何时先向交易员确认。
5. **托管 Checkpoint**。
   - 命名空间：`live` → `account:<accountId>`，`shadow` → `shadow:<runId>`，`collector` → `collector:<dataset>`，`readonly` → `readonly:<name || runId>`（常驻监听脚本可指定稳定名称以便重启延续去重状态）。
   - 结构体：`{ namespace, accountId?, version, epoch, schemaVersion, updatedAt, data }`，JSON 序列化硬上限 **128 KB**（超限返回 `422`）。
   - 按命名空间串行检查租约、递增版本，在 `core.db` 同一事务保存最新内容及 `state.checkpoint`。提交后发布事件、更新内存，并以临时文件、fsync、rename 和目录 fsync 刷新热备。重启以已提交版本为准，补写落后的热备。

### 4.3 脚本调度、提案与账户交接

1. **脚本启动与环境注入（`Bun.spawn`）**。core 与 storage（回测）直接通过 `Bun.spawn(["bun", "run", artifactPath], { env })` 拉起经校验的只读产物 `entry.mjs`，显式构造最小环境变量集合（不继承父进程敏感 env）：
   - `QUANT_SOCKET` 指向 `runtime.sock`（回测模式替换为当次临时 `QUANT_BACKTEST_SOCKET`）。
   - `QUANT_STORAGE_SOCKET` 指向 `storage.sock`（回测模式不注入）。
   - `QUANT_TOKEN`（32 字节随机 hex，仅存内存 `Map<token, Session>`）。
   - `QUANT_MODE`（`live` | `shadow` | `collector` | `readonly` | `backtest`）。
   - `QUANT_RUN_ID`，以及按模式附加的 `QUANT_ACCOUNT` / `QUANT_EPOCH`（`live`、`shadow`）、`QUANT_DATASET`（`collector`）、`QUANT_STATE_NAME`（常驻 `readonly` 监听可选）。
   - 严禁向脚本注入交易所密钥、IM 凭据或 `admin.sock` 路径。
2. **进程、日志与孤儿回收**。监督器将每个活跃运行的 `runId`、`pid` 与 `startedAt` 持久记录在 `core.db`，通过 `Bun.spawn` 管道收集 `stdout/stderr`，按 `{ ts, runId, stream }` 异步批量 POST 至 `storage.sock /v1/logs/append`（`storage` 暂不可达时环形保留最近 500 行，500 ms 超时不阻塞主进程）。停止或交接向子进程发送 `SIGTERM`，超时 `SIGKILL` 并撤销令牌。core 崩溃重启时，先扫描 `core.db` 中遗留的旧 PID 并确认/清理残留孤儿进程，再执行自动恢复；同时 SDK 在 `runtime.sock` 连续断连超过 60 秒时主动退出自保。
3. **实盘提案**。上线和替换读取 `requireApproval`，停止读取 `requireApprovalOnStop`。免审批时直接进入同一执行流程。需要确认时先持久化 `pending`、发布 `sys.proposal` 并返回 202；等待期间旧租约保持运行。
   - 每个账户同时最多一个 `pending`。显式 `supersede` 将旧提案置为 `superseded`。摘要 `proposalDigest` 绑定动作、账户、预期旧 run/epoch（初次上线为空）、目标策略/执行哈希及全部操作参数。stop 绑定目标 run/epoch 与 `cancelOpenOrders`。提案默认 15 分钟过期。
   - 账户配置要求的是提案确认这一步，可信 Agent 与人工入口均可执行。确认时验证摘要、当前租约和适用的产物哈希，条件更新 `pending → executing` 并落盘操作阶段。重复回调返回同一结果；驳回、过期或租约已变更的提案不能执行。完成写 `applied` 或 `failed`。notifier、ops 与 Agent 可执行确认，来源与实际确认引用各自记录。提示词要求 Agent 在需确认且交易员尚未表态时先询问。
4. **replace 与 stop**。同一账户按以下持久阶段推进，`operationId` 标识一次操作。
   1. **冻结**。绑定旧 run/epoch，生成 `handoverId` 并置 `handover`，与下单受理共用账户串行入口。收回普通交易和状态写入权限。
   2. **交接终态**。定向发 `sys.handover` 并发送 SIGTERM。旧运行可在 3,000 ms 内凭本次 `handoverId` 执行一次逻辑终态保存。校验 run、epoch、状态与 ID，在 Checkpoint 事务内记下提交结果。相同内容重试返回同一版本，第二份不同内容返回 409。回调正常退出但未提交新状态时沿用最后已提交版本并记录；超时则 SIGKILL、记 `uncleanHandover` 并告警。
   3. **收敛在途订单**。确认旧运行退出、作废未发送意图，等已送出的开单、撤单等请求完成对账。已在交易所挂出的订单取得确定状态即可纳入挂单清单。任何结果未知的请求都保留在本阶段，重新查单并告警。脚本退出超时不会缩短交易所核对。
   4. **处置挂单**。`cancel_open` 或 stop 的 `cancelOpenOrders=true` 撤清并确认挂单为空。`adopt` 保留核对过的挂单及其原始归属。重取真实持仓。stop 记为 `stopped`、释放租约；若仍有持仓，发送含双腿摘要的高优告警。
   5. **启动新版**。replace 在同一事务写入新 epoch、目标哈希、`starting` 及可选的空 Checkpoint（`resetState` 每次操作至多一次）。通过 `Bun.spawn` 启动新版进程，SDK 握手在产物校验、状态交接及账户就绪后激活对应 Token 并将 `starting` 改为 `running`。策略在握手完成后进行首笔交易。初次上线从此步骤进入。新版在 5 秒内异常退出则记 `failed` 并告警，保留持仓监控。
5. **core 恢复**。`state/core.db` 由 core 的单写 Worker 管理（SQLite WAL，`synchronous=FULL`）。数据库保存预留序号、关键事件和订阅确认、每个账户的有版本租约和操作阶段、活跃脚本 PID 清单、Kill Switch、Checkpoint、订单意图、交易所对账水位及提案。启动先清理上次崩溃遗留的孤儿 PID、恢复控制状态与订阅，再核对账户。未完成的交接按原 `operationId` 续行；已停止/失败的运行保持终态。恢复活跃 live 时校验同一执行哈希、重新分配 epoch 并核对未决意图。Kill Switch 生效时只开放合法的减仓和撤单。`collector` 与标记 `persistent: true` 的 `readonly` 行情监听脚本自动恢复，其余一次性 `readonly` 与 `shadow` 标为 `interrupted`，释放 Shadow 账户前先处理未决请求。

**core.db 的持久键**。以下记录由同一个写连接更新，单个事务可以同时修改控制状态和插入关键事件。

| 记录 | 主键与恢复所需字段 |
| :--- | :--- |
| `sequence_state`、`critical_events`、`consumer_ack` | 单例 `reserved_until`；按 `seq` 的事件信封；按持久消费者的 `through_seq` |
| `leases`、`proposals` | `(mode, accountId)` 对应 run/epoch、目标、操作 ID、动作及当前阶段。提案按 `proposalId` 保存摘要、预期租约、参数、到期时间和结果，待处理提案按账户唯一 |
| `order_intents` | `(mode, accountId, symbol, clientOrderId)` 唯一，交易所客户端订单号另设唯一约束，存规范化请求、原 run/epoch、发送状态与已知交易所订单 ID、结果 |
| `reconciliation_cursors` | 按环境、账户、交易对和历史接口记录已完整处理的时间窗口、分页边界与业务 ID。旧挂单和未决意图另行追查，不按时间窗口过滤 |
| `kill_switches`、`checkpoint_latest` | 按作用范围存熔断状态；按命名空间存完整 Checkpoint、终态 `handoverId` 和版本。交接操作记录保留该次提交结果，供重试查询 |
| `task_requests` | 按 `taskId` 保存来源 `runId`、请求事件 seq 和投递状态，和 `agent.request` 同事务创建。用于区分尚未被消费、无权查询和不存在的任务 |

---

## 5. `@quant/tools` SDK、静态预检与离线回测

### 5.1 `@quant/tools` 核心 API 契约

策略与采集源码使用 `@quant/tools`。storage 在预检、封存和按路径启动时，按部署版本把该名称解析到仓库发布的 SDK 入口，用 `Bun.build({ target: "bun", format: "esm" })` 打成单个 `entry.mjs`（§6.1）。托管子进程按绝对路径执行该文件，关闭运行时自动安装。任意 `QUANT_DATA_DIR` 均不依赖上层目录的 `node_modules`。[Bun 打包文档](https://bun.com/docs/bundler)

```ts
export type OrderParams = {
  qty: number;                    // BTC 数量，有限正数
  clientOrderId: string;          // 稳定业务意图键，1–16 位 [A-Za-z0-9_-]
} & (
  | { type: "MARKET"; price?: never; timeInForce?: never }
  | { type: "LIMIT"; price: number; timeInForce?: "GTC" | "IOC" | "FOK" | "GTX" }
);

export interface PositionLeg {
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

export type KlineInterval = "1m" | "5m" | "15m" | "1h" | "4h" | "1d";

// 统一导出对象
export declare const market: {
  snapshot(): Promise<MarketSnapshot>;
  klines(interval: KlineInterval, opts: { start: number; end: number }): Promise<Kline[]>;
  watch(
    topics: Array<"kline" | "ticker" | "mark_price">,
    opts?: { intervals?: KlineInterval[]; afterSeq?: number } // intervals 默认 ["1m"]，仅在订阅 kline 时生效
  ): AsyncIterable<MarketEvent>;
};

export declare const data: {
  publish(dataset: string, sample: { ts: number; key?: string; payload: Record<string, unknown> }): Promise<{ seq: number }>;
  query(dataset: string, opts: { start?: number; end?: number; limit?: number }): Promise<{ items: DataSample[]; maxSeq: number }>;
  watch(dataset: string, opts?: { afterSeq?: number }): AsyncIterable<DataSampleEvent>;
};

export declare const trade: {
  openLong(params: OrderParams): Promise<OrderAck>;
  closeLong(params: OrderParams): Promise<OrderAck>;
  openShort(params: OrderParams): Promise<OrderAck>;
  closeShort(params: OrderParams): Promise<OrderAck>;
  cancel(ref: { orderId?: string; clientOrderId?: string }): Promise<CancelAck>;
  cancelAll(): Promise<{ canceled: number }>;
  positions(): Promise<{ long: PositionLeg; short: PositionLeg }>;
  account(): Promise<AccountSnapshot>;
  setLeverage(leverage: number): Promise<{ leverage: number }>;
  getOrder(clientOrderId: string): Promise<OrderIntentStatus>;
  openOrders(): Promise<OpenOrder[]>;
  watchFills(opts?: { afterSeq?: number }): AsyncIterable<TradeFillEvent>;
};

export declare const state: {
  load<T = Record<string, unknown>>(): Promise<CheckpointEnvelope<T> | null>;
  save<T = Record<string, unknown>>(data: T, opts?: { schemaVersion?: number }): Promise<CheckpointEnvelope<T>>;
};

export declare const agents: {
  startTask(req: {
    dedupKey: string;
    prompt: string;
    title?: string;                              // 侧边栏与 IM 话题标题
    backend?: string;                            // 指定后端（如 "codex" | "antigravity"），默认 defaultBackend
    visibility?: "visible" | "hidden" | "ephemeral"; // 默认跟随 profile（script_triggered 默认 "visible"）
    context?: Record<string, unknown>;
  }): Promise<{ taskId: string }>;
  getTask(taskId: string): Promise<AgentTaskStatus>;
};

export declare const harness: {
  readonly mode: "live" | "shadow" | "collector" | "readonly" | "backtest";
  readonly runId: string;
  readonly accountId?: string;
  readonly epoch?: number;
  now(): number; // 实盘/Shadow 返回 Date.now()，回测返回虚拟时钟
  onHandover(cb: () => Promise<void> | void): void;
};
```

- **续传、断连自保与交接**。SDK 按已消费的事件或水位携带 `after_seq` 重连，遇到缺口按 §3.2 补齐后再交付后续帧；若 `runtime.sock` 连续断连超过 60 秒则主动退出防止孤儿失控。SDK 对同一 `handoverId` 只调度一次 `onHandover`。若先收到 SIGTERM，查询 `GET /v1/handover`；未注册 `onHandover` 的一次性 `readonly` 脚本直接退出，注册了回调的脚本将 `state.save` 自动附加当前交接 ID，等待持久确认后退出。

### 5.2 同码运行与离线回测引擎（`quant-storage/backtest.ts`）

策略代码在回测、Shadow、小资金子账户与主力子账户下**完全零改动**：
- `backtest_run(target, range, params?)` 让 storage 在临时 UDS（`QUANT_BACKTEST_SOCKET`）启动专属 `BacktestDriver`，再以 `Bun.spawn(["bun", "run", artifactPath], { env })` 拉起 `QUANT_MODE=backtest` 子进程。已封存目标直接加载对应执行产物，草稿走相同的打包路径并记录本次执行哈希。
- `@quant/tools` 检测到 `QUANT_MODE=backtest` 自动装载 `BacktestTransport`：
  - `market.klines()` 仅返回 `closeTime <= virtualNow` 的已闭合 K 线；`market.watch()` 复用与 `quant-core` 完全一致的多周期聚合器（按 `1m` 历史闭合 K 线滚动产出请求的 `5m/15m/1h/4h/1d` 闭合帧）。因子同时保存业务 `ts` 和 core 首次接收的 `ingestedTs`。`data.query()` 将请求的业务结束时间截断在虚拟当前时刻，并将可见行限定为 `(ingested_ts, seq) <= (virtualNow, visibleSeq)`。时钟处于某毫秒的首帧之前时，该毫秒任何因子都不可见。
  - `data.watch()` 按接收时间和同毫秒序号回放，使用与查询相同的可见边界。与 K 线使用同一驱动器，固定同时间事件的交付次序，完成当前帧决策后推进时钟。例如业务时间 10:00、10:05 首次发布的样本，10:01 的查询和订阅都不能看到；10:05 交付该帧起可见。
  - `trade.openLong/closeLong/openShort/closeShort/cancel/cancelAll/positions/account/setLeverage/getOrder/openOrders` 在驱动器内置 Hedge Mode 撮合账本中完整结算：
    - 响应闭合 K 线而提交的 `MARKET` 单按下一个可成交价格（分钟回放取下根开盘价）加可配滑点，默认 `2 bps`。
    - `LIMIT` 单在后续 bar `[low, high]` 触及委托价时成交（`GTX` 若当下即穿透盘口则拒单）；
    - 按 Binance U 本位永续口径独立计算 `long` / `short` 双腿均价、已实现盈亏、Maker/Taker 手续费（默认 `0.02% / 0.05%`）与每 8 小时（UTC 00/08/16）资金费率；
    - 资金曲线、最大回撤、手续费、资金费及关键成交明细在结束时返回。`backtest_runs.code_hash` 记录实际执行产物，若用作上线证据，应与提案目标的 `codeHash` 完全一致。

### 5.3 `script_validate(path, mode)` 静态预检

基于 `typescript` Compiler API（`ts.createProgram` + AST 检查），不执行脚本代码：
1. **类型与依赖**。从本次源码快照和固定 SDK 的类型定义执行严格 TS 检查。可封存的业务源码为一个 `.ts` 文件，唯一运行时导入为 `@quant/tools`。预检拒绝其他运行时导入、动态加载代码和依赖可变外部模块；SDK 自身的内部模块由打包器一起收录。研究和采集脚本的本地依赖在准备本次运行时一并打包。
2. **运行时误用检查**。AST 报告直接访问控制面、生产数据库、交易凭据及绕开网关的调用。实际的文件、网络、UDS 和交易权限由 §3.3、§4.3 的运行边界执行。
3. **按 `mode` 能力预检**：
   - `collector`：禁止引用 `trade.*`；必须调用 `data.publish`；
   - `readonly`：禁止引用 `trade.*` 与 `data.publish`；
   - `live` / `shadow`：必须存在 `harness.onHandover(...)` 注册调用；禁止使用 `Date.now()` 作为回测不安全时间源时给出警告（推荐 `harness.now()`）。

---

## 6. 四个外围常驻进程详细设计

### 6.1 `quant-storage`（分库 SQLite + 不可变策略库）

`ledger.db` 与 `timeseries.db` 分别使用独立 SQLite 连接、WAL、`synchronous=FULL`、`busy_timeout=5000` 和外键检查。每个库的 `sync_cursor` 与本库数据同事务提交。agent 的持久任务/收件事务也使用 FULL。WAL 的原子提交边界是单个数据库文件。[SQLite 多库事务说明](https://sqlite.org/lang_attach.html)

#### (1) `ledger.db` 核心表结构

```sql
CREATE TABLE sync_cursor (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_seq INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
INSERT INTO sync_cursor VALUES (1, 0, 0);

CREATE TABLE orders (
  exchange_order_id TEXT NOT NULL,
  client_order_id TEXT,          -- 交易所回报的客户端标识，本系统生成的值以 qh_ 开头
  mode TEXT NOT NULL,            -- live | shadow
  account_id TEXT NOT NULL,
  run_id TEXT,                   -- 外部订单可能无法归属本地运行
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
  status TEXT NOT NULL,          -- 保存交易所原始状态，含 FILLED / CANCELED / EXPIRED 等终态
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (mode, account_id, symbol, exchange_order_id)
);
CREATE INDEX idx_orders_acct_time ON orders(account_id, created_at DESC);

CREATE TABLE fills (
  trade_id TEXT NOT NULL,
  exchange_order_id TEXT NOT NULL,
  client_order_id TEXT,
  mode TEXT NOT NULL,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  event_seq INTEGER NOT NULL UNIQUE,  -- 第一次发布该笔成交的 seq
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
  amount TEXT NOT NULL,          -- 交易所十进制金额，保留符号
  ts INTEGER NOT NULL,
  PRIMARY KEY (mode, account_id, income_type, tran_id)
);

CREATE TABLE operation_audit (
  seq INTEGER PRIMARY KEY,       -- sys.operation 的序号，重放幂等
  operation_id TEXT NOT NULL,
  action TEXT NOT NULL,
  actor TEXT NOT NULL,
  source TEXT NOT NULL,
  target_json TEXT NOT NULL,
  confirmation_ref TEXT,         -- 对应已验证的消息，可为空
  reason TEXT,
  result TEXT NOT NULL,
  ts INTEGER NOT NULL
);

CREATE TABLE risk_rejects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seq INTEGER NOT NULL UNIQUE,
  account_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  code TEXT NOT NULL,
  reason TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  ts INTEGER NOT NULL
);

CREATE TABLE account_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seq INTEGER NOT NULL UNIQUE,
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
  source_hash TEXT NOT NULL,     -- 只读源文件字节的 SHA-256
  sdk_hash TEXT NOT NULL,        -- 构建时所用 SDK 发布版本的内容哈希
  code_hash TEXT NOT NULL,       -- 实际执行的 entry.mjs 字节的 SHA-256
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
  author TEXT NOT NULL,          -- im_user | cron_review | human
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
  ts INTEGER NOT NULL,           -- 采集方业务时间
  ingested_ts INTEGER,           -- 新样本必填，旧记录的未知接收时间保留 NULL
  sample_key TEXT,
  payload_json TEXT NOT NULL,
  PRIMARY KEY (dataset, seq)
);
CREATE INDEX idx_data_samples_ts ON data_samples(dataset, ts DESC);
CREATE INDEX idx_data_samples_visible ON data_samples(dataset, ingested_ts, seq);
```

- **双游标**。每 200 ms 或 200 条组成一批。对连续处理至水位 `W` 的数据，分别在两库事务内写本库负责的行，并把本库游标推进到 `W`；批内没有本库的主题也要推进。两库均持久提交后，才向 core 确认 `min(ledgerSeq, timeseriesSeq)`。掉电重启从这个最小值重放。每个库只处理高于自己游标的段，订单按交易所订单号更新，成交/收入按各自业务主键去重，其余事件用 `seq` 或版本去重。成交重复时保留原始 `event_seq`。
- **缺口处理**。优先从 core 的关键事件日志取回成交、Checkpoint、提案和因子。交易所侧缺口交由 §4.1 对账。易失行情缺口单独标明覆盖范围并回补闭合 K 线；无法重建的盘口区间保留缺口标记。历史账户 PnL 的手续费取自成交，资金费取 `account_income` 的实际资金费流水，相同费用只计一次。
- **旧数据**。旧样本缺少接收时间时，仅在有原始接收记录可核验时回填，其他行不进入时点回测并在报告中计数。新写入必须携带 core 生成的 `ingestedTs`，重放使用第一次接收值。若从旧单游标库升级，保留原文件，基于两库可核验覆盖范围重放/对账，不能证明的区间登记缺口，再建立各库水位。
- **不可变封存**。先复制草稿字节到私有暂存区，对同一快照预检、用发布目录中固定的 SDK 显式解析 `@quant/tools` 并打成单文件。记录 `source_hash`、`sdk_hash` 和 `code_hash = SHA256(entry.mjs)`。将产物发布到 `artifacts/<codeHash>/entry.mjs`，已有同名产物先核实哈希。按族串行分配版本，以排他创建发布只读 `strategies/<family>/<version>.ts`，文件和目录持久化后在 ledger 一个事务内登记元数据、标签、谱系并清理草稿记录。临时文件和无登记的中断发布由 storage 启动时核对清理，同名封存绝不覆盖已有版本。
- **执行与衍生**。`fork` 复制已核验的原始业务源码。上线、替换、回测和崩溃恢复按登记哈希读取同一只读执行文件，启动前校验源码及执行文件。SDK 升级后旧版本仍取其原产物；使用新 SDK 的策略另行构建封存。

### 6.2 `quant-notifier`（告警与实盘审批网关，纯 Webhook）

- **通知投递**。订阅成交、拒单、提案、熔断、告警和运行事件。先把相应 `seq` 和收件目标写入本地持久待投递队列，再确认订阅水位。重放按 `(seq, 目标)` 去重，普通同类告警按 `alertKey` 冷却 60 秒。审批按 `proposalId` 投递，失败保留结果并按指数退避重试。
- **两个 Bot 共用入站校验**。按实际入口选用 notifier 或 agent 自己的凭据，以及本平台配置的 `alertChatId` 或 `agentChatId`。业务事件必须来自该入口对应的会话，且发送者属于本平台操作员白名单。对回复、指令和卡片回调均先做校验，再查话题、创建任务或变更状态。
  - Telegram 校验 `X-Telegram-Bot-Api-Secret-Token`、实际消息/回调的 `chat.id` 与 `from.id`。使用 `update_id` 做持久去重。[Telegram Webhook 文档](https://core.telegram.org/bots/api#setwebhook)
  - 飞书使用各应用配置的 Encrypt Key 对原始请求体验签，校验时间、应用身份、会话与 `open_id`，按事件 ID 或交互的唯一投递标识去重。若卡片只带消息标识，按已投递消息记录核对会话。配置阶段的 `url_verification` 按平台约定验证后仅回送 challenge。[飞书回调校验](https://open.feishu.cn/document/event-subscription-guide/callback-subscription/receive-and-handle-callbacks)
- **审批与运维**。在 `127.0.0.1:18101` 接收审批按钮及 `/unkill`、`/reload`、`/status`。按钮引用与投递记录绑定的短 ID，服务端查回完整 `proposalId + proposalDigest` 后调用控制面。记录已验证操作者与消息 ID，按 core 返回的实际状态更新卡片。平台重试、两个 IM 平台或 MCP 重复确认只执行一次。
- **本地 `notifier.sock`**：暴露 `GET /v1/health` 与 `POST /v1/notify`（供 Agent MCP Tool `notify_send` 投递诊断/复盘摘要，带每分钟 10 条限流保护）。

### 6.3 `quant-agent`（多后端 Agent 网关 + 侧边栏可见性控制 + 护栏）

#### (1) `agent.db` 表结构

```sql
CREATE TABLE sessions (
  session_id TEXT PRIMARY KEY,        -- 内部唯一 ID
  backend TEXT NOT NULL,              -- codex | antigravity | ...
  backend_session_id TEXT NOT NULL,   -- Codex threadId / agy conversationId
  profile TEXT NOT NULL,              -- im_user | cron_review | script_triggered
  sidebar_visibility TEXT NOT NULL,   -- visible | hidden | ephemeral
  title TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE im_thread_map (
  platform TEXT NOT NULL,
  thread_key TEXT NOT NULL,           -- 含已验证的 bot、chatId 和话题/根消息 ID
  session_id TEXT NOT NULL REFERENCES sessions(session_id),
  created_at INTEGER NOT NULL,
  PRIMARY KEY (platform, thread_key)
);

CREATE TABLE im_deliveries (
  bot TEXT NOT NULL,                  -- 平台及应用标识
  delivery_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  PRIMARY KEY (bot, delivery_id)
);

CREATE TABLE tasks (
  task_id TEXT PRIMARY KEY,
  session_id TEXT REFERENCES sessions(session_id),
  profile TEXT NOT NULL,
  backend TEXT NOT NULL,
  source TEXT NOT NULL,               -- im | cron | script | api
  dedup_key TEXT,
  depth INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,               -- queued | running | completed | failed | canceled
  prompt TEXT NOT NULL,
  output_summary TEXT,
  error TEXT,
  cost_usd REAL NOT NULL DEFAULT 0,
  started_at INTEGER NOT NULL,
  finished_at INTEGER
);

CREATE TABLE task_aliases (
  task_id TEXT PRIMARY KEY,           -- core 已返回给调用者的 ID
  canonical_task_id TEXT NOT NULL REFERENCES tasks(task_id)
);

CREATE TABLE dedup_windows (
  source_scope TEXT NOT NULL,         -- 如脚本 runId，隔离不同调用者
  dedup_key TEXT NOT NULL,
  last_task_id TEXT NOT NULL REFERENCES tasks(task_id),
  expires_at INTEGER NOT NULL,
  PRIMARY KEY (source_scope, dedup_key)
);

CREATE TABLE daily_usage (
  day_utc TEXT PRIMARY KEY,           -- YYYY-MM-DD
  task_count INTEGER NOT NULL DEFAULT 0,
  cost_usd REAL NOT NULL DEFAULT 0
);
```

#### (2) `AgentHarnessAdapter` 多后端抽象与侧边栏可见性设计

```ts
export type SidebarVisibility = "visible" | "hidden" | "ephemeral";
export type AgentProfile = "im_user" | "cron_review" | "script_triggered";

export interface AdapterStartOptions {
  prompt: string;
  profile: AgentProfile;
  visibility: SidebarVisibility;
  title?: string;
  maxTurns: number;
  timeoutSec: number;
  mcpEnv: Record<string, string>; // 传入任务上下文，Profile 用于提示词及审计
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

- **后端 1：`CodexAdapter`（`codex app-server --listen stdio://` JSON-RPC）**
  - 常驻托管一个 `codex app-server` 子进程（断连自动重启），启动时完成 `initialize` 握手。
  - 所有可信会话使用同一个 `mcp_servers.quant_harness` 配置及运维工具集。`QUANT_MCP_PROFILE` 与任务来源用于组装提示词和审计。各 Profile 共用部署配置的工作区及草稿写目录，运行时限单独设置。
  - **侧边栏可见性精准控制（已核对本机 `codex 0.169.0` 协议 Schema）**：
    - `visible`：`thread/start({ ephemeral: false, ... })` + `thread/name/set` 写入主 `$CODEX_HOME`，直接显示在 Codex App 侧边栏；
    - `hidden`：`thread/start({ ephemeral: false, ... })` 执行完首轮后调用 `thread/archive({ threadId })` 隐藏出侧边栏，保留完整上下文可随时 `thread/resume`；若交易员后续在 IM 接管或显式切可见，调用 `thread/unarchive({ threadId })` 即刻恢复在侧边栏显示；
    - `ephemeral`：`thread/start({ ephemeral: true, ... })`，不落盘、绝不出现在侧边栏。
  - 取消任务调用 `turn/interrupt`。
- **后端 2：`AntigravityAdapter`（`agy` / `agy acp`）**
  - 用 `agy --settings <tmpSettingsPath>` 给会话挂载同一运维 MCP 服务和配置的工作区，`--add-dir` 包含草稿目录，按任务来源注入 Profile 提示词。
  - **侧边栏可见性控制（基于 `agy` 数据目录与项目作用域）**：
    - `visible`：使用配置的 `visibleAppDataDir` 与 `--project <project>` 启动/续聊（`--conversation <id>`）；
    - `hidden`：通过 `agy acp --app-data-dir <hiddenAppDataDir> --stdio` 将会话隔离至独立数据目录，不污染主 App 侧边栏，同时持久保留 `conversationId` 供续聊；
    - `ephemeral`：分配一次性临时目录 `--app-data-dir <tmpDir>`，任务结束立即清理目录。
  - **实现期首个实测项**：编码对接 `AntigravityAdapter` 时第一时间在本机实测 `agy` 写入 `visibleAppDataDir` 后桌面/远程 Antigravity App 侧边栏的实时刷新行为，如底层数据目录路径或远程项目参数需微调，直接落入配置项而无需改上层接口。
- **多后端扩展**：在 `config.agent.backends` 中新增配置并向 `adapters/registry.ts` 注册即可扩展 Claude Code / OpenCode 等，消息或任务可通过指定 `backend`（或在 IM 指令前缀如 `@agy` / `@codex`）自由选择后端，未指定时使用 `defaultBackend`。

#### (3) IM 双向对话（独立 Bot，纯 Webhook，监听 `127.0.0.1:18102`）
- 对话 Bot 在创建会话、查找原话题、接管和 `/show`、`/hide` 前一律执行 §6.2 的入站校验。通过后，将投递 ID 与新任务在 `agent.db` 同事务登记，再应答平台；重复投递返回已有任务。
- 新话题以 `im_user`、`visible` 开始；同一已验证平台、Bot、会话和话题的回复按 `im_thread_map` 续聊。交易员接管时为新轮次注入 `im_user` 提示词，保留原会话 ID、来源和历史记录。
- 需要“侧边栏打开 / 回复即接管”的定时报告和脚本唤起任务默认使用持久 `visible` 会话（脚本也可通过 `agents.startTask({ visibility })` 显式指定 `hidden` 或 `ephemeral`）。投递报告到 IM 对话频道时同步登记 `im_thread_map`，服务重启后仍可从 Codex / Antigravity App 远端项目侧边栏或 IM 原话题继续聊。显式选择 `ephemeral` 的一次性输出标明“回复将开启新会话”，不登记续聊映射。

#### (4) 定时全局复盘与四道护栏
- **Cron Scheduler**：内置极简 5 字段 cron 解析器，每分钟检查 `config.agent.cron`，到期发起 `cron_review` 任务（自动注入当日聚合提示词，引导 Agent 查询各账户绩效、成交、Checkpoint 演进、采集健康，结合当下 BTC 状态给策略追加归因笔记并按 `regime` 召回可唤醒的 `shelved` 策略）。
- **四道护栏（`guardrails.ts`，针对所有 `agent.request` 订阅事件及 API 请求）**：
  1. **去重与查询**。收到 `agent.request` 时沿用 core 的 `taskId`。按 `(source_scope, dedupKey)` 在同一事务创建任务或登记原 ID 指向已有任务的别名，然后确认订阅。重放相同 ID 不重复计费或启动。查询别名返回 `{ taskId, canonicalTaskId, status, ... }`。预算等原因拒绝已接收请求时持久记录 `failed` 和原因。
  2. **资源限额**。并发、写会话互斥、超时和 UTC 日预算对所有任务统一适用。写会话退出或被终止后释放写锁。
  3. **Profile 提示词**。`im_user` 跟随经验证的交易员指令；`cron_review` 默认归因、更新备注并发报告；`script_triggered` 默认分析上下文并给出诊断。后两类遇到改变实盘状态的建议先报告并询问。所有 Profile 注册 §7.1 同一工具集。
  4. **反递归**。调度器根据已登记的父任务计算 `depth` 并执行 `maxDepth=1`，达到上限的任务创建请求返回结构化拒绝。工具目录对各 Profile 一致。

### 6.4 `quant-watchdog`（独立外部看门狗）

- **10 秒周期探测**：
  1. `quant-core` 的 `admin.sock /v1/health`（超时 2s）逐项报告 `bookTicker`、`kline_1m`、`markPrice` 的有效数据时间，任一必需主题超过 `maxPublicWsStaleSec` 则单独告警。私有 WS 使用连接心跳、续期结果与账户对账状态判断健康。另核查 Event Loop 延迟、脚本存活和数据集 TTL。
  2. `quant-storage` 的 `storage.sock /v1/health` 返回两个落盘水位，结合 core 中该消费者实际待处理事件数与滞后时间判断积压，超过 `maxStorageSeqLag` 条告警。磁盘余量低于 `minDiskFreeGb` 告警。
  3. `quant-notifier` `notifier.sock /v1/health` 与 `quant-agent` `agent.sock /v1/health`：检测外围进程宕机。
- **直连兜底告警**：独立读取 `secrets/im.json`，直接 POST Telegram / 飞书告警频道，完全绕过 `quant-notifier`；内置同类异常 5 分钟防刷屏抑制与恢复通知。
- **极端失联应急撤单（默认关闭）**：仅当 `emergencyCancel.enabled === true` 且 `quant-core` 连续 `consecutiveFailures`（默认 6 次 = 60 秒）探活失败无响应时，读取 `secrets/exchange.json` 调用 Binance `DELETE /fapi/v1/allOpenOrders?symbol=BTCUSDT` 为已配置实盘子账户撤清挂单，并连发紧急告警（**绝不擅自市价平仓**，仅清挂单防失控成交）。
- **本地 `watchdog.sock`**：暴露 `GET /v1/health`，直接返回最新一轮 5 进程聚合健康快照（供 MCP `system_health` 与 `quant-ops status` 读取；若 `watchdog` 不可达则由调用方回退直连 `admin.sock` 并标注 watchdog 异常）。

---

## 7. 可信运维工具与 `quant-ops`

### 7.1 MCP 运维工具

已接入的运维 Agent 共享以下 33 个工具。Profile 选择 §6.3 的任务提示词。对于需要确认的操作，Agent 可以向交易员询问后直接调用相应工具，审计保留真实执行来源。

| 分类 | 工具 | 底层路由 |
| :--- | :--- | :--- |
| 观测（9） | `market_snapshot`、`accounts_list`、`accounts_state`、`state_inspect`、`market_klines`、`data_list_datasets`、`data_query`、`ledger_query`、`ledger_pnl_summary` | `admin.sock` / `storage.sock` |
| 策略库（5） | `strategies_search`、`strategies_get`、`strategies_tag`、`strategies_fork`、`strategies_seal` | `storage.sock` |
| 预检与回测（2） | `script_validate`、`backtest_run` | `storage.sock` |
| 运行、提案与任务（12） | `runs_list`、`runs_logs`、`runs_start_research`、`runs_start_collector`、`runs_start_shadow`、`runs_stop`、`runs_propose_live`、`proposals_status`、`proposals_approve`、`proposals_reject`、`agents_start_task`、`agents_get_task` | `admin.sock` / `storage.sock` / `agent.sock` |
| 运维与通知（5） | `system_health`、`risk_kill_switch`、`risk_unkill`、`system_reload`、`notify_send` | `watchdog.sock` / `admin.sock` / `notifier.sock` |

`proposals_approve` 和 `proposals_reject` 对应提案状态机，`risk_unkill` 和 `system_reload` 分别对应解除熔断与热重载。盘中逐笔交易属于持租约脚本的 runtime 契约。交易所密钥由网关使用，封存脚本的修改通过 fork 生成新版本。

### 7.2 极简人工运维兜底命令（`quant-ops`）

SSH 本机运维入口复用上述控制面接口及审计记录。

```bash
bun run ops -- status                              # 查看 5 进程健康、活跃实盘账户与 Kill Switch 状态
bun run ops -- unkill <global|accountId>           # 人工解除 Kill Switch
bun run ops -- reload                              # 人工触发风控与账户配置热重载
bun run ops -- approve <proposalId> <proposalDigest> # 绑定上线、替换或停机的完整参数
bun run ops -- reject  <proposalId> [reason]       # IM 故障时的本机驳回兜底
```

---

## 8. 分阶段实施顺序与验收标准

严格自底向上实现，每阶段可独立验证；**全程不写任何模拟测试策略脚本**，测试代码仅验证底座自身正确性。

| 阶段 | 交付模块 | 验收标准（基础设施单测 / 真实联调） |
| :--- | :--- | :--- |
| **P1** | storage 双库、策略库、验证与打包 | 两库交错提交后强制中断并从较小游标恢复，核对行情、成交与费用各一份。验证同名并发封存、写入中断恢复、SDK 版本变化、来源或产物校验失败。用只引用 SDK 版本的基础设施入口在仓库外离线加载产物 |
| **P2** | core 事件、意图、风控、Checkpoint 与调度 | storage 离线时发布超过一个预留块并重启，订阅者游标持续前进。环缓冲溢出后重放关键事件。验证双信号只提交一次终态、旧 epoch 普通写拒绝、在途订单阻止换代、交接中重启及 stop 提案重复确认 |
| **P3** | Binance 分路行情、私有对账、Shadow、SDK 与回测 | 独立中断标记价时盘口可用且开仓被拒。覆盖发单后丢回执、崩溃后同键重试、断线期间一开一平的成交及费用、10 倍杠杆收紧到 5 倍后仍能减仓。验证 10:05 发布的 10:00 因子在 10:01 的 query 和 watch 均不可见。使用配置的 Demo 账户联调 Hedge Mode 与账户独占 |
| **P4** | 通知/对话的 Webhook 校验、运维及审计 | 两个 Bot 均覆盖错误凭据、错误会话、非白名单发送者、重复投递和回复接管。覆盖 notifier、ops 与 Agent 的批准/驳回、操作人记录、停机后持仓告警和 Kill Switch 解除 |
| **P5** | 共享 MCP 工具、两个 Agent 后端、任务与报告 | 三种 Profile 的工具名集合相同，提示词及审计来源可区分。验证去重别名按初始 taskId 可查、资源限额和反递归。实测持久隐藏报告在服务重启后的续聊，以及一次性报告回复开启新会话 |
| **P6** | `quant-watchdog`、5 个常驻 `systemd` unit、反向代理与整机联调 | 验证 `Bun.spawn` 孤儿 PID 清理与 SDK 断连自保退出、并行 `readonly` 行情监听脚本触发 `visible` Agent 会话在 App 侧边栏出现并可续聊；验证各行情主题健康、存储滞后、直连兜底告警及应急撤单触发条件 |
