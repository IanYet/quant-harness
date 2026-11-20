# 量化交易 Harness 架构设计

![量化交易 Harness 架构图](./architecture.svg)

---

## 1. 系统边界

- **运行环境**：单机部署，全栈纯 Bun + TypeScript，由 `systemd` 同级托管 5 个常驻进程（1 核 + 4 外围），无 Redis、消息队列或容器编排。
- **交易与行情标的**。对接 **Binance USDⓈ-M 永续合约**的 WebSocket 行情流与 Market Data REST 接口。配置分别指定默认标的、行情观测列表和账户交易标的。跨所或现货等站外数据由采集脚本发布进入数据集。
- **账户与持仓模型**：
  - 多子账户隔离，**单子账户同一时间至多运行 1 个实盘策略（1:1 独占）**；交易所子账户余额、持仓与挂单（含条件单）即唯一账本。
  - 所有实盘子账户在代码层完全同构，仅通过每账户独立配置（`allowedSymbols`、`requireApproval`、`requireApprovalOnStop`、`requireApprovalOnManualTrade`、风控阈值、`labels`）区分小资金灰度账户与主力账户。
  - 所有子账户统一启用 **Hedge Mode（双向持仓）**，交易支持普通单（`MARKET` / `LIMIT`）与交易所原生 Algo 条件单（`STOP_MARKET` / `TAKE_PROFIT_MARKET` / `STOP` / `TAKE_PROFIT` / `TRAILING_STOP_MARKET`）。
- **脚本双轨与策略资产模型**：
  - **终端自由直跑（`local`）**：Agent 编写的一次性研究、数据探索、向量化快测与参数扫描脚本（纯 Bun + TS，存放在 `scripts/research/`）直接在终端 `bun run` 执行，可只读直查本地 WAL SQLite 或本地只读接口，秒级迭代。
  - **常驻托管脚本（`live` / `shadow` / `worker`）**：由 `quant-core` 托管保活，维持单文件 `.ts` + 仅运行时导入 `@quant/tools` + 静态预检与单文件打包约束；其中 `shadow` 与 `worker` 支持直接运行草稿/工作区源码并记录哈希，**仅 `live` 实盘与正式归档强要求封存为 `0444` 不可变 `strategyId`**（Copy-on-Write 复制衍生 + 五维正交标签）。
- **人机分工与交互入口**：
  - 交易员通过 Codex / Antigravity App（SSH Connect）或 CLI 与 AI Agent 对话，并通过飞书 / Telegram 接收通知告警与完成高风险审批（IM 仅做通知与审批，不提供聊天对话功能）。
  - 交互入口采用 **统一本机 CLI（`quant`）+ 精简 11 核心 MCP Tool + 本地 SQLite WAL 只读直查** 协同工作，释放 Coding Agent 原生终端、SQL 与脚本迭代能力。
  - 盘中高频/自动化交易完全由确定性托管脚本完成；交易员在对话中明确指示的单次手动干预（如平半仓、挂保护止损、撤单、调杠杆）允许通过受控手动交易接口（同样过 `quant-core` 风控、账户审批门与全量审计）执行。
- **上线路径**：`本地快测 / 离线回测` → `Shadow 草稿联调（Demo/Testnet）` → `封存不可变版本` → `小资金子账户灰度 (live)` → `主力子账户 (live)`。

---

## 2. 进程拓扑

| 进程 | 类型 | 核心职责 | 故障隔离边界 |
| :--- | :--- | :--- | :--- |
| **`quant-core`** | 常驻主进程 | Binance USDⓈ-M 全量公共行情（基础流 + 按需高频旁路 + REST 代理缓存）、多子账户池（含 Algo 条件单）、账户独占锁与防呆风控、受控手动交易、托管 Checkpoint、脚本调度、持久化有序事件总线 | 核心交易链路；持续运行不重启，无同步重查询、无慢速外呼、无用户脚本代码，支持配置热重载 |
| **`quant-storage`** | 外围常驻进程 | 订阅主事件流异步批量落盘、分库存储与日志滚动、不可变策略库索引、承接历史数据回补 / 预热 / 标准同码离线回测；同时对外开放 SQLite WAL 只读直查 | 重启或故障不影响 `quant-core`；恢复后按 `seq` 断点补写 |
| **`quant-notifier`** | 外围常驻进程 | 飞书 / Telegram 通知投递（含脚本自定义 `sys.notify` 告警）、降噪冷却重试、实盘与手动交易审批交互并回调 `admin.sock` | 重启或外网超时不影响 `quant-core` |
| **`quant-agent`** | 外围常驻进程 | 复用 Codex / Antigravity 等 Agent Harness、暴露 `agent.sock` 任务接口、远端侧边栏可见性控制、定时全局复盘、承接脚本发起的异步任务（宽松预算型护栏） | 重启或 LLM 慢响应不影响 `quant-core` 与告警通道 |
| **`quant-watchdog`** | 外围常驻进程 | 外部探活 `quant-core` 与外围进程、多标的行情新鲜度监控、独立直连兜底告警、可选极端失联应急撤单（支持保留保护性止损条件单） | 独立于 `quant-core` 之外，持独立告警通道 |
| **托管 Bun 脚本进程** | 隔离工作进程 | 运行实盘策略（`live`）、测试网策略（`shadow`）、按需组合能力的常驻监控/采集脚本（`worker`） | 进程级隔离，单脚本崩溃或阻塞不影响主进程及其他脚本 |
| **本地研究脚本** | 非托管短任务 | Agent 终端直接 `bun run` 的一次性分析、探索、向量化快测与参数扫描脚本 | 通过只读接口访问行情快照和本地账本 |

本机进程间通信统一采用 **Unix Domain Socket（UDS）上的 HTTP/1.1 + JSON 与 NDJSON 持续流**。

---

## 3. 核心常驻进程：`quant-core`

### 3.1 控制面、托管调度与受控手动交易（`admin.sock`）

控制面同时承接 **精简 MCP Server**、**统一本机 CLI（`quant`）** 与 **`quant-notifier` 审批回调**，共用同一套操作契约与审计日志：

- **控制操作**：`start` / `stop` / `replace` / `manual-trade` / `approve` / `reject` / `status` / `kill-switch` / `reload`（热重载风控参数、观测标的列表与子账户配置）。
- **脚本拉起与审批门**：
  1. **`live`**：仅接受已封存的不可变 `strategyId`（如 `butterfly-grid@v3`），启动前校验只读源码与登记执行产物 `code_hash`；
  2. **`shadow`**：同时接受已封存 `strategyId` 或 `strategies/_drafts/*.ts` 草稿路径（启动前同样执行静态预检、打包与记录 `code_hash`，便于追踪成绩单且不污染策略资产库）；
  3. **`worker`**：接受 `scripts/workers/*.ts` 路径（经静态预检与打包）及本次运行声明的**能力组合（`capabilities`）**；
  4. **免审批路径**（`worker`、`shadow`、目标子账户配置 `requireApproval: false` 的实盘上线/替换或免审批手动交易）：立即执行并 `Bun.spawn(["bun", "run", artifactPath], { env })` 返回 `{ runId, pid, status }`；
  5. **审批路径**（启用 `requireApproval` / `requireApprovalOnStop` / `requireApprovalOnManualTrade` 的实盘变更或手动交易）：绑定目标与完整参数摘要返回 `{ status: "pending_approval", proposalId }`，经 `quant-notifier` 推给交易员；确认后再原子执行。
- **受控单次手动交易干预**：
  - 承接交易员在对话中明确指示的单次手动下单、平仓、挂条件止损/止盈单、撤单或调杠杆请求；
  - 与实盘策略走同一套 `quant-core` 风控（含减仓绿灯）、稳定幂等键、统一出口限频、账户审批门及审计留痕（标记 `source: "manual"`）；
  - **不抢占活跃实盘策略的独占租约**，成交回报照常经事件总线推送给活跃策略与 `quant-storage`。
- **子进程环境注入**：拉起托管脚本时显式注入最小只读环境变量：`QUANT_SOCKET`（`runtime.sock`）、`QUANT_STORAGE_SOCKET`（`storage.sock`）、`QUANT_TOKEN`、`QUANT_MODE`、`QUANT_RUN_ID`，以及按角色附加的 `QUANT_ACCOUNT`、`QUANT_EPOCH`、`QUANT_CAPABILITIES`、`QUANT_STATE_NAME`。

### 3.2 脚本通信协议、能力组合矩阵与双轨执行（`runtime.sock`）

托管脚本均由 `quant-core` 托管拉起并统一 `import @quant/tools`（单文件 `.ts`，唯一运行时导入 `@quant/tools`）；`runtime.sock` 拒绝未携带有效 `QUANT_TOKEN` 的托管写请求（`401`），并对本地研究脚本开放独立的只读行情/历史查询通道（`storage.sock` + 本地只读 SQLite WAL + `runtime.sock` 只读行情快照/流）。

#### 通信信道

| 方向 | 协议与载体 | 语义 |
| :--- | :--- | :--- |
| **托管脚本 → `quant-core`** | UDS HTTP + JSON（`Authorization: Bearer <QUANT_TOKEN>`） | 交易、只读账户查询、REST 行情代理、状态读写、数据发布、通知发送（`sys.notify`）与异步 Agent 触发，返回 JSON 或结构化错误码 |
| **`quant-core` → 脚本（可靠主事件流）** | UDS 长连接 HTTP + NDJSON（`GET /v1/stream?topics=...&after_seq=...`） | 基础行情（K 线/盘口/标记价/强平/合约变更）、外部数据集、私有成交与账户回报、`sys.handover`；SDK 封装为 `for await`，断线按 `after_seq` 自动续传 |
| **`quant-core` → 脚本（高频行情旁路）** | UDS 长连接 HTTP + NDJSON（`GET /v1/market/fast-stream?streams=...`） | 按需订阅的深度（`depth`/`depth_diff`/`rpi_depth`）、归集成交（`agg_trade`）、24h Ticker、连续 K 线、全市场推送等高频行情；无 `seq`、慢消费 `drop_oldest`，绝不阻塞主事件总线 |
| **脚本 → `quant-storage` / SQLite** | UDS HTTP + JSON（托管脚本）；本地 WAL 只读直连（`local` 研究脚本） | 历史 K 线、资金费率、强平记录、外部数据集与日志查询；本地一次性研究脚本可直接 `new Database(path, { readonly: true })` 执行任意 SQL 与向量化计算 |
| **托管脚本 ↔ `quant-core`（OS）** | `stdout/stderr` 管道 + `SIGTERM` | 输出写入滚动日志。收到 `sys.handover` 或终止信号时，SDK 按动作调用策略的 `onStop` 或 `onHandover`，收尾期限为 3 秒，超时由 core 接手 |

#### 托管模式与可组合能力矩阵

`quant-core` 内存维护 `Map<token, Session>`，保留**交易写权独占隔离**，同时放开非交易能力的自由组合：

| 角色（`mode`） | 启动来源 | 全量行情（基础流 + 高频旁路 + REST 代理） | 只读账户/持仓/成交观测 | `data.publish` | `trade.*`（含 Algo 条件单） | `notify.send` | `state.save / load` | `agents.startTask` |
| :--- | :--- | :---: | :--- | :--- | :--- | :---: | :--- | :---: |
| **`live`** | `admin`（绑定唯一 `account_id`，仅不可变 `strategyId`） | 允许 | 本账户 | 可选（声明 `publishDatasets`） | 实盘路由（`activeEpoch` + 风控 + `allowedSymbols`） | 允许（限流） | `account:<accountId>`（校验 `activeEpoch`） | 允许（经护栏） |
| **`shadow`** | `admin`（绑定 Shadow 池账户，支持草稿或 `strategyId`） | 允许 | 本 Shadow 账户 | 可选（声明 `publishDatasets`） | Shadow 路由（Demo/Testnet + 同风控） | 允许（限流） | `shadow:<runId>` | 允许（经护栏） |
| **`worker`** | `admin`（`scripts/workers/*.ts`，按需声明能力组合） | 默认允许 | 按 `accountRead: string[] \| "*"` 授权只读 | 按 `publishDatasets: string[]` 独占发布 1 或多个数据集 | 拒绝（403） | 按能力授权（默认开，限流） | `worker:<name>` | 按能力授权（默认开，经护栏） |
| **`local`（非托管）** | Agent 终端直接 `bun run`（`scripts/research/*.ts`） | 允许只读快照 / 历史 | 只读直查 `ledger.db` / `quant` CLI | 拒绝 | 拒绝 | 经 `quant` CLI / MCP | 本地文件自由读写 | 经 `quant` CLI / MCP |

#### `@quant/tools` 核心原语（统一 Hedge Mode + 全量行情 + 通知）

```ts
// 双向持仓四原语（支持普通单 MARKET/LIMIT 与交易所原生 Algo 条件单，可选 symbol 默认 defaultSymbol）
trade.openLong(params: OrderParams)   // BUY  + positionSide: LONG
trade.closeLong(params: OrderParams)  // SELL + positionSide: LONG（最多平至 0，支持挂条件止损/止盈）
trade.openShort(params: OrderParams)  // SELL + positionSide: SHORT
trade.closeShort(params: OrderParams) // BUY  + positionSide: SHORT（最多平至 0，支持挂条件止损/止盈）
trade.cancel(ref)                     // 支持普通单与 Algo 条件单撤单
trade.cancelAll(opts?)                // 支持按 symbol 及 includeConditional 控制是否同步撤条件单
trade.positions(symbol?)              // { long, short } 双腿持仓
trade.openOrders(opts?)               // 合并返回普通挂单与活跃 Algo 条件单

// 只读账户观测（对授权 accountRead 的 worker 开放，支持监控浮亏、强平距离与异常成交）
accounts.list()
accounts.state(accountId)
accounts.watch(accountIds, topics)

// 全量行情（基础实时流 + 按需高频流 + 官方 Market Data REST 代理）
market.snapshot(symbol?)
market.watch(topics, opts?)           // kline / book_ticker / mark_price / liquidation / contract_info
market.watchFast(streams)             // 按需订阅 depth / depth_diff / rpi_depth / agg_trade / ticker_24h / mini_ticker / continuous_kline / all_* / composite_index / asset_index
market.klines(...) / market.depth(...) / market.fundingRates(...) / market.openInterest(...) / market.longShortRatio(...) / market.takerVolume(...) / market.basis(...)

// 直接发通知（写事件总线 sys.notify，零阻塞 quant-core，由 quant-notifier 降噪投递至飞书/TG）
notify.send({ level, title, body, dedupKey?, cooldownSec? })

// 异步触发 Agent 任务（默认远端 App 侧边栏 visible 可见）
agents.startTask({ dedupKey, prompt, title?, backend?, visibility?, cooldownSec?, context? })
```

同一份策略代码在标准离线回测、`shadow` 与所有实盘子账户下零改动运行，由注入的 Transport 与 Token 决定执行去向。

### 3.3 交易所网关、全量行情分层与多子账户池

1. **Binance USDⓈ-M 全量公共行情分层（遵守 Binance 2026 `/public` 与 `/market` 分路）**：
   - **第一层 · 基础常驻流**按 `symbols` 订阅，进入主事件总线分配 `seq`。历史行情只归档主网数据。
     - `/public`：`{symbol}@bookTicker`（最优买卖盘口）；
     - `/market`：`{symbol}@kline_1m`（实时与闭合 `1m` K 线，并按标的在内存滚动聚合 `5m / 15m / 1h / 4h / 1d` 闭合 K 线）、`{symbol}@markPrice@1s`（标记价、指数价与资金费率）、`{symbol}@forceOrder`（强平/爆仓单推送）、`!contractInfo`（合约规则/保证金阶梯变更通知，自动刷新 `exchangeInfo` 缓存）。
   - **第二层 · 按需高频动态流（零订阅时不连流，有订阅时动态 `SUBSCRIBE`，走独立无 `seq` 有损旁路，默认不落盘）**：
     - `/public`：`{symbol}@depth5|10|20[@100ms|@250ms|@500ms]`（有限档深度快照）、`{symbol}@depth[@100ms|@250ms|@500ms]`（增量深度）、`{symbol}@rpiDepth@500ms`（RPI 深度）、`!bookTicker`（全市场最优盘口）；
     - `/market`：`{symbol}@aggTrade`（归集成交流）、`{pair}_{contractType}@continuousKline_{interval}`（连续合约 K 线）、`{symbol}@ticker` / `!ticker@arr`（24h 完整 Ticker）、`{symbol}@miniTicker` / `!miniTicker@arr`（24h 精简 Ticker）、`!markPrice@arr[@1s]`（全市场标记价）、`!forceOrder@arr`（全市场强平）、`{symbol}@compositeIndex`（综合指数）、`{asset}@assetIndex` / `!assetIndex@arr`（多资产保证金指数）。
   - **第三层 · 官方 Market Data REST 代理（统一出口 IP 权重限频桶 + 短 TTL 内存缓存）**：
     - 完整覆盖交易规则（`exchangeInfo`）、盘口与成交（`depth`、`trades`、`aggTrades`）、全系列 K 线与价格（`klines`、`continuousKlines`、`indexPriceKlines`、`markPriceKlines`、`premiumIndexKlines`、`premiumIndex`、`ticker/24hr|price|bookTicker`）、资金费与持仓/衍生品情绪指标（`fundingRate`、`fundingInfo`、`openInterest`、`openInterestHist`、`topLongShortAccountRatio`、`topLongShortPositionRatio`、`globalLongShortAccountRatio`、`takerlongshortRatio`、`basis`、`indexInfo`、`constituents`、`assetIndex`）。
     - Binance 站内自带的 OI、多空比、基差、主动买卖量与爆仓数据直接由平台原生提供，外部采集 `worker` 专注真正站外的数据源（如 Coinbase 溢价、现货价差、ETF 净流入、链上/宏观数据）。
2. **多子账户池（`Account Pool`）与 Algo 条件单**：
   - 所有实盘子账户与 Shadow 池账户共用同一个 `BinanceAccountClient`，按 `account_id` 独立配置 API 凭证、`allowedSymbols`、审批开关、防呆风控阈值与 `labels`；
   - 普通单（`MARKET` / `LIMIT`）走 `/fapi/v1/order`，原生条件单（`STOP_MARKET` / `TAKE_PROFIT_MARKET` / `STOP` / `TAKE_PROFIT` / `TRAILING_STOP_MARKET`）直连 Binance 新版 **Algo Order Service**（`/fapi/v1/algoOrder`、`/fapi/v1/algoOpenOrders`、`/fapi/v1/openAlgoOrders`）；
   - 私有 User Data Stream 同时监听 `ORDER_TRADE_UPDATE`、`ACCOUNT_UPDATE` 与 **`ALGO_UPDATE`**，断线对账同步拉取普通挂单与活跃条件单；
   - 启动按 `allowedSymbols` 自动校验 Hedge Mode、保证金模式与初始杠杆，所有 REST 请求共享统一出口 IP 权重限频桶。
3. **Shadow 测试通道**使用独立的 Demo/Testnet 账户凭据验证下单、条件单、撤单和成交回报。核对测试单触发价或预热时，按需读取测试网行情并在内存中使用。研究、回测与价格监听使用主网行情。

### 3.4 账户独占锁、防呆风控与托管 Checkpoint

1. **账户级独占租约**：`accountLease: Map<(mode, accountId), { strategyId?, targetPath, runId, epoch }>`。每个实盘 `account_id` 同一时刻仅 1 个活跃 `live` 租约；通过 `replace` 递增 `epoch` 原子交接，旧 `epoch` 请求返回 `409 STALE_EPOCH`。受控单次手动交易复用该账户串行化风控入口但不抢占策略租约。
2. **防呆风控与幂等**：
   - 下单频控、参数底线（拦截 `NaN` / 非正数 / 超出 `allowedSymbols` / 超出单笔名义上限 / 超出最大杠杆）；
   - **减仓绿灯**：Hedge 模式下 `closeLong` / `closeShort`（含挂出的条件止损/止盈保护单）不受开仓限额、Kill Switch 或杠杆收紧阻断；
   - 单子账户或全局一键 Kill Switch（立即拒绝开仓，可选联动撤普通挂单，默认可保留保护性条件止损单）；
   - 所有普通单与条件单强制携带稳定幂等键 `clientOrderId`（条件单映射为 `clientAlgoId`）。
3. **托管 Checkpoint（`state.save / state.load`）**：
   - 数据结构：`{ namespace, accountId?, version, epoch, schemaVersion, updatedAt, data }`，单份上限放宽至 **1 MB**（足以承载滑动特征窗口、网格层级、多标的冷却状态或去重集合）；
   - `live` 写入强校验 `activeEpoch` 防止旧版覆写；
   - `quant-core` 内存保留热份供即时读取，同步发 `state.checkpoint` 事件归档历史版本至 `ledger.db`，并写本地原子热备文件（`tmp + rename`）。

### 3.5 主事件总线与高频行情旁路

- **主事件总线（Event Bus，严格有序 `seq`）**：承载基础行情（`market.kline`、`market.book_ticker`、`market.mark_price`、`market.liquidation`、`market.contract_info`）、交易与条件单（`trade.*`、`shadow.*`）、外部数据集（`data.sample`）、状态（`state.checkpoint`）、通知（`sys.notify`）、Agent（`agent.*`）与系统审计事件（`sys.*`）。关键事件先落 `core.db` 后广播；内存环形缓冲保留最近 20,000 帧供 `after_seq` 续传。
- **高频行情旁路（Fast Stream Bypass，无 `seq`）**：承载按需订阅的深度（`depth`/`depth_diff`/`rpi_depth`）、`aggTrade`、24h Ticker、连续 K 线与全市场推送；采用每消费者有界队列 + `drop_oldest` 策略，绝不占主总线 `seq` 空间、绝不挤占环形缓冲、默认不落盘 `timeseries.db`，保障 `quant-core` 与 SQLite 在极端行情下稳如磐石。

---

## 4. 外围独立模块

### 4.1 `quant-storage`（持久化存储、WAL 只读直查与策略库）

- 持持久化游标 `last_seq` 订阅 `quant-core` 主事件流批量落盘，重启从 `last_seq` 追平。
- **物理存储分层**：
  1. `ledger.db`（SQLite WAL）按 `mode` 区分实盘（`live`）和测试网（`shadow`）的委托、条件单、成交、账户快照与资金流水，另存运维审计、Checkpoint 历史版本和策略资产索引。
  2. `timeseries.db`（SQLite WAL）的交易所行情表只存主网 K 线、分钟级指标、强平记录及历史资金费率。外部采集因子存入 `data_samples`。
  3. `strategies/` 与 `artifacts/`：策略草稿区、只读不可变策略源码及打包产物目录；
  4. `logs/<role>/<name>/<run_id>.log`：按大小/日期滚动压缩的文本日志。
- **WAL 只读直查与双模式回测**：
  - `ledger.db` 与 `timeseries.db` 明确允许 Agent 和 `scripts/research/` 一次性研究脚本以 SQLite 只读模式并发直查（任意 SQL 聚合、关联统计、向量化快测与参数网格扫描）；
  - 同时保留 `quant-storage` 内置的**标准同码事件回放引擎（`backtest_run`）**，用于策略上线前的防未来函数精确回测（同步支持多标的与 Algo 条件单撮合）。

### 4.2 `quant-notifier`（飞书 / Telegram 通知与审批网关）

- 订阅 `trade.*`、`sys.*`（含托管脚本经 `notify.send()` 写入的 **`sys.notify`** 自定义告警）及审批事件，并经 `notifier.sock` 接收 Agent 复盘与诊断摘要，推送至 **飞书 / Telegram 告警审批 Bot**，内置按 `dedupKey` 聚合降噪、冷却防刷屏与重试队列。
- 仅提供通知、实盘/手动交易审批交互与白名单管理员指令（回调 `admin.sock`），不承载 Agent 对话功能。

### 4.3 `quant-agent`（Agent 会话与任务网关）

统一承接底层 Agent Harness 复用、定时全局复盘与脚本主动发起任务：

- **统一对外接口（`agent.sock`）**：`POST /v1/tasks`、`POST /v1/sessions/:id/messages`、`GET /v1/tasks/:id`、`POST /v1/tasks/:id/cancel`。
- **底层 Agent Harness 适配与侧边栏可见性**：通过薄 Adapter 复用 Codex App Server 与 Antigravity（`agy` / `agy acp`），支持按任务控制会话在远端 App 侧边栏的可见性（`visible` / `hidden` / `ephemeral`；`user`、`cron_review` 与 `script_triggered` 均默认 `visible`）。任务完成后经 `quant-notifier` 推送摘要通知，交易员可直接从远端项目侧边栏打开会话续聊。
- **定时全局复盘（Cron Scheduler）**：定期拉起 `cron_review` 任务，直接结合只读 SQLite / `quant` CLI 汇总绩效、滑点、资金费、Checkpoint 与采集健康，给策略追加归因笔记并按 `regime` 召回可唤醒的 `shelved` 策略，推送复盘摘要。
- **宽松预算型护栏（不卡死正常深度研究）**：
  1. **去重与灵活冷却**：按 `(source_scope, dedupKey)` 在冷却窗口内合并重复请求（默认 5 分钟，支持脚本在 `agents.startTask` 中按需覆盖 `cooldownSec`）；
  2. **并发与预算上限（无全局代码仓库单写锁）**：允许并行运行多个研究/复盘任务（默认 `maxConcurrentTasks = 4`），按每日任务数与费用预算（默认 200 次 / 50 USD）做软告警 + 硬上限保护；配备充裕的单任务轮数与超时上限（`script_triggered` 30 轮 / 15 分钟，`cron_review` 40 轮 / 20 分钟，`user` 80 轮 / 30 分钟）；
  3. **Profile 提示词与审计约定**：区分 `user`、`cron_review`、`script_triggered` 职责与确认习惯，服务端如实记录执行来源与审计日志；
  4. **防失控反递归（`maxDepth = 3`）**：允许脚本触发的研究任务在必要时继续委派专项排查任务，同时防止无限自激循环。

### 4.4 `quant-watchdog`（独立外部看门狗）

- 定时探测 `quant-core` 健康端点与 Event Loop 响应延迟，按多标的 `symbols` 核查基础行情新鲜度与私有 WS 心跳，监控实盘策略与 `worker` 存活、数据集 TTL、`quant-storage` 落盘滞后、磁盘空间及 `quant-notifier` / `quant-agent` 存活。
- 持有独立直连 IM 的兜底告警通道；支持配置极端失联应急撤单开关，并可配置 `includeConditional`（默认 `false` 仅清普通挂单、保留交易所侧保护性止损条件单）。

---

## 5. 工作区划分、策略资产与版本管理（Strategy Registry）

### 5.1 自由编辑工作区 vs 不可变策略库

清晰区分「日常自由迭代区」与「只读不可变策略资产库」：

```text
strategies/
├── _drafts/                  # 策略草稿自由编辑区（支持直接回测与 shadow 联调）
│   └── butterfly-grid-v4.ts
├── butterfly-grid/           # 策略族（family）
│   ├── v1.ts                 # 封存即锁定只读（0444）+ sha256 入库，永不原地修改
│   ├── v2.ts                 # parent_id: butterfly-grid@v1
│   └── v3.ts                 # parent_id: butterfly-grid@v2
└── breakout-oi/
    └── v1.ts

scripts/
├── workers/                  # 常驻托管非交易脚本自由编辑区（行情监听、账户风控盯盘、外部数据采集）
│   ├── btc-alert-watcher.ts
│   └── etf-flow-collector.ts
└── research/                 # 一次性自由直跑脚本区（纯 Bun + TS，Agent 终端直接 bun run 探索与快测）
    └── funding-edge-scan.ts
```

- **单文件与封存规则**：
  - 托管脚本（`live` / `shadow` / `worker`）维持单文件 `.ts` + 唯一运行时导入 `@quant/tools` + 静态预检与打包，确保零依赖漂移与可复现；
  - `shadow` 允许直接运行 `strategies/_drafts/*.ts`（同样走预检打包并记录 `code_hash` 便于核对表现，不污染不可变策略库）；
  - **仅正式归档或上 `live` 实盘时**，必须经 `strategies_seal` 封存为只读不可变版本 `<family>@<version>`；基于旧版修改必须经 `strategies_fork` 复制到 `_drafts/` 衍生，保留完整谱系（`parent_id`、`source_hash`、`code_hash`、`diff_summary`）。

### 5.2 五维正交标签与生命周期

| 维度 | 规范值 / 示例 | 用途 |
| :--- | :--- | :--- |
| **`lifecycle`** | `draft` / `backtested` / `shadow` / `live` / `shelved`（行情不适配封存备查）/ `deprecated`（硬伤废弃） | 区分“待未来行情回归唤醒”与“彻底废弃” |
| **`regime`** | `trending-up` / `trending-down` / `ranging` / `high-vol` / `low-vol` / `squeeze` | 按当下市场状态召回历史适配策略 |
| **`style`** | `butterfly` / `grid` / `trend` / `mean-reversion` / `funding-arb` | 按策略机理与仓位形态归类 |
| **`account_fit`** | `small-cap` / `large-cap` / `low-lev` / `high-lev` / `maker-heavy` / `taker-heavy` | 匹配子账户资金量、杠杆与费率结构 |
| **`factors` & `notes`** | `uses:btc_oi`、`uses:etf_flow`；结构化归因记录（有效区间、失效特征与分段绩效摘要） | 沉淀历史归因，支撑复盘与再次启用判断 |

---

## 6. 人机与 Agent 交互入口（CLI + 精简 MCP + 只读 SQLite）

### 6.1 三层协同定位

采用三层协同入口，释放 Coding Agent 的原生研发效率，同时控制对话上下文开销：

1. **本地只读 SQLite WAL + 日志直查**（最灵活的数据研究面）：Agent 与 `scripts/research/` 本地脚本可直接只读查询 `ledger.db`、`timeseries.db` 和 `logs/`，完成任意复杂 SQL 聚合、跨表关联与向量化快测，零上下文膨胀。
2. **统一本机 CLI `quant`**（覆盖 100% 日常查询、开发与运维操作，支持 `--json`）：供 Agent 终端、本地脚本与人工 SSH 共享，底层直连 `admin.sock` / `storage.sock` / `agent.sock` / `notifier.sock` / `watchdog.sock`。
3. **精简核心 MCP Server（11 个强契约写/控制工具）**：大幅降低对话上下文税，仅将高价值、有状态变更与审批契约的核心动作保留为结构化 MCP Tool，与 `quant` CLI 共用同一套服务端路由和审计日志。

### 6.2 精简核心 MCP 工具集（11 个）

| Tool | 底层去向 | 用途 |
| :--- | :--- | :--- |
| `live_propose` | `admin.sock` | 按不可变 `strategyId` 发起实盘上线或 `replace` 切换（关联回测/Shadow 证据；免审批直接生效，需审批返回 `proposalId`） |
| `trade_manual` | `admin.sock` | 受控单次手动交易（开平仓/挂条件止损止盈/撤单/全撤/调杠杆），统一过风控、账户审批门与审计，不抢占策略租约 |
| `proposals_decide` | `admin.sock` | 批准或驳回待确认提案（实盘上线、替换、停机或需审批的手动交易），记录实际执行来源 |
| `runs_start` | `admin.sock` | 启动托管 `worker`（自由组合能力标签）或 `shadow`（支持 `_drafts/` 草稿或已封存 `strategyId`） |
| `runs_stop` | `admin.sock` | 停止指定运行并回收终态 Checkpoint。策略的挂单按 §7.1 的默认规则和可选 `onStop` 处理 |
| `strategies_seal` | `storage.sock` | 静态预检、打包并封存草稿为只读不可变版本 `strategies/<family>/<version>.ts` |
| `strategies_tag` | `storage.sock` | 更新五维标签、追加归因备注或切换 `lifecycle`（`shelved` / `deprecated` 等） |
| `backtest_run` | `storage.sock` | 对草稿路径或已封存 `strategyId` 执行防未来函数标准同码事件回放精测 |
| `risk_kill_switch` | `admin.sock` | 触发或解除全局/单账户 Kill Switch（支持 `cancelOpenOrders` 与 `includeConditional`） |
| `agents_start_task` | `agent.sock` | 发起异步 Agent 任务并控制远端 App 侧边栏可见性 |
| `notify_send` | `notifier.sock` | 向飞书 / Telegram 发送诊断报告、复盘摘要或人工介入请求 |

### 6.3 统一本机 CLI `quant` 覆盖面

涵盖所有上述 11 项控制操作，外加高频只读观测与辅助命令（均支持 `--json`）：
- **健康与运维**：`quant health`、`quant reload`、`quant kill` / `quant unkill`
- **行情与数据**：`quant market snapshot|klines|depth|funding|oi|ls-ratio|basis`、`quant data datasets|query`
- **账户与账本**：`quant accounts list|state`、`quant ledger query|pnl`、`quant state inspect`
- **策略与预检**：`quant strategy search|get|fork|validate|seal|tag`、`quant backtest run`
- **运行与提案**：`quant run list|logs|start|stop`、`quant live propose`、`quant trade ...`、`quant proposal status|approve|reject`、`quant agent start|get|cancel`

### 6.4 真正的硬边界（保留少而硬的底线）

- 交易凭据按 §8 的可信单机约定使用。人工与已接入的 Agent 均可按账户配置批准或驳回提案，审计记录实际执行者。
- 禁止原地修改或删除已封存的不可变策略文件（须经 `fork` 衍生新版本）；
- 实盘策略与受控单次手动交易统一经 `quant-core` 执行风控、账户审批和审计。

---

## 7. 关键运行流程与故障恢复

### 7.1 策略热切换与停机

将子账户 `sub_a` 从 `v1`（`epoch = N`）切换至不可变 `strategyId` `v2`（`epoch = N+1`）：

1. **校验与审批**：核验 `v2` 只读文件与 `code_hash`，按 `sub_a` 的 `requireApproval` 完成确认。
2. **冻结与终态回收**：将 `sub_a` 置为 `handover` 立即拒绝 `v1` 新单；触发优雅终止信号等待 `v1` 提交终态 Checkpoint 并退出。
3. **挂单与条件单处置**：按参数执行 `adopt`（保留原挂单与条件单由 `v2` 接管）或 `cancel_open`（先撤普通未成交挂单，并按 `includeConditional` 决定是否同步撤销 Algo 条件单，默认可保留保护性止损条件单）。
4. **原子切换**：原子递增 `activeEpoch: N → N+1` 并启动 `v2`；`v2` 加载终态 Checkpoint（按 `schemaVersion` 迁移，或 `resetState` 清空）及子账户真实 `{ long, short }` 持仓、挂单与条件单。全程 `quant-core` 不重启。

停止 `live` 或 `shadow` 策略时，默认撤掉它创建或随交接接管的全部挂单，只保留保护现有仓位的止盈止损。策略可以用 `harness.onStop` 保存终态、指定自己要保留或撤销的订单。core 负责执行和对账。脚本未定义回调、退出或超时且未交回有效结果时，core 执行默认规则。手动单和外部订单始终按各自归属处理。

### 7.2 外部数据采集与零缝隙订阅

1. 声明 `publishDatasets: ["ds_a", "ds_b", ...]` 的托管 `worker`（或可选声明的策略）拉取站外数据（现货价差、Coinbase 溢价、ETF 净流入、链上与宏观数据），清洗后调用 `data.publish(dataset, { ts, key?, payload })`。
2. `quant-core` 校验权限、体积与频控后分配 `seq` 广播至主事件总线，`quant-storage` 异步落盘至 `timeseries.db`。
3. 消费脚本启动时先从 `quant-storage` 读取该 `dataset` 最近 N 条样本及最大游标 `S`，再向 `quant-core` 建立 `data.watch(dataset, { afterSeq: S })` 实时流，经环形缓冲补齐 `seq > S` 后无缝切换实时推送。

### 7.3 故障恢复矩阵

| 故障场景 | 系统行为与恢复机制 |
| :--- | :--- |
| **策略 / `worker` 脚本崩溃** | 不影响 `quant-core` 及其他进程；按策略退避重启或停止告警，重启后从 Checkpoint 与子账户真实状态（含保护性条件单）恢复 |
| **`quant-storage` 故障或重启** | `quant-core` 交易与推流零中断；恢复后按 `last_seq` 追平补写，超出环形缓冲则告警并触发历史 K 线/资金费率回补 |
| **`quant-notifier` 故障或外呼超时** | `quant-core` 交易与脚本 `notify.send()`（入总线）零中断；`quant-watchdog` 检出后经直连兜底通道告警 |
| **`quant-agent` 故障或 Harness 超时** | 不影响 `quant-core` 实盘交易与 `quant-notifier` 告警通道；恢复后从 `agent.db` 恢复任务状态、去重窗口与定时复盘调度 |
| **`quant-core` 假死 / Event Loop 阻塞** | `quant-watchdog` 探活超时直连告警；持续失联超阈值且开启开关时触发应急撤单（按配置可保留保护性止损条件单） |
| **`quant-core` 崩溃被 `systemd` 拉起** | 清理孤儿 PID，对已配置子账户按 `allowedSymbols` 执行 **Hedge Mode 校验 + 持仓/普通挂单/Algo 条件单对账**，从 `core.db`/热备恢复最新 Checkpoint 与 `epoch`，恢复 `live` 与 `persistent` `worker` |

---

## 8. 安全约束

- **交易所 API Key**：按子账户独立签发，仅开启合约交易权限，禁用提币与资金划转权限，绑定本机固定出口 IP。
- **凭据使用**。`0600` 按系统用户保护密钥文件。同一用户下的 Agent 与脚本属于可信环境。通常由 `quant-core` 读取凭据签名，启用应急撤单时 `quant-watchdog` 也读取相应凭据。脚本通过运行令牌调用网关。
- **本机通信与产物权限**：各 `.sock` 文件通过文件系统权限隔离，托管运行时请求必须携带有效 `QUANT_TOKEN`；已封存策略文件以 `0444` 只读锁定并经 `code_hash` 校验；本地研究脚本与 Agent 对 `ledger.db` / `timeseries.db` 仅执行 WAL 只读查询，数据库写权限仍由对应守护进程独占。
