# quant-harness

面向 `BTC/USDT` U 本位永续合约的单机量化交易运行底座（Harness），基于 **Bun + TypeScript** 构建，零外部中间件（无 Redis、消息队列或容器编排）。

交易员通过手机 IM 与 AI Agent 对话提出需求并审批实盘变更；AI Agent 在离线/异步闭环中负责策略研发、外部数据采集、回测与全局复盘；所有盘中实时交易均由确定性独立 Bun 脚本进程执行。

![量化交易 Harness 架构图](./docs/architecture.svg)

---

## 核心设计

- **1 核 + 4 外围常驻进程**（`systemd` 同级托管，本机 UDS HTTP/JSON + NDJSON 通信）：
  - `quant-core`：常驻交易与行情主进程，持续运行不重启，负责交易所网关、多子账户池、账户独占锁、防呆风控、托管 Checkpoint、脚本调度与内存有序事件总线。
  - `quant-storage`：按 `seq` 订阅事件流异步落盘，维护分库 SQLite（`ledger.db` / `timeseries.db`）、滚动日志与不可变策略库，承接所有历史查询、预热与离线回测读取。
  - `quant-notifier`：交易、审批与异常通知投递（独立 Bot 频道），承接 IM 实盘审批交互并回调 `admin.sock`。
  - `quant-agent`：复用 Codex 等现有 Agent Harness，暴露统一 `agent.sock` 任务接口，承接手机 IM 双向对话（`话题 ↔ session_id`）、定时全局复盘与带护栏的脚本异步触发任务。
  - `quant-watchdog`：独立外部看门狗，探测主进程 Event Loop 延迟、WS 新鲜度与外围模块健康，持有直连 IM 兜底告警与可选极端失联应急撤单能力。
- **1 子账户 1 实盘策略 + 统一 Hedge Mode**：所有实盘子账户在代码层完全同构，按账户配置（`requireApproval`、风控阈值、`labels`）区分灰度与主力账户；统一启用双向持仓，`@quant/tools` 暴露 `openLong` / `closeLong` / `openShort` / `closeShort` 四原语。
- **不可变策略资产库与多维标签**：策略在 `strategies/_drafts/` 编辑预检，封存落地至 `strategies/<family>/<version>.ts`（`0444` 只读 + `sha256` 校验 + `parent_id` 谱系），配合五维正交标签（`lifecycle`、`regime`、`style`、`account_fit`、`factors & notes`）支持按行情状态封存（`shelved`）与未来唤醒。
- **Tool Call 唯一控制入口**：所有脚本（`live` / `shadow` / `collector` / `readonly`）统一由 Agent 通过 Tool Call 经 `admin.sock` 拉起并注入能力令牌（`QUANT_TOKEN`），支持 `离线回测 → Shadow（交易所 Demo / Testnet API）→ 小资金子账户 → 主力子账户` 闭环及基于托管 Checkpoint 的零中断 `replace` 热切换。

---

## 文档导航

| 文件 | 说明 |
| :--- | :--- |
| [docs/architecture.md](./docs/architecture.md) | 架构规格文档（进程边界、通信协议、多子账户池、策略资产管理、Agent 工具集与故障恢复） |
| [docs/architecture.svg](./docs/architecture.svg) | 架构图（矢量源文件） |
