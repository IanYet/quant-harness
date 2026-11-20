# quant-harness

## 项目愿景

在一台带公网 IP 的独立服务器上运行个人 Agent 量化交易工具（当前面向 Binance `BTC/USDT` U 本位永续合约）。

平时主要通过 **Codex App / Antigravity App 的 SSH Connect** 打开远端项目、或 SSH 登录终端用 Codex CLI / Antigravity CLI 与 Agent 对话，由 Agent 负责研究、编写脚本、管理策略与定期复盘；手机上的 **飞书 / Telegram** 仅用于接收交易告警、复盘摘要通知及实盘变更审批：

- **脚本驱动一切运行态**：Agent 编写脚本执行实盘交易、外部数据采集、回测验证或并行行情监听。例如 BTC 价格触及 60,000 USDT 时，行情监听脚本唤起另一个 Agent 新开任务研究过去一小时成交量变化；该会话默认显示在对应 App 的远端项目侧边栏中（并向飞书 / Telegram 推送一条报告摘要通知），随时从 App 侧边栏打开接着聊。
- **确定性盘中执行 + 零中断迭代**：盘中实时下单完全由独立的确定性脚本进程完成，Agent 不参与盘中实时决策；每个子账户同一时间最多运行 1 个实盘策略（统一 Hedge Mode 双向持仓），多个行情监听与采集脚本可并行运行；实盘策略支持先经历史回测、交易所 Demo/Testnet（`shadow`）与小资金子账户验证，并在交易主进程持续运行不重启的前提下热替换（`replace`）。
- **策略资产长期沉淀**：每个落地策略以不可变脚本文件持久化保存，配合衍生谱系、五维标签与验证归因记录，方便在未来行情状态回归时随时检索、复用与二次衍生。

---

## 核心原则与边界

以下约束用于指导后续全部设计与编码，防止系统过度复杂化：

1. **信任 Agent，不做重型防贼沙箱**
   - 本项目是个人单机工具，信任已接入的 Agent：运维与控制面工具对可信 Agent 开放，通过 Prompt 约定确认流程、服务端审计日志与防呆风控兜底，保留 Agent 的完整研发与运维能力。
   - **不做** OS 多用户隔离、`systemd DynamicUser` 脚本沙箱、挂载命名空间或复杂的内部零信任鉴权体系。
2. **极简单机，优先使用 Bun 原生能力**
   - 全栈 **Bun + TypeScript**，无 Redis、消息队列、Docker、ORM、Web 框架或重型 Bot SDK。
   - 仅 5 个常驻服务由 `systemd` 托管；所有策略、监听、采集与回测子进程一律由主进程/存储进程通过 **`Bun.spawn`** 直接拉起（PID 记入状态库用于重启清理孤儿进程）。
   - 存储直接使用 `bun:sqlite`（WAL 分库），本机进程通信使用 UDS HTTP/JSON + NDJSON 持续流，飞书/Telegram 仅实现公网 HTTPS 反代下的纯 Webhook + 原生 `fetch()`。
3. **按需直写，拒绝过度抽象**
   - 交易所直写 **Binance USDⓈ-M Futures**，不设计多交易所插件框架；所有实盘子账户在代码层完全同构，仅靠每账户配置（`requireApproval`、`marginType`、风控阈值、`labels`）区分小资金灰度账户与主力账户。
   - 仅在明确需要多后端的 **Agent Harness 适配层（Codex / Antigravity）** 保留最小接口抽象，统一控制远端 App 侧边栏可见性（`visible` / `hidden` / `ephemeral`）。
4. **真实验证，不写模拟策略脚本**
   - 仓库内不编写示例假策略、模拟行情生成器或内置实时 Paper 撮合引擎；策略验证依靠真实历史数据离线回放、`shadow`（直连 Binance Demo/Testnet）与小资金真实子账户。
5. **Tool Call 作为唯一脚本控制入口**
   - 交易员通过与 Agent 对话驱动策略与脚本迭代，不保留面向日常交易的命令行手工脚本启动入口（仅留极简本机故障应急运维命令 `quant-ops`）。

---

## 架构总览

![量化交易 Harness 架构图](./docs/architecture.svg)

- **1 个常驻主进程 + 4 个独立外围进程**（任一外围模块重启或故障不中断实盘交易）：
  - `quant-core`：常驻交易与行情内核（不重启），承载 Binance 公共行情与多子账户池、账户独占锁与防呆风控、托管 Checkpoint、`Bun.spawn` 脚本调度与持久化有序事件总线。
  - `quant-storage`：订阅事件流异步落盘至分库 SQLite（`ledger.db` / `timeseries.db`）与滚动日志，管理不可变策略资产库，承接历史查询、预热与离线回测。
  - `quant-notifier`：飞书与 Telegram 通知与审批 Bot（纯 Webhook），负责告警聚合降噪、复盘摘要推送、实盘审批卡片交互与管理员指令。
  - `quant-agent`：复用 Codex / Antigravity 的 Agent 会话与任务网关（`agent.sock`），承接定时全局复盘、带护栏的脚本异步唤起与远端 App 侧边栏可见性控制（默认 `visible`），任务完成后经 `quant-notifier` 推送通知摘要。
  - `quant-watchdog`：独立外部看门狗，探测 5 进程健康、Event Loop 延迟与行情新鲜度，持有直连 IM 兜底告警与可选极端失联应急撤单。
- **4 类隔离脚本角色**（均通过 `Bun.spawn` 拉起并注入 `QUANT_TOKEN`，统一 `import @quant/tools`）：
  - `live`：1 子账户 1 实盘策略，按不可变 `strategyId` 运行，受账户独占 Epoch 锁与防呆风控约束。
  - `shadow`：按不可变 `strategyId` 运行，直连 Binance Demo/Testnet 账户池联调与观察。
  - `collector`：外部数据采集脚本，独占目标 `dataset` 发布非基础 Ticker 因子数据。
  - `readonly`：无交易写权的只读脚本，支持一次性研究分析与可并行常驻的行情监听（可调用 `agents.startTask` 唤起 Agent 并使用独立 Checkpoint）。

---

## 文档导航

| 文件 | 说明 |
| :--- | :--- |
| [docs/architecture.md](./docs/architecture.md) | 架构设计（系统边界、进程拓扑、策略资产管理、Agent 工具集与故障恢复） |
| [docs/architecture.svg](./docs/architecture.svg) | 架构图（矢量源文件） |
| [docs/design.md](./docs/design.md) | 详细工程设计（目录结构、配置、UDS/事件契约、Binance 网关、SQLite DDL、Agent 适配与分阶段验收） |
