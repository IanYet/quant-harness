# 开发与测试

仓库使用 Bun 同时运行 TypeScript、测试、HTTP 服务和子进程。SQLite 来自 `bun:sqlite`。`@quant/tools` 通过 Bun workspace 链接到本项目，SDK 与宿主可以在同一 checkout 修改和验证。

## 建立开发环境

```sh
bun --version
bun install --frozen-lockfile
bun run typecheck
bun test
```

首版验证环境为 Linux x86_64、Bun 1.4.4。类型检查覆盖 `src`、`packages`、`examples` 和 `tests`。自动测试创建临时 SQLite 数据目录和回环端口，交易写入走模拟账户或本机替身。要从实际 CLI 运行两个独立脚本，按[首页的工作流](README.md)操作。

## 从调用链找到修改位置

| 改动 | 入口和相关联的实现 |
|---|---|
| 脚本 API 或命令参数 | 公共签名在 [SDK](packages/tools/index.ts) 和[类型定义](packages/tools/types.ts)，HTTP 路由在 [server.ts](src/server.ts)，命令参数在 [cli.ts](src/cli.ts)。可用 [API 手册](docs/api.md) 核对三者的映射。 |
| 行情或历史数据 | [Source 接口](src/market/source.ts) 约定时钟、所需订阅集合、上游就绪、行情与历史查询。模拟源和币安源由宿主选择。[ticker.ts](src/market/ticker.ts) 复用订阅和分发事件，[history.ts](src/market/history.ts) 只补缺段。 |
| 数据表或数据集 | [db.ts](src/db.ts) 建表并提供事务，数据集 append/read/watch 在 [server.ts](src/server.ts)。表结构变更需要考虑已有数据目录，`CREATE TABLE IF NOT EXISTS` 只负责建缺失的表。[表与查询](docs/storage.md) |
| 交易与对账 | [engine.ts](src/trade/engine.ts) 处理权限、十进制参数、幂等性与所有权。[ledger.ts](src/trade/ledger.ts) 处理订单状态和成交唯一键，账户执行分别位于 [paper.ts](src/trade/paper.ts) 和 [binance.ts](src/trade/binance.ts)。 |
| 换版或断流 | [脚本宿主](src/runs/manager.ts) 管理租约和进程，[预加载器](src/runs/preload.ts) 执行入口快照。[流交付](src/streams.ts) 与 [SDK 解码器](packages/tools/transport.ts) 分别处理缓冲、分帧和取消。 |
| 通知或 agent 服务 | [connectors.ts](src/connectors.ts) 负责路由、HTTP 请求和结果落库。[本机联调](docs/connectors.md) 给出可重复的收发场景。 |

## 复现对应的验收场景

| 验证命令 | 检查点 |
|---|---|
| `bun test tests/streams.test.ts` | 多脚本消费、完整 UTF-8/JSON 行、取消、慢订阅者和数据集游标。 |
| `bun test tests/market.test.ts` | 历史缓存和缺口、1,440 根预热与实时交接、两组 WS、盘口重建及日归档。 |
| `bun test tests/paper.test.ts` | 模拟账户步长、部分成交、幂等、显式撤单、平仓和重启读取。 |
| `bun test tests/binance.test.ts` | 本机核验签名、请求已送达后超时、回报重投、成交补查、持仓模式和执行权交接。 |
| `bun test tests/workflow.test.ts` | 真实 CLI 与独立脚本进程，验证源码快照、候选启动失败、观察者接任和旧挂单。 |
| `bun test tests/connectors.test.ts` | 两个 agent 后端、webhook 的目标与模板、投递失败及重启后的会话路由。 |

`tests/support.ts` 的 `sandbox()` 管理测试宿主、目录与重启。`tests/exchange.ts` 提供本机 REST 和 WebSocket，可以延迟响应、重投回报或中断连接。修改协议处理时，可在替身内复现相应交错，再通过 SDK 或 CLI 检查可观察结果。[测试宿主](tests/support.ts) · [交易所替身](tests/exchange.ts)

涉及行情的改动可核对历史、补发和实时三种事件的标志，确保旧信号不会被当作新指令。涉及交易的改动可核对发送前落库、租约复核、同键重试和重启对账。对外签名、路由、配置默认值发生变化时，相应的参考页分别是 [API](docs/api.md)、[配置](docs/configuration.md) 和[架构](docs/architecture.md)。
