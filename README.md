# Quant Harness

用普通 TypeScript 脚本采集行情、运行策略和调用外部服务。常驻的 `harnessd` 共享行情订阅、跟踪账户和持久化 SQLite 数据。交易员可以一边运行旧策略，一边用新脚本观察同一份行情和挂单。

默认实例使用加速模拟行情和本地模拟账户，无需交易所密钥。公共行情和交易适配器支持币安 USDⓈ-M 永续合约。

## 跑通第一笔模拟挂单

以下命令在 Linux x86_64、Bun 1.4.4 上验证。先打开一个终端，检出项目并启动宿主。Bun 可按[官方安装说明](https://bun.com/docs/installation)安装。

```sh
git clone https://github.com/IanYet/quant-harness.git
cd quant-harness
bun --version
bun install --frozen-lockfile
bun run harnessd
```

看到 `harnessd http://127.0.0.1:8787 market=sim` 后，在同一仓库根目录打开第二个终端。

```sh
bun run harness run examples/collector.ts --name hourly --observe
bun run harness run examples/strategy.ts --name swing --account paper
bun run harness status
```

采集脚本在每根小时 K 线收盘时用 `fetch` 取得未平仓量，追加到 `btc-hourly`。策略脚本预热 1,440 根小时 K 线，下一个实时收盘时提交限价单。默认一个模拟小时约用 6 秒。等待一个模拟小时后查看结果。

```sh
bun run harness logs hourly
bun run harness logs swing
bun -e 'import { data, trade } from "@quant/tools"; console.log(await data.read("btc-hourly", 0, 3), await trade.openOrders())'
```

在新的数据目录中，采集日志会出现 `sample` 和一个递增游标，策略日志会出现 `warmup 1440` 和 `limit`。订单中的 `clientId` 可用于查询状态和成交。示例中的限价低于模拟市场价格，便于在换版后核对同一笔挂单。[采集源码](examples/collector.ts) · [策略源码](examples/strategy.ts)

```sh
bun run harness stop swing
bun run harness stop hourly
```

停止脚本会收回其交易权限。已有挂单仍由宿主跟踪，撤单是单独的交易操作。两个脚本停止后，用第一个终端的 Ctrl+C 关闭宿主。数据保留在 `.harness/`，后续演练可用新的 `HARNESS_DIR` 建立空实例。

## 文档

| 要做的事 | 从这里开始 |
|---|---|
| 编写脚本、保存消费游标、观察新版并交接 | [脚本开发与 CLI](docs/scripts-and-cli.md) |
| 查函数签名、HTTP 参数、返回字段或流格式 | [SDK 与本机 HTTP API](docs/api.md) |
| 设置目录和端口，用公共行情驱动模拟账户 | [实例与配置](docs/configuration.md) |
| 下单、撤单、平仓，确认超时指令的结果 | [交易与对账](docs/trading.md) |
| 对接通知机器人和多个 agent 后端 | [连接器协议与联调](docs/connectors.md) |
| 查健康和日志，排错、备份、恢复实例 | [运维手册](docs/operations.md) |
| 查 SQLite 表、复盘报价和成交、回放盘口 | [存储与数据口径](docs/storage.md) |
| 修改宿主或 SDK，运行对应验收场景 | [开发与测试](CONTRIBUTING.md) |
| 查进程边界、模块位置、原始验收要求 | [架构与运行](docs/architecture.md) · [架构图](docs/architecture.svg) · [首版执行计划](docs/execution-plan.md) |

模拟撮合使用买卖一档可见数量，未计手续费、保证金、清算和排队优先级。币安账户写操作通过本机替身验收。脚本与宿主共享本机用户的系统权限，部署面向可信脚本。
