# 运维手册

`harnessd` 的生命周期独立于脚本。默认实例可用 `bun run harnessd` 在前台启动，用 Ctrl+C 或 SIGTERM 正常关闭。关闭时宿主停止子进程、完成在途处理并关闭 SQLite。日常管理从同一 checkout 运行 CLI，指定与宿主对应的 `HARNESS_DIR` 和端口。[实例配置](configuration.md)

## Linux 上交给 systemd 管理

使用 systemd 用户服务时，可以把下面的模拟实例配置保存为 `~/.config/systemd/user/quant-harness.service`。示例假设 checkout 位于 `~/projects/quant-harness`、依赖已经安装，Bun 位于 `~/.bun/bin/bun`。按实际路径调整 `WorkingDirectory` 和 `ExecStart`。直接运行宿主入口使服务管理器跟踪实际 Bun 进程。停止时先向宿主发送 SIGTERM，由宿主收回租约并关闭子进程。

```ini
[Unit]
Description=Quant Harness

[Service]
Type=simple
WorkingDirectory=%h/projects/quant-harness
ExecStart=%h/.bun/bin/bun src/main.ts
Environment=HARNESS_DIR=%h/.local/state/quant-harness
Environment=HARNESS_PORT=8792 HARNESS_MARKET=sim HARNESS_TRADING=paper
KillMode=mixed
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
```

```sh
systemctl --user daemon-reload
systemctl --user start quant-harness.service
journalctl --user -u quant-harness.service -n 20
HARNESS_DIR="$HOME/.local/state/quant-harness" HARNESS_URL=http://127.0.0.1:8792 bun run harness status
```

`systemctl --user stop quant-harness.service` 可正常停止宿主。需要随用户服务管理器启动时使用 `systemctl --user enable quant-harness.service`。自动重启作用于宿主，托管脚本的恢复方式见下文。

## 先看哪一项状态

```sh
bun run harness status
curl -fsS http://127.0.0.1:8787/health
bun run harness logs swing
```

`status` 即时读取状态，`/health` 返回独立定时器最近一次生成的快照。按名称查到的日志默认属于执行者，核对失败候选时用其运行 ID。

| 字段 | 如何判断 |
|---|---|
| `checkedAt,marketTime,market` | 前两项分别是巡检墙钟和行情时钟。模拟实例中行情时钟走得更快，计算预热窗口使用 `marketTime`。 |
| `ticker.connections,lastEvent,lastPong` | 公共 WS 分为 `public`、`market` 两组。连接状态、最后有效消息和 pong 可分别定位传输和行情停滞。模拟源无需 WS pong。 |
| `ticker.quotes` | 固定关注或有敞口合约的报价。分开看 `bookStale/markStale` 和每个分量的两个时间戳。断开或超过 15 秒会使相应分量过期。 |
| `ticker.streams,listeners` | 前者是上游需求，顶层 `listeners` 统计本机订阅者。同一条行情有多个脚本使用时只需一个上游订阅。 |
| `ticker.pendingBarRepairs,gaps` | 前者是持续行情待补区间数。顶层 `gaps` 列出最近 15 条缺口及原因，完整记录在运行库中。 |
| `trade.privateStream,privateLastPong,reconciliationError` | 实盘适配器的连接、心跳和最近对账错误。纯模拟账户显示 `privateStream: "disabled"`。 |
| `trade.unresolved,outstandingFillRepairs` | 未决指令 ID、客户订单号和状态，以及仍待补足成交的客户订单号。 |
| `runs` | 最近 100 次运行。结合 `id,mode,ready,ended,exitCode` 和同一 ID 的日志排查。 |

正常自行结束的脚本记为 `exited`，非零退出记为 `failed`，显式停止记为 `stopped`。宿主异常结束后，下一次启动把遗留的 `running` 标为 `interrupted` 并清除旧租约。持久化的行情、指令、订单和成交保留，脚本由 CLI 重新启动。需要继续控制原仓位时使用原账户、原策略名。

## 从现象定位原因

| 现象 | 核对与处置 |
|---|---|
| CLI 连不上或读不到 `operator.token` | 看宿主启动日志的 URL、数据目录，确认服务还在。CLI 在其工作目录解析相对路径。使用绝对 `HARNESS_DIR` 和正确 `HARNESS_URL` 可避免跨终端连错实例。 |
| 启动报告 `Use a separate data directory` 或数据库被占用 | 核对 `settings.market` 与 `HARNESS_MARKET`，为不同市场源使用各自目录。复用同一运行库前，先正常停止持有它的宿主。 |
| 观察版没有下单或出现 403 | 用运行 ID 查 `mode,ready,state`。写指令要求该名字的当前执行租约。用 `replace` 交接已验证的候选，旧版迟到请求被拒绝属于交接的预期结果。 |
| `Name already has an executor`、候选启动失败或超时 | 查旧版和候选各自的日志。候选用 `--observe` 启动，核对源码哈希、账户和预热。顶层长期轮询的候选可按 ID 明确交接。[换版步骤](scripts-and-cli.md) |
| `Fresh execution quote unavailable` 或报价字段为空 | 检查上述连接、`bookStale` 和 `book.eventTime`。确认合约规格和行情已到，重新评估对应信号。 |
| 历史不满或行情断线 | 对齐周期边界，核对实际已收盘的时段。用 `market.bars` 对指定时间范围补缺。`bar_repairs` 记录持续流的待补任务。日归档 404 时查看报错中的合约、日期和文件名。 |
| 消费者断流 | 检查日志中的流错误及处理耗时。数据集消费者从最后成功处理的 `cursor` 续读，行情消费者重新订阅并用预热窗口恢复状态。 |
| `Idempotency key has different parameters` 或 `Contract belongs to …` | 查原指令的 `params`（含决策报价）及原账户的仓位、挂单和未决指令，确认重试输入、策略名和所有权。[交易与对账](trading.md) |
| 指令 `unknown` 或成交记录待补 | 按原指令 ID、`clientId` 查询。确认私有流和 REST 对账状态。宿主一般每 15 秒对账，重连也会触发。一次未查到订单仍可能保持未知。 |
| webhook 或 agent 返回 502 | 用本地记录 ID 查 `notifications.detail` 或 `tasks.result`。核对路由、目标 URL、模板、宿主环境中的令牌变量及接收端响应。 |

## 停机备份与恢复演练

运行库启用 WAL。完整备份包含数据目录内的数据库及其附属文件、快照、日志、归档和操作令牌。外置的连接器配置、脚本依赖、宿主环境和部署凭证需从相应部署记录恢复。下例针对首页的模拟实例，在宿主正常退出后执行。选取数据目录之外的私有备份位置。[SQLite 的 WAL 文件说明](https://sqlite.org/wal.html)

```sh
instance_dir="${HARNESS_DIR:-$PWD/.harness}"
backup_root="${XDG_STATE_HOME:-$HOME/.local/state}/quant-harness/backups"
mkdir -p "$backup_root"
backup_dir=$(mktemp -d "$backup_root/instance-XXXXXXXX")
(umask 077 && tar -C "$instance_dir" -czf "$backup_dir/instance.tar.gz" .)
```

在同一个终端解包到新的空目录，先只读检查数据库完整性和保存的运行数量。

```sh
restore_dir=$(mktemp -d "${TMPDIR:-/tmp}/quant-restore-XXXXXXXX")
tar -xzf "$backup_dir/instance.tar.gz" -C "$restore_dir"
HARNESS_DIR="$restore_dir" bun -e 'import {Database} from "bun:sqlite"; import {join} from "node:path"; const db=new Database(join(process.env.HARNESS_DIR,"harness.sqlite"),{readonly:true}); console.log(db.query("PRAGMA quick_check").get(), db.query("SELECT count(*) AS runs FROM runs").get()); db.close()'
```

`quick_check` 应返回 `ok`。启动该模拟副本，健康输出中的 `market` 应为 `sim`，`runs` 可查备份时的运行记录。用 `trade.openOrders()` 核对原挂单。模拟行情恢复后，可成交的挂单可能继续成交。

```sh
HARNESS_DIR="$restore_dir" HARNESS_PORT=8791 HARNESS_MARKET=sim HARNESS_TRADING=paper bun run harnessd
```

另一个终端可执行以下只读查询。

```sh
HARNESS_URL=http://127.0.0.1:8791 bun -e 'import {trade} from "@quant/tools"; console.log(await trade.openOrders())'
```

恢复币安实例时应沿用原市场模式，并只让一个执行实例负责原账户。备份反映的是备份时刻的本地状态。恢复后还需核对交易所的实际持仓、挂单和期间成交，再启动策略。离线订单、成交和运行记录的查法见[存储手册](storage.md)。
