# 存储与数据口径

每个宿主的数据目录由 `HARNESS_DIR` 决定。在线查询使用 SDK、HTTP 或 CLI。下面的 SQL 用于停机后的运行库或其离线副本。宿主使用 SQLite WAL、`synchronous=FULL` 和独占锁。[建表语句](../src/db.ts) · [附加账户表](../src/trade/ledger.ts) · [备份和恢复](operations.md)

## 时间、精度与历史覆盖

- K 线按 UTC 的 `openTime` 归档，查询使用左闭右开区间。未收盘 K 线存在内存中，`bars` 只保存已收盘记录。以合约、周期、开盘时间识别同一根线。表中记录可来自 REST 或 WS，`history/backfill/live` 是**交付事件**上的标志。
- 市场快照每个合约每分钟最多一条。快照的 `minute` 使用市场时钟，book 和 mark 分别保存 `eventTime` 与宿主墙钟 `receivedAt` 及各自的过期标志。模拟时钟会加速，脚本日志、数据集追加和运行开始时间仍使用墙钟。
- 成交量、价格和仓位等十进制值以字符串保存，币安的大序号也保留为字符串。计算金额或数量使用十进制运算。盘口序号需要比较大小时可转换为 `BigInt`。
- 当前适配器以**昨日 UTC 零时**为聚合成交的 REST 与日归档分界。历史 OI 和全市场账户多空比以**今日 UTC 零时往前 28 天**为分界，较早数据读官方 `metrics` 日归档。资金费率历史使用实际结算记录。接口返回的 `Metric.source` 标明来源，缺少日文件会报错。[币安行情适配器](../src/market/binance.ts)
- 强平流记录交易所公开的强平快照，统计次数应标注为公开快照数。逐档深度从脚本订阅并成功衔接快照后开始记录。币安的百分比区间深度归档无法恢复每一条逐档更新。[采集口径](execution-plan.md)

## 运行库内有什么

| 表 | 查找键和用途 |
|---|---|
| `settings` | `key='market'` 记录该目录使用的行情模式。 |
| `bars` | 主键 `symbol,interval,time`，`time` 为开盘时间，`body` 是整根 K 线的 JSON。 |
| `stream_closes`、`bar_repairs` | 每条行情流最近记录的收盘位置，以及待补的 `symbol,interval,start,end` 和下次可重试时间。 |
| `snapshots`、`liquidations` | 前者按 `symbol,minute` 保存当时的 `Quote`。后者逐条保存 `symbol,time,body`，保留交易所原始公开强平事件。 |
| `depth_frames`、`gaps` | 前者按 `id` 记录建簿快照和增量。后者记录 `stream,time,reason`，包括断线、序号断裂和缺口。 |
| `samples` | 自增 `cursor`，按 `name,cursor` 建索引。`record` 是业务 JSON，`time` 为追加时间。 |
| `runs`、`leases`、`owners` | `runs.id` 定位入口源码、哈希、状态和时间。`leases.name → runId` 指向有执行权的运行。`owners.account,symbol → strategy` 保存合约归属。 |
| `instructions` | 按 `id` 查指令，唯一约束 `account,strategy,key`。保存 `params,decision,action,state,detail,runId,clientId` 等复盘字段。 |
| `orders`、`fills`、`fill_sync` | 订单按 `account,clientId` 查。成交按 `account,symbol,tradeId` 去重，可由 `account,clientId` 反查。`fill_sync` 记录待补足币安成交的客户订单号。 |
| `positions`、`leverage` | 仓位按 `account,symbol,positionSide` 保存。`leverage` 保存模拟账户的合约杠杆设置。 |
| `notifications`、`tasks` | 投递记录含 `id,type,content,state,detail,time`。任务记录含 `id,backend,sessionId,state,result,time`。 |

运行目录 `runs/<id>/` 包含 `entry.ts`、`stdout.log` 和 `stderr.log`。本地归档按 `archive/<dataset>/<symbol>/` 缓存日 ZIP。操作令牌存放在 `operator.token`。完整实例备份会包含这些私有文件。

## 离线复盘

将一条查询保存为 `query.sql`，在仓库根目录读取停机副本。用下文示例依次替换文件内容即可，每次执行一条查询。

```sh
HARNESS_DIR="$restore_dir" bun -e 'import {Database} from "bun:sqlite"; import {join} from "node:path"; const db=new Database(join(process.env.HARNESS_DIR,"harness.sqlite"),{readonly:true}); console.table(db.query(await Bun.stdin.text()).all()); db.close()' < query.sql
```

其中 `restore_dir` 是运维手册中解包后的目录，也可替换为其他离线实例的绝对路径。下面的查询按指令找出策略当时看到的买价、报价来源时间及订单的最终进度。撤单指令与对应下单指令可关联到同一笔订单。

```sql
SELECT i.strategy, i.runId, i.key, i.action, i.state AS instruction_state,
       i.clientId, json_extract(i.decision, '$.book.bid') AS decision_bid,
       json_extract(i.decision, '$.book.eventTime') AS quote_time,
       o.status AS order_status, json_extract(o.body, '$.executed') AS executed
FROM instructions AS i
LEFT JOIN orders AS o ON o.account = i.account AND o.clientId = i.clientId
ORDER BY i.created DESC, i.rowid DESC
LIMIT 20;
```

检查尚待补齐的 K 线区间。运行时可先从健康快照的 `ticker.pendingBarRepairs` 看到待办数量。

```sql
SELECT symbol, interval, start, end, retryAt
FROM bar_repairs
ORDER BY retryAt, symbol, start;
```

下面从 BTCUSDT 最近一个已存快照取最多 100 帧。第一帧建立价量表，增量给出价位上的绝对数量，零表示删除。初始增量覆盖快照序号，后续增量核对 `previous` 与上一帧 `last`。遇到新快照就重新建簿。按入库的 `id` 顺序回放，可避免混用快照采集时间和交易所事件时间。

```sql
WITH anchor AS (
  SELECT MAX(id) AS id FROM depth_frames
  WHERE symbol = 'BTCUSDT' AND json_extract(body, '$.kind') = 'snapshot'
)
SELECT f.id, f.time, f.body
FROM depth_frames AS f JOIN anchor ON f.id >= anchor.id
WHERE f.symbol = 'BTCUSDT'
ORDER BY f.id LIMIT 100;
```

没有开过深度订阅的实例会返回空结果。运行库、日志及 ZIP 的保留期由部署方管理。删除执行状态会影响幂等性、挂单对账及运行追溯。
