# Quant Harness 架构与运行

`harnessd` 是单机上唯一需要常驻的 Bun 服务。它接收行情、维护账户、写 SQLite，也负责启动和停止脚本。采集和策略都是普通 TypeScript 入口，依赖由脚本所在项目解析。换版时，行情连接和已有订单留在宿主。设计基线见[执行文档](execution-plan.md)。

![脚本进程、宿主和外部服务的连接关系](architecture.svg)

首次运行和按任务查文档可从[项目首页](../README.md)开始。开发脚本时可查 [SDK 与 HTTP 接口](api.md)，追查异常可查[运维手册](operations.md)和[存储与数据口径](storage.md)。

## 跑通两个脚本

在仓库根目录使用 Bun 1.4.4。第一个终端启动默认的模拟行情和模拟账户。

```sh
bun install --frozen-lockfile
bun run typecheck
bun test
bun run harnessd
```

第二个终端从同一目录执行下面的命令。默认地址是 `http://127.0.0.1:8787`。模拟时钟每 100 ms 推进一分钟，一个模拟小时约需 6 秒。

```sh
bun run harness run examples/collector.ts --name hourly --observe
bun run harness run examples/strategy.ts --name swing
bun run harness status
bun run harness logs hourly
bun run harness logs swing
bun -e 'import {data,trade} from "@quant/tools"; console.log(await data.read("btc-hourly"), await trade.openOrders())'
```

采集脚本在 BTCUSDT 小时收盘时自行 `fetch` 额外指标，默认读取模拟未平仓量。首条样本即创建 `btc-hourly`。启动宿主前设置 `SCRIPT_INDICATOR_URL` 可以换成返回 JSON 的外部数据源。策略先查仓位和挂单，再通过同一条收盘流预热 60 天。日志出现 `warmup 1440` 后，下一次实时收盘会按均价的九折向下对齐价格步长，在模拟账户挂一笔买入限价单。两个示例的完整源码见[采集脚本](../examples/collector.ts)和[策略脚本](../examples/strategy.ts)。

在本地复制并修改策略，观察新版，核对日志里的已有挂单，再交接执行权。

```sh
cp examples/strategy.ts examples/strategy-v2.ts
# 修改并保存 examples/strategy-v2.ts
observer=$(bun run harness run examples/strategy-v2.ts --name swing --observe | bun -e 'console.log((await Bun.stdin.json()).id)')
bun run harness logs "$observer"
bun run harness replace swing examples/strategy-v2.ts
bun run harness status
bun run harness stop swing
bun run harness stop hourly
```

`run` 返回本次运行的 `id`。按名称查日志和停止时优先选当前执行者，观察进程可按 ID 操作。停止策略会收回交易权限，原挂单仍可通过 `trade.openOrders()` 查询。普通研究脚本可直接 `bun run FILE.ts` 连接已启动的宿主。

## 代码入口

| 模块 | 实现与职责 |
|---|---|
| 宿主和 CLI | [main.ts](../src/main.ts) 启停服务，[server.ts](../src/server.ts) 绑定回环地址、分派 HTTP 请求并启动独立巡检定时器。[cli.ts](../src/cli.ts) 读取本机操作令牌调用管理接口。 |
| SDK 与订阅 | [index.ts](../packages/tools/index.ts) 是工作区包 `@quant/tools` 的入口。[types.ts](../packages/tools/types.ts) 定义参数及返回值。[transport.ts](../packages/tools/transport.ts) 负责 HTTP 和增量解码，[streams.ts](../src/streams.ts) 管理宿主监听和交付缓冲。 |
| 主 ticker | [ticker.ts](../src/market/ticker.ts) 合并订阅、收包、采样和登记缺口。[socket.ts](../src/market/socket.ts) 管理两组公共 WS，[binance.ts](../src/market/binance.ts) 对接公共 REST 和日归档，[sim.ts](../src/market/sim.ts) 提供同一接口的确定性行情。 |
| 历史和盘口 | [history.ts](../src/market/history.ts) 按时间键补 K 线，[depth.ts](../src/market/depth.ts) 衔接快照与增量，[archive-worker.ts](../src/market/archive-worker.ts) 在临时 Worker 中分块解压读取 CSV。 |
| 账户与交易 | [engine.ts](../src/trade/engine.ts) 校验精度、幂等键和执行权，[ledger.ts](../src/trade/ledger.ts) 维护订单与成交。[paper.ts](../src/trade/paper.ts) 撮合模拟账户，[交易适配器](../src/trade/binance.ts) 签名、接收私有回报并对账。 |
| 脚本和外部服务 | [manager.ts](../src/runs/manager.ts) 调用 `Bun.spawn`，管理运行、日志和交接。[preload.ts](../src/runs/preload.ts) 在原入口路径加载快照。[connectors.ts](../src/connectors.ts) 投递 webhook 并路由 agent 请求。 |

## 一根收盘 K 线的路径

1. 固定关注列表、持仓和挂单决定基础合约集合。每个合约常驻一档报价、标记价格、分钟 K 线和公开强平流。小时线等原生周期、聚合成交和多档盘口按监听者引用计数订阅。同一流在同一宿主只有一份上游订阅。
2. 买卖一档和差量深度走 `/public/stream`，K 线、标记价、强平和聚合成交走 `/market/stream`。ticker 在内存更新当前 K 线，仅在 `k.x === true` 时处理收盘。`bars` 的主键是合约、周期和开盘时间，`stream_closes` 保存最近的流收盘位置。
3. 脚本发起 `GET /stream/bars?symbol=BTCUSDT&interval=1h&warmup=1440`。宿主先挂监听并缓冲新事件，等上游订阅生效，再确定最后一根已收盘 K 线的边界。宿主按顺序发送 `source: "history"` 的预热记录，随后交付边界以后的实时收盘。预热请求漏掉的迟到收盘按 K 线键补一次。断线重订阅或跳过分钟时，REST 回补标为 `backfill`。策略可按 K 线键更新状态，仅在 `live` 上生成新指令。
4. 宿主通过同一 HTTP 响应发送 NDJSON。数据帧携带 `data`，初始回放结束发送 `ready`，错误帧携带 `error`。服务端对流关闭请求空闲超时，每 15 秒发送空行。SDK 处理分块 UTF-8 和跨块 JSON，支持 `break` 和 `AbortSignal`。每个监听最多积压 256 个实时事件，溢出会断开该客户端。

#### 历史、快照与数据游标

时间范围使用 UTC 毫秒和左闭右开区间。K 线按开盘时间筛选，返回已收盘的实际记录。固定时长从 1 分钟到 1 天，完整周期列表在 SDK 的 `periods` 中。一次查询最多一年、50,000 根。下面的命令按宿主时钟补最近 60 天的小时线，也适用于加速的模拟行情。

```sh
bun -e 'import {market} from "@quant/tools"; const {marketTime}=await (await fetch("http://127.0.0.1:8787/health")).json(); const h=3600000,end=Math.floor(marketTime/h)*h; console.log((await market.bars("BTCUSDT","1h",{start:end-1440*h,end})).length)'
```

`GET /market/bars` 先查 SQLite，只请求缺少的连续区间，每页最多 1,000 根。相同的在途分页请求共用一次下载。流回补中失败或仍然缺失的区间保存在 `bar_repairs`，宿主定时重试，重启可接续。延后补齐的记录带原始开盘时间。

| 数据 | 读取与保存口径 |
|---|---|
| 报价、标记价 | `market.quote/watchQuotes` 返回各自的事件时间、接收时间和过期标志。最新一档含价格与数量，标记数据含指数价、可见资金费率和下次结算时间。每分钟合写一条 `snapshots`。连接断开或数据超过 15 秒即标记相应分量过期。 |
| 强平 | `market.watchLiquidations` 推送公开强平快照，收到即写 `liquidations`。交易所每个合约每秒至多公开该窗口最后一笔，相关统计按公开快照计数。 |
| 成交、资金与 OI | `market.trades/watchTrades` 使用聚合成交口径，查询最多 1,000 条。近期成交按小于一小时的 REST 窗口查询，较早部分读 UTC 日 `aggTrades`。实际结算费率用 `funding`，当前或历史 OI 用 `openInterest`，全市场账户多空比用 `longShortRatio`。近期历史指标取 5 分钟记录，更早读官方 `metrics` 日文件。归档缺失会报告文件名和 HTTP 状态。 |
| 合约与摘要 | `market.stats` 按需取 24 小时统计。合约规格在启动时和每小时刷新，也可用 `bun run harness refresh` 立即刷新，再用 `market.spec` 查询。 |
| 多档盘口 | `market.depth` 直接取快照。`watchDepth` 开录 `@depth@100ms`，先暂存增量，再找覆盖快照序号的首帧，丢弃旧帧。后续帧要求 `pu` 等于上一个 `u`。数量是该价位的绝对量，零表示删除。序号断裂时写 `gaps` 并重新衔接。回放按 `depth_frames.id` 顺序读取，从每个 `snapshot` 重新建簿。 |
| 命名数据集 | `data.append(name, record)` 写 `samples` 并返回稳定递增的 `cursor`。`data.read(name, after, limit)` 和 `data.watch(name, after)` 都从游标之后开始。watch 先订阅，再回放至固定游标，随后衔接新追加。脚本可持久化最后处理成功的游标，断开后恢复。 |

公共行情协议见币安的[市场流](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)、[盘口衔接规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)、[REST 行情](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)和[历史归档](https://github.com/binance/binance-public-data#futures)。

## 一笔订单的路径

1. 托管脚本把策略决策使用的 `quote` 随 `trade.place({key, symbol, side, type, quantity, price?, quote})` 一起提交给 `POST /trade/place`。价格、数量使用十进制字符串。宿主从运行令牌取得策略、运行 ID 和账户，检查 `leases` 中当前执行者及 `owners` 中的合约所有者。同一账户和合约的命令串行处理。可转交给另一策略的条件是仓位已平、挂单已清、旧指令已有明确结果。
2. 宿主按合约步长、最小名义金额和数量范围校验。以账户、策略、业务键建立唯一约束，在 SQLite 事务中保存 `instructions`、所用报价、来源时间和稳定的客户订单号。相同键和参数返回原指令。参数冲突返回 HTTP 409。交易所预查存在等待时，宿主在持久化和发送前再次核对执行权。
3. 模拟账户在当时可用的买一或卖一撮合，限价单可留在簿上，单次消耗受该档数量限制。币安账户向 `/fapi/v1/order` 发送带时间戳的 HMAC-SHA256 签名请求。超时等不确定结果保留为 `unknown`，通过原客户订单号、私有 WS 和 REST 对账确认。宿主不会自动重发不确定的下单请求。
4. `trade.instruction(id)` 返回指令、决策报价、参数和结果。`trade.order(clientId)` 返回交易所订单号及状态，包括 `NEW`、`PARTIALLY_FILLED`、`FILLED`、`CANCELED`、`REJECTED`、`EXPIRED` 和 `UNKNOWN`。`trade.fills(clientId)` 查询成交，唯一键为账户、合约、成交 ID。订单、成交和仓位分别保存在 `orders`、`fills`、`positions`。
5. `trade.cancel({key,symbol,clientId})` 显式撤单。`trade.close({key,symbol,positionSide?})` 读取实际仓位与持仓模式。单向模式按实际数量反向下市价单并发送 `reduceOnly=true`，双向模式要求指定 `LONG` 或 `SHORT`，发送对应的 `positionSide`。`trade.setLeverage({key,symbol,leverage})` 作用于本次运行的账户和合约。

币安适配器通过 listen key 接入 `/private/ws/<listenKey>` 并续期。启动、私有流重连和定时巡检都会读取仓位、挂单、待确认订单及缺失成交。重启时原先处于 `pending` 的指令转为 `unknown`。可在 `harness status` 的 `trade.unresolved`、`outstandingFillRepairs` 和 `reconciliationError` 查看未决事项。交易协议见[币安合约交易接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)和[执行结果未知的说明](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)。

## 运行交接、健康与本地文件

每次 `run` 保存源文件绝对路径、入口内容和 SHA-256、名称、账户、观察或执行模式、开始与结束时间、退出码。子进程在原入口目录运行，Bun 的相对导入、项目依赖和 `import.meta.main` 照常工作。快照覆盖入口源码，依赖版本由项目和锁文件管理。标准输出和错误分别写文件，CLI 保留两个输出通道。

`replace` 复用文件路径、源码哈希、名称和账户都匹配的观察进程，或启动新候选。普通脚本执行完顶层初始化，或 SDK 消费完订阅的预热阶段，即可报告就绪。候选启动失败或等待超时，旧版继续运行。对永远停留在顶层 `await` 的自定义轮询脚本，可以先以 `--observe` 运行，检查日志后用 `replace NAME FILE --from RUN_ID` 明确选中已观察的进程。交接在同一事务里更新租约和运行模式，随后停止旧进程。已送达交易所的旧单继续对账。

`GET /health` 返回独立定时器每秒生成的状态，CLI 的 `status` 读取最新状态。可查看 WS 连接和最近 pong、最后有效行情、分量过期标志、订阅数、缺口和最近 100 次运行。HTTP 流的空行心跳与交易所 WS 的 ping/pong 各自独立。

默认数据目录是 `.harness/`，可用 `HARNESS_DIR` 更改。目录内有 `harness.sqlite`、WAL 文件、权限为 0600 的 `operator.token`、`runs/<id>/` 下的入口快照和日志，以及 `archive/` 下的行情 ZIP。宿主独占运行库，运行时通过 HTTP 查询。进程异常终止后，下一次启动将遗留的运行标为 `interrupted`，收回旧租约，恢复账户跟踪。需要继续运行的脚本由 CLI 启动。表结构和索引见 [db.ts](../src/db.ts)。

## 部署配置

| 设置 | 用途 |
|---|---|
| `HARNESS_MARKET`、`HARNESS_SYMBOLS` | 默认 `sim`、`BTCUSDT`。设为 `binance` 可读取公共行情，多个基础合约用逗号分隔。模拟与公共市场使用不同数据目录。 |
| `HARNESS_TRADING` | 默认 `paper`。启用币安执行需要 `binance` 行情、`HARNESS_TRADING=binance` 和宿主环境中的 `BINANCE_API_KEY`、`BINANCE_API_SECRET`。运行具体脚本时另选 `--account binance`。每个数据目录对应一个币安账户。两个示例用于模拟验收，策略主动检查账户。 |
| `BINANCE_REST`、`BINANCE_WS`、`BINANCE_ARCHIVE` | 分别覆盖 REST 根地址、WS 根地址和 USDⓈ-M daily 归档目录。本机替身通过这些入口接入。 |
| `HARNESS_PORT`、`HARNESS_URL` | 默认端口 8787，宿主绑定 127.0.0.1。CLI 和 SDK 可通过 `HARNESS_URL` 选择实例，CLI 同时需要该实例的 `HARNESS_DIR`。 |
| `HARNESS_SIM_TICK_MS`、`HARNESS_READY_MS` | 调整每个模拟分钟的墙钟时长和候选启动等待时间，默认分别为 100 ms、30 秒。 |
| `HARNESS_CONNECTORS`、`SCRIPT_*` | 前者指定宿主读取的 JSON 配置文件。后者显式传给子进程，例如采集脚本的指标地址。配置字段见 [config.ts](../src/config.ts)。 |

只读公共行情加模拟交易可从另一个数据目录启动。

```sh
HARNESS_MARKET=binance HARNESS_DIR=.harness/public HARNESS_PORT=8788 bun run harnessd
```

#### 通知与 agent 后端

启动宿主时指定 `HARNESS_CONNECTORS=config/connectors.example.json`。将示例中的 URL 换成接收服务地址，按通知类型选择机器人和目标。模板中的 `{{字段}}` 取自结构化内容。机器人或 agent 后端均可配置 `tokenEnv`，由宿主从指定环境变量读取 Bearer 令牌。[配置示例](../config/connectors.example.json)

- `notify.send("fill", {symbol:"BTCUSDT", quantity:"0.01"})` 使宿主向机器人 URL 发送 JSON，字段为 `type`、`target`、`text`、`content`。HTTP 2xx 记为已送达。失败原因和投递 ID 写入 `notifications`。
- `agents.startTask("research", prompt)` 向该后端的 `POST /tasks` 发送 `{taskId,prompt}`，约定响应 `{sessionId,state,result?}`。`tasks` 保存本地任务 ID、后端名和远端会话 ID。
- `agents.sendMessage(id,message)` 调用 `POST /tasks/<sessionId>/messages`，正文是 `{message}`。`agents.getStatus(id)` 调用同一会话的 `GET /tasks/<sessionId>`。两者均读取 `{state,result?}`。可用 `bun test tests/connectors.test.ts` 运行完整的双后端和 webhook 本机收发示例。

## 验证记录与使用边界

2026-11-18 在 Linux x86_64、Bun 1.4.4 上从干净目录按锁文件安装。类型检查通过，6 个测试文件的 16 项测试全部通过。另从 CLI 启动宿主和两个示例，确认换版后数据游标继续增长，停止策略后原限价单仍为 `NEW`。自动验收只连接本机，测试目录、订单、签名密钥和回报均为临时数据。

| 验收入口 | 实际核对内容 |
|---|---|
| [传输与数据集](../tests/streams.test.ts) | 两个独立脚本消费同一事件，退出一个后另一个继续消费。另测游标恢复、显式取消、慢消费者断开和跨网络块的中文及 emoji 解码。 |
| [行情回放](../tests/market.test.ts) | 60 天预热和预热期间到达的下一根实时线、命中缓存与单独补洞、重复收盘、重订阅、分钟快照、公开强平、盘口断序重建，以及 REST 和日归档的历史口径。 |
| [模拟账户](../tests/paper.test.ts)与[本机币安替身](../tests/binance.test.ts) | 十进制步长、部分成交、撤单、平仓、杠杆、签名、请求已接受后超时、重复回报、拒单、重启对账、双向持仓和交接期间的迟到指令。 |
| [CLI 工作流](../tests/workflow.test.ts) | 运行两个示例，查日志，观察新版已有挂单，候选失败保留旧版，成功换版后原挂单可查，采集仍继续。 |
| [外部调用](../tests/connectors.test.ts) | 接收目标与模板、成功和失败的投递记录、两个 agent 后端的完整请求响应、宿主重启后继续同一远端会话。 |

另行只读抽查了币安 BTCUSDT 的 public 和 market WS、一档报价及标记价格、合约规格、连续 1,440 根已收盘小时线、多档盘口的首个快照与后续增量，以及官方 2025-01-08 OI 日归档中首小时的 12 条记录。币安账户写操作和私有回报仅在本机替身验证。

模拟账户按一档可见量撮合，支持市价和 GTC 限价、单向净仓。模拟杠杆保存设置。手续费、保证金、清算和排队优先级不在模拟模型内。实盘可成交价格、账户权限、限流及交易所最终校验仍取决于实际账户与网络。历史深度从开始监听时开录，历史日文件受官方覆盖范围限制。SDK 客户端断流后由脚本按游标或预热窗口续读。脚本和宿主共享本机用户的操作系统权限，进程分离适合可信脚本。运行库、归档和日志的保留期由部署方管理。
