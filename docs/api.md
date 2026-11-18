# SDK 与本机 HTTP API

`@quant/tools` 是仓库的 Bun 工作区包。脚本从中导入 `market`、`trade`、`data`、`notify`、`agents` 和 `currentRun`。普通方法返回 Promise，`watch*` 和 `data.watch` 返回异步迭代器。实现与完整 TypeScript 字段见 [SDK 入口](../packages/tools/index.ts)、[公共类型](../packages/tools/types.ts)和[服务端路由](../src/server.ts)。

## 连接和调用权限

默认连接 `HARNESS_URL`，未设置时使用 `http://127.0.0.1:${HARNESS_PORT}`，默认端口为 8787。托管脚本自动获得实例 URL 和运行令牌。直接运行的研究脚本也可以显式选择实例。

```ts
import { connect } from "@quant/tools";

const client = connect("http://127.0.0.1:8787");
const quote = await client.market.quote("BTCUSDT");
console.log(quote.symbol, quote.book?.bid, quote.bookStale);
```

| 入口 | 所需身份 |
|---|---|
| 行情、数据集、健康、账户查询、通知和 agent | 可由受信任的本机客户端调用。`trade.positions/openOrders/order/fills` 可显式指定账户。省略时使用托管运行的账户，普通研究进程默认查询 `paper`。查询 `binance` 账户要求实例启用该执行适配器。 |
| `GET /run` | 当前托管运行令牌。 |
| 四个交易 POST | 有效运行令牌、运行中且持有该策略的执行租约。账户取自运行记录。 |
| `/admin/*` | 本实例操作令牌。CLI 从 `HARNESS_DIR/operator.token` 读取。 |

HTTP 凭证放在 `Authorization: Bearer <token>`。GET 参数使用查询字符串，POST 发送 JSON 对象，单个请求体上限 1 MiB。普通成功响应直接返回表中所列的对象或数组。回环地址上的数据写入和连接器调用也有副作用，部署的信任边界见[实例配置](configuration.md)。

需要直接处理传输时，子路径 `@quant/tools/transport` 还导出 `endpoint(base,path,params?)`、`check(response)`、`decodeLines<T>(body)` 和 `transport(base,token?)`。最后一项提供 `get<T>`、`post<T>` 和 `stream<T>`，供自定义客户端调用同一套端点。

## 市场数据

合约代码使用 2–30 个大写字母、数字或下划线，例如 `BTCUSDT`。时间为 UTC Unix 毫秒。SDK 的 `range` 是 `{start, end}`，HTTP 中展开为两个参数。`[start,end)` 要求 `0 ≤ start < end`，单次显式查询跨度最多 366 天。

| SDK 调用 | HTTP | 参数和结果 |
|---|---|---|
| `market.quote(symbol)` | `GET /market/quote` | `symbol` → `Quote`。启动初期一档报价和标记价可能先后到达，检查 `book`、`mark` 和各自的过期标志。 |
| `market.bars(symbol, interval, range)` | `GET /market/bars` | `symbol, interval, start, end` → `Bar[]`。按开盘时间升序，只返回已收盘记录，最多 50,000 根。 |
| `market.trades(symbol, range?, limit?)` | `GET /market/trades` | 可选 `start, end` 和 `limit`。默认 500，范围 1–1,000，返回 `AggregateTrade[]`。省略时间取近期数据。 |
| `market.depth(symbol)` | `GET /market/depth` | 当前深度快照 `{lastUpdateId,bids,asks}`。 |
| `market.funding(symbol, range)` | `GET /market/funding` | `symbol, start, end` → 实际结算资金费率 `Metric[]`。 |
| `market.openInterest(symbol, range?)` | `GET /market/oi` | 有范围取历史，无范围取当前未平仓量。两者均返回 `Metric[]`。 |
| `market.longShortRatio(symbol, range)` | `GET /market/ratio` | `symbol, start, end` → 全市场账户多空比 `Metric[]`。 |
| `market.stats(symbol)` | `GET /market/stats` | 24 小时统计对象，字段取决于行情源。 |
| `market.spec(symbol)` | `GET /market/spec` | 返回 `Spec`，含 `tickSize`、限价数量的 `stepSize/minQty/maxQty`、市价数量的 `marketStep/marketMin/marketMax` 和 `minNotional`。 |

`Interval` 支持 `1m`、`3m`、`5m`、`15m`、`30m`、`1h`、`2h`、`4h`、`6h`、`8h`、`12h`、`1d`。SDK 导出的 `periods[interval]` 给出对应的毫秒数。

- `Bar` 带 `symbol, interval, openTime, closeTime, open, high, low, close, volume`。价格和数量是十进制字符串。
- `Quote.book` 含 `bid, bidQty, ask, askQty`，`Quote.mark` 含 `price, index, fundingRate, nextFundingTime`。两者各有 `eventTime` 和 `receivedAt`。相应数据尚未到达时，整个 `book` 或 `mark` 为 `null`。过期标志 `bookStale`、`markStale` 在顶层。
- `AggregateTrade` 带字符串 `id`、`price`、`quantity` 和 `time, buyerMaker`。`Metric` 带 `time, value, source`，来源为 `sim`、`rest` 或 `archive`。成交 ID 和深度序号保留为字符串，避免大整数精度损失。

## 订阅与命名数据集

| SDK 调用 | HTTP | 参数和交付内容 |
|---|---|---|
| `market.watchBars(symbol, interval, opts?)` | `GET /stream/bars` | `symbol, interval, warmup?`。预热默认 0，最多 50,000 根。每条为 `{source,bar}`，来源为 `history/backfill/live` 三者之一。SDK 取消信号放在 `opts.signal`。 |
| `market.watchQuotes(symbol, signal?)` | `GET /stream/quotes` | 一档或标记价更新时交付最新 `Quote`。 |
| `market.watchTrades(symbol, signal?)` | `GET /stream/trades` | 按需订阅聚合成交，每条为 `AggregateTrade`。 |
| `market.watchDepth(symbol, signal?)` | `GET /stream/depth` | 交付 `DepthFrame`。从 `kind: "snapshot"` 建簿，从 `kind: "delta"` 更新。`bids/asks` 为 `[价格, 数量][]`，`first/last/previous?` 是更新序号。 |
| `market.watchLiquidations(symbol, signal?)` | `GET /stream/liquidations` | 交付 `{kind:"publicLiquidationSnapshot",event}`。SDK 将负载标为 `unknown`，读取原始交易所字段前检查数据形状。 |
| `data.append(name, record)` | `POST /data/append` | 正文 `{name,record}`，首次追加即创建数据集，返回 `Sample`。 |
| `data.read(name, after?, limit?)` | `GET /data/read` | `after` 默认为 0 且不含该游标。`limit` 默认 500，范围 1–5,000。返回升序 `Sample[]`。 |
| `data.watch(name, after?, signal?)` | `GET /stream/data` | `name, after?`。从同一排他游标开始，先读已有记录，再读后续追加。 |

`Sample<T>` 的字段是 `{cursor,name,time,record}`，`time` 是宿主追加时的墙钟毫秒。`name` 和业务键可使用 1–120 个 ASCII 字母、数字、下划线、点、冒号和短横线。下面的调用可以直接在默认实例试用。

```sh
curl -fsS -H 'Content-Type: application/json' -d '{"name":"notes","record":{"message":"已预热"}}' http://127.0.0.1:8787/data/append
curl -fsS 'http://127.0.0.1:8787/data/read?name=notes&after=0&limit=10'
curl -NsS 'http://127.0.0.1:8787/stream/data?name=notes&after=0'
```

最后一个请求保持打开，Ctrl+C 即断开。流的 MIME 类型是 `application/x-ndjson`。下面是已有一条样本、随后又追加一条时的响应示意。游标、时间与实际运行有关。

```ndjson
{"data":{"cursor":7,"name":"notes","time":1700000000000,"record":{"message":"已预热"}}}
{"ready":true}
{"data":{"cursor":9,"name":"notes","time":1700000001000,"record":{"message":"已收盘"}}}
```

非 Bun 客户端应增量解码 UTF-8，按完整换行解析 JSON。忽略用于刷新响应和保活的空行。`ready` 表示初始回放已结束，`error` 帧携带错误信息，传输也可能直接断开。空闲流每 15 秒发送空行，每次订阅的实时队列上限为 256 条。SDK 在 `break` 或取消时释放连接，异常断流由脚本按游标或预热窗口续接。[可运行的断点消费示例](scripts-and-cli.md)

## 账户与交易

| SDK 调用 | HTTP | 参数和结果 |
|---|---|---|
| `trade.positions(account?)` | `GET /trade/positions` | `Position[]`，只含非零仓位，字段为 `symbol, positionSide, quantity`。 |
| `trade.openOrders(account?)` | `GET /trade/openOrders` | 本机跟踪的 `Order[]`，包含 `NEW`、`PARTIALLY_FILLED` 和待确认的 `UNKNOWN`。 |
| `trade.place(input)` | `POST /trade/place` | `{key,symbol,side,type,quantity,price?,positionSide?,quote}` → `Instruction`。`side` 为 `BUY/SELL`，`type` 为 `MARKET/LIMIT`。限价单提供 `price`，市价单省略它。 |
| `trade.cancel(input)` | `POST /trade/cancel` | `{key,symbol,clientId}` → 撤单指令。 |
| `trade.close(input)` | `POST /trade/close` | `{key,symbol,positionSide?,quote?}` → 平仓指令。数量来自实际仓位，未传决策报价时使用宿主报价。 |
| `trade.setLeverage(input)` | `POST /trade/setLeverage` | `{key,symbol,leverage}` → 调杠杆指令，整数范围 1–125，币安仍会校验该合约与账户。 |
| `trade.instruction(id)` | `GET /trade/instruction` | `id` → `Instruction`，含 `params,decision,state,detail` 和关联的 `runId,clientId`。 |
| `trade.order(clientId, account?)` | `GET /trade/order` | `clientId, account?` → `Order`。含 `exchangeId,executed,status,updated` 等字段。 |
| `trade.fills(clientId, account?)` | `GET /trade/fills` | `clientId, account?` → `{tradeId,price,quantity,time}[]`。 |

账户为 `paper` 或 `binance`。交易 POST 使用运行绑定的账户，四种写指令均需业务幂等键。HTTP 成功后还要检查 `Instruction.state` 和订单状态。[模拟交易演练与状态解释](trading.md)

## 通知、任务与运行管理

| SDK 或 CLI | HTTP | 参数和结果 |
|---|---|---|
| `notify.send(type, content)` | `POST /notify/send` | `{type,content}` → `{id,state:"delivered"}`。内容是 JSON 对象。 |
| `agents.startTask(backend, prompt)` | `POST /agents/startTask` | `{backend,prompt}` → `Task`。 |
| `agents.sendMessage(id, message)` | `POST /agents/sendMessage` | `{id,message}` → 更新后的 `Task`。 |
| `agents.getStatus(id)` | `GET /agents/status` | `id` → 远端查询后的 `Task`，字段为 `id, backend, sessionId, state, result`。 |
| `currentRun()` | `GET /run` | 当前托管进程的 `Run`。 |
| 健康查询 | `GET /health` | 最近一次巡检的状态快照。 |
| `harness status` | `GET /admin/status` | 即时读取与健康快照同结构的状态。 |
| `harness run` | `POST /admin/run` | `{file,name?,observe?,account?}` → `Run`。`file` 是宿主上的文件，CLI 传绝对路径。 |
| `harness logs` | `GET /admin/logs` | `ref` 为运行 ID 或名称，返回 `{id,stdout,stderr}`。 |
| `harness stop` | `POST /admin/stop` | `{ref}` → `Run`。 |
| `harness replace` | `POST /admin/replace` | `{name,file,from?}` → 接任的 `Run`。`from` 为已观察候选的运行 ID。 |
| `harness refresh` | `POST /admin/refresh` | 发送空对象 `{}`，返回 `{symbols}`。 |

`Run` 包含 `id,name,file,hash,account,mode,state,ready,started,ended,exitCode`。其中 `mode` 为 `execute/observe`，`ended` 和 `exitCode` 可为 `null`。消息目标、模板、远端 URL 与会话映射见[连接器协议](connectors.md)。

## 错误处理

宿主路由返回的错误正文通常为 `{"error":"说明"}`。SDK 对非 2xx 抛出含 HTTP 状态和正文内容的 Error。已经打开的订阅可能在响应体中返回 `{"error":"说明"}`。

| 状态 | 常见原因 |
|---|---|
| 400 | 周期不支持、缺少时间边界、数量步长不符或名称格式无效。 |
| 403 | 管理令牌不匹配、运行令牌失效或当前进程没有执行租约。 |
| 404 | 运行、订单、合约规格、任务或后端名称未找到。 |
| 409 | 相同交易键的参数冲突、合约所有者冲突、报价过期或换版条件未满足。 |
| 500 | 宿主文件读取、行情上游或其他处理异常。查看正文和宿主输出。 |
| 502 | webhook 投递或远端任务请求失败。已创建记录时，正文含本地记录 ID。 |

交易所拒单也可能以 HTTP 200 和 `Instruction.state === "rejected"` 返回。处于 `unknown` 的指令需按原 ID 对账。[故障排查](operations.md)
