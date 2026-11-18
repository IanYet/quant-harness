# 实例与配置

宿主在启动时读取环境变量和连接器 JSON。每个实例绑定本机回环地址，独占一个数据目录。CLI 需要找到该目录中的操作令牌，SDK 只需知道要连接的 URL。所有相对路径相对于相应进程的工作目录。[配置实现](../src/config.ts)

## 选择行情与账户

| 用途 | 宿主环境 | 脚本账户 |
|---|---|---|
| 离线演练和测试 | `HARNESS_MARKET=sim`、`HARNESS_TRADING=paper`，两者均为默认值 | `paper` |
| 观察公共行情并做模拟交易 | `HARNESS_MARKET=binance`、`HARNESS_TRADING=paper`，使用单独数据目录 | `paper`。公共行情无需交易所密钥 |
| 对接一个币安执行账户 | 两个模式均设为 `binance`，宿主进程持有该账户的 API Key 与 Secret | `run` 仍默认使用 `paper`。显式选择 `--account binance` 的运行使用已绑定的执行适配器 |

下面是第二个实例的配置示例。在宿主终端和 CLI 终端中都设置前两行，宿主终端执行最后一行。SDK 也可通过 `HARNESS_URL` 直接选择这个端口。

```sh
export HARNESS_DIR="$PWD/.harness-public"
export HARNESS_PORT=8788
HARNESS_MARKET=binance HARNESS_TRADING=paper bun run harnessd
```

行情模式写入 `settings`。切换 `sim/binance` 时使用不同目录。币安实例的运行库包含本地订单、仓位和策略归属，运营时按交易所账户分别安排目录。切换端口不会更换账户或清空订单。

## 宿主、客户端与脚本变量

| 变量 | 默认或范围 | 作用 |
|---|---|---|
| `HARNESS_DIR` | `.harness` | 宿主创建 SQLite、运行快照、日志、令牌和归档。CLI 在同一路径寻找 `operator.token`。 |
| `HARNESS_PORT` | `8787`，0–65535 | 宿主监听 `127.0.0.1`。0 让系统分配空闲端口，实际 URL 打在启动日志。 |
| `HARNESS_URL` | `http://127.0.0.1:${HARNESS_PORT}` | CLI 与 SDK 的连接地址，优先于端口推导。使用端口 0 的实例时，把启动日志中的实际 URL 提供给客户端。 |
| `HARNESS_SYMBOLS` | `BTCUSDT` | 固定关注的合约，逗号分隔，例如 `BTCUSDT,ETHUSDT`。持仓和挂单的合约还会自动加入跟踪。 |
| `HARNESS_MARKET` | `sim` 或 `binance`，默认 `sim` | 选择模拟行情或币安市场数据源。 |
| `HARNESS_TRADING` | `paper` 或 `binance`，默认 `paper` | 是否启用币安执行账户。启用时要求币安行情及两项凭证。 |
| `HARNESS_SIM_TICK_MS` | 100，5–60,000 ms | 推进一个模拟市场分钟所用的墙钟时长。默认每约 6 秒产生一根小时收盘。 |
| `HARNESS_READY_MS` | 30,000，100–600,000 ms | 普通 `replace` 等候选自动就绪的时长。 |
| `BINANCE_REST` | `https://fapi.binance.com` | 公共和私有 REST 的根地址。 |
| `BINANCE_WS` | `wss://fstream.binance.com` | WS 根地址。适配器在其下选择 public、market 或 private 路径。 |
| `BINANCE_ARCHIVE` | `https://data.binance.vision/data/futures/um/daily` | USDⓈ-M daily 归档根目录，用于历史聚合成交和指标。 |
| `BINANCE_API_KEY`、`BINANCE_API_SECRET` | 默认空 | 由宿主持有。私有请求使用 API Key，签名在宿主完成。 |
| `HARNESS_CONNECTORS` | 默认无路由 | JSON 文件路径。包含 `bots`、`notifications`、`agents`。修改文件后重启宿主加载。[配置示例](../config/connectors.example.json) |
| `SCRIPT_*` | 按需设置 | 宿主把带该前缀的环境变量传给每个托管子进程。采集示例使用 `SCRIPT_INDICATOR_URL` 指向可返回 JSON 的额外指标。 |

托管运行还获得 `HARNESS_RUN_ID`、`HARNESS_RUN_TOKEN`、`HARNESS_ENTRY`、`HARNESS_SNAPSHOT` 和宿主填写的 `HARNESS_URL`。运行令牌仅在该进程存续期间有效。子进程继承 `PATH`、`LANG`、`TZ` 和 `SCRIPT_*`，以 `--no-env-file` 启动。直接运行的 Bun 研究脚本使用调用者自己的环境。

脚本与宿主同属一个系统用户，可访问该用户有权读取的文件。操作令牌保护 `/admin/*`，运行租约控制交易写入。本机数据集写入、通知投递及 agent 调用均面向可信调用方。宿主持有的凭证应放在其私有运行环境，`SCRIPT_*` 适合所有托管进程都可以获得的设置。

## 固定的运行参数

健康快照每秒更新。宿主每 15 秒发起币安账户对账并检查到期行情回补任务，每小时刷新合约规格。公共和私有 REST、连接器 HTTP 的常规请求超时为 10 秒，日归档下载超时为 120 秒。连接断开后的 WS 重连延迟为 1 秒。这些是当前实现值。上表列出可从环境调整的设置。[宿主定时器](../src/server.ts) · [归档读取](../src/market/archive-worker.ts)
