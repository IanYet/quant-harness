# 交易与对账

`trade` 使用当前托管运行的账户、策略名和执行租约。观察进程可读取仓位、挂单、指令和成交。停止或替换执行者会收回旧进程的交易权限，已提交的订单由宿主继续跟踪。

## 在空模拟账户演练一次

在独立终端启动一个新的模拟实例。下面的导出变量同时供此实例的 CLI 使用，可在第二个终端设置为相同的目录和端口。

```sh
export HARNESS_DIR="$PWD/.harness-order-demo"
export HARNESS_PORT=8789
HARNESS_MARKET=sim HARNESS_TRADING=paper bun run harnessd
```

将下段保存为仓库内的 `examples/paper-order.ts`。它演示留单、原请求重试、撤单、市价成交和平仓，每一步读取宿主记录。

```ts
import { currentRun, market, trade, type Instruction, type Place } from "@quant/tools";

const run = await currentRun();
const health = await (await fetch(new URL("/health", process.env.HARNESS_URL))).json();
if (health.market !== "sim" || run.account !== "paper" || run.mode !== "execute") {
  throw new Error("This walkthrough requires simulated prices and a paper executor");
}

const symbol = "BTCUSDT";
const [positions, orders] = await Promise.all([trade.positions(), trade.openOrders()]);
if ([...positions, ...orders].some(row => row.symbol === symbol)) {
  throw new Error("Use a fresh demo account for BTCUSDT");
}

function orderId(instruction: Instruction) {
  if (instruction.state !== "accepted" || !instruction.clientId) {
    throw new Error(instruction.id + " " + instruction.state + " " + instruction.detail);
  }
  return instruction.clientId;
}

const key = (action: string) => run.id + "-" + action;
const limit: Place = {
  key: key("limit"), symbol, side: "BUY", type: "LIMIT",
  quantity: "0.01", price: "50000.00", quote: await market.quote(symbol),
};
const placed = await trade.place(limit);
const restingId = orderId(placed);
console.log("limit", (await trade.order(restingId)).status);
console.log("sameInstruction", (await trade.place(limit)).id === placed.id);

await trade.cancel({ key: key("cancel"), symbol, clientId: restingId });
console.log("cancel", (await trade.order(restingId)).status);
const entry = await trade.place({
  key: key("market"), symbol, side: "BUY", type: "MARKET",
  quantity: "0.01", quote: await market.quote(symbol),
});
const entryId = orderId(entry);
console.log("entry", (await trade.order(entryId)).status, await trade.fills(entryId));
const exit = await trade.close({ key: key("close"), symbol });
console.log("close", (await trade.order(orderId(exit))).status, await trade.positions());
```

```sh
export HARNESS_DIR="$PWD/.harness-order-demo"
export HARNESS_PORT=8789
bun run harness run examples/paper-order.ts --name order-demo --account paper
bun run harness logs order-demo
```

脚本完成后，日志依次包含 `limit NEW`、`sameInstruction true`、`cancel CANCELED`、`entry FILLED` 和 `close FILLED []`。演练用运行 ID 区分每次人工操作。持续策略可以用合约、信号时间和动作构造稳定业务键，使同一决策在换版后仍可识别。

## 从业务键查到成交

| 标识 | 归属和用途 |
|---|---|
| `key` | 调用者为一次决策指定的业务键。唯一范围为 `account + strategy + key`，涵盖下单、撤单、平仓和调杠杆。 |
| `Instruction.id` | 宿主返回的指令 ID。`trade.instruction(id)` 可查输入参数、决策报价、所属运行、状态及错误详情。 |
| `Instruction.clientId` | 下单或平仓时预先分配的稳定客户订单号，撤单指向原单，调杠杆可为 `null`。`trade.order(clientId)` 和 `trade.fills(clientId)` 用它查询。 |
| `Order.exchangeId`、`tradeId` | 前者是订单的交易所编号，确认前可为空。后者标识单笔成交，按账户、合约和成交 ID 去重。 |

重试同一业务键需要复用**完整原请求**，包括策略当时看到的 `quote`。重新取报价会改变请求参数，即使用同一个价格和数量也可能返回 409。将幂等输入和响应 ID 随策略决策保存，可以在客户端重启后继续查询。

| 指令状态 | 处理方式 |
|---|---|
| `pending` | 已记录，执行尚在进行。用原指令 ID 查询。 |
| `accepted` | 已确认执行请求。对下单或平仓继续查订单状态和实际成交量。 |
| `rejected` | 明确拒绝。检查 `detail`、合约参数和账户状态。 |
| `unknown` | 发送后超时或回报不确定，或宿主恢复了未完成指令。保留原客户订单号，等待私有回报和 REST 核对。 |

订单独立记录为 `UNKNOWN`、`NEW`、`PARTIALLY_FILLED`、`FILLED`、`CANCELED`、`REJECTED` 或 `EXPIRED`。撤单前可能已有成交，复盘时同时读取 `executed` 和成交列表。

## 账户规则和故障恢复

- 数量和价格传正的十进制字符串。限价单按 `tickSize` 对齐价格，按 `stepSize/minQty/maxQty` 检查数量。市价单使用 `marketStep/marketMin/marketMax`。普通开单还检查最小名义金额。决策报价带来源时间，执行前宿主另查当前一档是否新鲜。
- 每个账户与合约由一个策略名控制。同名换版可以接续原仓位和挂单。不同策略接手需等合约上无仓位、活动订单或未决指令，币安账户还需成功对账。
- 模拟账户采用单向净仓，市价和可成交限价使用买卖一档及可见量，较大订单可以部分成交。`setLeverage` 保存模拟设置。该模型不计算费用、保证金和强平。
- 平仓数量取自匹配仓位。单向使用 `BOTH`，币安请求带 `reduceOnly`。币安双向模式指定 `LONG` 或 `SHORT`，交易所请求以对应的 `positionSide` 减仓。调整杠杆按账户和合约生效。

遇到 `unknown`，先查看 `harness status` 中的 `trade.unresolved`、`privateStream`、`reconciliationError` 和 `outstandingFillRepairs`，再查原指令、订单及成交。宿主重连和重启后会查询挂单、按原客户订单号核对本地活动单，并补查缺少的成交。交易所暂查不到原客户订单号时，结果可能继续保持未知。再次提交新的业务键会创建另一条独立指令。

签名、已接受后超时、重复回报、持仓模式及重启对账由本机替身验证。实际交易所的账户权限、可成交价格和订单校验取决于该账户。[模拟成交测试](../tests/paper.test.ts) · [币安适配器测试](../tests/binance.test.ts)
