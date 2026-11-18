# 脚本开发与 CLI

临时查询可以直接 `bun run FILE.ts`。持续运行、查看分开的标准输出和错误输出、持有交易权限或进行同名换版时，使用 `bun run harness`。两种入口连接同一宿主。以下假设已按[首页](../README.md)启动默认实例。

## 写一个收盘观察脚本

将下段保存为仓库内的 `examples/observe-hour.ts`，执行 `bun run examples/observe-hour.ts`。它先读两根已收盘小时线，再等下一根实时小时线，打印后结束。SDK 在退出循环时释放本次监听。

```ts
import { market } from "@quant/tools";

for await (const { source, bar } of market.watchBars("BTCUSDT", "1h", { warmup: 2 })) {
  console.log(source, new Date(bar.openTime).toISOString(), bar.close);
  if (source === "live") break;
}
```

`history` 是本次预热，`backfill` 是缺口或迟到收盘，`live` 是实时触发。指标状态可用 `symbol + interval + openTime` 覆盖同一根 K 线。需要新信号的逻辑放在 `live` 分支。未凑满所需历史时，策略可继续积累样本。[完整策略示例](../examples/strategy.ts)

在仓库内安装依赖后，普通导入、相对路径和顶层 `await` 都可使用。托管脚本的工作目录是**入口文件所在目录**。宿主保存并执行本次入口源码的快照，同时保留原模块路径。要复现依赖或其他本地模块，需要保留相应源码版本和锁文件。SDK 的 `currentRun()` 仅在托管进程中有运行身份。

## CLI 命令

在仓库根目录运行。表中省略共同前缀 `bun run harness`。成功的管理命令输出 JSON，`logs` 将两个日志写回各自的输出通道。CLI 出错时写标准错误并返回非零退出码。

| 命令 | 结果与参数 |
|---|---|
| `run FILE [--name NAME] [--observe] [--account ACCOUNT]` | 启动本机可读的 `.ts` 文件，返回 `Run`。省略名称时使用生成的运行 ID。账户可选 `paper` 或 `binance`，默认为 `paper`。默认模式是 `execute`。`--observe` 允许与同名执行者并行读行情、查账户和写数据集。 |
| `status` | 即时读取宿主、市场和账户健康、监听数、缺口及最近的运行记录。 |
| `logs RUN_OR_NAME` | 一次读取本次运行已写出的 stdout、stderr。保留返回的 ID 可定位历史版本。 |
| `stop RUN_OR_NAME` | 先收回令牌和执行租约，再终止该子进程。宿主持续跟踪已有订单。 |
| `replace NAME FILE [--from RUN_ID]` | 将同名执行者交接给候选。返回取得租约的新 `Run`。 |
| `refresh` | 重新获取合约规格，返回当前合约数 `symbols`。 |
| `--help` | 显示实际支持的命令和选项，可在宿主启动前查看。 |

`Run.id` 标识单次进程，`Run.name` 是可跨版本延续的策略名。按名称查询优先命中当前执行者，若没有执行者则选最近一次运行。名称和业务键允许 1–120 个 ASCII 字母、数字、下划线、点、冒号和短横线。

## 观察、换版和回退

下面的命令承接首页中仍在运行的 `swing`。在另一份入口文件修改策略后启动观察进程。

```sh
cp examples/strategy.ts examples/strategy-v2.ts
# 在 examples/strategy-v2.ts 保存修改
observer_id=$(bun run harness run examples/strategy-v2.ts --name swing --observe | bun -e 'console.log((await Bun.stdin.json()).id)')
bun run harness logs "$observer_id"
bun run harness status
```

检查观察版的 `startup` 是否读到旧挂单、预热数量是否符合策略要求。确认后执行交接。

```sh
bun run harness replace swing examples/strategy-v2.ts
bun run harness logs swing
```

宿主按名称、账户、入口真实路径和入口 SHA-256 选择仍在运行的匹配观察进程。找不到匹配项时启动新候选。默认等顶层初始化完成，或 SDK 消费完订阅的初始回放。候选退出或就绪超时，旧执行者保有租约。交接成功后，即使旧进程已有请求在等待交易所预查，发送前的权限复核也会拒绝它。

长期停在顶层 `await` 的自定义轮询脚本可能一直没有自动就绪标记。观察者已被人工核实后，可执行 `bun run harness replace NAME FILE --from RUN_ID` 选定它并跳过自动就绪等待。选择仍需匹配进程、文件版本和账户。要回到上一版，先恢复对应入口和依赖，再对同一策略名执行 `replace`。若旧执行者已停止，用 `run --name NAME` 重新启动。原挂单、仓位和幂等键随账户与策略名称继续保留。

## 从已处理的数据游标继续

`data.watch(name, after)` 会先交付游标之后已有的记录，再继续接收追加。下面是可重复执行的一次性消费者。保存为项目内的 `examples/read-next.ts`，将 `SCRIPT_CURSOR_FILE` 指向已有私有目录中的文件。

```ts
import { rename } from "node:fs/promises";
import { data } from "@quant/tools";

const path = process.env.SCRIPT_CURSOR_FILE;
if (!path) throw new Error("Set SCRIPT_CURSOR_FILE");
const file = Bun.file(path);
const after = (await file.exists()) ? Number(await file.text()) : 0;
if (!Number.isSafeInteger(after) || after < 0) throw new Error("Invalid saved cursor");

for await (const row of data.watch<{ close: string }>("btc-hourly", after)) {
  console.log(row.cursor, row.record.close);
  const temp = path + "." + process.pid + ".tmp";
  await Bun.write(temp, String(row.cursor));
  await rename(temp, path);
  break;
}
```

```sh
SCRIPT_CURSOR_FILE="$PWD/.harness/read-next.cursor" bun run examples/read-next.ts
```

再次执行会从保存的游标之后开始。游标在所有数据集间递增，同一名称下可能跳号。每个独立消费者使用自己的游标文件。进程若在处理成功后、保存游标前退出，下一次会再次拿到该条记录，带副作用的处理可用 `name + cursor` 去重。托管运行时，`SCRIPT_*` 由启动宿主的环境传入。[实例配置](configuration.md)
