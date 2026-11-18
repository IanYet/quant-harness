# 通知机器人和 agent 后端

脚本发送通知时给出类型和内容，宿主从配置选择机器人、目标和模板。创建 agent 任务时，脚本给出后端名称，此后的请求使用宿主返回的任务 ID。两个接口均可通过本机 HTTP 接收器联调。[宿主实现](../src/connectors.ts) · [配置示例](../config/connectors.example.json)

## 连接自己的服务

将示例 JSON 复制到部署方的配置目录，修改下列字段。启动 `harnessd` 时，用 `HARNESS_CONNECTORS` 指向该文件。

| 配置项 | 用途 |
|---|---|
| `bots.<机器人名>.url` | webhook 的完整目标 URL。 |
| `notifications.<类型>` | `bot` 指向机器人名，`target` 按接收服务约定标识接收方，`template` 生成投递文本。 |
| `agents.<后端名>.url` | 任务服务的根 URL。例如示例的 `research` 指向 `http://127.0.0.1:9899/research`。 |
| `bots.<名>.tokenEnv`、`agents.<名>.tokenEnv` | 可选。值是宿主环境中的变量名称。配置后，请求带 `Authorization: Bearer <变量值>`。 |

模板 `{{symbol}} 成交 {{quantity}}` 取 `content` 中同名的顶层字段。占位符支持字母、数字、下划线、点和短横线，点也是字段名的一部分。缺少的字段替换为空字符串，原始结构化内容随请求一并发送。

## 接收端协议

请求和 agent 响应均使用 JSON。webhook 只需返回 2xx。下面的路径相对于配置的 URL。

| 脚本操作 | 宿主发往接收器 | 接收器响应 |
|---|---|---|
| `notify.send("fill", {symbol:"BTCUSDT",quantity:"0.01"})` | 向机器人 URL POST `{"type":"fill","target":"trader","text":"BTCUSDT 成交 0.01","content":{"symbol":"BTCUSDT","quantity":"0.01"}}` | 任意 2xx，包括空的 204。脚本收到宿主投递 ID 和 `state:"delivered"`。 |
| `agents.startTask("research", prompt)` | `POST /tasks`，正文 `{taskId,prompt}`，`taskId` 由宿主生成 | `{"sessionId":"remote-17","state":"running","result":null}`。会话 ID 和状态为字符串，`result` 可省略。 |
| `agents.sendMessage(id, message)` | `POST /tasks/<sessionId>/messages`，正文 `{message}` | `{"state":"running","result":{"received":true}}`。 |
| `agents.getStatus(id)` | `GET /tasks/<sessionId>` | `{"state":"done","result":"复核完成"}`。远端定义状态字符串和结果内容。 |

宿主持久化 `id → backend + sessionId`，路径中的远端会话 ID 会进行 URL 编码。即使两个后端各自使用相同的会话编号，本地任务 ID 也能区分它们。HTTP 调用等待上限是 10 秒，重定向视为投递失败。通知失败时写入 `notifications.detail`，任务请求失败时记录 `state:"error"` 和原因。调用者重新执行 `notify.send` 或 `startTask` 会创建新的本地记录。远端已处理但响应丢失时，接收器的日志和 `taskId` 可用于核对。

## 本机联调

运行下面的命令即可验证真实的本机 HTTP 请求与响应。测试覆盖双后端路由、模板投递、失败记录，以及宿主重启后向原会话继续发消息。

```sh
bun test tests/connectors.test.ts
```

需要观察每个请求时，把下面的接收器保存为 `examples/local-receiver.ts`，在独立终端执行 `bun run examples/local-receiver.ts`。它与仓库的示例配置匹配，会话保存在接收器内存中。

```ts
const sessions = new Map<string, string[]>();

Bun.serve({
  hostname: "127.0.0.1", port: 9899,
  async fetch(req) {
    const { pathname } = new URL(req.url);
    if (req.method === "POST" && pathname === "/hook") {
      console.log("hook", await req.json());
      return new Response(null, { status: 204 });
    }

    const [, backend, resource, sessionId, action] = pathname.split("/");
    if (!["research", "review"].includes(backend) || resource !== "tasks") {
      return new Response(null, { status: 404 });
    }
    if (req.method === "POST" && !sessionId) {
      const { prompt } = await req.json() as { prompt: string };
      const id = crypto.randomUUID();
      sessions.set(backend + "/" + id, [prompt]);
      return Response.json({ sessionId: id, state: "running" });
    }

    const messages = sessions.get(backend + "/" + sessionId);
    if (!messages) return new Response(null, { status: 404 });
    if (req.method === "POST" && action === "messages") {
      const { message } = await req.json() as { message: string };
      messages.push(message);
    } else if (req.method !== "GET" || action) {
      return new Response(null, { status: 404 });
    }
    return Response.json({ state: "running", result: { messages } });
  },
});
```

在第二个终端启动联调实例。

```sh
HARNESS_DIR="$PWD/.harness-connectors" HARNESS_PORT=8790 HARNESS_MARKET=sim HARNESS_TRADING=paper HARNESS_CONNECTORS="$PWD/config/connectors.example.json" bun run harnessd
```

在第三个终端调用 SDK。接收器会打印填好目标和模板的 `hook`，客户端最后得到包含两条消息的 `result.messages`。

```sh
HARNESS_URL=http://127.0.0.1:8790 bun -e 'import {notify,agents} from "@quant/tools"; console.log(await notify.send("fill",{symbol:"BTCUSDT",quantity:"0.01"})); const t=await agents.startTask("research","复核收盘数据"); await agents.sendMessage(t.id,"附上时间范围"); console.log(await agents.getStatus(t.id))'
```
