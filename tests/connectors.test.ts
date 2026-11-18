import { expect, test } from "bun:test";
import { sandbox, until } from "./support";
test("webhook route, target, template and delivery outcome; independent agent backends reuse durable sessions", async () => {
  const requests: { path: string; method: string; body: any }[] = [];
  const remote = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const path = new URL(req.url).pathname,
        body = req.method === "POST" ? await req.json() : null;
      requests.push({ path, method: req.method, body });
      if (path === "/bad") return new Response(null, { status: 503 });
      if (path === "/webhook") {
        await Bun.sleep(120);
        return new Response(null, { status: 204 });
      }
      if (path.endsWith("/tasks"))
        return Response.json({ sessionId: "remote/1", state: "running", result: { echo: body } });
      return Response.json({ state: req.method === "GET" ? "done" : "running", result: body ?? "completed" });
    },
  });
  const url = remote.url.origin;
  const box = await sandbox({
    tickMs: 10,
    destinations: {
      notifications: {
        fill: { bot: "ops", target: "trader", template: "{{symbol}} filled {{quantity}}" },
        bad: { bot: "offline", target: "test", template: "x" },
      },
      bots: { ops: { url: url + "/webhook" }, offline: { url: url + "/bad" } },
      agents: { blue: { url: url + "/blue" }, green: { url: url + "/green" } },
    },
  });
  try {
    const now = box.host.ticker.source.now(),
      pending = box.sdk.notify.send("fill", { symbol: "BTCUSDT", quantity: "0.01" });
    await until(() => box.host.ticker.source.now() >= now + 2 * 60000);
    expect((await pending).state).toBe("delivered");
    expect(requests[0]).toEqual({
      path: "/webhook",
      method: "POST",
      body: {
        type: "fill",
        target: "trader",
        text: "BTCUSDT filled 0.01",
        content: { symbol: "BTCUSDT", quantity: "0.01" },
      },
    });
    await expect(box.sdk.notify.send("bad", {})).rejects.toThrow("502");
    expect(
      box.host.db.all<{ state: string }>("SELECT state FROM notifications ORDER BY rowid").map((r) => r.state),
    ).toEqual(["delivered", "failed"]);
    const blue = await box.sdk.agents.startTask("blue", "review"),
      green = await box.sdk.agents.startTask("green", "analyze");
    expect(blue.sessionId).toBe("remote/1");
    expect(green.id).not.toBe(blue.id);
    await box.restart();
    expect((await box.sdk.agents.sendMessage(blue.id, "continue")).state).toBe("running");
    expect((await box.sdk.agents.getStatus(green.id)).result).toBe("completed");
    expect(requests.slice(2)).toEqual([
      { path: "/blue/tasks", method: "POST", body: { taskId: blue.id, prompt: "review" } },
      { path: "/green/tasks", method: "POST", body: { taskId: green.id, prompt: "analyze" } },
      { path: "/blue/tasks/remote%2F1/messages", method: "POST", body: { message: "continue" } },
      { path: "/green/tasks/remote%2F1", method: "GET", body: null },
    ]);
  } finally {
    await box.close();
    await remote.stop(true);
  }
});
