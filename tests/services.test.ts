import { expect, test } from "bun:test";
import { createCipheriv, createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "../src/shared/db";
import { initLayout } from "../src/shared/config";
import { NoticeQueue, notifierSchema } from "../src/notifier/dedup-queue";
import { webhook } from "../src/notifier/webhook-server";
import { backtestRun, killSwitch, liveProposal, notifySend } from "../src/shared/control-client";
import { requestFor, tools } from "../src/mcp/server";
import { openAgentDb } from "../src/agent/db";
import { Guardrails } from "../src/agent/guardrails";
import { scheduler } from "../src/agent/scheduler";
import { emergencyPaths } from "../src/watchdog/emergency-cancel";
import { checkCore } from "../src/watchdog/probes";
import type { HarnessConfig } from "../src/shared/config";
import { WeightBucket } from "../src/core/binance/signer";
import { Mailbox } from "../src/shared/mailbox";

test("Telegram and encrypted Feishu callbacks enforce identity and persist deduplication", async () => {
  const db = openDb(":memory:", notifierSchema);
  try {
    const queue = new NoticeQueue(db, ["telegram", "feishu"]);
    const telegram = { botToken: "1:unused", webhookSecret: "local-secret", chat: "-100", users: ["21"] };
    const feishu = { appId: "cli_fixture", appSecret: "internal", encryptKey: "only-local", verificationToken: "verified",
      chat: "oc_fixture", users: ["ou_fixture"] };
    const handler = webhook({ host: "127.0.0.1", port: 18101, telegram, feishu }, queue);
    const telegramCallback = (secret: string, user: number, chat = -100) => handler(new Request("http://localhost/webhooks/telegram", {
      method: "POST", headers: { "x-telegram-bot-api-secret-token": secret }, body: JSON.stringify({ update_id: 10,
        callback_query: { id: "cb", from: { id: user }, message: { chat: { id: chat } }, data: "a:" + "f".repeat(20) } }),
    }));
    expect((await telegramCallback("wrong", 21)).status).toBe(401);
    expect((await telegramCallback("local-secret", 22)).status).toBe(401);
    expect((await telegramCallback("local-secret", 21, -200)).status).toBe(401);
    expect(await (await telegramCallback("local-secret", 21)).json()).toMatchObject({ text: "Received" });
    expect(await (await telegramCallback("local-secret", 21)).json()).toMatchObject({ text: "Already received" });

    const unixSeconds = (age = 0) => String(Math.floor(Date.now() / 1000) - age);
    const formattedTime = (age = 0) => {
      const clock = new Date(Date.now() + 8 * 3_600_000 - age * 1000).toISOString().slice(0, -1).replace("T", " ");
      return clock + "123456 +0800 CST m=+1024.000000001";
    };
    const feishuCallback = (appId: string, ts = unixSeconds(), corrupt = false) => {
      const raw = JSON.stringify({ header: { event_type: "card.action.trigger", event_id: "event", token: "verified", app_id: appId },
        event: { operator: { open_id: "ou_fixture" }, context: { open_chat_id: "oc_fixture" },
          action: { value: { key: "f".repeat(20), decision: "reject" } } } });
      const iv = Buffer.alloc(16, 23);
      const cipher = createCipheriv("aes-256-cbc", createHash("sha256").update(feishu.encryptKey).digest(), iv);
      const body = JSON.stringify({ encrypt: Buffer.concat([iv, cipher.update(raw), cipher.final()]).toString("base64") });
      const nonce = "unique";
      const signature = createHash("sha256").update(ts + nonce + feishu.encryptKey + body).digest("hex");
      return handler(new Request("http://localhost/webhooks/feishu", { method: "POST", body,
        headers: { "x-lark-request-timestamp": ts, "x-lark-request-nonce": nonce, "x-lark-signature": corrupt ? "wrong" : signature } }));
    };
    expect((await feishuCallback("cli_fixture", unixSeconds(360))).status).toBe(401);
    expect((await feishuCallback("cli_fixture", formattedTime(360))).status).toBe(401);
    expect((await feishuCallback("cli_fixture", formattedTime(-360))).status).toBe(401);
    expect((await feishuCallback("cli_other")).status).toBe(401);
    expect((await feishuCallback("cli_fixture", formattedTime(), true)).status).toBe(401);
    expect((await feishuCallback("cli_fixture", formattedTime())).status).toBe(200);
    expect((await feishuCallback("cli_fixture", String(Date.now()))).status).toBe(200);
    expect(await (await feishuCallback("cli_fixture")).json()).toMatchObject({ toast: { content: "已受理过" } });
    expect(queue.pending.callbacks).toBe(2);
  } finally { db.close(); }
});

test("sys.notify is queued before acknowledgement and deduplication cools per run and destination", () => {
  const db = openDb(":memory:", notifierSchema);
  try {
    const queue = new NoticeQueue(db, ["telegram", "feishu"]);
    const note = { level: "warn", title: "Threshold", body: "Event", runId: "one", dedupKey: "threshold", cooldownSec: 40 };
    queue.receive({ seq: 8, ts: Date.now(), source: "run:one", topic: "sys.notify", payload: note }, 8);
    queue.receive({ seq: 9, ts: Date.now(), source: "run:one", topic: "sys.notify", payload: note }, 9);
    const row = { seq: 9, ts: Date.now(), source: "run:one", topic: "sys.notify", payload: note };
    queue.receive(row, 9);
    expect(queue.cursor).toBe(9);
    expect(db.query("SELECT channel,count FROM notices ORDER BY channel").all()).toEqual([
      { channel: "feishu", count: 2 }, { channel: "telegram", count: 2 },
    ]);
  } finally { db.close(); }
});

test("11 MCP tools share CLI targets and required mutation options", () => {
  expect(tools).toHaveLength(11);
  const values = { accountId: "safe", strategyId: "family@v1", handoverOrders: "cancel_open", includeConditional: true };
  expect(requestFor("live_propose", values)).toEqual(liveProposal(values));
  const kill = { scope: "safe", enabled: true, cancelOpenOrders: true, includeConditional: true };
  expect(requestFor("risk_kill_switch", kill)).toEqual(killSwitch(true, kill));
  expect(requestFor("notify_send", {})).toEqual(notifySend({}));
  const test = { draftPath: "my-draft.ts", start: 1, end: 3 };
  expect(requestFor("backtest_run", test)).toEqual(backtestRun(test));
});

test("Agent queue aliases duplicates, admits parallel claims, limits depth and consumes budgets", async () => {
  const dir = await mkdtemp(join(tmpdir(), "quant-agent-test-"));
  await initLayout(dir);
  const db = openAgentDb(dir);
  try {
    const base = Date.now();
    const guard = new Guardrails(db, { guardrails: { maxConcurrentTasks: 2, dailyMaxTasks: 7, dailyMaxBudgetUsd: 5 } }, new Set(["codex"]));
    const task = guard.input({ prompt: "Infrastructure", dedupKey: "same", cooldownSec: 2 }, "codex");
    const origin = { source: "unit", scope: "source", taskId: "root" };
    expect(guard.enqueue(task, origin, base).visibility).toBe("visible");
    expect(guard.enqueue(task, { ...origin, taskId: "alias" }, base + 1000)).toMatchObject({ canonicalTaskId: "root" });
    expect(guard.enqueue(task, { ...origin, taskId: "later" }, base + 2100)).toMatchObject({ canonicalTaskId: "later" });
    for (let depth = 1; depth <= 3; depth++) expect(guard.enqueue({ ...task, dedupKey: undefined }, {
      ...origin, taskId: "depth-" + depth, parent: depth === 1 ? "root" : "depth-" + (depth - 1),
    }).depth).toBe(depth);
    expect(() => guard.enqueue({ ...task, dedupKey: undefined }, { ...origin, taskId: "too-deep", parent: "depth-3" })).toThrow(
      expect.objectContaining({ code: "AGENT_DEPTH_LIMIT" }));
    const two = [guard.claim(), guard.claim()];
    expect(two[0]?.task_id).not.toBe(two[1]?.task_id);
    expect(guard.claim()).toBeUndefined();
    for (const entry of two) guard.finish(entry!, { status: "completed", summary: "OK", costUsd: 2.5 });
    expect(guard.usage().cost_usd).toBe(5);
    expect(() => guard.enqueue({ ...task, dedupKey: undefined }, { ...origin, taskId: "budget" })).toThrow(expect.objectContaining({ code: "AGENT_BUDGET_LIMIT" }));
  } finally { db.close(); await rm(dir, { recursive: true, force: true }); }
});

test("cron submits exactly once per UTC slot across scheduler reconstruction", async () => {
  const dir = await mkdtemp(join(tmpdir(), "quant-cron-test-"));
  await initLayout(dir);
  const db = openAgentDb(dir);
  try {
    const guards = new Guardrails(db, {}, new Set(["codex"]));
    const jobs = [{ name: "daily", expr: "5 0 * * *", visibility: "visible" }];
    const point = new Date("2026-11-20T00:05:30Z").getTime();
    scheduler(jobs, db, guards, "codex")(point);
    scheduler(jobs, db, guards, "codex")(point + 2000);
    expect(db.query("SELECT COUNT(*) AS n FROM tasks WHERE profile='cron_review'").get()).toEqual({ n: 1 });
  } finally { db.close(); await rm(dir, { recursive: true, force: true }); }
});

test("public weight and lossy fast queues preserve capacity for core and cancels", async () => {
  const bucket = new WeightBucket(100, 20);
  bucket.take(80);
  expect(() => bucket.take(1)).toThrow(expect.objectContaining({ code: "RATE_LIMITED" }));
  bucket.take(20, true);
  const queue = new Mailbox<number>(2, true);
  for (let i = 1; i <= 4; i++) queue.offer(i);
  expect(queue.dropped).toBe(2);
  expect(await queue.take()).toBe(3);
  expect(await queue.take()).toBe(4);
  queue.close();
});

test("watchdog reports independent symbol freshness and preserves conditions by default", () => {
  const now = Date.now();
  const config = { symbols: ["BTCUSDT", "ETHUSDT"], watchdog: {} } as HarnessConfig;
  const issues = checkCore({ ok: true, seq: 1_000_000, pendingStorageEvents: 1, eventLoopLagMs: 1,
    accounts: [], runs: [], freshness: { mainnet: { BTCUSDT: { kline: now, book_ticker: now, mark_price: now },
      ETHUSDT: { kline: now, book_ticker: now - 60_000, mark_price: now } } } }, config, now);
  expect(issues.map(issue => issue.key)).toEqual(["market:ETHUSDT:book_ticker"]);
  expect(emergencyPaths(false)).toEqual(["/fapi/v1/allOpenOrders"]);
  expect(emergencyPaths(true)).toEqual(["/fapi/v1/allOpenOrders", "/fapi/v1/algoOpenOrders"]);
});
