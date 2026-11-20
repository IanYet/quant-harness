import type { ServiceAuth } from "../shared/auth";
import { socketPath, type HarnessConfig } from "../shared/config";
import type { BusFrame, BusEvent } from "../shared/events";
import { body, Fault, handler, ID, missing, oneOf, str } from "../shared/http";
import { call, readNdjson, serveUnix, unixFetch, withQuery } from "../shared/uds";
import type { AgentHarnessAdapter, AgentProfile } from "./adapters/interface";
import { Guardrails, visibilities } from "./guardrails";
import type { Task, TaskInput } from "./guardrails";
import { scheduler } from "./scheduler";

const guidance: Record<AgentProfile, string> = {
  user: "Use the quant CLI, MCP and read-only SQLite as appropriate. Trade or change live runs only within the trader's explicit authorization.",
  cron_review: "Review operations and history, using quant CLI, read-only SQLite and research scripts. Write evidence-based strategy notes and a factual summary. Live trading changes require an explicit trader instruction.",
  script_triggered: "Investigate the bounded script request using quant CLI or local research. Summarize evidence and any action needed. Live trading changes require an explicit trader instruction.",
};

export async function agentServer(config: HarnessConfig, auth: ServiceAuth, guards: Guardrails,
    adapters: Map<string, AgentHarnessAdapter>, signal: AbortSignal) {
  const { db } = guards;
  const defaultBackend = str(config.agent.defaultBackend ?? "codex", "defaultBackend", ID);
  if (!adapters.has(defaultBackend)) missing("Default Agent backend is not configured");
  const tickCron = scheduler(config.agent.cron, db, guards, defaultBackend);
  const active = new Map<string, { controller: AbortController; job: Promise<void> }>();
  const admin = socketPath(config.dataDir, "admin");
  const notifier = socketPath(config.dataDir, "notifier");
  const token = auth.token("agent");
  let backendHealth: Record<string, unknown> = {};
  let checking = false;
  let closing = false;
  db.query("UPDATE tasks SET status='interrupted',code='AGENT_RESTARTED',summary='Agent process restarted during the task',finished_at=? WHERE status='running'")
    .run(Date.now());

  async function run(task: Task, controller: AbortController) {
    const input = JSON.parse(task.request_json) as TaskInput;
    const settings = guards.profiles[task.profile];
    const adapter = adapters.get(task.backend)!;
    const session = guards.session(task.session_id);
    const deadline = setTimeout(() => controller.abort(new Fault(422, "TASK_TIMEOUT", "Agent task exceeded its time allowance")), settings.timeoutSec * 1000);
    const opts = {
      profile: task.profile, visibility: input.visibility, previousVisibility: session.visibility,
      title: input.title, maxTurns: settings.maxTurns, timeoutSec: settings.timeoutSec,
      signal: controller.signal, mcpEnv: { QUANT_DATA_DIR: config.dataDir, QUANT_PARENT_TASK: task.task_id,
        QUANT_ACTOR: "agent:" + task.backend + ":" + task.task_id },
      onSession(id: string) {
        db.query("UPDATE sessions SET backend_session_id=?,updated_at=? WHERE session_id=?").run(id, Date.now(), task.session_id);
      },
    };
    const prompt = guidance[task.profile] + "\n\n" + input.prompt + (input.context ? "\n\nContext:\n" + JSON.stringify(input.context) : "");
    try {
      const response = input.resume && session.backend_session_id ? await adapter.resumeSession(session.backend_session_id, prompt, opts) :
        await adapter.startSession({ ...opts, prompt });
      controller.signal.throwIfAborted();
      db.query("UPDATE sessions SET visibility=?,updated_at=? WHERE session_id=?").run(input.visibility, Date.now(), task.session_id);
      guards.finish(task, { status: "completed", summary: response.summary, costUsd: response.costUsd });
    } catch (error) {
      const failure = controller.signal.aborted ? controller.signal.reason : error;
      const code = failure instanceof Fault ? failure.code : "AGENT_BACKEND_ERROR";
      guards.finish(task, { status: code === "TASK_CANCELED" ? "canceled" : code === "AGENT_RESTARTED" ? "interrupted" : "failed",
        summary: (failure as Error).message, code });
    } finally { clearTimeout(deadline); }
  }

  function pump() {
    if (closing) return;
    while (active.size < guards.config.maxConcurrentTasks) {
      const task = guards.claim();
      if (!task) break;
      const controller = new AbortController();
      const job = run(task, controller).finally(() => { active.delete(task.task_id); pump(); });
      active.set(task.task_id, { controller, job });
    }
  }

  const cursor = () => db.query<{ seq: number }, []>("SELECT seq FROM consumer_cursor").get()!.seq;
  function inbound(event: BusEvent | undefined, through: number) {
    db.transaction(() => {
      if (through <= cursor()) return;
      if (event?.topic === "agent.request") {
        const p = event.payload;
        const id = str(p.taskId, "taskId", ID);
        try {
          const input = guards.input(p, defaultBackend, "script_triggered");
          guards.enqueue(input, { taskId: id, source: "script:" + String(p.runId), scope: String(p.sourceScope),
            parent: typeof p.parentTaskId === "string" ? p.parentTaskId : undefined });
        } catch (error) {
          if (!(error instanceof Fault)) throw error;
          db.query("INSERT INTO rejected_requests VALUES (?,?,?,?) ON CONFLICT DO NOTHING").run(id, error.code, error.message, Date.now());
        }
      }
      db.query("UPDATE consumer_cursor SET seq=? WHERE id=1").run(through);
    }).immediate();
    pump();
  }

  async function follow() {
    let ack = 0;
    while (!signal.aborted) {
      try {
        const response = await unixFetch(admin, withQuery("/v1/stream", { after_seq: cursor() }),
          { headers: { authorization: "Bearer " + token }, signal });
        for await (const frame of readNdjson<BusFrame>(response)) {
          if ("seq" in frame) inbound(frame, frame.seq);
          if ("type" in frame && frame.type === "watermark") {
            inbound(undefined, frame.throughSeq);
            if (cursor() > ack) {
              await call(admin, "/v1/stream/ack", { token, method: "POST", body: { throughSeq: cursor() } });
              ack = cursor();
            }
          }
        }
      } catch (error) { if (!signal.aborted) console.warn("Agent stream reconnecting", (error as Error).message); }
      if (!signal.aborted) await Bun.sleep(1000);
    }
  }

  async function report() {
    while (!signal.aborted) {
      try {
        const tasks = db.query<Task, []>("SELECT * FROM tasks WHERE reported=0 AND finished_at IS NOT NULL ORDER BY finished_at LIMIT 20").all();
        for (const task of tasks) {
          // Publish the summary only to the local notifier. Provider delivery is handled independently.
          if (task.profile !== "user") {
            const session = guards.session(task.session_id);
            await call(notifier, "/v1/notify", { token, method: "POST", timeoutMs: 2500, body: {
              level: task.status === "completed" ? "info" : "warn", title: (session.title ?? task.profile) + " · " + task.status,
              body: (task.summary ?? task.code ?? task.status).slice(0, 5450) + "\nSession " + task.session_id +
                (session.backend_session_id ? "\nBackend session " + session.backend_session_id : ""),
              dedupKey: "agent-report:" + task.task_id,
            } });
          }
          await call(admin, "/v1/agent/task-update", { token, method: "POST", timeoutMs: 2500, body: guards.status(task.task_id) }).catch(() => {});
          db.query("UPDATE tasks SET reported=1 WHERE task_id=?").run(task.task_id);
        }
        const data = guards.usage();
        if (!data.soft_warned && (data.task_count >= guards.config.dailyMaxTasks * 0.8 || data.cost_usd >= guards.config.dailyMaxBudgetUsd * 0.8)) {
          await call(notifier, "/v1/notify", { token, method: "POST", timeoutMs: 2500, body: {
            level: "warn", title: "Agent budget threshold", body: "UTC " + data.day + " · tasks " + data.task_count + "/" + guards.config.dailyMaxTasks +
              " · recorded USD " + data.cost_usd.toFixed(2) + "/" + guards.config.dailyMaxBudgetUsd,
            dedupKey: "agent-budget:" + data.day, cooldownSec: 86_400,
          } });
          db.query("UPDATE daily_usage SET soft_warned=1 WHERE day=?").run(data.day);
        }
      } catch (error) { if (!signal.aborted) console.warn("Agent reports pending", (error as Error).message); }
      if (!signal.aborted) await Bun.sleep(2000);
    }
  }

  async function probe() {
    if (checking || closing) return;
    checking = true;
    try { backendHealth = Object.fromEntries(await Promise.all([...adapters].map(async ([name, adapter]) => [name, await adapter.health()]))); }
    finally { checking = false; }
  }

  const server = await serveUnix(socketPath(config.dataDir, "agent"), handler(async (req, url) => {
    const method = req.method;
    const path = url.pathname;
    if (method === "GET" && path === "/v1/health") return { ok: true, pid: process.pid, running: active.size, backends: backendHealth,
      usage: guards.usage(), limits: guards.config, lastSeq: cursor() };
    const source = auth.require(req, ["cli", "mcp", "core"]);
    const parent = req.headers.get("x-quant-parent-task") ?? undefined;
    const actor = req.headers.get("x-quant-actor") ?? source;
    if (method === "POST" && path === "/v1/tasks" && source !== "core") {
      const input = guards.input(await body(req), defaultBackend);
      const scoped = parent ? "task:" + parent : "user:" + actor;
      const task = guards.enqueue(input, { source: source + ":" + actor, scope: scoped, parent });
      pump();
      return Response.json(task, { status: 202 });
    }
    if (method === "GET" && /^\/v1\/tasks\/[^/]+$/.test(path)) return guards.status(str(path.slice(10), "taskId", ID));
    const cancel = /^\/v1\/tasks\/([A-Za-z0-9_-]+)\/cancel$/.exec(path);
    if (method === "POST" && cancel && source !== "core") {
      const id = cancel[1];
      const task = guards.find(id) ?? missing("Unknown Agent task");
      if (task.status === "queued") db.query("UPDATE tasks SET status='canceled',code='TASK_CANCELED',finished_at=? WHERE task_id=?").run(Date.now(), task.task_id);
      if (task.status === "running") {
        const running = active.get(task.task_id);
        running?.controller.abort(new Fault(422, "TASK_CANCELED", "Agent task canceled"));
        const backendId = guards.session(task.session_id).backend_session_id;
        if (backendId) void adapters.get(task.backend)!.cancel(backendId).catch(() => {});
      }
      return guards.status(id);
    }
    const resume = /^\/v1\/sessions\/([A-Za-z0-9_-]+)\/messages$/.exec(path);
    if (method === "POST" && resume && source !== "core") {
      const input = await body(req);
      const id = resume[1], session = guards.session(id);
      if (!session.backend_session_id || session.visibility === "ephemeral") throw new Fault(422, "SESSION_UNAVAILABLE", "Session cannot be resumed");
      const visibility = oneOf(input.sidebarVisibility ?? session.visibility, "visibility", visibilities);
      if (visibility === "ephemeral") throw new Fault(422, "VISIBILITY_UNSUPPORTED", "Persistent sessions cannot be resumed as ephemeral");
      const task = guards.enqueue({ ...guards.input({ prompt: input.message, backend: session.backend, visibility,
        title: session.title ?? undefined }, defaultBackend, session.profile), resume: id },
      { source: source + ":" + actor, scope: "session:" + id, parent });
      pump();
      return Response.json(task, { status: 202 });
    }
    return missing("Unknown Agent endpoint");
  }));
  const cron = setInterval(() => { tickCron(); pump(); }, 1000);
  const health = setInterval(() => void probe(), 30_000);
  tickCron();
  pump();
  void probe();
  const tasks = [follow(), report()];
  return { server, async close() {
    closing = true;
    clearInterval(cron);
    clearInterval(health);
    for (const task of active.values()) task.controller.abort(new Fault(503, "AGENT_RESTARTED", "Agent service stopped"));
    await Promise.allSettled([...adapters.values()].map(adapter => adapter.close()));
    await server.stop(true);
    await Promise.allSettled([...tasks, ...[...active.values()].map(task => task.job)]);
  } };
}
