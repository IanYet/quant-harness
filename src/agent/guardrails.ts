import { randomUUID } from "node:crypto";
import type { Database } from "bun:sqlite";
import { bad, Fault, ID, integer, missing, num, oneOf, record, str } from "../shared/http";
import type { AgentProfile, SidebarVisibility } from "./adapters/interface";

export const profiles = ["user", "cron_review", "script_triggered"] as const;
export const visibilities = ["visible", "hidden", "ephemeral"] as const;
type Config = { dedupCooldownSec: number; maxConcurrentTasks: number; dailyMaxTasks: number; dailyMaxBudgetUsd: number; maxDepth: number };
export type Profile = { defaultVisibility: SidebarVisibility; maxTurns: number; timeoutSec: number };
export type SessionRow = { session_id: string; backend: string; backend_session_id: string | null; profile: AgentProfile;
  visibility: SidebarVisibility; title: string | null; created_at: number; updated_at: number };
export type Task = {
  task_id: string; session_id: string; profile: AgentProfile; backend: string; source: string; depth: number; parent_task_id: string | null;
  status: string; request_json: string; summary: string | null; code: string | null; cost_usd: number | null;
  created_at: number; started_at: number | null; finished_at: number | null; reported: number; usage_day: string;
};
export type TaskInput = { prompt: string; profile: AgentProfile; visibility: SidebarVisibility; backend: string; title?: string; context?: unknown;
  cooldownSec?: number; dedupKey?: string; resume?: string };
type Origin = { source: string; scope: string; parent?: string; taskId?: string };
export const utcDate = (at = Date.now()) => new Date(at).toISOString().slice(0, 10);

const baseline: Record<AgentProfile, Profile> = {
  user: { defaultVisibility: "visible", maxTurns: 80, timeoutSec: 1800 },
  cron_review: { defaultVisibility: "visible", maxTurns: 40, timeoutSec: 1200 },
  script_triggered: { defaultVisibility: "visible", maxTurns: 30, timeoutSec: 900 },
};
type Usage = { day: string; task_count: number; cost_usd: number; soft_warned: number; unmetered: number };

export class Guardrails {
  readonly config: Config;
  readonly profiles: Record<AgentProfile, Profile>;
  constructor(readonly db: Database, raw: Record<string, unknown>, private readonly adapters: Set<string>) {
    const limits = record(raw.guardrails ?? {});
    this.config = { dedupCooldownSec: integer(limits.dedupCooldownSec ?? 300, "dedupCooldownSec", 0, 86_400),
      maxConcurrentTasks: integer(limits.maxConcurrentTasks ?? 4, "maxConcurrentTasks", 1, 64),
      dailyMaxTasks: integer(limits.dailyMaxTasks ?? 200, "dailyMaxTasks", 1),
      dailyMaxBudgetUsd: num(limits.dailyMaxBudgetUsd ?? 50, "dailyMaxBudgetUsd", 0.01),
      maxDepth: integer(limits.maxDepth ?? 3, "maxDepth", 0, 3) };
    const settings = record(raw.profiles ?? {});
    this.profiles = Object.fromEntries(profiles.map(profile => {
      const base = baseline[profile], p = record(settings[profile] ?? {});
      return [profile, { defaultVisibility: oneOf(p.defaultVisibility ?? base.defaultVisibility, "visibility", visibilities),
        maxTurns: integer(p.maxTurns ?? base.maxTurns, "maxTurns", 1, 1000),
        timeoutSec: integer(p.timeoutSec ?? base.timeoutSec, "timeoutSec", 1, 24 * 3600) }];
    })) as Record<AgentProfile, Profile>;
  }

  usage(at = Date.now()) {
    const day = utcDate(at);
    return this.db.query<Usage, [string]>("SELECT * FROM daily_usage WHERE day=?").get(day) ??
      { day, task_count: 0, cost_usd: 0, soft_warned: 0, unmetered: 0 };
  }

  input(raw: Record<string, unknown>, backend: string, profile?: AgentProfile): TaskInput {
    const mode = profile ?? oneOf(raw.profile ?? "user", "profile", profiles);
    const selected = str(raw.backend ?? backend, "backend", ID);
    if (!this.adapters.has(selected)) missing("Agent backend is not configured");
    return {
      prompt: str(raw.prompt, "prompt", /^[\s\S]{1,20000}$/), profile: mode, backend: selected,
      visibility: oneOf(raw.sidebarVisibility ?? raw.visibility ?? this.profiles[mode].defaultVisibility, "visibility", visibilities),
      title: raw.title === undefined ? undefined : str(raw.title, "title", /^[\s\S]{1,160}$/),
      cooldownSec: raw.cooldownSec === undefined ? undefined : integer(raw.cooldownSec, "cooldownSec", 0, 86_400),
      dedupKey: raw.dedupKey === undefined ? undefined : str(raw.dedupKey, "dedupKey", /^[\s\S]{1,160}$/), context: raw.context,
    };
  }

  session(id: string) {
    return this.db.query<SessionRow, [string]>("SELECT * FROM sessions WHERE session_id=?").get(id) ?? missing("Agent session not found");
  }

  find(id: string) {
    return this.db.query<Task, [string, string]>("SELECT * FROM tasks WHERE task_id=? OR task_id=(SELECT task_id FROM task_aliases WHERE alias_id=?)")
      .get(id, id);
  }

  status(id: string) {
    const task = this.find(id);
    if (!task) {
      const denied = this.db.query<{ code: string; summary: string }, [string]>("SELECT code,summary FROM rejected_requests WHERE task_id=?").get(id);
      if (denied) return { taskId: id, status: "rejected", ...denied };
      return missing("Agent task not found");
    }
    const session = this.session(task.session_id);
    return { taskId: id, canonicalTaskId: task.task_id, sessionId: task.session_id, backendSessionId: session.backend_session_id,
      profile: task.profile, backend: task.backend, source: task.source, visibility: session.visibility, depth: task.depth,
      status: task.status, summary: task.summary, code: task.code, costUsd: task.cost_usd, createdAt: task.created_at, startedAt: task.started_at,
      elapsedMs: task.started_at ? (task.finished_at ?? Date.now()) - task.started_at : null };
  }

  budgetAvailable(at = Date.now()) {
    const usage = this.usage(at);
    if (usage.task_count >= this.config.dailyMaxTasks || usage.cost_usd >= this.config.dailyMaxBudgetUsd) {
      throw new Fault(429, "AGENT_BUDGET_LIMIT", "UTC daily Agent budget is exhausted");
    }
    return usage;
  }

  enqueue(input: TaskInput, origin: Origin, now = Date.now()) {
    return this.db.transaction(() => {
      const id = origin.taskId ?? randomUUID();
      if (this.find(id)) return this.status(id);
      const parent = origin.parent ? this.find(origin.parent) : undefined;
      if (origin.parent && !parent) bad("Parent Agent task is unknown");
      const depth = parent ? parent.depth + 1 : 0;
      if (depth > this.config.maxDepth) throw new Fault(429, "AGENT_DEPTH_LIMIT", "Maximum Agent task depth exceeded");
      if (input.dedupKey) {
        const canonical = this.db.query<{ task_id: string }, [string, string, number]>(
          "SELECT task_id FROM dedup_windows WHERE source_scope=? AND dedup_key=? AND expires_at>?")
          .get(origin.scope, input.dedupKey, now);
        if (canonical) {
          this.db.query("INSERT INTO task_aliases VALUES (?,?)").run(id, canonical.task_id);
          return this.status(id);
        }
      }
      const usage = this.budgetAvailable(now);
      const sessionId = input.resume ?? randomUUID();
      if (!input.resume) this.db.query("INSERT INTO sessions VALUES (?,?,NULL,?,?,?,?,?)")
        .run(sessionId, input.backend, input.profile, input.visibility, input.title ?? null, now, now);
      else if (this.db.query("SELECT 1 FROM tasks WHERE session_id=? AND status IN ('queued','running')").get(sessionId)) {
        throw new Fault(409, "SESSION_BUSY", "Session already has an active task");
      }
      this.db.query("INSERT INTO tasks(task_id,session_id,profile,backend,source,depth,parent_task_id,status,request_json,created_at,usage_day)" +
        "VALUES (?,?,?,?,?,?,?,'queued',?,?,?)")
        .run(id, sessionId, input.profile, input.backend, origin.source, depth, parent?.task_id ?? null, JSON.stringify(input), now, usage.day);
      this.db.query("INSERT INTO daily_usage(day,task_count) VALUES (?,1) ON CONFLICT(day) DO UPDATE SET task_count=task_count+1").run(usage.day);
      const cooldown = input.cooldownSec ?? this.config.dedupCooldownSec;
      if (input.dedupKey && cooldown > 0) {
        this.db.query("INSERT INTO dedup_windows VALUES (?,?,?,?) ON CONFLICT(source_scope,dedup_key) DO UPDATE SET task_id=excluded.task_id,expires_at=excluded.expires_at")
          .run(origin.scope, input.dedupKey, id, now + cooldown * 1000);
      }
      return this.status(id);
    }).immediate();
  }

  claim() {
    return this.db.transaction(() => {
      const active = this.db.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM tasks WHERE status='running'").get()!.n;
      if (active >= this.config.maxConcurrentTasks) return undefined;
      const task = this.db.query<Task, []>("SELECT * FROM tasks WHERE status='queued' ORDER BY created_at,task_id LIMIT 1").get();
      if (!task) return undefined;
      if (this.usage().cost_usd >= this.config.dailyMaxBudgetUsd) {
        this.db.query("UPDATE tasks SET status='failed',code='AGENT_BUDGET_LIMIT',finished_at=? WHERE status='queued'").run(Date.now());
        return undefined;
      }
      this.db.query("UPDATE tasks SET status='running',started_at=? WHERE task_id=?").run(Date.now(), task.task_id);
      return task;
    }).immediate();
  }

  finish(task: Task, result: { status: string; summary: string; code?: string; costUsd?: number }) {
    const cost = result.costUsd !== undefined && Number.isFinite(result.costUsd) && result.costUsd >= 0 ? result.costUsd : null;
    this.db.transaction(() => {
      const updated = this.db.query("UPDATE tasks SET status=?,summary=?,code=?,cost_usd=?,finished_at=? WHERE task_id=? AND status='running'")
        .run(result.status, result.summary, result.code ?? null, cost, Date.now(), task.task_id);
      if (updated.changes > 0) this.db.query("UPDATE daily_usage SET cost_usd=cost_usd+?,unmetered=unmetered+? WHERE day=?")
        .run(cost ?? 0, Number(cost === null), task.usage_day);
    }).immediate();
  }
}
