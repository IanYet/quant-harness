import type { Task } from "@quant/tools";
import type { Config } from "./config";
import type { Store } from "./db";
import { HttpError, message, nameOf, requireThat, uid } from "./util";

export class Connectors {
  constructor(
    private db: Store,
    private cfg: Config,
  ) {}
  private async http(dest: { url: string; tokenEnv?: string }, method: string, path: string, payload?: unknown) {
    const token = dest.tokenEnv && process.env[dest.tokenEnv];
    requireThat(!dest.tokenEnv || token, "Missing connector credential", 503);
    const res = await fetch(dest.url.replace(/\/$/, "") + path, {
      method,
      signal: AbortSignal.timeout(this.cfg.timeoutMs),
      redirect: "error",
      headers: { "Content-Type": "application/json", ...(token ? { Authorization: "Bearer " + token } : {}) },
      body: payload === undefined ? undefined : JSON.stringify(payload),
    });
    if (!res.ok) throw new Error("Remote HTTP " + res.status);
    return res;
  }
  async notify(type: string, content: Record<string, unknown>) {
    nameOf(type);
    requireThat(content && typeof content === "object" && !Array.isArray(content), "Expected structured content");
    const id = uid(),
      cfg = this.cfg.destinations;
    this.db.run(
      "INSERT INTO notifications VALUES (?,?,?,?,?,?)",
      id,
      type,
      JSON.stringify(content),
      "pending",
      null,
      Date.now(),
    );
    try {
      const route = cfg.notifications?.[type],
        bot = route && cfg.bots?.[route.bot];
      requireThat(route && bot, "Notification route unavailable");
      const text = route.template.replace(/\{\{([\w.-]+)\}\}/g, (_, k: string) => String(content[k] ?? ""));
      const res = await this.http(bot, "POST", "", { type, target: route.target, text, content });
      await res.body?.cancel();
      this.db.run("UPDATE notifications SET state='delivered',detail=? WHERE id=?", String(res.status), id);
      return { id, state: "delivered" };
    } catch (err) {
      this.db.run("UPDATE notifications SET state='failed',detail=? WHERE id=?", message(err), id);
      throw new HttpError(502, `${id} ${message(err)}`);
    }
  }
  private task(id: string): Task {
    const r = this.db.one<Omit<Task, "result"> & { result: string }>(
      "SELECT id,backend,sessionId,state,result FROM tasks WHERE id=?",
      id,
    );
    requireThat(r, "Task not found", 404);
    return { ...r, result: JSON.parse(r.result) };
  }
  async agent(action: string, p: Record<string, string>) {
    const start = action === "startTask",
      id = start ? uid() : nameOf(p.id);
    const task = start ? undefined : this.task(id),
      backend = task?.backend ?? nameOf(p.backend);
    const dest = this.cfg.destinations.agents?.[backend];
    requireThat(dest, "Unknown agent backend", 404);
    if (start)
      this.db.run("INSERT INTO tasks VALUES (?,?,?,?,?,?)", id, backend, null, "starting", "null", Date.now());
    try {
      const send = action === "sendMessage";
      if (start || send) requireThat(typeof (start ? p.prompt : p.message) === "string", "Missing prompt/message");
      if (!start) requireThat(task?.sessionId, "Remote session unavailable");
      const path = start ? "/tasks" : "/tasks/" + encodeURIComponent(task!.sessionId!) + (send ? "/messages" : "");
      const payload = start ? { prompt: p.prompt, taskId: id } : send ? { message: p.message } : undefined;
      const r = (await (await this.http(dest, start || send ? "POST" : "GET", path, payload)).json()) as Record<
        string,
        unknown
      >;
      const session = task?.sessionId ?? r.sessionId;
      requireThat(typeof session === "string" && session && typeof r.state === "string", "Invalid agent response");
      this.db.run(
        "UPDATE tasks SET sessionId=?,state=?,result=? WHERE id=?",
        session,
        r.state,
        JSON.stringify(r.result ?? null),
        id,
      );
      return this.task(id);
    } catch (err) {
      this.db.run("UPDATE tasks SET state='error',result=? WHERE id=?", JSON.stringify(message(err)), id);
      throw new HttpError(502, `${id} ${message(err)}`);
    }
  }
}
