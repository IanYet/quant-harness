import type { Database } from "bun:sqlite";
import { bad, oneOf, record, str } from "../shared/http";
import type { Guardrails } from "./guardrails";
import { visibilities } from "./guardrails";

function field(raw: string, min: number, max: number) {
  const values = new Set<number>();
  for (const part of raw.split(",")) {
    const hit = /^(\*|\d{1,2}(?:-\d{1,2})?)(?:\/(\d{1,2}))?$/.exec(part) ?? bad("Invalid cron field " + raw);
    const [start, end] = hit[1] === "*" ? [min, max] : hit[1].split("-").map(Number);
    const step = hit[2] === undefined ? 1 : Number(hit[2]);
    const stop = end ?? (hit[2] === undefined ? start : max);
    if (start < min || stop > max || stop < start || step < 1) bad("Cron field out of range " + raw);
    for (let i = start; i <= stop; i += step) values.add(i);
  }
  return values;
}

export function cronMatcher(expression: string) {
  const words = expression.trim().split(/\s+/);
  if (words.length !== 5) bad("Cron requires five UTC fields");
  const [minutes, hours, days, months, weekdays] = words.map((word, i) => field(word, [0, 0, 1, 1, 0][i], [59, 23, 31, 12, 7][i]));
  if (weekdays.has(7)) weekdays.add(0);
  return (now: Date) => {
    const dom = days.has(now.getUTCDate()), dow = weekdays.has(now.getUTCDay());
    const date = words[2].startsWith("*") || words[4].startsWith("*") ? dom && dow : dom || dow;
    return date && minutes.has(now.getUTCMinutes()) && hours.has(now.getUTCHours()) && months.has(now.getUTCMonth() + 1);
  };
}

export function scheduler(settings: unknown, db: Database, guards: Guardrails, backend: string) {
  const raw = settings ?? [];
  if (!Array.isArray(raw)) bad("Invalid cron list");
  const jobs = raw.map(item => {
    const v = record(item);
    const name = str(v.name, "cron name");
    const test = cronMatcher(str(v.expr, "cron expression"));
    return { name, test, input: guards.input({ backend: v.backend, title: name,
      visibility: oneOf(v.visibility ?? "visible", "visibility", visibilities),
      prompt: v.prompt ?? "复盘本次 " + name + " 周期。用只读 SQLite 与 quant 汇总各账户绩效、成交滑点、资金费、Checkpoint 与采集健康。" +
        "根据主网行情评估当前 regime，检索匹配的 shelved 策略，给相关版本追加有依据的归因笔记，输出简洁复盘摘要。" }, backend, "cron_review") };
  });
  if (new Set(jobs.map(job => job.name)).size !== jobs.length) bad("Cron names must be unique");
  return (now = Date.now()) => {
    const date = new Date(now);
    const slot = Math.floor(now / 60_000);
    for (const job of jobs) if (job.test(date)) {
      try {
        db.transaction(() => {
          const added = db.query("INSERT INTO cron_runs VALUES (?,?) ON CONFLICT DO NOTHING").run(job.name, slot).changes;
          if (added) guards.enqueue(job.input, { source: "cron", scope: "cron:" + job.name }, now);
        }).immediate();
      } catch (error) {
        const added = db.query("INSERT INTO cron_runs VALUES (?,?) ON CONFLICT DO NOTHING").run(job.name, slot).changes;
        if (added) console.warn("Cron task could not be queued", job.name, (error as Error).message);
      }
    }
  };
}
