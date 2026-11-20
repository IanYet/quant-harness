import type { Database } from "bun:sqlite";
import { FeishuBot } from "../notifier/feishu";
import type { Notice } from "../notifier/protocol";
import type { ImSettings, Channel } from "../notifier/settings";
import { sendTelegram } from "../notifier/telegram";
import type { Issue } from "./probes";

export class FallbackAlerts {
  private readonly feishu?: FeishuBot;
  private readonly channels: Channel[];
  constructor(private readonly im: ImSettings, private readonly db: Database) {
    this.feishu = im.feishu && new FeishuBot(im.feishu);
    this.channels = [...(im.telegram ? ["telegram" as const] : []), ...(im.feishu ? ["feishu" as const] : [])];
  }

  update(issues: Issue[], now = Date.now()) {
    const current = new Map(issues.map(issue => [issue.key, issue]));
    const previous = this.db.query<{ key: string; message: string; next_send: number }, []>("SELECT * FROM incidents").all();
    this.db.transaction(() => {
      for (const item of previous) if (!current.has(item.key)) {
        this.enqueue({ level: "info", title: "Quant Harness recovered", body: item.message }, now);
        this.db.query("DELETE FROM incidents WHERE key=?").run(item.key);
      }
      const ready = new Map(previous.map(item => [item.key, item.next_send]));
      for (const issue of current.values()) {
        if (now < (ready.get(issue.key) ?? 0)) continue;
        this.enqueue({ level: issue.level, title: "Quant Harness watchdog", body: issue.message }, now);
        this.db.query("INSERT INTO incidents VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET message=excluded.message,next_send=excluded.next_send")
          .run(issue.key, issue.message, now + 300_000);
      }
    }).immediate();
  }

  private enqueue(note: Notice, now: number) {
    console.log(note.title, note.body);
    for (const channel of this.channels) this.db.query("INSERT INTO fallback_outbox(channel,note,due) VALUES (?,?,?)").run(channel, JSON.stringify(note), now);
  }

  async deliver(signal: AbortSignal) {
    while (!signal.aborted) {
      const row = this.db.query<{ id: number; channel: Channel; note: string; attempts: number }, [number]>(
        "SELECT * FROM fallback_outbox WHERE due<=? ORDER BY due,id LIMIT 1").get(Date.now());
      if (!row) { await Bun.sleep(500); continue; }
      try {
        const note = JSON.parse(row.note) as Notice;
        if (row.channel === "feishu" && this.feishu) await this.feishu.send(note);
        if (row.channel === "telegram" && this.im.telegram) await sendTelegram(this.im.telegram, note);
        this.db.query("DELETE FROM fallback_outbox WHERE id=?").run(row.id);
      } catch (error) {
        console.warn("Independent alert delivery delayed", row.channel, (error as Error).message);
        this.db.query("UPDATE fallback_outbox SET attempts=attempts+1,due=? WHERE id=?")
          .run(Date.now() + Math.min(300, 2 ** Math.min(row.attempts + 1, 10)) * 1000, row.id);
      }
    }
  }
}
