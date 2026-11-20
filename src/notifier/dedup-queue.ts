import type { Database } from "bun:sqlite";
import type { BusEvent } from "../shared/events";
import { sha256 } from "../shared/fs";
import type { Notice, VerifiedUpdate } from "./protocol";
import type { Channel } from "./settings";

export const notifierSchema = [
  "CREATE TABLE consumer_cursor(id INTEGER PRIMARY KEY CHECK(id=1), seq INTEGER NOT NULL); INSERT INTO consumer_cursor VALUES(1,0);",
  "CREATE TABLE notices(id INTEGER PRIMARY KEY, channel TEXT NOT NULL, note TEXT NOT NULL, count INTEGER NOT NULL DEFAULT 1,",
  "  attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL, sent INTEGER, failure TEXT);",
  "CREATE INDEX notices_due ON notices(channel,due) WHERE sent IS NULL;",
  "CREATE TABLE cooldown(key TEXT PRIMARY KEY, expires INTEGER NOT NULL, note_id INTEGER NOT NULL, suppressed INTEGER NOT NULL DEFAULT 0);",
  "CREATE TABLE callbacks(key TEXT PRIMARY KEY, proposal_id TEXT NOT NULL, digest TEXT NOT NULL, expires INTEGER NOT NULL);",
  "CREATE TABLE updates(channel TEXT NOT NULL, id TEXT NOT NULL, document TEXT NOT NULL, accepted INTEGER NOT NULL,",
  "  status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL, PRIMARY KEY(channel,id));",
  "CREATE INDEX incoming_due ON updates(status,due);",
].join("\n");

export class NoticeQueue {
  constructor(readonly db: Database, readonly channels: Channel[]) {}

  private insert(note: Notice, channels: Channel[], now = Date.now()) {
    for (const channel of channels) {
      const key = channel + ":" + (note.dedupKey ?? sha256(note.title + "\n" + note.body));
      const previous = this.db.query<{ expires: number; note_id: number; suppressed: number }, [string]>("SELECT * FROM cooldown WHERE key=?").get(key);
      if (previous && now < previous.expires) {
        const delivered = this.db.query("UPDATE notices SET count=count+1 WHERE id=? AND sent IS NULL").run(previous.note_id);
        if (!delivered.changes) this.db.query("UPDATE cooldown SET suppressed=suppressed+1 WHERE key=?").run(key);
        continue;
      }
      const { lastInsertRowid } = this.db.query("INSERT INTO notices(channel,note,count,due) VALUES (?,?,?,?)")
        .run(channel, JSON.stringify(note), 1 + (previous?.suppressed ?? 0), now);
      this.db.query("INSERT INTO cooldown VALUES (?,?,?,0) ON CONFLICT(key) DO UPDATE SET expires=excluded.expires,note_id=excluded.note_id,suppressed=0")
        .run(key, now + (note.cooldownSec ?? 60) * 1000, Number(lastInsertRowid));
    }
  }

  send(note: Notice, channels = this.channels, now = Date.now()) {
    this.db.transaction(() => this.insert(note, channels, now)).immediate();
    return { queued: channels.length > 0, channels };
  }

  get cursor() {
    return this.db.query<{ seq: number }, []>("SELECT seq FROM consumer_cursor").get()!.seq;
  }

  receive(event: BusEvent | undefined, through: number) {
    this.db.transaction(() => {
      if (through <= this.cursor) return;
      if (event) {
        const p = event.payload;
        if (event.topic === "sys.proposal") {
          const key = sha256(String(p.proposalId)).slice(0, 20);
          const expires = Number(p.expiresAt);
          if (expires > Date.now()) {
            this.db.query("INSERT INTO callbacks VALUES (?,?,?,?) ON CONFLICT(key) DO NOTHING").run(key, String(p.proposalId), String(p.proposalDigest), expires);
            const target = p.target as { plan?: { spec?: unknown; policy?: string; includeConditional?: boolean; resetState?: boolean; artifact?: { codeHash?: string } } };
            const summary = target.plan ? { ...target, plan: { ...target.plan, artifact: { codeHash: target.plan.artifact?.codeHash } } } : target;
            this.insert({ level: "warn", title: "Approval · " + p.action,
              body: JSON.stringify(summary, null, 2).slice(0, 2600) + "\n" + (p.reason ?? "") + "\nProposal " + p.proposalId,
              cardKey: key, proposalId: String(p.proposalId), expiresAt: expires, dedupKey: "proposal:" + key }, this.channels);
          }
        } else if (event.topic === "sys.notify") {
          const note = p as Notice;
          this.insert({ ...note, dedupKey: (p.runId ?? "core") + ":" + (note.dedupKey ?? sha256(note.title + "\n" + note.body)) }, this.channels);
        } else if (!event.topic.endsWith(".algo_order") || ["TRIGGERING", "TRIGGERED", "FINISHED", "REJECTED"].includes(String(p.status))) {
          const title = String(p.title ?? event.topic);
          const level = p.level === "critical" || event.topic === "sys.kill_switch" ? "critical" :
            p.level === "warn" || event.topic.includes("risk") ? "warn" : "info";
          const fallback = JSON.stringify(p).slice(0, 3500);
          const body = [p.mode, p.accountId, p.symbol, p.body ? String(p.body) + "\n" + fallback : fallback].filter(Boolean).join(" ");
          this.insert({ level, title, body, dedupKey: event.topic + ":" + [p.accountId, p.runId, p.symbol, p.code,
            p.tradeId, p.algoId, p.status, p.enabled, p.scope, p.title].join(":") }, this.channels);
        }
      }
      this.db.query("UPDATE consumer_cursor SET seq=? WHERE id=1").run(through);
    }).immediate();
  }

  inbound(channel: Channel, payload: VerifiedUpdate, now = Date.now()) {
    const accepted = this.db.query("INSERT INTO updates(channel,id,document,accepted,status,due) VALUES (?,?,?,?,'queued',?) ON CONFLICT DO NOTHING")
      .run(channel, payload.update, JSON.stringify(payload), now, now).changes > 0;
    return { accepted };
  }

  clean() {
    const now = Date.now();
    this.db.query("DELETE FROM cooldown WHERE expires<?").run(now - 86_400_000);
    this.db.query("DELETE FROM callbacks WHERE expires<?").run(now - 86_400_000);
    this.db.query("DELETE FROM notices WHERE sent<?").run(now - 7 * 86_400_000);
    this.db.query("DELETE FROM updates WHERE status IN ('done','failed') AND accepted<?").run(now - 7 * 86_400_000);
  }

  get pending() {
    return { deliveries: this.db.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM notices WHERE sent IS NULL").get()!.n,
      callbacks: this.db.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM updates WHERE status='queued'").get()!.n,
      failures: this.db.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM notices WHERE failure IS NOT NULL").get()!.n };
  }
}
