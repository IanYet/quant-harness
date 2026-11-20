import { join } from "node:path";
import { serviceAuth } from "../shared/auth";
import { config, initLayout, socketPath } from "../shared/config";
import { openDb } from "../shared/db";
import type { BusFrame } from "../shared/events";
import { body, Fault, handler, missing } from "../shared/http";
import { notification } from "../shared/notify";
import { call, readNdjson, serveUnix, unixFetch, withQuery } from "../shared/uds";
import { NoticeQueue, notifierSchema } from "./dedup-queue";
import { FeishuBot } from "./feishu";
import type { Notice, VerifiedUpdate } from "./protocol";
import { imSettings } from "./settings";
import type { Channel } from "./settings";
import { sendTelegram } from "./telegram";
import { webhook } from "./webhook-server";

const settings = config();
await initLayout(settings.dataDir);
const auth = await serviceAuth(settings.dataDir);
const im = imSettings(settings);
const channels: Channel[] = [];
if (im.telegram) channels.push("telegram");
if (im.feishu) channels.push("feishu");
const bot = im.feishu && new FeishuBot(im.feishu);
const db = openDb(join(settings.dataDir, "db/notifier.db"), notifierSchema, 0o600);
const queue = new NoticeQueue(db, channels);
const admin = socketPath(settings.dataDir, "admin");
const token = auth.token("notifier");
const abort = new AbortController();
for (const signal of ["SIGTERM", "SIGINT"] as const) process.once(signal, () => abort.abort());

async function subscribe() {
  let ack = 0;
  while (!abort.signal.aborted) {
    try {
      const res = await unixFetch(admin, withQuery("/v1/stream", { after_seq: queue.cursor }),
        { headers: { authorization: "Bearer " + token }, signal: abort.signal });
      for await (const frame of readNdjson<BusFrame>(res)) {
        if ("seq" in frame) queue.receive(frame, frame.seq);
        if ("type" in frame && frame.type === "watermark") {
          queue.receive(undefined, frame.throughSeq);
          if (queue.cursor > ack) {
            await call(admin, "/v1/stream/ack", { token, method: "POST", body: { throughSeq: queue.cursor } });
            ack = queue.cursor;
          }
        }
      }
    } catch (error) { if (!abort.signal.aborted) console.warn("Notifier reconnecting", (error as Error).message); }
    if (!abort.signal.aborted) await Bun.sleep(1000);
  }
}

async function deliver(channel: Channel) {
  while (!abort.signal.aborted) {
    const row = db.query<{ id: number; note: string; count: number; attempts: number }, [string, number]>(
      "SELECT id,note,count,attempts FROM notices WHERE channel=? AND sent IS NULL AND due<=? ORDER BY due,id LIMIT 1").get(channel, Date.now());
    if (!row) { await Bun.sleep(300); continue; }
    try {
      const notice = { ...JSON.parse(row.note) as Notice, count: row.count };
      if (!notice.expiresAt || notice.expiresAt > Date.now()) {
        if (channel === "telegram") await sendTelegram(im.telegram!, notice);
        else await bot!.send(notice);
      }
      db.query("UPDATE notices SET sent=?,failure=NULL WHERE id=?").run(Date.now(), row.id);
    } catch (error) {
      const attempts = row.attempts + 1;
      db.query("UPDATE notices SET attempts=?,due=?,failure=? WHERE id=?")
        .run(attempts, Date.now() + Math.min(3600, 2 ** Math.min(attempts, 12)) * 1000, (error as Error).message, row.id);
    }
  }
}

async function callbacks() {
  while (!abort.signal.aborted) {
    const row = db.query<{ channel: Channel; id: string; document: string; attempts: number }, [number]>(
      "SELECT channel,id,document,attempts FROM updates WHERE status='queued' AND due<=? ORDER BY due,accepted LIMIT 1").get(Date.now());
    if (!row) { await Bun.sleep(100); continue; }
    const request = JSON.parse(row.document) as VerifiedUpdate;
    let title = "Webhook action complete", status = "done", summary = "";
    try {
      if ("command" in request.action) summary = JSON.stringify(await call(admin, "/v1/health", { token })).slice(0, 3500);
      else {
        const proposal = db.query<{ proposal_id: string; digest: string; expires: number }, [string]>("SELECT * FROM callbacks WHERE key=?").get(request.action.key);
        if (!proposal || proposal.expires < Date.now()) throw new Fault(409, "PROPOSAL_CONFLICT", "Approval card has expired");
        const result = await call(admin, "/v1/proposals/" + proposal.proposal_id + "/" + request.action.decision, {
          token, method: "POST", timeoutMs: 90_000, body: { actor: request.actor, proposalDigest: proposal.digest,
            confirmationRef: row.channel + ":" + row.id },
        });
        summary = JSON.stringify(result).slice(0, 3500);
      }
    } catch (error) {
      const transient = error instanceof Fault && error.code === "UNAVAILABLE";
      if (transient && row.attempts < 8) {
        db.query("UPDATE updates SET attempts=attempts+1,due=? WHERE channel=? AND id=?")
          .run(Date.now() + 1000 * 2 ** row.attempts, row.channel, row.id);
        continue;
      }
      title = "Webhook action failed";
      summary = (error as Error).message;
      status = "failed";
    }
    db.transaction(() => {
      queue.send({ level: status === "done" ? "info" : "warn", title, body: request.actor + "\n" + summary,
        dedupKey: "callback:" + row.channel + ":" + row.id }, [row.channel]);
      db.query("UPDATE updates SET status=? WHERE channel=? AND id=?").run(status, row.channel, row.id);
    }).immediate();
  }
}

const server = await serveUnix(socketPath(settings.dataDir, "notifier"), handler(async (req, url) => {
  if (req.method === "GET" && url.pathname === "/v1/health") return { ok: true, pid: process.pid, channels, lastSeq: queue.cursor, ...queue.pending };
  if (req.method === "POST" && url.pathname === "/v1/notify") {
    auth.require(req, ["cli", "mcp", "agent", "watchdog"]);
    return queue.send(notification(await body(req)));
  }
  return missing("Unknown notifier endpoint");
}));
const incoming = channels.length ? Bun.serve({ hostname: im.host, port: im.port, maxRequestBodySize: 256 * 1024, fetch: webhook(im, queue) }) : undefined;
const clean = setInterval(() => queue.clean(), 60_000);
console.log("quant-notifier listening on " + server.url);
await Promise.all([subscribe(), callbacks(), ...channels.map(deliver)]);
clearInterval(clean);
await Promise.all([server.stop(true), incoming?.stop(true)]);
db.close();
