import { createDecipheriv, createHash } from "node:crypto";
import { oneOf, record, str } from "../shared/http";
import { equal, imJson, parseObject, unauthorized } from "./protocol";
import type { Notice, VerifiedUpdate } from "./protocol";
import type { FeishuSettings } from "./settings";

function requestTimeMs(value: string): number {
  if (/^\d{10}$/.test(value)) return Number(value) * 1000;
  if (/^\d{13}$/.test(value)) return Number(value);
  // Feishu card callbacks can send Go's time.Time.String format.
  const formatted = /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))? ([+-]\d{2})(\d{2}) \S+(?: m=[+-]\d+(?:\.\d+)?)?$/.exec(value);
  if (!formatted) return NaN;
  const [, date, clock, fraction = "", offsetHours, offsetMinutes] = formatted;
  const millis = fraction.slice(0, 3).padEnd(3, "0");
  return Date.parse(`${date}T${clock}.${millis}${offsetHours}:${offsetMinutes}`);
}

export function parseFeishu(settings: FeishuSettings, headers: Headers, body: string, now = Date.now()): VerifiedUpdate | { challenge: string } | null {
  const ts = headers.get("x-lark-request-timestamp") ?? "";
  const nonce = headers.get("x-lark-request-nonce") ?? "";
  const signed = headers.get("x-lark-signature") ?? "";
  const validTime = Math.abs(now - requestTimeMs(ts)) <= 5 * 60_000;
  const expected = createHash("sha256").update(ts + nonce + settings.encryptKey + body).digest("hex");
  if (signed && (!validTime || !nonce || !equal(signed, expected))) unauthorized();
  let value = parseObject(body);
  if (typeof value.encrypt === "string") {
    try {
      const bytes = Buffer.from(value.encrypt, "base64");
      const decipher = createDecipheriv("aes-256-cbc", createHash("sha256").update(settings.encryptKey).digest(), bytes.subarray(0, 16));
      value = parseObject(Buffer.concat([decipher.update(bytes.subarray(16)), decipher.final()]).toString("utf8"));
    } catch { return unauthorized(); }
  }
  if (value.type === "url_verification") {
    if (!equal(String(value.token), settings.verificationToken)) unauthorized();
    return { challenge: str(value.challenge, "challenge") };
  }
  if (!signed || !validTime) unauthorized();
  const header = record(value.header);
  if (header.app_id !== settings.appId || !equal(String(header.token), settings.verificationToken)) unauthorized();
  if (header.event_type !== "card.action.trigger") return null;
  const event = record(value.event);
  const from = String(record(event.operator).open_id);
  if (record(event.context).open_chat_id !== settings.chat || !settings.users.includes(from)) unauthorized();
  const action = record(record(event.action).value);
  return { update: str(header.event_id, "event_id"), actor: "feishu:" + from,
    action: { decision: oneOf(action.decision, "decision", ["approve", "reject"]), key: str(action.key, "callback key", /^[a-f0-9]{20}$/) } };
}

export class FeishuBot {
  private token?: { value: string; expires: number };
  private flight?: Promise<string>;
  constructor(private readonly config: FeishuSettings) {}

  private async access() {
    if (this.token && Date.now() < this.token.expires) return this.token.value;
    this.flight ??= imJson("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", {
      app_id: this.config.appId, app_secret: this.config.appSecret,
    }).then(data => {
      const value = str(data.tenant_access_token, "tenant access token");
      this.token = { value, expires: Date.now() + Math.max(0, Number(data.expire ?? 7200) - 90) * 1000 };
      return value;
    }).finally(() => { this.flight = undefined; });
    return this.flight;
  }

  async send(notice: Notice) {
    const button = (decision: string, label: string, type: string) => ({
      tag: "button", text: { tag: "plain_text", content: label }, type,
      behaviors: [{ type: "callback", value: { decision, key: notice.cardKey } }],
    });
    const content = notice.body + ((notice.count ?? 1) > 1 ? "\nOccurrences: " + notice.count : "");
    const card = notice.cardKey ? {
      schema: "2.0",
      header: { title: { tag: "plain_text", content: notice.title }, template: "orange" },
      body: { elements: [
        { tag: "div", text: { tag: "plain_text", content: content.slice(0, 5800) } },
        button("approve", "Approve", "primary"), button("reject", "Reject", "danger"),
      ] },
    } : { text: "[" + notice.level.toUpperCase() + "] " + notice.title + "\n" + content };
    const token = await this.access();
    try {
      return await imJson("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id", {
        receive_id: this.config.chat, msg_type: notice.cardKey ? "interactive" : "text", content: JSON.stringify(card),
      }, { authorization: "Bearer " + token });
    } catch (error) {
      this.token = undefined;
      throw error;
    }
  }
}
