import { record, str } from "../shared/http";
import { equal, imJson, parseObject, unauthorized } from "./protocol";
import type { Notice, VerifiedUpdate } from "./protocol";
import type { TelegramSettings } from "./settings";

export function parseTelegram(settings: TelegramSettings, secret: string | null, body: string): VerifiedUpdate | null {
  if (!secret || !equal(secret, settings.webhookSecret)) unauthorized();
  const value = parseObject(body);
  const update = str(String(value.update_id), "update_id", /^\d+$/);
  if (value.callback_query) {
    const callback = record(value.callback_query);
    const from = String(record(callback.from).id);
    const chat = String(record(record(callback.message ?? {}).chat ?? {}).id);
    if (chat !== settings.chat || !settings.users.includes(from)) unauthorized();
    const data = /^([ar]):([a-f0-9]{20})$/.exec(String(callback.data));
    if (!data) return null;
    return { update, actor: "telegram:" + from, reply: str(callback.id, "callback id"),
      action: { decision: data[1] === "a" ? "approve" : "reject", key: data[2] } };
  }
  if (value.message) {
    const message = record(value.message);
    if (!/^\/health(?:@[A-Za-z0-9_]+)?$/.test(String(message.text))) return null;
    const from = String(record(message.from).id);
    if (String(record(message.chat).id) !== settings.chat || !settings.users.includes(from)) unauthorized();
    return { update, actor: "telegram:" + from, action: { command: "health" } };
  }
  return null;
}

export function sendTelegram(settings: TelegramSettings, notice: Notice) {
  const text = "[" + notice.level.toUpperCase() + "] " + notice.title + "\n" + notice.body +
    ((notice.count ?? 1) > 1 ? "\nOccurrences: " + notice.count : "");
  return imJson("https://api.telegram.org/bot" + settings.botToken + "/sendMessage", {
    chat_id: settings.chat, text: Array.from(text).slice(0, 4000).join(""),
    reply_markup: notice.cardKey ? { inline_keyboard: [[
      { text: "Approve", callback_data: "a:" + notice.cardKey },
      { text: "Reject", callback_data: "r:" + notice.cardKey },
    ]] } : undefined,
  });
}
