import { handler, missing } from "../shared/http";
import { parseFeishu } from "./feishu";
import { parseTelegram } from "./telegram";
import type { NoticeQueue } from "./dedup-queue";
import type { ImSettings } from "./settings";

export function webhook(settings: ImSettings, queue: NoticeQueue) {
  return handler(async (req, url) => {
    if (req.method !== "POST") return missing("Unknown webhook");
    if (url.pathname === "/webhooks/telegram" && settings.telegram) {
      const update = parseTelegram(settings.telegram, req.headers.get("x-telegram-bot-api-secret-token"), await req.text());
      if (!update) return { ok: true };
      const { accepted } = queue.inbound("telegram", update);
      if (update.reply) return { method: "answerCallbackQuery", callback_query_id: update.reply, text: accepted ? "Received" : "Already received" };
      return { ok: true };
    }
    if (url.pathname === "/webhooks/feishu" && settings.feishu) {
      const update = parseFeishu(settings.feishu, req.headers, await req.text());
      if (!update) return {};
      if ("challenge" in update) return update;
      const { accepted } = queue.inbound("feishu", update);
      return { toast: { type: "info", content: accepted ? "已受理" : "已受理过" } };
    }
    return missing("Unknown webhook");
  });
}
