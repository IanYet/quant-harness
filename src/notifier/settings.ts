import { readSecret, type HarnessConfig } from "../shared/config";
import { flag, integer, record, str, stringList } from "../shared/http";

export type Channel = "telegram" | "feishu";
export type TelegramSettings = { botToken: string; webhookSecret: string; chat: string; users: string[] };
export type FeishuSettings = { appId: string; appSecret: string; encryptKey: string; verificationToken: string; chat: string; users: string[] };

export function imSettings(config: HarnessConfig) {
  const im = config.im;
  const listen = record(im.notifierListen ?? {});
  const t = record(im.telegram ?? {});
  const f = record(im.feishu ?? {});
  const telegramEnabled = flag(t.enabled, "telegram.enabled");
  const feishuEnabled = flag(f.enabled, "feishu.enabled");
  const useSharedFile = (telegramEnabled && t.credentialFile === undefined) || (feishuEnabled && f.credentialFile === undefined);
  const secrets = useSharedFile ? readSecret(config.secretsDir, "secrets.im.json") : {};
  const ts = telegramEnabled && t.credentialFile !== undefined ?
    readSecret(config.secretsDir, str(t.credentialFile, "telegram.credentialFile")) : record(secrets.telegram ?? {});
  const fs = feishuEnabled && f.credentialFile !== undefined ?
    readSecret(config.secretsDir, str(f.credentialFile, "feishu.credentialFile")) : record(secrets.feishu ?? {});
  const telegramChat = t.alertChatId === undefined || t.alertChatId === "" ? ts.chatId : t.alertChatId;
  const telegram: TelegramSettings | undefined = !telegramEnabled ? undefined : {
    chat: str(typeof telegramChat === "number" && Number.isSafeInteger(telegramChat) ? String(telegramChat) : telegramChat,
      "telegram.alertChatId", /^-?\d+$/),
    users: stringList(t.allowedAdminUserIds ?? [], "telegram.allowedAdminUserIds", /^\d+$/),
    botToken: str(ts.botToken, "telegram.botToken", /^\d+:[A-Za-z0-9_-]+$/),
    webhookSecret: str(ts.webhookSecret, "telegram.webhookSecret", /^[A-Za-z0-9_-]{1,256}$/),
  };
  const feishu: FeishuSettings | undefined = !feishuEnabled ? undefined : {
    chat: str(f.alertChatId, "feishu.alertChatId", /^oc_[A-Za-z0-9_-]+$/),
    users: stringList(f.allowedAdminOpenIds ?? [], "feishu.allowedAdminOpenIds", /^ou_[A-Za-z0-9_-]+$/),
    appId: str(fs.appId, "feishu.appId"), appSecret: str(fs.appSecret, "feishu.appSecret"),
    encryptKey: str(fs.encryptKey, "feishu.encryptKey"), verificationToken: str(fs.verificationToken, "feishu.verificationToken"),
  };
  return { host: str(listen.host ?? "127.0.0.1", "notifierListen.host"), port: integer(listen.port ?? 18101, "notifierListen.port", 1, 65535),
    telegram, feishu };
}
export type ImSettings = ReturnType<typeof imSettings>;
