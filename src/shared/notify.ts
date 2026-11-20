import type { NotifyRequest } from "@quant/tools";
import { integer, oneOf, record, str } from "./http";

export function notification(value: unknown): NotifyRequest {
  const v = record(value);
  return {
    level: oneOf(v.level, "level", ["info", "warn", "critical"]),
    title: str(v.title, "title", /^[\s\S]{1,200}$/),
    body: str(v.body, "body", /^[\s\S]{1,6000}$/),
    dedupKey: v.dedupKey === undefined ? undefined : str(v.dedupKey, "dedupKey", /^[\s\S]{1,160}$/),
    cooldownSec: v.cooldownSec === undefined ? undefined : integer(v.cooldownSec, "cooldownSec", 0, 86_400),
  };
}
