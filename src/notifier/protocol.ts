import { timingSafeEqual } from "node:crypto";
import { bad, Fault, record } from "../shared/http";

export type WebhookAction =
  | { decision: "approve" | "reject"; key: string }
  | { command: "health" };

export type VerifiedUpdate = { update: string; actor: string; action: WebhookAction; reply?: string };
export type Notice = { level: "info" | "warn" | "critical"; title: string; body: string; dedupKey?: string; cooldownSec?: number;
  cardKey?: string; proposalId?: string; expiresAt?: number; count?: number };

export const unauthorized = (): never => { throw new Fault(401, "UNAUTHORIZED", "Webhook verification failed"); };
export function equal(left: string, right: string) {
  const a = Buffer.from(left), b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}
export function parseObject(text: string) {
  try { return record(JSON.parse(text)); } catch { return bad("Malformed webhook JSON"); }
}

export async function imJson(url: string, payload: unknown, headers: Record<string, string> = {}) {
  let response: Response;
  try {
    response = await fetch(url, { method: "POST", redirect: "error", signal: AbortSignal.timeout(8500),
      headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(payload) });
  } catch { throw new Error("IM provider did not respond"); }
  const data = await response.json().catch(() => ({})) as Record<string, unknown>;
  if (!response.ok || data.ok === false || (typeof data.code === "number" && data.code !== 0)) {
    throw new Error("IM provider response " + response.status + "/" + String(data.code ?? data.error_code ?? ""));
  }
  return data;
}
