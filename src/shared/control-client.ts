import { userInfo } from "node:os";
import { serviceAuth } from "./auth";
import { dataPath, socketPath } from "./config";
import type { Service } from "./config";
import { call, withQuery } from "./uds";

export type ControlRequest = { service: Service; path: string; method?: string; body?: unknown; params?: Record<string, unknown>; timeoutMs?: number };
export type ControlSource = "cli" | "mcp";

export const get = (service: Service, path: string, params?: Record<string, unknown>, timeoutMs?: number): ControlRequest =>
  ({ service, path, params, timeoutMs });
export const post = (service: Service, path: string, body: unknown = {}, timeoutMs?: number): ControlRequest =>
  ({ service, path, method: "POST", body, timeoutMs });
export const remove = (service: Service, path: string, body: unknown): ControlRequest => ({ service, path, method: "DELETE", body, timeoutMs: 90_000 });
export const patch = (service: Service, path: string, body: unknown): ControlRequest => ({ service, path, method: "PATCH", body });

const component = encodeURIComponent;
export const liveProposal = (data: unknown) => post("admin", "/v1/live/propose", data, 100_000);
export const runStart = (data: unknown) => post("admin", "/v1/runs/start", data, 70_000);
export const runStop = (runId: string, data: unknown) => post("admin", "/v1/runs/" + component(runId) + "/stop", data, 100_000);
export const approval = (id: string, decision: "approve" | "reject", data: unknown) =>
  post("admin", "/v1/proposals/" + component(id) + "/" + decision, data, 120_000);
export const manualTrade = (id: string, action: "order" | "cancel" | "cancel-all" | "leverage", data: unknown) => {
  const base = "/v1/accounts/" + component(id) + "/manual-";
  if (action === "cancel" || action === "cancel-all") return remove("admin", base + (action === "cancel" ? "order" : "orders"), data);
  return post("admin", base + action, data, 90_000);
};
export const killSwitch = (enabled: boolean, data: unknown) => enabled ? post("admin", "/v1/risk/kill-switch", data, 120_000) :
  remove("admin", "/v1/risk/kill-switch", data);
export const strategySeal = (data: unknown) => post("storage", "/v1/strategies/seal", data, 90_000);
export const strategyTag = (id: string, data: unknown) => patch("storage", "/v1/strategies/" + component(id) + "/tags", data);
export const backtestRun = (data: unknown) => post("storage", "/v1/backtest/run", data, 30 * 60_000);
export const taskStart = (data: unknown) => post("agent", "/v1/tasks", data);
export const notifySend = (data: unknown) => post("notifier", "/v1/notify", data);

export async function controlClient(source: ControlSource, dir = dataPath()) {
  const auth = await serviceAuth(dir);
  const token = auth.token(source);
  const actor = process.env.QUANT_ACTOR ?? userInfo().username;
  return <T = unknown>(request: ControlRequest) => call<T>(socketPath(dir, request.service), withQuery(request.path, request.params), {
    method: request.method, body: request.body, timeoutMs: request.timeoutMs, token,
    headers: { "x-quant-actor": actor, ...(process.env.QUANT_PARENT_TASK ? { "x-quant-parent-task": process.env.QUANT_PARENT_TASK } : {}) },
  });
}
