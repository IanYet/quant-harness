#!/usr/bin/env bun
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import type { ControlRequest } from "../shared/control-client";
import {
  approval, backtestRun, controlClient, killSwitch, liveProposal, manualTrade, notifySend, runStart, runStop,
  strategySeal, strategyTag, taskStart,
} from "../shared/control-client";
import { bad, Fault, flag, oneOf, record, str } from "../shared/http";

const string = { type: "string", minLength: 1 };
const boolean = { type: "boolean" };
const positive = { type: "number", exclusiveMinimum: 0 };
const timestamp = { type: "integer", minimum: 1 };
const strings = { type: "array", items: string };
const enumOf = (...values: string[]) => ({ type: "string", enum: values });
const obj = (properties: Record<string, unknown>, required: string[] = []) =>
  ({ type: "object" as const, properties, required, additionalProperties: false });
const capabilities = obj({ accountRead: { oneOf: [strings, { const: "*" }] }, publishDatasets: strings, notify: boolean, agentTask: boolean });
const reason = { reason: string, confirmationRef: string };
const tags = obj({ regime: strings, style: strings, account_fit: strings, factor: strings });
const lifecycle = enumOf("backtested", "shadow", "live", "shelved", "deprecated");
const visibility = enumOf("visible", "hidden", "ephemeral");

export const tools = [
  { name: "live_propose", description: "Propose a sealed live strategy and its order handover policy.",
    inputSchema: obj({ accountId: string, strategyId: string, handoverOrders: enumOf("adopt", "cancel_open"),
      includeConditional: boolean, resetState: boolean, capabilities, ...reason }, ["accountId", "strategyId", "reason"]) },
  { name: "trade_manual", description: "Submit one authorized account operation through risk checks and the account approval policy.",
    inputSchema: obj({ accountId: string, operation: enumOf("order", "cancel", "cancel-all", "leverage"), action: enumOf("openLong", "closeLong", "openShort", "closeShort"),
      symbol: string, params: obj({ type: enumOf("MARKET", "LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT", "TRAILING_STOP_MARKET"),
        qty: positive, clientOrderId: { type: "string", pattern: "^[A-Za-z0-9_-]{1,16}$" },
        symbol: string, price: positive, triggerPrice: positive, timeInForce: enumOf("GTC", "IOC", "FOK", "GTX"),
        workingType: enumOf("MARK_PRICE", "CONTRACT_PRICE"), callbackRate: positive, activationPrice: positive, priceProtect: boolean,
        protection: enumOf("take_profit", "stop_loss") }, ["type", "qty", "clientOrderId"]),
      orderId: string, algoId: string, clientOrderId: string, includeConditional: boolean, leverage: { type: "integer", minimum: 1 }, ...reason },
    ["accountId", "operation"]) },
  { name: "proposals_decide", description: "Approve the exact proposal digest or reject a pending proposal.",
    inputSchema: obj({ proposalId: string, decision: enumOf("approve", "reject"), proposalDigest: string, ...reason }, ["proposalId", "decision"]) },
  { name: "runs_start", description: "Start a worker with scoped capabilities or run a draft or sealed strategy on Demo.",
    inputSchema: obj({ mode: enumOf("worker", "shadow"), entryPath: string, strategyId: string, draftPath: string,
      persistent: boolean, stateName: string, capabilities, ...reason }, ["mode"]) },
  { name: "runs_stop", description: "Stop a managed run using its ownership-aware shutdown policy.",
    inputSchema: obj({ runId: string, ...reason }, ["runId"]) },
  { name: "strategies_seal", description: "Validate, pin the SDK, and publish an immutable strategy version.",
    inputSchema: obj({ draftPath: string, family: string, summary: string, parentId: string, tags, lifecycle, capabilities },
      ["draftPath", "family", "summary"]) },
  { name: "strategies_tag", description: "Update strategy tags, lifecycle, or research notes.",
    inputSchema: obj({ strategyId: string, tags, lifecycle, note: string, author: enumOf("user", "human", "cron_review", "script_triggered") }, ["strategyId"]) },
  { name: "backtest_run", description: "Replay the pinned strategy on closed Binance mainnet history.",
    inputSchema: obj({ strategyId: string, draftPath: string, symbols: strings, start: timestamp, end: timestamp,
      timeoutSec: positive, params: { type: "object" } }, ["start", "end"]) },
  { name: "risk_kill_switch", description: "Apply or clear an account or global opening-order block with optional exchange cancellation.",
    inputSchema: obj({ scope: string, enabled: boolean, cancelOpenOrders: boolean, includeConditional: boolean, ...reason }, ["scope", "enabled"]) },
  { name: "agents_start_task", description: "Queue a bounded Agent task with sidebar visibility and deduplication.",
    inputSchema: obj({ prompt: string, title: string, backend: string, sidebarVisibility: visibility,
      dedupKey: string, cooldownSec: { type: "integer", minimum: 0 }, profile: enumOf("user", "cron_review", "script_triggered") }, ["prompt"]) },
  { name: "notify_send", description: "Queue a notification for the enabled Telegram and Feishu channels.",
    inputSchema: obj({ level: enumOf("info", "warn", "critical"), title: string, body: string, dedupKey: string,
      cooldownSec: { type: "integer", minimum: 0 } }, ["level", "title", "body"]) },
];

export function requestFor(name: string, v: Record<string, unknown>): ControlRequest {
  switch (name) {
    case "live_propose": return liveProposal(v);
    case "trade_manual": return manualTrade(str(v.accountId, "accountId"),
      oneOf(v.operation, "operation", ["order", "cancel", "cancel-all", "leverage"]), v);
    case "proposals_decide": return approval(str(v.proposalId, "proposalId"), oneOf(v.decision, "decision", ["approve", "reject"]), v);
    case "runs_start": return runStart(v);
    case "runs_stop": return runStop(str(v.runId, "runId"), v);
    case "strategies_seal": return strategySeal(v);
    case "strategies_tag": return strategyTag(str(v.strategyId, "strategyId"), v);
    case "backtest_run": return backtestRun(v);
    case "risk_kill_switch": return killSwitch(flag(v.enabled, "enabled"), v);
    case "agents_start_task": return taskStart(v);
    case "notify_send": return notifySend(v);
    default: return bad("Unknown MCP tool");
  }
}

if (import.meta.main) {
  const server = new Server({ name: "quant-harness", version: "0.1.0" }, { capabilities: { tools: {} } });
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));
  let client: Awaited<ReturnType<typeof controlClient>> | undefined;
  server.setRequestHandler(CallToolRequestSchema, async request => {
    try {
      const args = record(request.params.arguments ?? {});
      const schema = tools.find(tool => tool.name === request.params.name)?.inputSchema ?? bad("Unknown MCP tool");
      if (schema.required.some(name => !Object.hasOwn(args, name))) bad("Missing required MCP argument");
      if (Object.keys(args).some(name => !Object.hasOwn(schema.properties, name))) bad("Unknown MCP argument");
      client ??= await controlClient("mcp");
      const result = await client(requestFor(request.params.name, args));
      return { content: [{ type: "text", text: JSON.stringify(result) }] };
    } catch (error) {
      const fault = error instanceof Fault ? { code: error.code, message: error.message, details: error.details } : { code: "ERROR", message: (error as Error).message };
      return { isError: true, content: [{ type: "text", text: JSON.stringify({ error: fault }) }] };
    }
  });
  await server.connect(new StdioServerTransport());
}
