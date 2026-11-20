#!/usr/bin/env bun
import type { ControlRequest } from "../shared/control-client";
import {
  approval, backtestRun, controlClient, get, killSwitch, liveProposal, manualTrade, notifySend,
  post, runStart, runStop, strategySeal, strategyTag, taskStart,
} from "../shared/control-client";
import type { Service } from "../shared/config";
import { bad, Fault, oneOf, str } from "../shared/http";

const booleanFlags = new Set(["--json", "--backfill", "--persistent", "--include-conditional", "--cancel-open", "--reset-state", "--no-notify", "--no-agent", "--price-protect"]);
const component = encodeURIComponent;
const target = (value: string) => value.includes("@") && !value.includes("/") ? { strategyId: value } : { draftPath: value };

class Options {
  readonly words: string[] = [];
  private readonly options = new Map<string, string | true>();

  constructor(args: string[]) {
    for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      if (!arg.startsWith("--")) { this.words.push(arg); continue; }
      const [name, inline] = arg.split(/=(.*)/s, 2);
      if (this.options.has(name)) bad("Repeated option " + name);
      const value = inline ?? (booleanFlags.has(name) ? true : args[++i]);
      if (value === undefined || (inline === undefined && typeof value === "string" && value.startsWith("--"))) bad("Missing value for " + name);
      this.options.set(name, value);
    }
  }

  word(name: string) { return str(this.words.shift(), name); }
  text(name: string) {
    const value = this.options.get("--" + name);
    this.options.delete("--" + name);
    return value === undefined ? undefined : str(value, name);
  }
  bool(name: string) {
    const value = this.options.get("--" + name);
    this.options.delete("--" + name);
    if (value === undefined) return undefined;
    return value === true || oneOf(value, name, ["true", "false"]) === "true";
  }
  number(name: string) {
    const value = this.text(name);
    if (value === undefined) return undefined;
    if (!Number.isFinite(Number(value))) bad("Invalid " + name);
    return Number(value);
  }
  time(name: string) {
    const value = this.text(name);
    if (value === undefined) return undefined;
    const time = /^\d+$/.test(value) ? Number(value) : Date.parse(value);
    if (!Number.isSafeInteger(time) || time < 0) bad("Invalid " + name + " date");
    return time;
  }
  list(name: string) { return this.text(name)?.split(",").map(item => item.trim()).filter(Boolean); }
  json(name: string) {
    const value = this.text(name);
    if (value === undefined) return undefined;
    try { return JSON.parse(value) as unknown; } catch { return bad("Invalid JSON for " + name); }
  }
  done() { if (this.words.length || this.options.size) bad("Unexpected arguments " + [...this.words, ...this.options.keys()].join(" ")); }
}

function common(options: Options) {
  return { reason: options.text("reason"), confirmationRef: options.text("confirmation-ref") };
}
const range = (options: Options) => ({ start: options.time("start"), end: options.time("end"), limit: options.number("limit") });
const caps = (options: Options) => {
  const accountRead = options.list("account-read");
  return { accountRead: accountRead?.[0] === "*" ? "*" : accountRead, publishDatasets: options.list("publish-datasets"),
    notify: options.bool("no-notify") ? false : undefined, agentTask: options.bool("no-agent") ? false : undefined };
};

function dispatch(command: string, args: Options): ControlRequest {
  if (command === "reload") return post("admin", "/v1/reload", common(args), 60_000);
  if (command === "kill" || command === "unkill") return killSwitch(command === "kill", { scope: args.word("scope"),
    cancelOpenOrders: args.bool("cancel-open"), includeConditional: args.bool("include-conditional"), ...common(args) });
  const sub = args.word("subcommand");
  switch (command + " " + sub) {
    case "market snapshot": return get("admin", "/v1/market/snapshot", { symbol: args.words.shift() });
    case "market klines": return get("storage", "/v1/market/klines", { interval: args.word("interval"), symbol: args.text("symbol"),
      ...range(args), backfill: args.bool("backfill") }, 10 * 60_000);
    case "market funding": return get("storage", "/v1/market/funding-rates", { symbol: args.text("symbol"), ...range(args), backfill: args.bool("backfill") }, 120_000);
    case "market liquidations": return get("storage", "/v1/market/liquidations", { symbol: args.text("symbol"), ...range(args) });
    case "market depth": return get("admin", "/v1/market/rest/depth", { symbol: args.text("symbol"), limit: args.number("limit") });
    case "market oi": return get("admin", "/v1/market/rest/open-interest-hist", { symbol: args.text("symbol"), period: args.text("period"), ...range(args) });
    case "market ls-ratio": {
      const group = oneOf(args.word("kind"), "kind", ["top_accounts", "top_positions", "global_accounts"]);
      return get("admin", "/v1/market/rest/" + { top_accounts: "top-ls-accounts", top_positions: "top-ls-positions", global_accounts: "global-ls-accounts" }[group],
        { symbol: args.text("symbol"), period: args.text("period"), ...range(args) });
    }
    case "market taker-volume": return get("admin", "/v1/market/rest/taker-volume", { symbol: args.text("symbol"), period: args.text("period"), ...range(args) });
    case "market basis": return get("admin", "/v1/market/rest/basis", { pair: args.text("pair"),
      contractType: args.text("contract-type") ?? "PERPETUAL", period: args.text("period"), ...range(args) });
    case "market rest": return get("admin", "/v1/market/rest/" + component(args.word("endpoint")), {
      symbol: args.text("symbol"), pair: args.text("pair"), period: args.text("period"), interval: args.text("interval"),
      contractType: args.text("contract-type"), fromId: args.text("from-id"), ...range(args),
    });
    case "data datasets": return get("storage", "/v1/data/datasets");
    case "data query": return get("storage", "/v1/data/query", { dataset: args.word("dataset"), ...range(args) });
    case "accounts list": return get("admin", "/v1/accounts");
    case "accounts state": return get("admin", "/v1/accounts/" + component(args.word("accountId")) + "/state");
    case "ledger query": return get("storage", "/v1/ledger/query", { accountId: args.text("account"), mode: args.text("mode"),
      type: args.text("type"), symbol: args.text("symbol"), ...range(args), after_seq: args.number("after-seq"), through_seq: args.number("through-seq") });
    case "ledger pnl": return get("storage", "/v1/ledger/pnl", { target: args.words.shift(), mode: args.text("mode"), window: args.text("window"), symbol: args.text("symbol") });
    case "state inspect": {
      const namespace = args.word("namespace");
      const version = args.number("version");
      return version === undefined ? get("admin", "/v1/state/" + component(namespace)) : get("storage", "/v1/checkpoints", { namespace, version });
    }
    case "strategy search": return get("storage", "/v1/strategies/search", {
      family: args.text("family"), lifecycle: args.text("lifecycle"), style: args.text("style"), regime: args.text("regime"),
      account_fit: args.text("account-fit"), factor: args.text("factor"),
    });
    case "strategy get": return get("storage", "/v1/strategies/" + component(args.word("strategyId")));
    case "strategy fork": return post("storage", "/v1/strategies/fork", { fromStrategyId: args.word("fromStrategyId"), draftName: args.word("draftName"),
      reason: args.text("reason") });
    case "strategy validate": return post("storage", "/v1/scripts/validate", { path: args.word("path"), mode: args.text("mode"),
      capabilities: caps(args) }, 60_000);
    case "strategy seal": return strategySeal({ draftPath: args.word("draftPath"), family: args.text("family"), tags: args.json("tags"),
      summary: args.text("summary"), lifecycle: args.text("lifecycle"), parentId: args.text("parent"), capabilities: caps(args) });
    case "strategy tag": return strategyTag(args.word("strategyId"), {
      tags: args.json("tags"), note: args.text("note"), lifecycle: args.text("lifecycle"), author: args.text("author"),
    });
    case "backtest run": return backtestRun({ ...target(args.word("target")), start: args.time("start"), end: args.time("end"),
      symbols: args.list("symbols"), params: args.json("params"), timeoutSec: args.number("timeout-sec") });
    case "run list": return get("admin", "/v1/runs", { mode: args.text("mode"), status: args.text("status") });
    case "run logs": return get("storage", "/v1/logs/" + component(args.word("runId")),
      { tail: args.number("tail"), since: args.time("since"), grep: args.text("grep") });
    case "run start": {
      const mode = oneOf(args.word("mode"), "mode", ["worker", "shadow"]);
      const script = args.word("target");
      return runStart({ mode, ...(mode === "worker" ? { entryPath: script, stateName: args.text("state-name"), persistent: args.bool("persistent") } :
        target(script)), capabilities: caps(args), ...common(args) });
    }
    case "run stop": return runStop(args.word("runId"), common(args));
    case "live propose": return liveProposal({ strategyId: args.word("strategyId"), accountId: args.text("account"), handoverOrders: args.text("handover"),
      includeConditional: args.bool("include-conditional"), resetState: args.bool("reset-state"), capabilities: caps(args), ...common(args) });
    case "trade order": return manualTrade(args.word("accountId"), "order", {
      action: args.word("action"), params: { symbol: args.text("symbol"), type: args.text("type"), qty: args.number("qty"),
        clientOrderId: args.text("client-order-id"), price: args.number("price"), triggerPrice: args.number("trigger-price"),
        timeInForce: args.text("time-in-force"), workingType: args.text("working-type"), callbackRate: args.number("callback-rate"),
        activationPrice: args.number("activation-price"), protection: args.text("protection"),
        priceProtect: args.bool("price-protect") }, ...common(args),
    });
    case "trade cancel": return manualTrade(args.word("accountId"), "cancel", { orderId: args.text("order-id"), algoId: args.text("algo-id"),
      clientOrderId: args.text("client-order-id"), symbol: args.text("symbol"), ...common(args) });
    case "trade cancel-all": return manualTrade(args.word("accountId"), "cancel-all", { symbol: args.text("symbol"),
      includeConditional: args.bool("include-conditional"), ...common(args) });
    case "trade leverage": return manualTrade(args.word("accountId"), "leverage", { leverage: Number(args.word("leverage")),
      symbol: args.text("symbol"), ...common(args) });
    case "proposal status": return get("admin", "/v1/proposals/" + component(args.word("proposalId")));
    case "proposal approve": return approval(args.word("proposalId"), "approve", { proposalDigest: args.word("proposalDigest"), ...common(args) });
    case "proposal reject": return approval(args.word("proposalId"), "reject", { ...common(args), reason: args.words.shift() ?? args.text("reason") });
    case "agent start": return taskStart({ prompt: args.text("prompt"), title: args.text("title"), backend: args.text("backend"),
      sidebarVisibility: args.text("visibility"), dedupKey: args.text("dedup-key"), cooldownSec: args.number("cooldown-sec"), profile: args.text("profile") });
    case "agent get": return get("agent", "/v1/tasks/" + component(args.word("taskId")));
    case "agent cancel": return post("agent", "/v1/tasks/" + component(args.word("taskId")) + "/cancel");
    case "agent resume": return post("agent", "/v1/sessions/" + component(args.word("sessionId")) + "/messages",
      { message: args.text("message"), sidebarVisibility: args.text("visibility") });
    case "notify send": return notifySend({ level: args.text("level") ?? "info", title: args.text("title"), body: args.text("body"),
      dedupKey: args.text("dedup-key"), cooldownSec: args.number("cooldown-sec") });
    default: return bad("Unknown quant command " + command + " " + sub);
  }
}

const help = [
  "quant [--json] <command>",
  "health | reload | kill | unkill",
  "market snapshot|klines|funding|liquidations|depth|oi|ls-ratio|taker-volume|basis|rest",
  "data datasets|query | accounts list|state | ledger query|pnl | state inspect",
  "strategy search|get|fork|validate|seal|tag | backtest run",
  "run list|logs|start|stop | live propose | trade order|cancel|cancel-all|leverage",
  "proposal status|approve|reject | agent start|get|cancel|resume | notify send",
].join("\n");

if (import.meta.main) {
  const json = process.argv.slice(2).some(value => value === "--json" || value === "--json=true");
  try {
    const raw = process.argv.slice(2);
    if (!raw.length || raw.includes("--help") || raw[0] === "help") {
      console.log(json ? JSON.stringify({ help }) : help);
    } else {
      const args = new Options(raw);
      args.bool("json");
      const command = args.word("command");
      let result: unknown;
      if (command === "health") {
        args.done();
        const request = await controlClient("cli");
        const services: Service[] = ["admin", "storage", "notifier", "agent", "watchdog"];
        result = Object.fromEntries(await Promise.all(services.map(async service => {
          try { return [service === "admin" ? "core" : service, await request(get(service, "/v1/health", undefined, 3000))]; }
          catch (error) { return [service === "admin" ? "core" : service, { ok: false, detail: (error as Error).message }]; }
        })));
      } else {
        const invocation = dispatch(command, args);
        args.done();
        result = await (await controlClient("cli"))(invocation);
      }
      console.log(JSON.stringify(result, null, json ? 0 : 2));
    }
  } catch (error) {
    const failure = { code: error instanceof Fault ? error.code : "ERROR", message: (error as Error).message,
      details: error instanceof Fault ? error.details : undefined };
    if (json) console.log(JSON.stringify({ error: failure }));
    else console.error(failure.code + " " + failure.message);
    process.exitCode = 1;
  }
}
