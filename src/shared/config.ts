import { readFileSync, statSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { join, isAbsolute } from "node:path";
import { resolveLocalPath } from "../../packages/tools/paths";
import { bad, flag, ID, integer, num, oneOf, record, str, stringList, SYMBOL } from "./http";

export type AccountMode = "live" | "shadow";
export type RunMode = AccountMode | "worker";

export interface RiskLimits {
  maxOrderNotionalUsd: number;
  maxLeverage: number;
  maxOrdersPerMin: number;
}
export interface AccountConfig {
  allowedSymbols: string[];
  requireApproval: boolean;
  requireApprovalOnStop: boolean;
  requireApprovalOnManualTrade: boolean;
  marginType: "CROSSED" | "ISOLATED";
  initialLeverage: number;
  risk: RiskLimits;
  labels: string[];
  credentialFile?: string;
}
export interface BinanceConfig {
  mainnetRest: string;
  mainnetWs: string;
  testnetRest: string;
  testnetWs: string;
  ipWeightLimitPerMin: number;
  publicRestCacheTtlMs: Record<string, number>;
}
export type Service = "admin" | "runtime" | "storage" | "agent" | "notifier" | "watchdog";

const credentialFilename = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\.json$/;
export const configPath = () => resolveLocalPath(process.env.QUANT_CONFIG_PATH ?? "config/harness.json");
export const dataPath = () => dataDirOverride() ?? config().dataDir;
export const socketPath = (dir: string, name: Service) => join(dir, "run", name + ".sock");

function dataDirOverride() {
  const dir = process.env.QUANT_DATA_DIR;
  return dir === undefined ? undefined : resolveLocalPath(str(dir, "QUANT_DATA_DIR"));
}

function risk(value: unknown): RiskLimits {
  const obj = record(value ?? {});
  return {
    maxOrderNotionalUsd: num(obj.maxOrderNotionalUsd ?? 100, "maxOrderNotionalUsd", 0.01),
    maxLeverage: integer(obj.maxLeverage ?? 5, "maxLeverage", 1, 125),
    maxOrdersPerMin: integer(obj.maxOrdersPerMin ?? 20, "maxOrdersPerMin", 1, 1200),
  };
}

function accounts(value: unknown, shadowRisk?: RiskLimits): Record<string, AccountConfig> {
  return Object.fromEntries(Object.entries(record(value ?? {})).map(([name, raw]) => {
    str(name, "accountId", ID);
    if (name === "global") bad("global is reserved for the global Kill Switch");
    const v = record(raw);
    const limits = shadowRisk ?? risk(v.risk);
    const allowedSymbols = stringList(v.allowedSymbols, "allowedSymbols", SYMBOL);
    return [name, {
      allowedSymbols,
      requireApproval: !shadowRisk && flag(v.requireApproval, "requireApproval", true),
      requireApprovalOnStop: !shadowRisk && flag(v.requireApprovalOnStop, "requireApprovalOnStop"),
      requireApprovalOnManualTrade: flag(v.requireApprovalOnManualTrade, "requireApprovalOnManualTrade", !shadowRisk),
      marginType: oneOf(v.marginType ?? "CROSSED", "marginType", ["CROSSED", "ISOLATED"]),
      initialLeverage: integer(v.initialLeverage ?? 1, "initialLeverage", 1, 125),
      risk: limits,
      labels: stringList(v.labels ?? [], "labels"),
      credentialFile: v.credentialFile === undefined ? undefined : str(v.credentialFile, "credentialFile", credentialFilename),
    }];
  }));
}

function endpoint(value: unknown, fallback: string, protocol: string): string {
  const url = new URL(str(value ?? fallback, "Binance URL"));
  if (url.protocol !== protocol || url.username || url.password || url.search || url.hash) bad("Invalid Binance endpoint");
  return url.href.replace(/\/$/, "");
}

export function config() {
  const raw = record(JSON.parse(readFileSync(configPath(), "utf8")));
  const b = record(raw.binance ?? {});
  const shadow = record(raw.shadow ?? {});
  const dataDir = dataDirOverride() ?? str(raw.dataDir ?? "/var/lib/quant-harness", "dataDir");
  if (!isAbsolute(dataDir)) bad("dataDir must be absolute");
  const secretsDir = resolveLocalPath(str(process.env.QUANT_SECRETS_DIR ?? "~/.screets", "QUANT_SECRETS_DIR"));
  const defaultSymbol = str(raw.defaultSymbol ?? "BTCUSDT", "defaultSymbol", SYMBOL);
  const symbols = stringList(raw.symbols ?? [defaultSymbol], "symbols", SYMBOL);
  if (!symbols.includes(defaultSymbol)) bad("symbols must contain defaultSymbol");
  const binance: BinanceConfig = {
    mainnetRest: endpoint(b.mainnetRest, "https://fapi.binance.com", "https:"),
    mainnetWs: endpoint(b.mainnetWs, "wss://fstream.binance.com", "wss:"),
    testnetRest: endpoint(b.testnetRest, "https://demo-fapi.binance.com", "https:"),
    testnetWs: endpoint(b.testnetWs, "wss://demo-fstream.binance.com", "wss:"),
    ipWeightLimitPerMin: integer(b.ipWeightLimitPerMin ?? 1800, "ipWeightLimitPerMin", 100),
    publicRestCacheTtlMs: Object.fromEntries(Object.entries(record(b.publicRestCacheTtlMs ?? {}))
      .map(([key, value]) => [key, integer(value, key, 0, 3_600_000)])),
  };
  if (new URL(binance.mainnetRest).origin === new URL(binance.testnetRest).origin ||
      new URL(binance.mainnetWs).origin === new URL(binance.testnetWs).origin) bad("Binance mainnet and Demo endpoints must be distinct");
  for (const value of [binance.mainnetRest, binance.mainnetWs]) {
    if (/(^|[.-])(demo|testnet)([.-]|$)|binancefuture\.com$/i.test(new URL(value).hostname)) bad("Mainnet market settings resolve to a test environment");
  }
  const liveAccounts = accounts(raw.accounts);
  const shadowAccounts = accounts(shadow.accounts, risk(shadow.risk));
  if (Object.keys(liveAccounts).some(id => Object.hasOwn(shadowAccounts, id))) bad("Live and shadow account IDs must be distinct");
  return { dataDir, secretsDir, defaultSymbol, symbols, binance, liveAccounts, shadowAccounts,
    im: record(raw.im ?? {}), agent: record(raw.agent ?? {}), watchdog: record(raw.watchdog ?? {}) };
}
export type HarnessConfig = ReturnType<typeof config>;

export async function initLayout(dir: string) {
  await Promise.all([
    ...["run", "db", "state/checkpoints", "strategies/_drafts", "scripts/workers", "scripts/research", "artifacts", "logs"]
      .map(path => mkdir(join(dir, path), { mode: 0o750, recursive: true })),
    mkdir(join(dir, "secrets"), { mode: 0o700, recursive: true }),
  ]);
}

export function readSecret(dir: string, file: string): Record<string, unknown> {
  const path = join(dir, str(file, "credential filename", credentialFilename));
  try {
    const info = statSync(path);
    if (!info.isFile() || (info.mode & 0o077) !== 0) throw new Error("Restrict permissions on " + path + " to 0600");
    const text = readFileSync(path, "utf8");
    let value: unknown;
    try { value = JSON.parse(text); } catch { throw new Error("Invalid credential JSON at " + path); }
    return record(value, "Expected a credential object in " + path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
    throw error;
  }
}

export function credentials(config: HarnessConfig, mode: AccountMode, id: string) {
  const accounts = mode === "live" ? config.liveAccounts : config.shadowAccounts;
  const account = Object.hasOwn(accounts, id) ? accounts[id] : bad("Unknown " + mode + " account " + id);
  const entries = account.credentialFile ? undefined : record(
    readSecret(config.secretsDir, "exchange.json")[mode === "live" ? "accounts" : "shadow"] ?? {},
  );
  const v = account.credentialFile ? readSecret(config.secretsDir, account.credentialFile) :
    record(entries && Object.hasOwn(entries, id) ? entries[id] : undefined, "Missing " + mode + " credentials for " + id);
  return { apiKey: str(v.apiKey, "apiKey"), apiSecret: str(v.apiSecret, "apiSecret") };
}
