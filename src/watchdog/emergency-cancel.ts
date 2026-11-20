import type { Database } from "bun:sqlite";
import { credentials, type HarnessConfig } from "../shared/config";
import { flag, integer, record } from "../shared/http";
import { BinanceRest, WeightBucket } from "../core/binance/signer";
import type { Issue } from "./probes";

export function emergencyPaths(includeConditional: boolean) {
  return includeConditional ? ["/fapi/v1/allOpenOrders", "/fapi/v1/algoOpenOrders"] : ["/fapi/v1/allOpenOrders"];
}

export class EmergencyCancel {
  private failures = 0;
  private readonly enabled: boolean;
  private readonly threshold: number;
  private readonly conditional: boolean;
  private readonly bucket: WeightBucket;
  constructor(private readonly db: Database, private readonly config: HarnessConfig) {
    const policy = record(config.watchdog.emergencyCancel ?? {});
    this.enabled = flag(policy.enabled, "emergencyCancel.enabled");
    this.conditional = flag(policy.includeConditional, "emergencyCancel.includeConditional");
    this.threshold = integer(policy.consecutiveFailures ?? 6, "emergencyCancel.consecutiveFailures", 2);
    this.bucket = new WeightBucket(config.binance.ipWeightLimitPerMin);
  }

  get consecutiveFailures() { return this.failures; }

  async inspect(coreAvailable: boolean, signal: AbortSignal) {
    if (coreAvailable) {
      if (this.failures || this.db.query("SELECT 1 FROM emergency_done LIMIT 1").get()) this.db.query("DELETE FROM emergency_done").run();
      this.failures = 0;
      return [];
    }
    this.failures++;
    if (!this.enabled || this.failures < this.threshold) return [];
    const issues: Issue[] = [];
    for (const [id, config] of Object.entries(this.config.liveAccounts)) {
      try {
        for (const symbol of config.allowedSymbols) for (const path of emergencyPaths(this.conditional)) {
          if (signal.aborted) return issues;
          const key = id + ":" + symbol + ":" + path;
          if (this.db.query("SELECT 1 FROM emergency_done WHERE key=?").get(key)) continue;
          const params = credentials(this.config, "live", id);
          const rest = new BinanceRest(this.config.binance.mainnetRest, this.bucket, params);
          await rest.request(path, { symbol }, { signed: true, priority: true, method: "DELETE", signal });
          this.db.query("INSERT INTO emergency_done VALUES (?,?)").run(key, Date.now());
        }
        issues.push({ key: "emergency:" + id, message: "Core loss emergency cancellation completed for " + id +
          (this.conditional ? " (standard and conditional)" : " (standard orders)"), level: "critical" });
      } catch (error) {
        issues.push({ key: "emergency:" + id, message: "Emergency cancellation needs retry for " + id + "\n" + (error as Error).message, level: "critical" });
      }
    }
    return issues;
  }
}
