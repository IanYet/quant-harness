import { type AccountMode, credentials, type HarnessConfig } from "../../shared/config";
import { bad } from "../../shared/http";
import { accountKey } from "../gateway";
import { Orders } from "../orders";
import { BinanceAccount } from "./account-client";
import { PublicMarket } from "./public-ws";
import { WeightBucket } from "./signer";

export class AccountPool {
  private readonly accounts = new Map<string, BinanceAccount>();
  private readonly running: Promise<void>[] = [];
  constructor(readonly settings: HarnessConfig, readonly orders: Orders, market: PublicMarket, bucket: WeightBucket) {
    for (const [mode, accounts] of [["live", settings.liveAccounts], ["shadow", settings.shadowAccounts]] as const) {
      for (const [id, config] of Object.entries(accounts)) {
        if (config.initialLeverage > config.risk.maxLeverage) bad("Initial leverage is above the risk cap for " + id);
        const client = new BinanceAccount(mode, id, config, settings.binance, credentials(settings, mode, id), bucket, market, orders);
        this.accounts.set(accountKey(mode, id), client);
        orders.accounts.set(accountKey(mode, id), client);
      }
    }
  }

  start() { for (const account of this.accounts.values()) this.running.push(account.run()); }

  validateReload(next: HarnessConfig) {
    for (const [mode, accounts] of [["live", next.liveAccounts], ["shadow", next.shadowAccounts]] as const) {
      const ids = [...this.accounts.values()].filter(account => account.mode === mode).map(account => account.id);
      if (ids.length !== Object.keys(accounts).length || ids.some(id => !Object.hasOwn(accounts, id))) {
        bad("Adding or removing account credentials requires a service restart");
      }
      if (ids.some(id => this.accounts.get(accountKey(mode, id))!.config.credentialFile !== accounts[id].credentialFile)) {
        bad("Changing account credential files requires a service restart");
      }
    }
  }

  reload(next: HarnessConfig) {
    for (const account of this.accounts.values()) {
      const source = account.mode === "live" ? next.liveAccounts : next.shadowAccounts;
      const after = source[account.id];
      this.update(account.mode, account.id, { ...after, marginType: account.config.marginType, initialLeverage: account.config.initialLeverage });
    }
  }

  private update(mode: AccountMode, id: string, next: BinanceAccount["config"]) {
    this.accounts.get(accountKey(mode, id))!.reload(next);
  }

  async close() {
    for (const account of this.accounts.values()) account.close();
    await Promise.allSettled(this.running);
  }
}
