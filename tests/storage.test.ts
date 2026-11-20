import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { cp, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { initLayout } from "../src/shared/config";
import type { BusEvent } from "../src/shared/events";
import { openStorage } from "../src/storage/db";
import { writeLedger, writeTimeseries } from "../src/storage/ingestor";
import { StrategyRegistry } from "../src/storage/strategy-registry";
import { ledgerQuery, readKlines } from "../src/storage/queries";

async function layout() {
  const dir = await mkdtemp(join(tmpdir(), "quant-storage-test-"));
  await initLayout(dir);
  return dir;
}

test("interleaved WAL commits recover from the lesser cursor without duplicating ledger rows", async () => {
  const dir = await layout();
  let storage = openStorage(dir);
  try {
    const ts = 1_700_000_040_000;
    const market = {
      symbol: "BTCUSDT", interval: "1m", openTime: ts, closeTime: ts + 59_999,
      o: 100, h: 100, l: 100, c: 100, v: 1, quoteVolume: 100, closed: true,
    };
    const events: BusEvent[] = [
      { seq: 1, ts, topic: "market.kline", source: "binance.mainnet", payload: market },
      { seq: 2, ts, topic: "market.kline", source: "binance.testnet", payload: { ...market, c: 200 } },
      { seq: 3, ts, topic: "shadow.fill", source: "account:demo", payload: {
        mode: "shadow", accountId: "demo", symbol: "BTCUSDT", tradeId: "100", exchangeOrderId: "20",
        side: "BUY", positionSide: "LONG", price: 200, qty: 1, fee: 0.1, feeAsset: "USDT",
      } },
      { seq: 4, ts, topic: "shadow.algo_order", source: "account:demo", payload: {
        accountId: "demo", symbol: "BTCUSDT", algoId: "10", side: "SELL", positionSide: "LONG",
        orderType: "STOP_MARKET", qty: 1, triggerPrice: 195, status: "NEW",
      } },
      { seq: 5, ts, topic: "market.funding_rate", source: "binance.testnet", payload: {
        symbol: "BTCUSDT", fundingTime: ts, fundingRate: 0.002,
      } },
      { seq: 6, ts, topic: "market.funding_rate", source: "binance.mainnet", payload: {
        symbol: "BTCUSDT", fundingTime: ts, fundingRate: 0.001,
      } },
    ];
    writeLedger(storage.ledger, events, 6);
    storage.close();
    storage = openStorage(dir);
    expect(storage.cursor()).toEqual({ ledgerSeq: 6, timeseriesSeq: 0, lastSeq: 0 });
    const reader = new Database(join(dir, "db", "ledger.db"), { readonly: true });
    try {
      reader.exec("PRAGMA query_only=ON; BEGIN");
      expect(reader.query("SELECT COUNT(*) AS n FROM fills").get()).toEqual({ n: 1 });
      writeLedger(storage.ledger, events, 6);
      writeTimeseries(storage.timeseries, events, 6);
      writeLedger(storage.ledger, [{ ...events[2], seq: 7, payload: { ...events[2].payload, tradeId: "101" } }], 7);
      expect(reader.query("SELECT COUNT(*) AS n FROM fills").get()).toEqual({ n: 1 });
      reader.exec("COMMIT");
      expect(reader.query("SELECT COUNT(*) AS n FROM fills").get()).toEqual({ n: 2 });
    } finally {
      reader.close();
    }
    expect(readKlines(storage, "BTCUSDT", "1m", ts, ts + 59_999).map(row => row.c)).toEqual([100]);
    expect(storage.timeseries.query("SELECT funding_rate FROM funding_rates").all()).toEqual([{ funding_rate: 0.001 }]);
    expect(ledgerQuery(storage, new URL("http://local/?type=algo_orders&mode=shadow"))).toHaveLength(1);
    expect(ledgerQuery(storage, new URL("http://local/?mode=shadow"))).toHaveLength(2);
    expect(ledgerQuery(storage, new URL("http://local/?mode=live"))).toHaveLength(0);
    storage.close();
    storage = openStorage(dir);
    writeLedger(storage.ledger, events, 7);
    writeTimeseries(storage.timeseries, events, 7);
    expect(storage.cursor()).toEqual({ ledgerSeq: 7, timeseriesSeq: 7, lastSeq: 7 });
  } finally {
    storage.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("precheck and artifact publication keep identical source pinned to the SDK build", async () => {
  const dir = await layout();
  const storage = openStorage(dir);
  try {
    const sdk = join(dir, "sdk");
    await cp(resolve(import.meta.dir, "../packages/tools"), sdk, { recursive: true });
    const registry = new StrategyRegistry(dir, storage.ledger, sdk);
    const source = "export type CompilerProbe = import('@quant/tools').WorkerCapabilities;\n";
    const valid = "import type { WorkerCapabilities } from '@quant/tools';\nexport type CompilerProbe = WorkerCapabilities;\n";
    const worker = join(dir, "scripts/workers/precheck.ts");
    await writeFile(worker, source);
    expect((await registry.validate(worker, "worker")).valid).toBe(false);
    await writeFile(worker, valid);
    const first = await registry.prepare({ mode: "worker", path: worker });
    const again = await registry.prepare({ mode: "worker", path: worker });
    expect(first.codeHash).toBe(again.codeHash);
    await writeFile(join(sdk, "types.ts"), "// SDK revision\n", { flag: "a" });
    const later = await registry.prepare({ mode: "worker", path: worker });
    expect(first.sourceHash).toBe(later.sourceHash);
    expect(first.codeHash).not.toBe(later.codeHash);
    expect(registry.search(new URLSearchParams())).toEqual([]);
    await writeFile(join(dir, "strategies/_drafts/precheck.ts"), valid);
    await expect(registry.prepare({ mode: "shadow", path: "precheck.ts" })).rejects.toMatchObject({ code: "VALIDATION_FAILED" });
    const publish = registry["publishSeal"].bind(registry);
    const options = { family: "precheck", lifecycle: "backtested", parent: null, tagValues: {}, summary: "Atomic registry test" };
    const sealed = await Promise.all([
      publish(Buffer.from(valid), first, options), publish(Buffer.from(valid), later, options),
    ]);
    expect(new Set(sealed.map(version => version.strategyId))).toEqual(new Set(["precheck@v1", "precheck@v2"]));
    for (const item of sealed) {
      expect((await stat(join(dir, item.path))).mode & 0o777).toBe(0o444);
      expect(await readFile(join(dir, item.path), "utf8")).toBe(valid);
      expect((await registry.prepare({ mode: "live", strategyId: item.strategyId })).codeHash).toBe(item.codeHash);
    }
  } finally {
    storage.close();
    await rm(dir, { recursive: true, force: true });
  }
}, 30_000);
