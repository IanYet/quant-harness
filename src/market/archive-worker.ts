import { createReadStream } from "node:fs";
import { mkdir, rename, rm } from "node:fs/promises";
import { join } from "node:path";
import { Unzip, UnzipInflate } from "fflate";
import type { ArchiveRequest } from "./archive";
import { message } from "../util";
declare var self: Worker;

async function readDay(path: string, request: ArchiveRequest) {
  const metrics = request.kind !== "trades";
  const rows: unknown[] = [];
  let header: string[] = [],
    found = false,
    done = false,
    pastEnd = false;
  function row(line: string) {
    if (!line || pastEnd || rows.length >= request.limit) return;
    const fields = line.trim().split(",");
    if (!/^\d/.test(fields[0])) {
      header = fields;
      return;
    }
    const field = (name: string, fallback: number) => fields[header.length ? header.indexOf(name) : fallback];
    const time = metrics
      ? Date.parse(field("create_time", 0).replace(" ", "T") + "Z")
      : Number(field("transact_time", 5));
    if (!Number.isFinite(time)) throw new Error("Unexpected archive timestamp");
    if (time >= request.end) {
      pastEnd = true;
      return;
    }
    if (time < request.start) return;
    if (metrics) {
      const value = field(
        request.kind === "oi" ? "sum_open_interest" : "count_long_short_ratio",
        request.kind === "oi" ? 2 : 6,
      );
      if (value === undefined) throw new Error("Unexpected metrics columns");
      if (value !== "") rows.push({ time, value, source: "archive" });
    } else
      rows.push({
        id: field("agg_trade_id", 0),
        time,
        price: field("price", 1),
        quantity: field("quantity", 2),
        buyerMaker: field("is_buyer_maker", 6).toLowerCase() === "true",
      });
  }
  const unzip = new Unzip((file) => {
    if (found || !file.name.endsWith(".csv")) return;
    found = true;
    let pending = "";
    const decoder = new TextDecoder();
    file.ondata = (err, data, final) => {
      if (err) throw err;
      const lines = (pending + decoder.decode(data, { stream: !final })).split("\n");
      pending = lines.pop()!;
      for (const line of lines) row(line);
      if (final) {
        row(pending);
        done = true;
      }
    };
    file.start();
  });
  unzip.register(UnzipInflate);
  for await (const chunk of createReadStream(path)) {
    unzip.push(chunk);
    if (done || pastEnd || rows.length >= request.limit) return rows;
  }
  unzip.push(new Uint8Array(), true);
  if (!found || !done) throw new Error("Incomplete CSV archive");
  return rows;
}

self.onmessage = async ({ data: a }: MessageEvent<ArchiveRequest>) => {
  try {
    const dataset = a.kind === "trades" ? "aggTrades" : "metrics";
    const filename = `${a.symbol}-${dataset}-${a.day}.zip`;
    const dir = join(a.dir, "archive", dataset, a.symbol),
      path = join(dir, filename);
    await mkdir(dir, { recursive: true });
    if (!(await Bun.file(path).exists())) {
      const res = await fetch(`${a.url}/${dataset}/${a.symbol}/${filename}`, {
        signal: AbortSignal.timeout(120000),
      });
      if (!res.ok) throw new Error(`Archive ${filename}: HTTP ${res.status}`);
      const temp = path + "." + crypto.randomUUID();
      try {
        await Bun.write(temp, res);
        await rename(temp, path);
      } finally {
        await rm(temp, { force: true });
      }
    }
    try {
      self.postMessage({ rows: await readDay(path, a) });
    } catch (err) {
      await rm(path, { force: true });
      throw err;
    }
  } catch (e) {
    self.postMessage({ error: message(e) });
  }
};
