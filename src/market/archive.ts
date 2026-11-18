import type { Config } from "../config";
import type { Range } from "@quant/tools";
export interface ArchiveRequest extends Range {
  kind: "oi" | "ratio" | "trades";
  symbol: string;
  day: string;
  limit: number;
  url: string;
  dir: string;
}
export async function archiveDay(cfg: Config, args: Omit<ArchiveRequest, "dir" | "url">): Promise<any[]> {
  const worker = new Worker(new URL("./archive-worker.ts", import.meta.url).href);
  try {
    return await new Promise<any[]>((resolve, reject) => {
      worker.onmessage = (e) => (e.data.error ? reject(new Error(e.data.error)) : resolve(e.data.rows));
      worker.onerror = (e) => reject(new Error(e.message));
      worker.postMessage({ ...args, url: cfg.archive, dir: cfg.dir });
    });
  } finally {
    worker.terminate();
  }
}
