import { randomBytes, createHmac, timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { atomicFile } from "./fs";
import { Fault } from "./http";

export type Client = "cli" | "mcp" | "core" | "storage" | "notifier" | "agent" | "watchdog";
const clients: Client[] = ["cli", "mcp", "core", "storage", "notifier", "agent", "watchdog"];

export async function serviceAuth(dataDir: string) {
  const path = join(dataDir, "secrets", "control.key");
  let key: Buffer;
  try {
    key = await readFile(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    try { await atomicFile(path, randomBytes(32), 0o600, true); }
    catch (err) { if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err; }
    key = await readFile(path);
  }
  if (key.byteLength !== 32) throw new Error("Invalid control service key");
  const tokens = new Map(clients.map(client => [client, createHmac("sha256", key).update(client).digest("hex")]));
  return {
    token: (client: Client) => tokens.get(client)!,
    require<T extends Client>(req: Request, allowed: readonly T[]): T {
      const provided = req.headers.get("authorization") ?? "";
      if (provided.length === 71) {
        const supplied = Buffer.from(provided);
        for (const client of allowed) {
          if (supplied.length === 71 && timingSafeEqual(supplied, Buffer.from("Bearer " + tokens.get(client)!))) return client;
        }
      }
      throw new Fault(401, "UNAUTHORIZED", "A service credential is required");
    },
  };
}

export type ServiceAuth = Awaited<ReturnType<typeof serviceAuth>>;
