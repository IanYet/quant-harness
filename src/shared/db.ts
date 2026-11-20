import { Database } from "bun:sqlite";
import { chmodSync, existsSync } from "node:fs";

export function openDb(path: string, schema: string, mode = 0o640) {
  const db = new Database(path, { create: true, strict: true });
  try {
    for (const file of [path, path + "-wal", path + "-shm"]) if (existsSync(file)) chmodSync(file, mode);
    db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON");
    db.transaction(() => {
      const { user_version } = db.query<{ user_version: number }, []>("PRAGMA user_version").get()!;
      if (user_version === 0) db.exec(schema + "\nPRAGMA user_version=1");
      else if (user_version !== 1) throw new Error("Unsupported schema version " + user_version + " in " + path);
    }).immediate();
    return db;
  } catch (error) {
    db.close();
    throw error;
  }
}
