import { createHash, randomUUID } from "node:crypto";
import { closeSync, fsyncSync, openSync } from "node:fs";
import { link, mkdir, open, rename, unlink } from "node:fs/promises";
import { dirname, join } from "node:path";

export function sha256(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

export function syncDirectory(path: string) {
  const fd = openSync(path, "r");
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

export async function atomicFile(path: string, content: string | Uint8Array, mode: number, exclusive = false) {
  const dir = dirname(path);
  await mkdir(dir, { recursive: true, mode: 0o750 });
  const temp = join(dir, "." + randomUUID());
  try {
    const fd = await open(temp, "wx", mode);
    try {
      await fd.writeFile(content);
      await fd.chmod(mode);
      await fd.sync();
    } finally {
      await fd.close();
    }
    if (exclusive) await link(temp, path);
    else await rename(temp, path);
    syncDirectory(dir);
  } finally {
    await unlink(temp).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}

export function canonical(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(item => canonical(item ?? null)).join(",") + "]";
  if (value !== null && typeof value === "object") {
    const properties = value as Record<string, unknown>;
    return "{" + Object.keys(properties).filter(key => properties[key] !== undefined).sort()
      .map(key => JSON.stringify(key) + ":" + canonical(properties[key])).join(",") + "}";
  }
  return JSON.stringify(value);
}
