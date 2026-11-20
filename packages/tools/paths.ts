import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, resolve } from "node:path";

export function resolveLocalPath(path: string) {
  if (path === "~") return homedir();
  if (path.startsWith("~/")) return resolve(homedir(), path.slice(2));
  return resolve(path);
}

export const harnessConfigPath = () => resolveLocalPath(process.env.QUANT_CONFIG_PATH ?? "config/harness.json");

export function runtimeDataDirectory(file: string, configured = "/var/lib/quant-harness") {
  const override = process.env.QUANT_DATA_DIR;
  return override === undefined ? resolve(dirname(file), configured) : resolveLocalPath(override);
}

export function localDataDirectory() {
  const file = harnessConfigPath();
  if (process.env.QUANT_DATA_DIR !== undefined) return runtimeDataDirectory(file);
  try {
    const config = JSON.parse(readFileSync(file, "utf8")) as { dataDir?: string };
    return runtimeDataDirectory(file, config.dataDir ?? undefined);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" && process.env.QUANT_CONFIG_PATH === undefined) {
      return runtimeDataDirectory(file);
    }
    throw error;
  }
}
