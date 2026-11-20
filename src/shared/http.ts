export class Fault extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly details: Record<string, unknown> = {},
  ) {
    super(message);
  }
}

export function bad(message: string): never {
  throw new Fault(400, "BAD_REQUEST", message);
}

export function missing(message: string): never {
  throw new Fault(404, "NOT_FOUND", message);
}

export function record(value: unknown, label = "Expected an object"): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) bad(label);
  return value as Record<string, unknown>;
}

export function str(value: unknown, name: string, pattern?: RegExp): string {
  if (typeof value !== "string" || !value.trim() || (pattern && !pattern.test(value))) bad("Invalid " + name);
  return value;
}

export function num(value: unknown, name: string, min = -Infinity, max = Infinity): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < min || value > max) bad("Invalid " + name);
  return value;
}

export function integer(value: unknown, name: string, min = 0, max = Number.MAX_SAFE_INTEGER): number {
  const result = num(value, name, min, max);
  if (!Number.isSafeInteger(result)) bad("Invalid " + name);
  return result;
}

export function flag(value: unknown, name: string, fallback = false): boolean {
  if (value === undefined) return fallback;
  if (typeof value !== "boolean") bad("Invalid " + name);
  return value;
}

export function oneOf<const T extends string>(value: unknown, name: string, choices: readonly T[]): T {
  if (!(choices as readonly unknown[]).includes(value)) bad("Invalid " + name);
  return value as T;
}

export function stringList(value: unknown, name: string, pattern?: RegExp): string[] {
  if (!Array.isArray(value)) bad("Invalid " + name);
  return [...new Set(value.map(item => str(item, name, pattern)))];
}

export const ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
export const SYMBOL = /^[A-Z0-9]{2,32}$/;

export function queryNumber(url: URL, key: string, fallback: number, min = 0, max = Number.MAX_SAFE_INTEGER): number {
  const value = url.searchParams.get(key);
  if (value === null) return fallback;
  if (!value.trim()) bad("Invalid " + key);
  return integer(Number(value), key, min, max);
}

export function queryFlag(url: URL, key: string, fallback = false): boolean {
  const value = url.searchParams.get(key);
  if (value === null) return fallback;
  return oneOf(value, key, ["true", "false"]) === "true";
}

export async function body(req: Request): Promise<Record<string, unknown>> {
  const text = await req.text();
  if (!text) return {};
  try {
    return record(JSON.parse(text));
  } catch {
    return bad("Invalid JSON object");
  }
}

export function handler(route: (req: Request, url: URL) => unknown | Promise<unknown>) {
  return async (req: Request): Promise<Response> => {
    try {
      const result = await route(req, new URL(req.url));
      return result instanceof Response ? result : Response.json(result ?? null);
    } catch (error) {
      if (error instanceof Fault) {
        return Response.json({ error: { code: error.code, message: error.message, details: error.details } }, { status: error.status });
      }
      console.error(error);
      return Response.json({ error: { code: "INTERNAL_ERROR", message: "Internal service error", details: {} } }, { status: 500 });
    }
  };
}
