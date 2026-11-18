import { Decimal } from "decimal.js";
export { Decimal };
Decimal.set({ precision: 64 });
export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
export function requireThat(value: unknown, message: string, status = 400): asserts value {
  if (!value) throw new HttpError(status, message);
}
export function nameOf(value: unknown) {
  requireThat(typeof value === "string" && /^[\w.:-]{1,120}$/.test(value), "Invalid name or key");
  return value;
}
export function symbolOf(value: unknown) {
  requireThat(typeof value === "string" && /^[A-Z0-9_]{2,30}$/.test(value), "Invalid contract symbol");
  return value;
}
export function integer(value: unknown, min: number, max = Number.MAX_SAFE_INTEGER) {
  const n = Number(value);
  requireThat(
    value !== null && value !== "" && Number.isSafeInteger(n) && n >= min && n <= max,
    "Integer outside allowed range",
  );
  return n;
}
export function positive(value: unknown) {
  requireThat(
    typeof value === "string" && /^\d{1,28}(\.\d{1,24})?$/.test(value) && new Decimal(value).gt(0),
    "Expected positive decimal string",
  );
  return new Decimal(value).toFixed();
}
export function message(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
export function recordError(error: unknown) {
  console.error(message(error));
}
export function stable(value: unknown): string {
  return JSON.stringify(value, (_key, item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return item;
    return Object.fromEntries(
      Object.keys(item)
        .sort()
        .map((k) => [k, item[k]]),
    );
  });
}
export const uid = () => crypto.randomUUID();
// Binance sequence numbers and trade IDs can exceed the exact range of a JS number.
export function wireJSON(text: string) {
  return JSON.parse(text, (_key, value, context?: { source?: string }) =>
    typeof value === "number" && Number.isInteger(value) && !Number.isSafeInteger(value) && context?.source
      ? context.source
      : value,
  );
}
