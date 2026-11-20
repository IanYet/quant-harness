import { QuantError, UdsTransport } from "./transport-uds";
import type { Endpoint, Params } from "./transport-uds";

export class LocalTransport extends UdsTransport {
  override request<T>(endpoint: Endpoint, path: string, args?: Params, method = "GET", body?: unknown): Promise<T> {
    const allowed = endpoint === "runtime" ? path === "/v1/market/snapshot" || path.startsWith("/v1/market/rest/") :
      path.startsWith("/v1/market/") || path === "/v1/data/query";
    if (method !== "GET" || !allowed) throw new QuantError("FORBIDDEN", "This operation requires a managed run");
    return super.request<T>(endpoint, path, args, method, body);
  }

  override stream<T>(path: string, args: Params, opts?: { reliable?: boolean; signal?: AbortSignal }) {
    if (path !== "/v1/market/fast-stream") throw new QuantError("FORBIDDEN", "Reliable streams require a managed run");
    return super.stream<T>(path, args, opts);
  }
}
