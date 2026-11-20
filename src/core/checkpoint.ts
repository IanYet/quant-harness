import { join } from "node:path";
import type { CheckpointEnvelope } from "@quant/tools";
import type { Emit } from "./event-bus";
import { EventBus } from "./event-bus";
import { atomicFile, sha256 } from "../shared/fs";
import { Fault, integer } from "../shared/http";

export class Checkpoints {
  private readonly cache = new Map<string, CheckpointEnvelope<unknown>>();
  private readonly writes = new Map<string, Promise<void>>();
  constructor(private readonly dir: string, private readonly bus: EventBus) {
    for (const row of bus.db.query<{ checkpoint_json: string }, []>("SELECT checkpoint_json FROM checkpoint_latest").all()) {
      const envelope = JSON.parse(row.checkpoint_json) as CheckpointEnvelope<unknown>;
      this.cache.set(envelope.namespace, envelope);
      this.backup(envelope);
    }
  }

  private backup(envelope: CheckpointEnvelope<unknown>) {
    const namespace = envelope.namespace;
    const path = join(this.dir, "state", "checkpoints", sha256(namespace) + ".json");
    const pending = (this.writes.get(namespace) ?? Promise.resolve()).then(() => atomicFile(path, JSON.stringify(envelope), 0o640))
      .catch(error => console.error("Checkpoint backup failed", namespace, error));
    this.writes.set(namespace, pending);
    void pending.finally(() => { if (this.writes.get(namespace) === pending) this.writes.delete(namespace); });
  }

  drain() { return Promise.all(this.writes.values()); }
  read(namespace: string) { return this.cache.get(namespace) ?? null; }

  stage(emit: Emit, namespace: string, epoch: number, accountId: string | undefined,
      value: unknown, schemaVersion: unknown, handoverId?: string) {
    const row = this.bus.db.query<{ handover_id: string | null; checkpoint_json: string; version: number }, [string]>(
      "SELECT handover_id,checkpoint_json,version FROM checkpoint_latest WHERE namespace=?").get(namespace);
    if (handoverId && row?.handover_id === handoverId) return JSON.parse(row.checkpoint_json) as CheckpointEnvelope<unknown>;
    if (value === undefined) throw new Fault(400, "BAD_REQUEST", "Checkpoint data is required");
    const envelope: CheckpointEnvelope<unknown> = {
      namespace, accountId, version: (row?.version ?? 0) + 1, epoch,
      schemaVersion: integer(schemaVersion ?? 1, "schemaVersion", 1), updatedAt: Date.now(), data: value,
    };
    const serialized = JSON.stringify(envelope);
    if (Buffer.byteLength(serialized) > 1024 * 1024) throw new Fault(422, "CHECKPOINT_TOO_LARGE", "Checkpoint exceeds 1 MiB");
    this.bus.db.query("INSERT INTO checkpoint_latest VALUES (?,?,?,?) ON CONFLICT(namespace) DO UPDATE SET " +
      "version=excluded.version,handover_id=excluded.handover_id,checkpoint_json=excluded.checkpoint_json")
      .run(namespace, envelope.version, handoverId ?? null, serialized);
    emit("state.checkpoint", "core", { ...envelope });
    return envelope;
  }

  remember(envelope: CheckpointEnvelope<unknown>) {
    this.cache.set(envelope.namespace, envelope);
    this.backup(envelope);
    return envelope;
  }
}
