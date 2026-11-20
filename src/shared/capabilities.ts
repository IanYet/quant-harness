import type { WorkerCapabilities } from "@quant/tools";
import { flag, ID, record, stringList } from "./http";

export function capabilities(value: unknown): WorkerCapabilities {
  const v = record(value ?? {});
  return {
    accountRead: v.accountRead === "*" ? "*" : stringList(v.accountRead ?? [], "accountRead", ID),
    publishDatasets: stringList(v.publishDatasets ?? [], "publishDatasets", ID),
    notify: flag(v.notify, "notify", true),
    agentTask: flag(v.agentTask, "agentTask", true),
  };
}
