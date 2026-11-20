import { serviceAuth } from "../shared/auth";
import { config, initLayout } from "../shared/config";
import { backends } from "./adapters/registry";
import { openAgentDb } from "./db";
import { Guardrails } from "./guardrails";
import { agentServer } from "./server";

const settings = config();
await initLayout(settings.dataDir);
const auth = await serviceAuth(settings.dataDir);
const db = openAgentDb(settings.dataDir);
const adapters = backends(settings);
const guard = new Guardrails(db, settings.agent, new Set(adapters.keys()));
const abort = new AbortController();
const running = await agentServer(settings, auth, guard, adapters, abort.signal);
console.log("quant-agent listening on " + running.server.url);
await new Promise<void>(resolve => {
  const stop = () => { abort.abort(); resolve(); };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
});
await running.close();
db.close();
