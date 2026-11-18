import { config } from "./config";
import { startHost } from "./server";
const cfg = await config();
const host = await startHost(cfg);
console.log(`harnessd ${host.url} market=${cfg.market} data=${cfg.dir}`);
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await host.stop();
  process.exit(0);
}
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
