import { resolve } from "node:path";
import { parseArgs } from "node:util";
import { transport } from "@quant/tools/transport";
import { message, requireThat } from "./util";
const usage = `harness run FILE [--name NAME] [--observe] [--account paper|binance]
harness replace NAME FILE [--from RUN_ID]
harness stop RUN_OR_NAME
harness logs RUN_OR_NAME
harness status
harness refresh`;
try {
  const {
    values,
    positionals: [cmd, a, b],
  } = parseArgs({
    args: Bun.argv.slice(2),
    allowPositionals: true,
    options: {
      name: { type: "string" },
      from: { type: "string" },
      observe: { type: "boolean" },
      account: { type: "string" },
      help: { type: "boolean" },
    },
  });
  if (values.help || !cmd) {
    console.log(usage);
    process.exit(0);
  }
  const base = process.env.HARNESS_URL ?? `http://127.0.0.1:${process.env.HARNESS_PORT ?? 8787}`;
  const token = (await Bun.file(resolve(process.env.HARNESS_DIR ?? ".harness", "operator.token")).text()).trim();
  const { get, post } = transport(base, token);
  let result;
  switch (cmd) {
    case "status":
      result = await get("/admin/status");
      break;
    case "refresh":
      result = await post("/admin/refresh", {});
      break;
    case "run":
      requireThat(a, usage);
      result = await post("/admin/run", {
        file: resolve(a),
        name: values.name,
        observe: values.observe,
        account: values.account,
      });
      break;
    case "replace":
      requireThat(a && b, usage);
      result = await post("/admin/replace", { name: a, file: resolve(b), from: values.from });
      break;
    case "stop":
      requireThat(a, usage);
      result = await post("/admin/stop", { ref: a });
      break;
    case "logs": {
      requireThat(a, usage);
      const log = await get<{ stdout: string; stderr: string }>("/admin/logs", { ref: a });
      process.stdout.write(log.stdout);
      process.stderr.write(log.stderr);
      process.exit(0);
    }
    default:
      throw new Error(usage);
  }
  console.log(JSON.stringify(result, null, 2));
} catch (err) {
  console.error(message(err));
  process.exitCode = 1;
}
