import { expect, test } from "bun:test";
import { join } from "node:path";
import { type Run } from "@quant/tools";
import { project, sandbox, until } from "./support";

test("CLI runs both examples, observes edited source, rejects failed replacement, promotes and preserves an old order", async () => {
  const box = await sandbox({ tickMs: 10 });
  const cmd = async (...args: string[]) => {
    const proc = Bun.spawn([process.execPath, "run", "harness", ...args], {
      cwd: project,
      env: { ...process.env, HARNESS_DIR: box.cfg.dir, HARNESS_URL: box.host.url },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    if (code) throw new Error(stderr);
    return stdout;
  };
  try {
    const collector = JSON.parse(await cmd("run", "examples/collector.ts", "--name", "hourly")) as Run;
    const file = join(box.root, "strategy.ts");
    const original = await Bun.file(join(project, "examples/strategy.ts")).text();
    await Bun.write(file, original);
    const originalRun = JSON.parse(await cmd("run", file, "--name", "swing")) as Run;
    await until(async () => (await box.sdk.trade.openOrders()).length === 1);
    const order = (await box.sdk.trade.openOrders())[0];
    expect(await cmd("logs", originalRun.id)).toContain("warmup 1440");
    const oldSnapshot = await Bun.file(join(box.cfg.dir, "runs", originalRun.id, "entry.ts")).text();
    expect(oldSnapshot).toBe(original);
    const broken = join(box.root, "broken.ts");
    await Bun.write(broken, 'console.error("startup failed"); throw new Error("bad candidate");');
    await expect(cmd("replace", "swing", broken)).rejects.toThrow("Candidate exited");
    expect(box.host.runs.get("swing").id).toBe(originalRun.id);
    await until(async () =>
      ((await (await fetch(box.host.url + "/health")).json()) as any).runs.some(
        (r: Run) => r.file === broken && r.state === "failed",
      ),
    );
    const marker = join(box.root, "observer.token");
    await Bun.write(
      file,
      `await Bun.write(${JSON.stringify(marker)}, process.env.HARNESS_RUN_TOKEN!); console.log("v2");\n` + original,
    );
    const observed = JSON.parse(await cmd("run", file, "--name", "swing", "--observe")) as Run;
    expect(observed.hash).not.toBe(originalRun.hash);
    await until(() => box.host.runs.get(observed.id).ready);
    expect(await cmd("logs", observed.id)).toContain(order.clientId);
    const token = await Bun.file(marker).text();
    const attempt = () =>
      fetch(box.host.url + "/trade/setLeverage", {
        method: "POST",
        headers: { Authorization: "Bearer " + token },
        body: JSON.stringify({ symbol: "BTCUSDT", key: "probe", leverage: 2 }),
      });
    expect((await attempt()).status).toBe(403);
    const last = (await box.sdk.data.read("btc-hourly")).at(-1)!.cursor;
    const replacement = JSON.parse(await cmd("replace", "swing", file)) as Run;
    expect(replacement.id).toBe(observed.id);
    expect(replacement.mode).toBe("execute");
    expect(box.host.runs.get(originalRun.id).state).toBe("stopped");
    expect((await attempt()).status).toBe(200);
    expect((await box.sdk.trade.order(order.clientId)).status).toBe("NEW");
    await until(async () => (await box.sdk.data.read("btc-hourly", last)).length > 0);
    expect(await cmd("logs", collector.id)).toContain("sample");
    expect(JSON.parse(await cmd("status")).runs.find((r: Run) => r.id === collector.id).state).toBe("running");
    await cmd("stop", "swing");
    expect((await attempt()).status).toBe(403);
    expect((await box.sdk.trade.openOrders())[0].clientId).toBe(order.clientId);
    await cmd("stop", "hourly");
  } finally {
    await box.close();
  }
}, 20000);

test("managed snapshot keeps import resolution and hides host secrets; an old run cannot submit after handover", async () => {
  const box = await sandbox();
  process.env.QH_TEST_HOST_ONLY = "fixture";
  try {
    await Bun.write(join(box.root, ".env"), "QH_TEST_DOTENV=fixture");
    const actor = await box.actor("one"),
      file = join(box.root, "new.ts"),
      dep = join(box.root, "dep.ts");
    await Bun.write(dep, 'export const name = "relative dependency";');
    await Bun.write(
      file,
      `import { name } from "./dep"; import { Decimal } from "decimal.js";
console.log(import.meta.main, name, new Decimal("0.1").add("0.2").toString(),process.env.QH_TEST_HOST_ONLY,process.env.QH_TEST_DOTENV);
setInterval(()=>{},60000);`,
    );
    const replacement = await box.host.runs.replace("one", file);
    expect(box.host.runs.logs(replacement.id).stdout).toContain("true relative dependency 0.3 undefined undefined");
    await expect(actor.client.trade.setLeverage({ key: "late", symbol: "BTCUSDT", leverage: 4 })).rejects.toThrow(
      "403",
    );
    expect(box.host.db.one("SELECT id FROM instructions WHERE key='late'")).toBeNull();
  } finally {
    delete process.env.QH_TEST_HOST_ONLY;
    await box.close();
  }
});

test("operator can promote an observed polling script with perpetual top-level await by its exact run ID", async () => {
  const box = await sandbox();
  try {
    await box.actor("poller");
    const file = join(box.root, "poll.ts");
    await Bun.write(file, 'while (true) {console.log("polling"); await Bun.sleep(60000);}');
    const obs = box.host.runs.start(file, "poller", true);
    await until(() => box.host.runs.logs(obs.id).stdout.includes("polling"));
    expect(box.host.runs.get(obs.id).ready).toBe(false);
    await expect(box.host.runs.replace("poller", file, "different-run")).rejects.toThrow("Selected observer");
    expect((await box.host.runs.replace("poller", file, obs.id)).mode).toBe("execute");
  } finally {
    await box.close();
  }
});
