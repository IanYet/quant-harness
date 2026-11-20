import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { readSecret } from "../src/shared/config";

test("dotenv resolves the credential, config and runtime paths independently", async () => {
  const dir = await mkdtemp(join(tmpdir(), "quant-env-"));
  try {
    const secrets = join(dir, "private");
    await mkdir(secrets, { mode: 0o700 });
    const account = { allowedSymbols: ["BTCUSDT"] };
    const fixture = {
      accounts: { live: account },
      shadow: { accounts: { demo: { ...account, credentialFile: "demo.json" } } },
      im: { telegram: { enabled: true, alertChatId: "-100", credentialFile: "bot.json" } },
    };
    await Promise.all([
      writeFile(join(dir, "harness.json"), JSON.stringify(fixture)),
      writeFile(join(dir, ".env"),
        "QUANT_CONFIG_PATH=./harness.json\nQUANT_DATA_DIR=./runtime\nQUANT_SECRETS_DIR=./private\n", { mode: 0o600 }),
      writeFile(join(secrets, "exchange.json"), JSON.stringify({ accounts: { live: { apiKey: "live-id", apiSecret: "live-test" } } }), { mode: 0o600 }),
      writeFile(join(secrets, "demo.json"), JSON.stringify({ apiKey: "demo-id", apiSecret: "demo-test" }), { mode: 0o600 }),
      writeFile(join(secrets, "bot.json"), JSON.stringify({ botToken: "123:test", webhookSecret: "webhook-test" }), { mode: 0o600 }),
    ]);
    const configPath = fileURLToPath(new URL("../src/shared/config.ts", import.meta.url));
    const notifierPath = fileURLToPath(new URL("../src/notifier/settings.ts", import.meta.url));
    const run = Bun.spawnSync([process.execPath, "-e", [
      "import { config, credentials, dataPath } from " + JSON.stringify(configPath),
      "import { imSettings } from " + JSON.stringify(notifierPath),
      "const settings = config();",
      "const result = { dataDir: dataPath(), secretsDir: settings.secretsDir,",
      "  live: credentials(settings, 'live', 'live').apiKey === 'live-id',",
      "  demo: credentials(settings, 'shadow', 'demo').apiKey === 'demo-id',",
      "  telegram: imSettings(settings).telegram?.webhookSecret === 'webhook-test' };",
      "delete process.env.QUANT_SECRETS_DIR;",
      "console.log(JSON.stringify({ ...result, defaultSecretsDir: config().secretsDir }));",
    ].join("\n")], { cwd: dir, env: Object.fromEntries(Object.entries(process.env).filter(([name]) =>
      !["QUANT_CONFIG_PATH", "QUANT_DATA_DIR", "QUANT_SECRETS_DIR"].includes(name))) });
    expect(run.exitCode).toBe(0);
    expect(JSON.parse(run.stdout.toString())).toEqual({
      dataDir: join(dir, "runtime"), secretsDir: secrets, defaultSecretsDir: join(homedir(), ".secrets"),
      live: true, demo: true, telegram: true,
    });
    expect(() => readSecret(secrets, "../harness.json")).toThrow("Invalid credential filename");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
