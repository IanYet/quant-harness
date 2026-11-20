import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { readSecret } from "../src/shared/config";
import { imSettings } from "../src/notifier/settings";

test("services and local SDK read runtime paths and credentials from the JSON config", async () => {
  const dir = await mkdtemp(join(tmpdir(), "quant-config-"));
  try {
    const secrets = join(dir, "settings");
    await mkdir(secrets, { mode: 0o700 });
    const account = { allowedSymbols: ["BTCUSDT"] };
    const fixture = {
      dataDir: "../runtime",
      accounts: { live: account },
      shadow: { accounts: { demo: { ...account, credentialFile: "demo.json" } } },
      im: { telegram: { enabled: true } },
    };
    await Promise.all([
      writeFile(join(secrets, "harness.json"), JSON.stringify(fixture)),
      writeFile(join(secrets, "secrets.exchange.json"), JSON.stringify({ accounts: { live: { apiKey: "live-id", apiSecret: "live-test" } } }), { mode: 0o600 }),
      writeFile(join(secrets, "demo.json"), JSON.stringify({ apiKey: "demo-id", apiSecret: "demo-test" }), { mode: 0o600 }),
      writeFile(join(secrets, "secrets.im.json"), JSON.stringify({ telegram: { chatId: -100, botToken: "123:test", webhookSecret: "webhook-test" } }), { mode: 0o600 }),
    ]);
    const configPath = fileURLToPath(new URL("../src/shared/config.ts", import.meta.url));
    const notifierPath = fileURLToPath(new URL("../src/notifier/settings.ts", import.meta.url));
    const paths = fileURLToPath(new URL("../packages/tools/paths.ts", import.meta.url));
    const run = Bun.spawnSync([process.execPath, "--no-env-file", "-e", [
      "import { config, credentials, dataPath } from " + JSON.stringify(configPath),
      "import { imSettings } from " + JSON.stringify(notifierPath),
      "import { localDataDirectory } from " + JSON.stringify(paths),
      "const settings = config();",
      "const telegram = imSettings(settings).telegram;",
      "const result = { dataDir: dataPath(), sdkDataDir: localDataDirectory(), secretsDir: settings.secretsDir,",
      "  live: credentials(settings, 'live', 'live').apiKey === 'live-id',",
      "  demo: credentials(settings, 'shadow', 'demo').apiKey === 'demo-id',",
      "  telegram: telegram?.webhookSecret === 'webhook-test' && telegram.chat === '-100' };",
      "process.env.QUANT_DATA_DIR = './deployed';",
      "console.log(JSON.stringify({ ...result, override: config().dataDir, sdkOverride: localDataDirectory() }));",
    ].join("\n")], { cwd: dir, env: { QUANT_CONFIG_PATH: "./settings/harness.json" } });
    expect(run.exitCode).toBe(0);
    expect(JSON.parse(run.stdout.toString())).toEqual({
      dataDir: join(dir, "runtime"), sdkDataDir: join(dir, "runtime"), secretsDir: secrets, live: true, demo: true, telegram: true,
      override: join(dir, "deployed"), sdkOverride: join(dir, "deployed"),
    });
    expect(() => readSecret(secrets, "../harness.json")).toThrow("Invalid credential filename");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("Feishu reads approvers from its credential file and falls back to legacy config", async () => {
  const dir = await mkdtemp(join(tmpdir(), "quant-im-config-"));
  try {
    const feishu = { enabled: true, alertChatId: "oc_fixture" };
    const settings = { secretsDir: dir, im: { feishu } };
    const legacy = { ...feishu, allowedAdminOpenIds: ["ou_legacy"] };
    const app = { appId: "cli_fixture", appSecret: "app-test", encryptKey: "encrypt-test", verificationToken: "verify-test" };
    const shared = join(dir, "secrets.im.json");
    const dedicated = "secrets.feishu.json";
    await Promise.all([
      writeFile(shared, JSON.stringify({ feishu: { ...app, allowedAdminOpenIds: ["ou_current"] } }), { mode: 0o600 }),
      writeFile(join(dir, dedicated), JSON.stringify({ ...app, allowedAdminOpenIds: [] }), { mode: 0o600 }),
    ]);
    expect(imSettings(settings).feishu?.users).toEqual(["ou_current"]);
    expect(imSettings({ ...settings, im: { feishu: legacy } }).feishu?.users).toEqual(["ou_current"]);
    expect(imSettings({ ...settings, im: { feishu: { ...legacy, credentialFile: dedicated } } }).feishu?.users).toEqual([]);

    await writeFile(shared, JSON.stringify({ feishu: app }), { mode: 0o600 });
    expect(imSettings({ ...settings, im: { feishu: legacy } }).feishu?.users).toEqual(["ou_legacy"]);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
