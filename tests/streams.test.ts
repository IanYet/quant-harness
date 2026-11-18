import { expect, test } from "bun:test";
import { decodeLines } from "@quant/tools/transport";
import { join } from "node:path";
import { ndjson } from "../src/streams";
import { sandbox, until } from "./support";
test("NDJSON splits UTF-8, combines lines, and detects a truncated final frame", async () => {
  const text = new TextEncoder().encode('{"v":"收盘 🕰"}\n\n{"v":2}\r\n');
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      for (const byte of text) c.enqueue(Uint8Array.of(byte));
      c.close();
    },
  });
  expect(await Array.fromAsync(decodeLines(body))).toEqual([{ v: "收盘 🕰" }, { v: 2 }]);
  await expect(Array.fromAsync(decodeLines(new Response('{"v":').body!))).rejects.toThrow("Incomplete");
});
test("a slow reader has a bounded backlog and releases its listener", async () => {
  let emit!: (n: number) => void,
    released = false;
  const response = ndjson<number>(new Request("http://localhost"), (push) => {
    emit = push;
    return {
      release: () => {
        released = true;
      },
    };
  });
  for (let i = 0; i < 300; i++) emit(i);
  expect(released).toBe(true);
  await expect(response.text()).rejects.toThrow("Slow subscriber");
});
test("independent Bun processes share a dataset, break releases one listener, cursor resumes", async () => {
  const box = await sandbox();
  try {
    const src = `import {data} from "@quant/tools";
for await (const row of data.watch("prices")) { console.log(JSON.stringify(row)); if (STOP) break; }`;
    const a = join(box.root, "a.ts"),
      b = join(box.root, "b.ts");
    await Bun.write(a, src.replace("STOP", "true"));
    await Bun.write(b, src.replace("STOP", "false"));
    const one = box.host.runs.start(a),
      two = box.host.runs.start(b);
    await until(() => box.host.hub.counts()["data/prices"] === 2);
    const first = await box.sdk.data.append("prices", { p: "一" });
    await until(() => box.host.runs.get(one.id).state === "exited" && box.host.hub.counts()["data/prices"] === 1);
    const second = await box.sdk.data.append("prices", { p: "二" });
    await until(() => box.host.runs.logs(two.id).stdout.includes("二"));
    expect(box.host.runs.logs(one.id).stdout).toContain("一");
    expect(await box.sdk.data.read("prices", first.cursor)).toEqual([second]);
    const ac = new AbortController(),
      stream = box.sdk.data.watch("prices", first.cursor, ac.signal);
    expect((await stream.next()).value).toEqual(second);
    const waiting = stream.next();
    ac.abort();
    await expect(waiting).rejects.toThrow();
    await until(() => box.host.hub.counts()["data/prices"] === 1);
  } finally {
    await box.close();
  }
});
