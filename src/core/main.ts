const worker = new Worker(new URL("./kernel.ts", import.meta.url));
let stopping = false;
for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.on(signal, () => {
    if (stopping) return;
    stopping = true;
    worker.postMessage("shutdown");
    setTimeout(() => {
      worker.terminate();
      process.exit(1);
    }, 25_000).unref();
  });
}
worker.addEventListener("message", event => {
  if (event.data !== "stopped") return;
  worker.terminate();
  process.exit(0);
});
worker.addEventListener("error", event => {
  console.error("quant-core kernel failed", event.message);
  worker.terminate();
  process.exitCode = 1;
});
