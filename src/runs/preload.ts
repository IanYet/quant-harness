import { plugin } from "bun";
const entry = process.env.HARNESS_ENTRY!;
const source = await Bun.file(process.env.HARNESS_SNAPSHOT!).text();
// Intercept only the entry. Its URL, relative imports and import.meta.main keep their normal values.
plugin({
  name: "run-source",
  setup(build) {
    build.onLoad({ filter: new RegExp("^" + RegExp.escape(entry) + "$") }, () => ({
      loader: "ts",
      contents: source + '\n;globalThis.process.send?.("harness:ready");\n',
    }));
  },
});
process.once("disconnect", () => process.exit(0));
process.channel?.unref();
