import ts from "typescript";
import type { RunMode } from "../shared/config";
import type { WorkerCapabilities } from "@quant/tools";

export type Diagnostic = { level: "error" | "warning"; line: number; message: string };

const forbiddenApis = /^(?:Bun\.(?:spawn(?:Sync)?|file|write|serve|listen|connect|dlopen)|process\.(?:dlopen|binding|getBuiltinModule)|(?:globalThis\.)?(?:require|eval|Function))$/;
const forbiddenTargets = /admin\.sock|(?:secrets[\\/]|(?:exchange|im)\.json$)|(?:core|ledger|timeseries)\.db|unix:|https?:\/\/(?:localhost|127\.0\.0\.1|\[::1\])(?:[:/]|$)/i;

export function inspect(source: ts.SourceFile, mode: RunMode, capabilities: WorkerCapabilities = {}): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];
  const bindings = new Map<string, string>();
  let handover = false;
  let stop = false;
  let publish = false;
  const add = (node: ts.Node, message: string, level: Diagnostic["level"] = "error") =>
    diagnostics.push({ level, line: source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1, message });

  function target(node: ts.Node): string {
    if (ts.isIdentifier(node)) return bindings.get(node.text) ?? node.text;
    if (ts.isPropertyAccessExpression(node)) return target(node.expression) + "." + node.name.text;
    if (ts.isElementAccessExpression(node) && ts.isStringLiteralLike(node.argumentExpression)) return target(node.expression) + "." + node.argumentExpression.text;
    return "";
  }

  for (const node of source.statements) {
    if (ts.isImportDeclaration(node)) {
      if (!ts.isStringLiteral(node.moduleSpecifier) || node.moduleSpecifier.text !== "@quant/tools") {
        add(node, "Managed source can only import @quant/tools");
        continue;
      }
      const names = node.importClause?.namedBindings;
      if (names && ts.isNamespaceImport(names)) bindings.set(names.name.text, "@quant/tools");
      if (names && ts.isNamedImports(names)) {
        for (const spec of names.elements) bindings.set(spec.name.text, (spec.propertyName ?? spec.name).text);
      }
    }
    if (ts.isExportDeclaration(node) && node.moduleSpecifier) add(node, "Re-exporting another file is not allowed");
  }

  function visit(node: ts.Node) {
    if (ts.isVariableDeclaration(node) && node.initializer) {
      const origin = target(node.initializer).replace(/^@quant\/tools\./, "");
      if (/^(trade|market|harness|data)(\.|$)/.test(origin)) {
        if (ts.isIdentifier(node.name)) bindings.set(node.name.text, origin);
        if (ts.isObjectBindingPattern(node.name)) {
          for (const element of node.name.elements) {
            if (ts.isIdentifier(element.name)) bindings.set(element.name.text, origin + "." + (element.propertyName ?? element.name).getText());
          }
        }
      }
    }
    if (ts.isImportEqualsDeclaration(node) || node.kind === ts.SyntaxKind.ImportKeyword || ts.isImportTypeNode(node)) {
      add(node, "Dynamic and indirect imports are not allowed");
    }
    if (ts.isIdentifier(node) || ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
      const name = target(node).replace(/^@quant\/tools\./, "");
      if (mode === "worker" && /^trade\.(?:openLong|openShort|closeLong|closeShort|cancel|cancelAll|setLeverage)$/.test(name)) {
        add(node, "Workers use accounts.* for read-only account access");
      }
      if (forbiddenApis.test(name)) add(node, "Managed scripts must use the SDK instead of " + name);
      if (name === "data.publish") {
        publish = true;
        if (!capabilities.publishDatasets?.length) add(node, "Declare publishDatasets before using data.publish");
      }
    }
    if (ts.isCallExpression(node)) {
      const name = target(node.expression).replace(/^@quant\/tools\./, "");
      if (name === "harness.onHandover") handover = true;
      if (name === "harness.onStop") stop = true;
      if (name === "Date.now" && mode !== "worker") add(node, "Use harness.now() with the backtest clock", "warning");
      if (name === "market.watchFast" && mode !== "worker") add(node, "High-frequency feeds are unavailable in backtests", "warning");
    }
    if (ts.isStringLiteralLike(node) && forbiddenTargets.test(node.text)) add(node, "Direct access to control, secrets or production storage is not allowed");
    ts.forEachChild(node, visit);
  }
  if (source.referencedFiles.length || source.typeReferenceDirectives.length || source.libReferenceDirectives.length) {
    add(source, "External triple-slash references are not allowed");
  }
  visit(source);
  if (mode !== "worker" && !handover) add(source, "Trading scripts must register harness.onHandover()");
  if (mode !== "worker" && !stop) add(source, "Stop uses the default order-ownership rules", "warning");
  if (mode !== "worker" && stop && /\b(?:cancelAll|trade\.cancel)\s*\(/.test(source.text)) {
    add(source, "Return order references from onStop. Trading writes are frozen during the callback", "warning");
  }
  if (capabilities.publishDatasets?.length && !publish) add(source, "publishDatasets declared without data.publish", "warning");
  return diagnostics;
}

export function validateFile(entry: string, sdk: string, project: string, mode: RunMode, capabilities?: WorkerCapabilities) {
  const program = ts.createProgram([entry], {
    noEmit: true, strict: true, target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler, types: ["bun"], typeRoots: [project + "/node_modules/@types"],
    skipLibCheck: true, baseUrl: project, paths: { "@quant/tools": [sdk] },
  });
  const source = program.getSourceFile(entry);
  if (!source) throw new Error("Cannot read source snapshot");
  const syntax = inspect(source, mode, capabilities);
  const diagnostics = ts.getPreEmitDiagnostics(program).map<Diagnostic>(diag => ({
    level: "error",
    line: diag.file && diag.start !== undefined ? diag.file.getLineAndCharacterOfPosition(diag.start).line + 1 : 0,
    message: ts.flattenDiagnosticMessageText(diag.messageText, "\n"),
  }));
  return [...new Map([...syntax, ...diagnostics].map(diag => [JSON.stringify(diag), diag])).values()];
}
