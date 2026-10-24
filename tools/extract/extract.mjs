#!/usr/bin/env node
// Extracts machine bundles for xstate-codegen from TypeScript machine modules.
//
//   node tools/extract/extract.mjs [--check] [--out-dir <dir>] [--globals a,b] <module.ts>...
//
// Each module exports
//
//   export const machine = { id: 'checkout', ... } satisfies MachineJSON;
//   export const schemas = {
//     context, input, output,            // Zod schemas
//     events: { type: zodObject },       // public events
//     internalEvents: { type: zodObject },
//     emitted: { type: zodObject },
//     actions: { name: { params? } },    // params of custom actions
//     guards: { name: { params? } },     // params of custom guards
//     children: {                        // invoked children, by invoke id
//       id: { events?: { type: zodObject }, output?: zodSchema },
//     },
//   };
//
// `machine` is the JSON-portable definition (transitions as objects, guards
// and actions referenced by name). A machine built with createMachine() is
// accepted too and serialized with machineToJSON(); what that loses is
// reported (lint.mjs): invokes of inline logic and JavaScript code reading
// names from its module (errors), setup() implementations the host must
// supply (warnings). --globals allows names the host's evaluator provides.
//
// The script converts the schemas with Zod's JSON Schema converter, checks
// the definition with XState's own machineFromJSON (with stub sources), and
// writes <out-dir>/<module name>.xstate.json:
//
//   { "machine": MachineJSON, "schemas": { ...JSON Schemas } }
//
// --check compares instead of writing and exits 1 when a bundle is stale.
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { basename, dirname, join, resolve, extname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { registerHooks, stripTypeScriptTypes } from 'node:module';
import { machineFromJSON, machineToJSON, createAsyncLogic } from 'xstate';
import { z } from 'zod';
import { lintCode, lintMachine } from './lint.mjs';

// Machine modules import `zod` and `xstate`; resolve those to this tool's
// copies so modules need no node_modules of their own and share one Zod.
registerHooks({
  resolve(specifier, context, next) {
    if (/^(zod|xstate)(\/|$)/.test(specifier)) return next(specifier, { ...context, parentURL: import.meta.url });
    return next(specifier, context);
  }
});

const SCHEMA_KEYS = ['context', 'input', 'output', 'events', 'internalEvents', 'emitted', 'actions', 'guards', 'children'];

function fail(message) {
  throw new Error(message);
}

function toJSONSchema(schema, path) {
  if (!schema || typeof schema !== 'object' || !('_zod' in schema)) fail(`${path}: expected a Zod schema`);
  let json;
  try {
    json = z.toJSONSchema(schema, { unrepresentable: 'throw', io: 'output' });
  } catch (e) {
    fail(`${path}: ${e.message}`);
  }
  delete json.$schema;
  return json;
}

function schemaMap(map, path) {
  if (map === undefined) return undefined;
  if (!map || typeof map !== 'object') fail(`${path}: expected an object of Zod schemas`);
  return Object.fromEntries(Object.entries(map).map(([k, v]) => [k, toJSONSchema(v, `${path}.${k}`)]));
}

function sourceMap(map, path) {
  if (map === undefined) return undefined;
  if (!map || typeof map !== 'object') fail(`${path}: expected an object`);
  return Object.fromEntries(
    Object.entries(map).map(([name, def]) => {
      const p = `${path}.${name}`;
      if (!def || typeof def !== 'object') fail(`${p}: expected { params?: <Zod schema> }`);
      for (const k of Object.keys(def)) if (k !== 'params') fail(`${p}: unknown key "${k}"`);
      return [name, def.params === undefined ? {} : { params: toJSONSchema(def.params, `${p}.params`) }];
    })
  );
}

function childMap(map, path) {
  if (map === undefined) return undefined;
  if (!map || typeof map !== 'object') fail(`${path}: expected an object`);
  return Object.fromEntries(
    Object.entries(map).map(([id, def]) => {
      const p = `${path}.${id}`;
      if (!def || typeof def !== 'object' || '_zod' in def) {
        fail(`${p}: expected { events?: { type: <Zod object> }, output?: <Zod schema> } (an ActorRef type is not portable)`);
      }
      const out = {};
      for (const k of Object.keys(def)) {
        if (k === 'events') out.events = schemaMap(def.events, `${p}.events`);
        else if (k === 'output') out.output = toJSONSchema(def.output, `${p}.output`);
        else fail(`${p}: unknown key "${k}"`);
      }
      return [id, out];
    })
  );
}

function convertSchemas(schemas = {}) {
  for (const k of Object.keys(schemas)) {
    if (!SCHEMA_KEYS.includes(k)) fail(`schemas.${k}: not supported (expected one of ${SCHEMA_KEYS.join(', ')})`);
  }
  const out = {};
  for (const k of ['context', 'input', 'output']) {
    if (schemas[k] !== undefined) out[k] = toJSONSchema(schemas[k], `schemas.${k}`);
  }
  for (const k of ['events', 'internalEvents', 'emitted']) {
    const m = schemaMap(schemas[k], `schemas.${k}`);
    if (m) out[k] = m;
  }
  for (const k of ['actions', 'guards']) {
    const m = sourceMap(schemas[k], `schemas.${k}`);
    if (m) out[k] = m;
  }
  const children = childMap(schemas.children, 'schemas.children');
  if (children) out.children = children;
  return out;
}

// Code nodes: in the `rust` language the source names a Rust expression
// (`{ "@code": "addLine", "@lang": "rust" }`); other languages run through
// the evaluator the Rust host registers for them (Registry::evaluator, or
// codegen's `evaluator_<lang>`). TypeScript sources lose their types here
// (Node's stripTypeScriptTypes), so a JavaScript engine can run them.
function prepareCode(value, path, lang) {
  if (Array.isArray(value)) return value.map((v, i) => prepareCode(v, `${path}[${i}]`, lang));
  if (!value || typeof value !== 'object') return value;
  for (const key of ['@code', '@expr']) {
    if (!(key in value)) continue;
    const valueLang = value['@lang'] ?? lang;
    if (!valueLang) fail(`${path}: ${key} without @lang (and no root @exprLang)`);
    if (typeof value[key] !== 'string') fail(`${path}: ${key} must be a string`);
    if (valueLang !== 'ts') return value;
    return { ...value, [key]: stripTypes(value[key], `${path}.${key}`) };
  }
  return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, prepareCode(v, `${path}.${k}`, lang)]));
}

function stripTypes(source, path) {
  const emit = process.emitWarning;
  // The API is experimental in Node 22; its warning is noise here.
  process.emitWarning = () => {};
  try {
    // An expression as a statement: parenthesized, so an object literal or
    // a function expression stays one; the parentheses come off again.
    const stripped = stripTypeScriptTypes(`(${source})`).trim();
    return stripped.slice(1, -1).trimEnd();
  } catch (e) {
    fail(`${path}: ${e.message}`);
  } finally {
    process.emitWarning = emit;
  }
}

// machineFromJSON with stubs for every source it reports missing.
function checkWithXState(machine) {
  const stubLogic = createAsyncLogic({ run: async () => undefined });
  // Code cannot run here: in every language, it evaluates to nothing.
  const evaluators = new Proxy({}, { get: () => () => undefined });
  const sources = { guards: {}, actions: {}, actors: {}, delays: {}, evaluators };
  const stub = { guard: ['guards', () => false], action: ['actions', () => {}], actor: ['actors', stubLogic], delay: ['delays', 1] };
  for (let i = 0; i < 10_000; i++) {
    try {
      machineFromJSON(machine, sources);
      return;
    } catch (e) {
      const missing = /^Missing (guard|action|actor|delay) source "(.*)"$/.exec(e.message);
      if (!missing) fail(`XState rejects the definition: ${e.message}`);
      const [kind, value] = stub[missing[1]];
      sources[kind][missing[2]] = value;
    }
  }
}

async function extract(modulePath, { globals }) {
  const mod = await import(pathToFileURL(resolve(modulePath)).href);
  if (!mod.machine) fail('the module must export `machine` (a MachineJSON definition)');
  const built = typeof mod.machine.getInitialSnapshot === 'function';
  let machine = built ? machineToJSON(mod.machine) : mod.machine;
  machine = JSON.parse(JSON.stringify(machine));
  machine = prepareCode(machine, '$', machine['@exprLang']);
  // What serialization lost: fail now rather than at run time.
  const { errors, warnings } = built ? lintMachine(mod.machine, machine) : { errors: [], warnings: [] };
  errors.push(...lintCode(machine, { globals }));
  for (const w of warnings) console.error(`${modulePath}: warning: ${w}`);
  if (errors.length) fail(errors.length === 1 ? errors[0] : `\n  ${errors.join('\n  ')}`);
  const schemas = convertSchemas(mod.schemas);
  const internal = Object.keys(schemas.internalEvents ?? {});
  if (internal.length && machine.internalEvents === undefined) machine.internalEvents = internal;
  checkWithXState(machine);
  return { machine, schemas };
}

const args = process.argv.slice(2);
let check = false;
let outDir;
const globals = [];
const modules = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--check') check = true;
  else if (args[i] === '--out-dir') outDir = args[++i];
  else if (args[i] === '--globals') globals.push(...args[++i].split(','));
  else modules.push(args[i]);
}
if (!modules.length) {
  console.error('usage: extract.mjs [--check] [--out-dir <dir>] [--globals a,b] <module.ts>...');
  process.exit(2);
}

let stale = 0;
for (const modulePath of modules) {
  const name = basename(modulePath, extname(modulePath));
  const out = join(outDir ?? dirname(modulePath), `${name}.xstate.json`);
  let text;
  try {
    text = JSON.stringify(await extract(modulePath, { globals }), null, 2) + '\n';
  } catch (e) {
    console.error(`${modulePath}: ${e.message}`);
    process.exit(1);
  }
  if (check) {
    if (!existsSync(out) || readFileSync(out, 'utf8') !== text) {
      console.error(`${out} is out of date; run extract.mjs ${modulePath}`);
      stale++;
    }
  } else {
    writeFileSync(out, text);
    console.log(`wrote ${out}`);
  }
}
process.exit(stale ? 1 : 0);
