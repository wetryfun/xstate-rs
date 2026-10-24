// Ports the fixtures with Rust expressions to JavaScript code nodes: every
// `{ "@code" | "@expr": name, "@lang": "rust" }` becomes
// `{ "@code": <source of exprs[name]>, "@lang": "js" }`, so the same
// machines run through a JavaScript evaluator (xstate-js's QuickJS on the
// Rust side, XState's own evaluator convention here). Writes
// fixtures/js_<name>.json and fixtures/system/js_<name>.json with
// `"languages": ["js"]`; regenerate their traces with generate.mjs and
// generate-system.mjs.
//
//   node port-js.mjs
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { exprs } from './exprs.mjs';

const root = join(dirname(fileURLToPath(import.meta.url)), '../../fixtures');

const port = (value, lang) => {
  if (Array.isArray(value)) return value.map((v) => port(v, lang));
  if (!value || typeof value !== 'object') return value;
  const key = ['@code', '@expr'].find((k) => k in value);
  if (key) {
    if ((value['@lang'] ?? lang) !== 'rust') return value;
    const f = exprs[value[key]];
    if (!f) throw new Error(`no expression '${value[key]}'`);
    return { '@code': f.toString(), '@lang': 'js' };
  }
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    if (k === '@exprLang') out[k] = v === 'rust' ? 'js' : v;
    else out[k] = port(v, lang);
  }
  return out;
};

// A machine's own `@exprLang` applies to its nodes.
const portMachine = (machine) => port(machine, machine['@exprLang']);

for (const dir of [root, join(root, 'system')]) {
  for (const file of readdirSync(dir).sort()) {
    if (!file.endsWith('.json') || file.endsWith('.expected.json') || file.startsWith('js_')) continue;
    const text = readFileSync(join(dir, file), 'utf8');
    if (!text.includes('"rust"')) continue;
    const fixture = JSON.parse(text);
    const ported = { ...fixture, languages: ['js'] };
    if (fixture.machine) ported.machine = portMachine(fixture.machine);
    if (fixture.logic) ported.logic = fixture.logic.map((l) => (l.machine ? { ...l, machine: portMachine(l.machine) } : l));
    ported.notes = `${fixture.notes ? `${fixture.notes} ` : ''}(Ported from ${file} by port-js.mjs: JavaScript code nodes.)`;
    writeFileSync(join(dir, `js_${file}`), JSON.stringify(ported, null, 2) + '\n');
    console.log(`js_${file}`);
  }
}
