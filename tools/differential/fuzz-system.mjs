// Generates random actor systems (a root machine invoking random child
// machines and async logic) with random sends, clock advances and effect
// results, runs them through XState's pure system API, and writes a corpus
// the Rust system differential test replays
// (crates/xstate-rt/tests/system_differential.rs).
//
//   node fuzz-system.mjs [--seed N] [--count K] [--out file]
//
// Each case is a system fixture (see system-runner.mjs) plus `name` and
// `expected` (the records).
import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runSystemFixture } from './system-runner.mjs';
import { prng, generator } from './gen.mjs';

const args = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : fallback;
};
const seed = Number(opt('seed', 1));
const count = Number(opt('count', 100));
const out = opt('out', join(dirname(fileURLToPath(import.meta.url)), '../../fixtures/fuzz/system-corpus.json'));

const rand = prng(seed);
const chance = (p) => rand() < p;
const int = (n) => Math.floor(rand() * n);
const pick = (xs) => xs[int(xs.length)];
const EVENTS = ['a', 'b', 'c', 'x.y', 'x.z'];

const kid = generator(rand, { id: 'k', atLeast: false, depth: 2 });
const parent = generator(rand, { id: 'm', atLeast: false, invoke: { sources: ['kid', 'job'], chance: 0.3 } });

function steps() {
  const n = 4 + int(14);
  let time = 0;
  const list = [];
  for (let i = 0; i < n; i++) {
    const r = rand();
    if (r < 0.45) list.push({ send: 'm', event: { type: pick(EVENTS), n: int(3) } });
    else if (r < 0.65) list.push({ send: `m/c${int(3)}`, event: { type: pick(EVENTS), n: int(3) } });
    else if (r < 0.82) list.push({ advance: (time += pick([50, 100, 300, 1000])) });
    else {
      const step = { resolve: int(2) };
      const e = rand();
      if (e < 0.5) step.event = { type: 'xstate.async.resolve', data: { ok: int(2) } };
      else if (e < 0.8) step.event = { type: 'xstate.async.reject', data: { code: int(2) } };
      list.push(step);
    }
  }
  return list;
}

const cases = [];
for (let i = 0; i < count; i++) {
  const fixture = {
    logic: [
      { name: 'kid', machine: kid() },
      { name: 'job', async: chance(0.3) ? { timeout: pick([200, 800]) } : {} },
      { name: 'root', machine: parent(), actors: ['kid', 'job'] }
    ],
    root: 'root',
    init: {},
    steps: steps()
  };
  let expected;
  try {
    expected = runSystemFixture(fixture);
  } catch {
    continue; // a definition XState rejects
  }
  if (expected[0].error) continue;
  // alpha.70 crashes decoding a child snapshot with history for onSnapshot
  // (SystemReduction.decode without its machine); see CLAUDE.md.
  if (expected.some((r) => r.error?.includes("reading 'getStateNodeById'"))) continue;
  cases.push({ name: `system${seed}-${i}`, ...fixture, expected });
}
mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, JSON.stringify(cases) + '\n');
const errors = cases.filter((c) => c.expected.some((r) => r.error)).length;
console.log(`${out}: ${cases.length} cases (${errors} with error records)`);
