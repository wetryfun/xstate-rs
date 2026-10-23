// Generates random machines and event sequences, runs them through XState,
// and writes a corpus the Rust differential test replays
// (crates/xstate-rt/tests/differential.rs, `fuzz_corpus_matches_xstate`).
//
//   node fuzz.mjs [--seed N] [--count K] [--out file]
//
// The committed corpus (fixtures/fuzz/corpus.json) is a fixed seed; run
// `tools/differential/fuzz.sh` to try fresh seeds. Each case is
//   { name, machine, steps, expected }
// where `expected` is the trace (see runner.mjs), or { "loadError": message }
// when XState rejects the definition: Rust must reject it too.
import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { machineFromJSON } from 'xstate';
import { runFixture, guards, actions, delays } from './runner.mjs';
import { prng, generator, steps, explicitIds } from './gen.mjs';

const args = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : fallback;
};
const seed = Number(opt('seed', 1));
const count = Number(opt('count', 300));
const out = opt('out', join(dirname(fileURLToPath(import.meta.url)), '../../fixtures/fuzz/corpus.json'));

const rand = prng(seed);
const makeMachine = generator(rand, { routes: 0.12 });
const cases = [];
let rejected = 0;
for (let i = 0; i < count; i++) {
  const machine = makeMachine();
  const fixture = { machine, steps: steps(rand, explicitIds(machine)) };
  let expected;
  try {
    machineFromJSON(machine, { guards, actions, delays });
  } catch (e) {
    // Keep a few rejected definitions: Rust must reject them too.
    if (rejected++ % 5 !== 0) continue;
    cases.push({ name: `seed${seed}-${i}`, ...fixture, expected: { loadError: String(e.message) } });
    continue;
  }
  try {
    expected = runFixture(fixture);
  } catch {
    // SimulatedClock gives up on zero-delay timer loops; nothing to compare.
    continue;
  }
  cases.push({ name: `seed${seed}-${i}`, ...fixture, expected });
}
mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, JSON.stringify(cases) + '\n');
const errors = cases.filter((c) => Array.isArray(c.expected) && c.expected.some((r) => r.status === 'error')).length;
console.log(`${out}: ${cases.length} cases (${cases.filter((c) => !Array.isArray(c.expected)).length} rejected definitions, ${errors} ending in error)`);
