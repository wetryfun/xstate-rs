// Runs every fixture in ../../fixtures through XState (machineFromJSON) and
// writes the observed trace to <name>.expected.json. The Rust test
// `crates/xstate-rt/tests/differential.rs` replays the same fixtures with
// equivalent stubs and compares step by step.
//
// Fixture format:
//   { "machine": MachineJSON,
//     "steps": [ { "at": ms, "send": event } | { "at": ms } ] }
// See runner.mjs for what a trace records.
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runFixture } from './runner.mjs';

const fixturesDir = join(dirname(fileURLToPath(import.meta.url)), '../../fixtures');

const only = process.argv[2];
for (const file of readdirSync(fixturesDir).sort()) {
  if (!file.endsWith('.json') || file.endsWith('.expected.json')) continue;
  const name = file.slice(0, -'.json'.length);
  if (only && name !== only) continue;
  const fixture = JSON.parse(readFileSync(join(fixturesDir, file), 'utf8'));
  const trace = runFixture(fixture);
  writeFileSync(join(fixturesDir, `${name}.expected.json`), JSON.stringify(trace, null, 2) + '\n');
  console.log(`${name}: ${trace.length} records`);
}
