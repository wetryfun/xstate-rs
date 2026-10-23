// Runs every fixture in ../../fixtures through XState (machineFromJSON) and
// writes the observed trace to <name>.expected.json. The Rust test
// `crates/xstate-rt/tests/differential.rs` replays the same fixtures with
// equivalent stubs and compares step by step.
//
// Fixture format:
//   { "machine": MachineJSON,
//     "steps": [ { "at": ms, "send": event } | { "at": ms } ] }
// A step first advances the clock to `at` (firing due timers), then sends
// `send` if present. The trace records, after start and after every step:
// value, context, status, output and the events emitted since the last record.
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { machineFromJSON, createActor, SimulatedClock } from 'xstate';

const fixturesDir = join(dirname(fileURLToPath(import.meta.url)), '../../fixtures');

// Stub implementations. Keep in sync with `stubs()` in differential.rs.
const guards = {
  yes: () => true,
  no: () => false,
  ctxEquals: ({ context }, p) => context[p.key] === p.value,
  ctxAtLeast: ({ context }, p) => context[p.key] >= p.value,
  eventEquals: ({ event }, p) => event[p.key] === p.value
};
const actions = { noop: () => {} };
const delays = { short: 100, long: 1000 };

const only = process.argv[2];
for (const file of readdirSync(fixturesDir).sort()) {
  if (!file.endsWith('.json') || file.endsWith('.expected.json')) continue;
  const name = file.slice(0, -'.json'.length);
  if (only && name !== only) continue;
  const fixture = JSON.parse(readFileSync(join(fixturesDir, file), 'utf8'));
  const machine = machineFromJSON(fixture.machine, { guards, actions, delays });
  const clock = new SimulatedClock();
  const actor = createActor(machine, { clock });
  let emitted = [];
  actor.on('*', (e) => emitted.push(e));
  const record = (label) => {
    const s = actor.getSnapshot();
    const entry = { label, value: s.value, context: s.context ?? null, status: s.status, emitted };
    if (s.output !== undefined) entry.output = s.output;
    emitted = [];
    return entry;
  };
  let now = 0;
  actor.start();
  const trace = [record('start')];
  for (const [i, step] of fixture.steps.entries()) {
    if (step.at > now) {
      clock.increment(step.at - now);
      now = step.at;
    }
    if (step.send) actor.send(step.send);
    trace.push(record(`step ${i}`));
  }
  writeFileSync(join(fixturesDir, `${name}.expected.json`), JSON.stringify(trace, null, 2) + '\n');
  console.log(`${name}: ${trace.length} records`);
}
