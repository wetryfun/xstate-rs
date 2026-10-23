// Runs one machine fixture through XState (machineFromJSON + createActor
// with a SimulatedClock) and returns the trace the Rust differential tests
// compare against. Shared by generate.mjs and fuzz.mjs.
import { machineFromJSON, createActor, SimulatedClock } from 'xstate';

// Stub implementations. Keep in sync with `stubs()` in differential.rs.
export const guards = {
  yes: () => true,
  no: () => false,
  ctxEquals: ({ context }, p) => context[p.key] === p.value,
  ctxAtLeast: ({ context }, p) => context[p.key] >= p.value,
  eventEquals: ({ event }, p) => event[p.key] === p.value
};
export const actions = { noop: () => {} };
export const delays = { short: 100, long: 1000 };

// The trace: after start and after every step, the value, context, status,
// output and the events emitted since the last record. A step first advances
// the clock to `at` (firing due timers), then sends `send` if present.
export function runFixture(fixture) {
  const machine = machineFromJSON(fixture.machine, { guards, actions, delays });
  // alpha.70's `_transitionFast` shortcut (an atomic child of the root with
  // one plain candidate) skips the `matches` check, so a transition whose
  // pattern fails is taken. Compare against XState's full algorithm.
  machine._transitionFast = () => undefined;
  const clock = new SimulatedClock();
  const actor = createActor(machine, { clock });
  let emitted = [];
  actor.on('*', (e) => emitted.push(e));
  // Errors are part of the trace (status "error"); keep them off the console.
  actor.subscribe({ error: () => {} });
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
  return trace;
}
