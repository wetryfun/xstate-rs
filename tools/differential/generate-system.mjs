// System-level differential fixtures: runs fixtures/system/*.json through
// XState's pure system API (initialSystemTransition, systemTransition,
// advanceSystemTime) and records the full system snapshot and the external
// effects after initialization and after every step, in
// <name>.expected.json. crates/xstate-rt/tests/system_differential.rs
// replays them on the Rust System.
//
// Fixture format:
//   { "logic": [ { "name", "machine": MachineJSON, "actors": [names] }
//              | { "name", "async": { "timeout"? } } ],   // dependencies first
//     "root": name,
//     "init": { "time"?, "input"?, "id"? },
//     "steps": [ { "send": address, "event": {...} }
//              | { "advance": time }
//              | { "resolve": index into the pending effects (in id order), "event"? } ] }
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  machineFromJSON,
  createAsyncLogic,
  initialSystemTransition,
  systemTransition,
  advanceSystemTime
} from 'xstate';

const dir = join(dirname(fileURLToPath(import.meta.url)), '../../fixtures/system');

// Keep in sync with the stubs in system_differential.rs.
const guards = {
  yes: () => true,
  no: () => false,
  ctxEquals: ({ context }, p) => context[p.key] === p.value,
  eventEquals: ({ event }, p) => event[p.key] === p.value
};
const delays = { short: 100, long: 1000 };

function build(fixture) {
  const logic = {};
  for (const def of fixture.logic) {
    if (def.async) {
      logic[def.name] = createAsyncLogic({ timeout: def.async.timeout, run: async () => undefined });
    } else {
      const actors = Object.fromEntries((def.actors ?? []).map((n) => [n, logic[n]]));
      // One implementation per machine and action name, as in Rust.
      const actions = { noop: () => {}, track: () => {} };
      logic[def.name] = machineFromJSON(def.machine, { guards, delays, actors, actions });
    }
  }
  return { root: logic[fixture.root] };
}

const pendingIds = (s) =>
  Object.keys(s.externalEffects).sort((a, b) => Number(a.split(':')[1]) - Number(b.split(':')[1]));

const only = process.argv[2];
for (const file of readdirSync(dir).sort()) {
  if (!file.endsWith('.json') || file.endsWith('.expected.json')) continue;
  const name = file.slice(0, -5);
  if (only && name !== only) continue;
  const fixture = JSON.parse(readFileSync(join(dir, file), 'utf8'));
  const system = build(fixture);
  const records = [];
  let snapshot;
  const record = (label, run) => {
    try {
      const [next, effects] = run();
      snapshot = next;
      records.push({ label, snapshot: JSON.parse(JSON.stringify(next)), effects: JSON.parse(JSON.stringify(effects)) });
    } catch (error) {
      if (process.env.STACK) console.error(error.stack);
      records.push({ label, error: String(error.message) });
    }
  };
  record('init', () => initialSystemTransition(system, fixture.init ?? {}));
  for (const [i, step] of fixture.steps.entries()) {
    const label = `step ${i}`;
    if ('send' in step) record(label, () => systemTransition(system, snapshot, step.send, step.event));
    else if ('advance' in step) record(label, () => advanceSystemTime(system, snapshot, { time: step.advance }));
    else if ('resolve' in step) {
      record(label, () => {
        const id = pendingIds(snapshot)[step.resolve];
        const effect = snapshot.externalEffects[id];
        if (!effect) throw new Error(`no pending effect at index ${step.resolve}`);
        return systemTransition(system, snapshot, effect.source.$actor, {
          type: 'xstate.system.effect.result',
          effectId: id,
          ...(step.event && { event: step.event })
        });
      });
    }
  }
  writeFileSync(join(dir, `${name}.expected.json`), JSON.stringify(records, null, 2) + '\n');
  console.log(`${name}: ${records.length} records${records.some((r) => r.error) ? ' (with errors)' : ''}`);
}
