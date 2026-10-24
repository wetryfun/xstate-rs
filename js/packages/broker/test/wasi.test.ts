/**
 * Runs on the xstate-rs runtime (the WASI component, transpiled with jco)
 * against the same machine on XState. Needs the component: `cargo build -p
 * xstate-wasi --target wasm32-wasip2 --release --target-dir target/wasi`
 * (`cargo test -p xstate-wasi-host` builds it too), or XSTATE_WASI_COMPONENT.
 * Skipped without it.
 */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { serve, type BrokerRecord } from '@xmachines/broker';
import { MemoryBroker } from '@xmachines/broker/memory';
import { componentImports, wasiRuns, type ComponentActors } from '@xmachines/broker/wasi';
import { MemorySnapshotStore, xstateRuns } from '@xmachines/broker/xstate';

const component =
  process.env.XSTATE_WASI_COMPONENT ??
  fileURLToPath(new URL('../../../../target/wasi/wasm32-wasip2/release/xstate_wasi.wasm', import.meta.url));
const skip = existsSync(component) ? false : `no component at ${component}`;

async function instantiate(): Promise<ComponentActors> {
  const dir = mkdtempSync(join(tmpdir(), 'xstate-jco-'));
  const jco = fileURLToPath(new URL('../../../node_modules/.bin/jco', import.meta.url));
  execFileSync(jco, ['transpile', component, '-o', dir, '--instantiation', 'async', '--name', 'xstate', '--quiet']);
  const { instantiate } = await import(pathToFileURL(join(dir, 'xstate.js')).href);
  const shim = await import('@bytecodealliance/preview2-shim');
  const runtime = await instantiate(
    (path: string) => WebAssembly.compile(readFileSync(join(dir, path))),
    componentImports(shim as never),
  );
  return runtime.actors;
}

const emit = (type: string) => [{ type: '@xstate.emit', event: { type } }];
const machine = {
  id: 'job',
  initial: 'idle',
  context: { pings: 0 },
  states: {
    idle: { on: { start: { target: 'running', actions: emit('started') } } },
    running: {
      after: { 150: { target: 'done', actions: emit('finished') } },
      on: { ping: { context: { pinged: true }, actions: emit('pong') } },
    },
    done: { on: { start: { target: 'running', actions: emit('restarted') } } },
  },
};

const decoder = new TextDecoder();
const types = (broker: MemoryBroker, key: string) =>
  broker
    .records('out')
    .filter((r: BrokerRecord) => r.key === key)
    .map((r: BrokerRecord) => JSON.parse(decoder.decode(r.value!)).type);

async function until(check: () => boolean, ms = 5000) {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
}

test('the xstate-rs runtime serves runs like XState does', { skip }, async () => {
  const actors = await instantiate();
  const topics = { input: 'in', output: 'out', deadLetter: 'dead' };
  const results: Record<string, string[]>[] = [];
  const stores = [new MemorySnapshotStore(), new MemorySnapshotStore()];
  const runs = [xstateRuns({ machine, store: stores[0] }), wasiRuns({ actors, machine, store: stores[1] })];
  for (const createRun of runs) {
    const broker = new MemoryBroker({ in: 2, out: 2, dead: 1 });
    const send = (key: string, type: string) => broker.append({ topic: 'in', key, value: JSON.stringify({ type }) });
    const serving = serve({ consumer: broker.consumer('g'), producer: broker.producer(), topics, createRun });
    for (const k of ['a', 'b']) {
      send(k, 'start');
      send(k, 'ping');
    }
    await until(() => types(broker, 'a').length === 3 && types(broker, 'b').length === 3);
    send('a', 'start');
    await until(() => types(broker, 'a').length === 4);
    await serving.close();
    assert.equal(broker.records('dead').length, 0);
    results.push({ a: types(broker, 'a'), b: types(broker, 'b') });
  }
  assert.deepEqual(results[0], { a: ['started', 'pong', 'finished', 'restarted'], b: ['started', 'pong', 'finished'] });
  assert.deepEqual(results[1], results[0]);
});

test('runtime runs restore from their stored snapshots, timers included', { skip }, async () => {
  const actors = await instantiate();
  const store = new MemorySnapshotStore();
  const broker = new MemoryBroker({ in: 1, out: 1 });
  const topics = { input: 'in', output: 'out' };
  const start = () =>
    serve({ consumer: broker.consumer('g'), producer: broker.producer(), topics, createRun: wasiRuns({ actors, machine, store }) });
  let serving = start();
  broker.append({ topic: 'in', key: 'j', value: JSON.stringify({ type: 'start' }) });
  await until(() => types(broker, 'j').length === 1);
  // Stop while the timer is pending; the next holder restores it.
  await serving.close();
  const persisted = store.snapshots.get('j') as { actors: Record<string, { snapshot: { value: string } }> };
  assert.equal(Object.values(persisted.actors)[0]!.snapshot.value, 'running');
  serving = start();
  broker.append({ topic: 'in', key: 'j', value: JSON.stringify({ type: 'ping' }) });
  await until(() => types(broker, 'j').length === 3);
  await serving.close();
  assert.deepEqual(types(broker, 'j'), ['started', 'pong', 'finished']);
});
