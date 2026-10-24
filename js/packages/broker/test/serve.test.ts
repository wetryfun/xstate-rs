import assert from 'node:assert/strict';
import { test } from 'node:test';
import * as fs from 'node:fs';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { serve, type BrokerProducer, type BrokerRecord, type BrokerRun, type OutboundRecord } from '@xmachines/broker';
import { MemoryBroker } from '@xmachines/broker/memory';
import { GitStore } from '@xmachines/git';
import { MemoryJournals, runner, type Event, type Input, type RunError } from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';

const decoder = new TextDecoder();
const json = (r: BrokerRecord) => JSON.parse(decoder.decode(r.value!));
const topics = { input: 'in', output: 'out', deadLetter: 'dead' };

async function until(check: () => boolean, ms = 5000) {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) throw new Error('timed out');
    await new Promise((r) => setTimeout(r, 5));
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * A run that numbers its inputs and outputs `{ n, event }` for each; `boom`
 * is reported on `err`, and `fatal` breaks the run (its write rejects).
 */
function counter(delayMs = 0, log?: string[]) {
  return (key: string): BrokerRun => {
    let n = 0;
    let out!: ReadableStreamDefaultController<Event>;
    let err!: ReadableStreamDefaultController<RunError>;
    return {
      in: new WritableStream<Input>({
        async write(input) {
          const event = input as Event;
          if (delayMs) await sleep(delayMs);
          if (event.type === 'fatal') throw new Error('fatal');
          if (event.type === 'boom') {
            err.enqueue({ run: key, seq: ++n, batch: [event], message: 'boom' });
            return;
          }
          log?.push(`${key}:${event.type}`);
          out.enqueue({ type: 'counted', n: ++n, event: event.type });
        },
        close() {
          out.close();
          err.close();
        },
      }),
      out: new ReadableStream({
        start(c) {
          out = c;
        },
      }),
      err: new ReadableStream({
        start(c) {
          err = c;
        },
      }),
    };
  };
}

function send(broker: MemoryBroker, key: string | null, value: unknown) {
  broker.append({ topic: 'in', key, value: typeof value === 'string' ? value : JSON.stringify(value) });
}

test('runs get their records in order and outputs go out under the run id', async () => {
  const broker = new MemoryBroker({ in: 3, out: 3, dead: 1 });
  for (let i = 0; i < 20; i++) for (const k of ['a', 'b', 'c', 'd']) send(broker, k, { type: `e${i}` });
  const serving = serve({ consumer: broker.consumer('g'), producer: broker.producer(), topics, open: counter() });
  await until(() => broker.records('out').length === 80);
  await serving.close();
  const byKey = new Map<string, unknown[]>();
  for (const r of broker.records('out')) {
    if (!byKey.has(r.key!)) byKey.set(r.key!, []);
    byKey.get(r.key!)!.push(json(r));
    assert.equal(r.headers['content-type'], 'application/json');
  }
  for (const k of ['a', 'b', 'c', 'd']) {
    assert.deepEqual(
      byKey.get(k),
      Array.from({ length: 20 }, (_, i) => ({ type: 'counted', n: i + 1, event: `e${i}` })),
    );
  }
  for (let p = 0; p < 3; p++) {
    assert.equal(broker.committed('g', 'in', p) ?? 0n, BigInt(broker.log('in')[p]!.length));
  }
});

test('a slow run pauses its partition and catches up', async () => {
  const broker = new MemoryBroker({ in: 1, out: 1, dead: 1 });
  for (let i = 0; i < 40; i++) send(broker, 'k', { type: `e${i}` });
  const consumer = broker.consumer('g', { highWaterMark: 4 });
  const serving = serve({ consumer, producer: broker.producer(), topics, open: counter(2) });
  await until(() => broker.records('out').length === 40);
  await serving.close();
  assert.ok(consumer.pauses > 0, `paused ${consumer.pauses} times`);
  assert.deepEqual(
    broker.records('out').map((r) => json(r).n),
    Array.from({ length: 40 }, (_, i) => i + 1),
  );
  assert.equal(broker.committed('g', 'in', 0), 40n);
});

test('offsets are committed only after their outputs are acknowledged', async () => {
  const broker = new MemoryBroker({ in: 1, out: 1, dead: 1 });
  const acknowledged: OutboundRecord[] = [];
  // A producer that takes a while to acknowledge.
  const producer: BrokerProducer = {
    async send(records) {
      await sleep(30);
      acknowledged.push(...records);
      for (const r of records) broker.append(r);
    },
    async close() {},
  };
  const consumer = broker.consumer('g');
  const checks: [bigint, number][] = [];
  const committed = () => broker.committed('g', 'in', 0) ?? 0n;
  let last = 0n;
  const watch = setInterval(() => {
    if (committed() !== last) {
      last = committed();
      checks.push([last, acknowledged.length]);
    }
  }, 1);
  const serving = serve({ consumer, producer, topics, open: counter(), commitIntervalMs: 5 });
  for (let i = 0; i < 30; i++) {
    send(broker, 'k', { type: `e${i}` });
    await sleep(3);
  }
  await until(() => committed() === 30n);
  clearInterval(watch);
  await serving.close();
  assert.ok(checks.length > 1, JSON.stringify(checks.map(([c, a]) => [String(c), a])));
  // Every committed input's output was acknowledged by then (one output per input).
  for (const [offset, acks] of checks) assert.ok(BigInt(acks) >= offset, `${offset} committed with ${acks} acknowledged`);
});

test('records that cannot be processed and run errors become dead letters', async () => {
  const broker = new MemoryBroker({ in: 1, out: 1, dead: 1 });
  send(broker, null, { type: 'x' });
  send(broker, 'k', 'not json');
  send(broker, 'k', { type: 'one' });
  send(broker, 'k', { type: 'boom' });
  send(broker, 'k', { type: 'two' });
  send(broker, 'k', { type: 'fatal' });
  send(broker, 'k', { type: 'three' });
  const serving = serve({ consumer: broker.consumer('g'), producer: broker.producer(), topics, open: counter() });
  await until(() => broker.records('dead').length === 4 && broker.records('out').length === 3);
  await serving.close();
  const reasons = broker.records('dead').map((r) => r.headers.reason!);
  assert.match(reasons[0]!, /no key/);
  assert.match(reasons[1]!, /undecodable/);
  assert.equal(reasons[2], 'boom', 'reported by the run, which goes on');
  assert.deepEqual(json(broker.records('dead')[2]!), { run: 'k', seq: 2, batch: [{ type: 'boom' }], message: 'boom' });
  assert.match(reasons[3]!, /run failed: fatal/);
  // The broken run was dropped; the next record opened a new one.
  assert.deepEqual(
    broker.records('out').map((r) => [json(r).event, json(r).n]),
    [
      ['one', 1],
      ['two', 3],
      ['three', 1],
    ],
  );
  assert.equal(broker.committed('g', 'in', 0), 7n);
});

test('a rebalance hands partitions over from the committed offsets', async () => {
  const broker = new MemoryBroker({ in: 2, out: 2, dead: 1 });
  const seen: string[] = [];
  const serving = serve({
    consumer: broker.consumer('g'),
    producer: broker.producer(),
    topics,
    open: counter(1, seen),
    commitIntervalMs: 5,
  });
  for (let i = 0; i < 30; i++) {
    for (const k of ['a', 'b', 'c']) send(broker, k, { type: `e${i}` });
    if (i === 15) broker.rebalance('g');
    await sleep(2);
  }
  await until(() => {
    const done = (k: string) => seen.lastIndexOf(`${k}:e29`) >= 0;
    return done('a') && done('b') && done('c');
  });
  await serving.close();
  // At least once: every event reached its run, in order per run instance.
  for (const k of ['a', 'b', 'c']) {
    const events = seen.filter((s) => s.startsWith(`${k}:`)).map((s) => Number(s.slice(3)));
    for (let i = 0; i < 30; i++) assert.ok(events.includes(i), `${k} got e${i}`);
  }
  for (let p = 0; p < 2; p++) assert.equal(broker.committed('g', 'in', p) ?? 0n, BigInt(broker.log('in')[p]!.length));
});

const light = {
  id: 'light',
  initial: 'green',
  states: {
    green: { on: { next: { target: 'yellow', actions: [{ type: '@xstate.emit', event: { type: 'yellow' } }] } } },
    yellow: { on: { next: { target: 'red', actions: [{ type: '@xstate.emit', event: { type: 'red' } }] } } },
    red: { on: { next: { target: 'green', actions: [{ type: '@xstate.emit', event: { type: 'green' } }] } } },
  },
};

for (const [name, journals] of [
  ['memory', () => new MemoryJournals()],
  ['Git', () => new GitStore({ fs, gitdir: mkdtempSync(join(tmpdir(), 'xmachines-broker-')) })],
] as const) {
  test(`runs keep their state in ${name} journals across restarts, and publish snapshots`, async () => {
    const store = journals();
    const broker = new MemoryBroker({ in: 2, out: 2, dead: 1, snapshots: 1 });
    const start = () =>
      serve({
        consumer: broker.consumer('g'),
        producer: broker.producer(),
        topics: { ...topics, snapshots: 'snapshots' },
        open: runner({ engine: xstateEngine(), machine: light, journals: store }),
      });
    let serving = start();
    send(broker, 'x', { type: 'next' });
    send(broker, 'y', { type: 'next' });
    send(broker, 'x', { type: 'next' });
    await until(() => broker.records('out').length === 3);
    await serving.close();

    serving = start();
    send(broker, 'x', { type: 'next' });
    send(broker, 'y', { type: 'unknown' });
    send(broker, 'y', { type: 'next' });
    await until(() => broker.records('out').length === 5);
    await serving.close();
    const types = (k: string) =>
      broker
        .records('out')
        .filter((r) => r.key === k)
        .map((r) => json(r).type);
    assert.deepEqual(types('x'), ['yellow', 'red', 'green']);
    assert.deepEqual(types('y'), ['yellow', 'red']);
    assert.equal(broker.records('dead').length, 0);
    const latest = new Map(broker.records('snapshots').map((r) => [r.key, json(r)]));
    assert.equal(latest.get('x').value, 'green');
    assert.equal(latest.get('y').value, 'red');
  });
}

const profile = {
  id: 'profile',
  initial: 'idle',
  states: {
    idle: { on: { load: { target: 'loading' } } },
    loading: {
      invoke: {
        src: 'fetchUser',
        id: 'fetch',
        input: { id: 7 },
        onDone: { target: 'loaded', actions: [{ type: '@xstate.emit', event: { type: 'loaded' } }] },
      },
    },
    loaded: {},
  },
};

test('runs do their async work: in their host, or through a work topic and the input topic', async () => {
  // In the host.
  let broker = new MemoryBroker({ in: 1, out: 1, dead: 1 });
  let serving = serve({
    consumer: broker.consumer('g'),
    producer: broker.producer(),
    topics,
    open: runner({ engine: xstateEngine(), machine: profile, work: { fetchUser: async (input) => ({ input }) } }),
  });
  send(broker, 'p', { type: 'load' });
  await until(() => broker.records('out').length === 1);
  await serving.close();
  assert.equal(json(broker.records('out')[0]!).type, 'loaded');

  // Through a work topic, by a worker that answers on the input topic.
  broker = new MemoryBroker({ in: 1, out: 1, dead: 1, work: 1 });
  serving = serve({
    consumer: broker.consumer('g'),
    producer: broker.producer(),
    topics: { ...topics, work: 'work' },
    open: runner({ engine: xstateEngine(), machine: profile }),
  });
  send(broker, 'p', { type: 'load' });
  await until(() => broker.records('work').length === 1);
  const work = broker.records('work')[0]!;
  assert.equal(work.key, 'p');
  const request = json(work);
  assert.deepEqual({ src: request.src, input: request.input, actor: typeof request.actor }, { src: 'fetchUser', input: { id: 7 }, actor: 'string' });
  send(broker, 'p', { type: 'xstate.system.effect.result', effectId: request.id, event: { type: 'xstate.async.resolve', data: {} } });
  await until(() => broker.records('out').length === 1);
  await new Promise((r) => setTimeout(r, 20));
  await serving.close();
  assert.equal(json(broker.records('out')[0]!).type, 'loaded');
  assert.equal(broker.records('work').length, 1, 'published once');
  assert.equal(broker.records('dead').length, 0);
});
