import assert from 'node:assert/strict';
import { test } from 'node:test';
import { serve, type BrokerProducer, type BrokerRecord, type OutboundRecord, type Run } from '@xmachines/broker';
import { MemoryBroker } from '@xmachines/broker/memory';
import { MemorySnapshotStore, xstateRuns } from '@xmachines/broker/xstate';

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

/** A run that numbers its inputs and outputs `{ n, event }` for each. */
function counter(delayMs = 0, log?: string[]) {
  return (runId: string): Run<{ type: string }, unknown> => {
    let n = 0;
    let out!: ReadableStreamDefaultController<unknown>;
    return {
      in: new WritableStream({
        async write(event) {
          if (delayMs) await sleep(delayMs);
          if (event.type === 'boom') throw new Error('boom');
          log?.push(`${runId}:${event.type}`);
          out.enqueue({ n: ++n, event: event.type });
        },
        close() {
          out.close();
        },
      }),
      out: new ReadableStream({
        start(c) {
          out = c;
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
  const serving = serve({ consumer: broker.consumer('g'), producer: broker.producer(), topics, createRun: counter() });
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
      Array.from({ length: 20 }, (_, i) => ({ n: i + 1, event: `e${i}` })),
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
  const serving = serve({ consumer, producer: broker.producer(), topics, createRun: counter(2) });
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
  const serving = serve({ consumer, producer, topics, createRun: counter(), commitIntervalMs: 5 });
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

test('records that cannot be processed become dead letters', async () => {
  const broker = new MemoryBroker({ in: 1, out: 1, dead: 1 });
  send(broker, null, { type: 'x' });
  send(broker, 'k', 'not json');
  send(broker, 'k', { type: 'one' });
  send(broker, 'k', { type: 'boom' });
  send(broker, 'k', { type: 'two' });
  const serving = serve({ consumer: broker.consumer('g'), producer: broker.producer(), topics, createRun: counter() });
  await until(() => broker.records('dead').length === 3 && broker.records('out').length === 2).catch((e) => {
    throw new Error(`${e.message}: dead ${JSON.stringify(broker.records('dead').map((r) => r.headers.reason))}, out ${broker.records('out').length}`);
  });
  await serving.close();
  const reasons = broker.records('dead').map((r) => r.headers.reason!);
  assert.match(reasons[0]!, /no run id/);
  assert.match(reasons[1]!, /undecodable/);
  assert.match(reasons[2]!, /run failed: boom/);
  // The failed run ended; the next record started a new one.
  assert.deepEqual(
    broker.records('out').map(json),
    [
      { n: 1, event: 'one' },
      { n: 1, event: 'two' },
    ],
  );
  assert.equal(broker.committed('g', 'in', 0), 5n);
});

test('a rebalance hands partitions over from the committed offsets', async () => {
  const broker = new MemoryBroker({ in: 2, out: 2, dead: 1 });
  const seen: string[] = [];
  const serving = serve({
    consumer: broker.consumer('g'),
    producer: broker.producer(),
    topics,
    createRun: counter(1, seen),
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

test('XState runs keep their state in a snapshot store across restarts', async () => {
  const machine = {
    id: 'light',
    initial: 'green',
    states: {
      green: { on: { next: { target: 'yellow', actions: [{ type: '@xstate.emit', event: { type: 'yellow' } }] } } },
      yellow: { on: { next: { target: 'red', actions: [{ type: '@xstate.emit', event: { type: 'red' } }] } } },
      red: { on: { next: { target: 'green', actions: [{ type: '@xstate.emit', event: { type: 'green' } }] } } },
    },
  };
  const store = new MemorySnapshotStore();
  const broker = new MemoryBroker({ in: 2, out: 2, dead: 1 });
  const start = () =>
    serve({ consumer: broker.consumer('g'), producer: broker.producer(), topics, createRun: xstateRuns({ machine, store }) });
  let serving = start();
  send(broker, 'x', { type: 'next' });
  send(broker, 'y', { type: 'next' });
  send(broker, 'x', { type: 'next' });
  await until(() => broker.records('out').length === 3);
  await serving.close();
  assert.equal((store.snapshots.get('x') as { value: string }).value, 'red');

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
});
