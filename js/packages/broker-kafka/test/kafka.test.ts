/**
 * The Kafka adapter over `@platformatic/kafka`, against librdkafka's mock
 * cluster (the Kafka protocol over TCP, in a Rust process): `cargo build -p
 * xstate-kafka --features kafka --example mock_cluster`, or set
 * XSTATE_MOCK_CLUSTER to the binary. Skipped when neither is there.
 *
 * The mock drops the flexible versions of the group APIs (JoinGroup 6,
 * SyncGroup 4) that @platformatic/kafka negotiates, and answers LeaveGroup 3
 * in a form it cannot read, so the clients the tests
 * inject are capped to earlier versions (`capped`); real brokers need none.
 */
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import * as fs from 'node:fs';
import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { after, before, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { Consumer, Producer } from '@platformatic/kafka';
import { serve, type BrokerRun } from '@xmachines/broker';
import { kafkaConsumer, kafkaProducer } from '@xmachines/broker-kafka';
import { redpandaClientOptions } from '@xmachines/broker-redpanda';
import { GitStore } from '@xmachines/git';
import { runner, type Event, type Input, type RunError } from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';

const binary =
  process.env.XSTATE_MOCK_CLUSTER ?? fileURLToPath(new URL('../../../../target/debug/examples/mock_cluster', import.meta.url));
const skip = existsSync(binary) ? false : `no mock cluster binary at ${binary}`;

let cluster: ChildProcess | undefined;
let brokers: string[] = [];

before(async () => {
  if (skip) return;
  cluster = spawn(binary, ['1', 'in:4', 'out:4', 'dead:1', 'in2:2', 'out2:2', 'rp-in:2', 'rp-out:2', 'rp-snap:1'], {
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const line = await new Promise<string>((resolve) => createInterface({ input: cluster!.stdout! }).once('line', resolve));
  brokers = line.split(',');
});

after(() => {
  cluster?.stdin?.end();
  cluster?.kill();
});

const caps: Record<string, number> = {
  JoinGroup: 5,
  SyncGroup: 3,
  Heartbeat: 3,
  LeaveGroup: 2,
  OffsetCommit: 7,
  OffsetFetch: 5,
  Fetch: 11,
  ListOffsets: 5,
  FindCoordinator: 2,
  Metadata: 8,
};

/** Caps a client's negotiated API versions (see the module comment). */
async function capped<C extends Consumer | Producer>(client: C): Promise<C> {
  const symbol = Object.getOwnPropertySymbols(client).find((s) => s.description === 'plt.kafka.base.apis')!;
  const apis = await client.listApis();
  (client as unknown as Record<symbol, unknown>)[symbol] = apis.map((api) =>
    caps[api.name] === undefined ? api : { ...api, maxVersion: Math.min(api.maxVersion, caps[api.name]!) },
  );
  return client;
}

const group = { sessionTimeout: 6000, heartbeatInterval: 300, rebalanceTimeout: 6000 };
const consumer = (groupId: string) =>
  capped(new Consumer({ clientId: `c-${groupId}`, bootstrapBrokers: brokers, groupId, ...group }));
const producer = () => capped(new Producer({ clientId: 'p', bootstrapBrokers: brokers }));

async function produce(topic: string, records: [string, unknown][]) {
  const p = await producer();
  await p.send({ messages: records.map(([key, v]) => ({ topic, key: Buffer.from(key), value: Buffer.from(JSON.stringify(v)) })) });
  await p.close();
}

/** Reads `topic` from the beginning until `count` records arrived. */
async function readAll(topic: string, count: number, ms = 20000) {
  const c = await consumer(`reader-${topic}-${Date.now()}`);
  const stream = await c.consume({ topics: [topic], mode: 'earliest', autocommit: false, maxWaitTime: 50, ...group });
  const out: { key: string; value: any }[] = [];
  stream.on('data', (m) => out.push({ key: m.key!.toString(), value: JSON.parse(m.value!.toString()) }));
  const deadline = Date.now() + ms;
  while (out.length < count && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
  await new Promise((r) => setTimeout(r, 300));
  await stream.close();
  await c.close(true);
  return out;
}

function counter(delayMs = 0, seen?: string[]) {
  return (key: string): BrokerRun => {
    let n = 0;
    let out!: ReadableStreamDefaultController<Event>;
    let err!: ReadableStreamDefaultController<RunError>;
    return {
      in: new WritableStream<Input>({
        async write(input) {
          const event = input as Event;
          if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
          seen?.push(`${key}:${event.type}`);
          out.enqueue({ type: 'counted', n: ++n, event: event.type });
        },
        close: () => {
          out.close();
          err.close();
        },
      }),
      out: new ReadableStream({ start: (c) => void (out = c) }),
      err: new ReadableStream({ start: (c) => void (err = c) }),
    };
  };
}

test('serves runs from Kafka partitions, pausing for slow ones', { skip }, async () => {
  const records: [string, unknown][] = [];
  for (let i = 0; i < 25; i++) for (const k of ['a', 'b', 'c', 'd', 'e']) records.push([k, { type: `e${i}` }]);
  await produce('in', records);
  const client = await consumer('g1');
  // Count the pauses of the client's message stream.
  let pauses = 0;
  const consume = client.consume.bind(client) as (o: unknown) => Promise<{ on(e: 'pause', f: () => void): unknown }>;
  (client as { consume: unknown }).consume = async (options: unknown) => {
    const stream = await consume(options);
    stream.on('pause', () => pauses++);
    return stream;
  };
  const serving = serve({
    consumer: kafkaConsumer({ consumer: client, highWaterMark: 3, consume: { maxWaitTime: 50, ...group } }),
    producer: kafkaProducer({ producer: await producer() }),
    topics: { input: 'in', output: 'out', deadLetter: 'dead' },
    open: counter(3),
    commitIntervalMs: 20,
  });
  const out = await readAll('out', 125);
  await serving.close();
  assert.equal(out.length, 125);
  for (const k of ['a', 'b', 'c', 'd', 'e']) {
    assert.deepEqual(
      out.filter((r) => r.key === k).map((r) => r.value.event),
      Array.from({ length: 25 }, (_, i) => `e${i}`),
    );
  }
  assert.ok(pauses > 0, 'paused');
  // Everything processed was committed.
  const check = await consumer('g1');
  const committed = await check.listCommittedOffsets({ topics: [{ topic: 'in', partitions: [0, 1, 2, 3] }] });
  await check.close(true);
  assert.equal(
    committed.get('in')!.reduce((sum, o) => sum + (o > 0n ? o : 0n), 0n),
    125n,
  );
});

// Two members at once cannot be tested here: the mock answers a follower's
// SyncGroup with INVALID_REQUEST once the group is stable (a real broker
// returns the assignment). A handover between members is the same path for
// the partitions: revoked by one, assigned to the other from the commits.
test('another member takes the partitions over from the committed offsets', { skip }, async () => {
  const seen: string[] = [];
  const start = async () =>
    serve({
      consumer: kafkaConsumer({ consumer: await consumer('g2'), consume: { maxWaitTime: 50, ...group } }),
      producer: kafkaProducer({ producer: await producer() }),
      topics: { input: 'in2', output: 'out2' },
      open: counter(0, seen),
      commitIntervalMs: 20,
    });
  const until = async (check: () => boolean) => {
    const deadline = Date.now() + 30000;
    while (!check()) {
      assert.ok(Date.now() < deadline, `timed out: ${seen.length} seen`);
      await new Promise((r) => setTimeout(r, 50));
    }
  };
  const keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6'];
  const first = await start();
  await produce('in2', keys.map((k) => [k, { type: 'e0' }]));
  await until(() => keys.every((k) => seen.includes(`${k}:e0`)));
  await first.close();
  const second = await start();
  await produce('in2', keys.map((k) => [k, { type: 'e1' }]));
  await until(() => keys.every((k) => seen.includes(`${k}:e1`)));
  await second.close();
  // Committed on close: the second member did not process e0 again.
  assert.equal(seen.filter((s) => s.endsWith(':e0')).length, keys.length);
});

test('XState runs journaled in Git, served from Redpanda-style clients', { skip }, async () => {
  const machine = {
    id: 'door',
    initial: 'closed',
    states: {
      closed: { on: { open: { target: 'opened', actions: [{ type: '@xstate.emit', event: { type: 'opened' } }] } } },
      opened: { on: { close: { target: 'closed', actions: [{ type: '@xstate.emit', event: { type: 'closed' } }] } } },
    },
  };
  const options = redpandaClientOptions({ brokers, clientId: 'doors' });
  const store = new GitStore({ fs, gitdir: mkdtempSync(join(tmpdir(), 'xmachines-kafka-')) });
  const serving = serve({
    consumer: kafkaConsumer({
      consumer: await capped(new Consumer({ ...options, groupId: 'g3', ...group })),
      consume: { maxWaitTime: 50, ...group },
    }),
    producer: kafkaProducer({ producer: await capped(new Producer(options)) }),
    topics: { input: 'rp-in', output: 'rp-out', snapshots: 'rp-snap' },
    open: runner({ engine: xstateEngine(), machine, journals: store }),
  });
  await produce('rp-in', [
    ['d1', { type: 'open' }],
    ['d2', { type: 'open' }],
    ['d1', { type: 'close' }],
  ]);
  const out = await readAll('rp-out', 3);
  const snapshots = await readAll('rp-snap', 2);
  await serving.close();
  assert.deepEqual(
    out.filter((r) => r.key === 'd1').map((r) => r.value.type),
    ['opened', 'closed'],
  );
  const latest = new Map(snapshots.map((r) => [r.key, r.value]));
  assert.equal(latest.get('d2').value, 'opened');
  // Each door is a run (a fork of the machine) in the repository.
  assert.equal((await store.runs()).length, 2);
});
