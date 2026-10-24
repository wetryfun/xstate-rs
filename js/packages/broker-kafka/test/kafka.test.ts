/**
 * The Kafka adapter against librdkafka's mock cluster (the Kafka protocol
 * over TCP, in a Rust process): `cargo build -p xstate-kafka --features
 * kafka --example mock_cluster`, or set XSTATE_MOCK_CLUSTER to the binary.
 * Skipped when neither is there.
 */
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { after, before, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import kafkajs from 'kafkajs';
import { serve, type Run } from '@xmachines/broker';
import { MemorySnapshotStore, xstateRuns } from '@xmachines/broker/xstate';
import { kafkaConsumer, kafkaProducer, type KafkaLike } from '@xmachines/broker-kafka';
import { redpanda } from '@xmachines/broker-redpanda';

const { Kafka, logLevel, Partitioners } = kafkajs;

const binary =
  process.env.XSTATE_MOCK_CLUSTER ?? fileURLToPath(new URL('../../../../target/debug/examples/mock_cluster', import.meta.url));
const skip = existsSync(binary) ? false : `no mock cluster binary at ${binary}`;

let cluster: ChildProcess | undefined;
let brokers: string[] = [];

before(async () => {
  if (skip) return;
  cluster = spawn(binary, ['1', 'in:4', 'out:4', 'dead:1', 'in2:2', 'out2:2', 'rp-in:2', 'rp-out:2'], {
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const line = await new Promise<string>((resolve) => createInterface({ input: cluster!.stdout! }).once('line', resolve));
  brokers = line.split(',');
});

after(() => {
  cluster?.stdin?.end();
  cluster?.kill();
});

const client = () => new Kafka({ brokers, logLevel: logLevel.NOTHING, retry: { retries: 10, initialRetryTime: 50 } });
const like = (kafka: InstanceType<typeof Kafka>) => kafka as unknown as KafkaLike;

const consumerConfig = { sessionTimeout: 6000, heartbeatInterval: 300, rebalanceTimeout: 6000, maxWaitTimeInMs: 50 };

async function produce(topic: string, records: [string, unknown][]) {
  const producer = client().producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  await producer.send({ topic, messages: records.map(([key, v]) => ({ key, value: JSON.stringify(v) })) });
  await producer.disconnect();
}

/** Reads `topic` from the beginning until `count` records arrived. */
async function readAll(topic: string, count: number, ms = 20000) {
  const consumer = client().consumer({ groupId: `reader-${topic}-${Date.now()}`, ...consumerConfig });
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  const out: { key: string; value: any }[] = [];
  await consumer.run({
    eachMessage: async ({ message }) => {
      out.push({ key: message.key!.toString(), value: JSON.parse(message.value!.toString()) });
    },
  });
  const deadline = Date.now() + ms;
  while (out.length < count && Date.now() < deadline) await new Promise((r) => setTimeout(r, 50));
  await new Promise((r) => setTimeout(r, 300));
  await consumer.disconnect();
  return out;
}

function counter(delayMs = 0, seen?: string[]) {
  return (runId: string): Run<{ type: string }, unknown> => {
    let n = 0;
    let out!: ReadableStreamDefaultController<unknown>;
    return {
      in: new WritableStream({
        async write(event) {
          if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
          seen?.push(`${runId}:${event.type}`);
          out.enqueue({ n: ++n, event: event.type });
        },
        close: () => out.close(),
      }),
      out: new ReadableStream({ start: (c) => void (out = c) }),
    };
  };
}

test('serves runs from Kafka partitions, pausing slow ones', { skip }, async () => {
  const records: [string, unknown][] = [];
  for (let i = 0; i < 25; i++) for (const k of ['a', 'b', 'c', 'd', 'e']) records.push([k, { type: `e${i}` }]);
  await produce('in', records);
  const kafka = client();
  let pauses = 0;
  // Count the adapter's pauses.
  const counting: KafkaLike = {
    consumer: (config) => {
      const c = like(kafka).consumer(config);
      const pause = c.pause.bind(c);
      c.pause = (topics) => {
        pauses++;
        pause(topics);
      };
      return c;
    },
    producer: (config) => like(kafka).producer(config),
  };
  const serving = serve({
    consumer: kafkaConsumer(counting, { groupId: 'g1', highWaterMark: 3, config: consumerConfig }),
    producer: kafkaProducer(like(kafka)),
    topics: { input: 'in', output: 'out', deadLetter: 'dead' },
    createRun: counter(3),
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
  const offsets = await kafka.admin().fetchOffsets({ groupId: 'g1', topics: ['in'] });
  const committed = offsets[0]!.partitions.reduce((sum, p) => sum + Math.max(Number(p.offset), 0), 0);
  assert.equal(committed, 125);
});

test('a second member takes partitions over; every event is processed at least once', { skip }, async () => {
  const seen: string[] = [];
  const kafka = client();
  const start = () =>
    serve({
      consumer: kafkaConsumer(like(kafka), { groupId: 'g2', config: consumerConfig }),
      producer: kafkaProducer(like(kafka)),
      topics: { input: 'in2', output: 'out2' },
      createRun: counter(0, seen),
      commitIntervalMs: 20,
    });
  const first = start();
  const keys = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6'];
  await produce('in2', keys.map((k) => [k, { type: 'e0' }]));
  const until = async (check: () => boolean) => {
    const deadline = Date.now() + 30000;
    while (!check()) {
      assert.ok(Date.now() < deadline, `timed out: ${seen.length} seen`);
      await new Promise((r) => setTimeout(r, 50));
    }
  };
  await until(() => keys.every((k) => seen.includes(`${k}:e0`)));
  const second = start();
  // Let the group rebalance, then send more.
  await new Promise((r) => setTimeout(r, 3000));
  await produce('in2', keys.map((k) => [k, { type: 'e1' }]));
  await until(() => keys.every((k) => seen.includes(`${k}:e1`)));
  await Promise.all([first.close(), second.close()]);
});

test('Redpanda clients drive XState runs', { skip }, async () => {
  const machine = {
    id: 'door',
    initial: 'closed',
    states: {
      closed: { on: { open: { target: 'opened', actions: [{ type: '@xstate.emit', event: { type: 'opened' } }] } } },
      opened: { on: { close: { target: 'closed', actions: [{ type: '@xstate.emit', event: { type: 'closed' } }] } } },
    },
  };
  const rp = redpanda({ brokers, client: { logLevel: logLevel.NOTHING } });
  const store = new MemorySnapshotStore();
  const serving = serve({
    consumer: rp.consumer({ groupId: 'g3', config: consumerConfig }),
    // KafkaJS's idempotent producer looks up the cluster's controller,
    // which the mock does not report (Redpanda does).
    producer: rp.producer({ config: { idempotent: false } }),
    topics: { input: 'rp-in', output: 'rp-out' },
    createRun: xstateRuns({ machine, store }),
  });
  await produce('rp-in', [
    ['d1', { type: 'open' }],
    ['d2', { type: 'open' }],
    ['d1', { type: 'close' }],
  ]);
  const out = await readAll('rp-out', 3);
  await serving.close();
  assert.deepEqual(
    out.filter((r) => r.key === 'd1').map((r) => r.value.type),
    ['opened', 'closed'],
  );
  assert.equal((store.snapshots.get('d2') as { value: string }).value, 'opened');
});
