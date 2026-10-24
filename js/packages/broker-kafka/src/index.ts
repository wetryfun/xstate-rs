/**
 * `@xmachines/broker-kafka`: `BrokerConsumer` and `BrokerProducer` for
 * Kafka-API brokers through `@platformatic/kafka` (pure JavaScript). The
 * clients are injected: create a `Consumer` and a `Producer` with the
 * default (Buffer) serialisers and pass them in.
 *
 * Each assigned partition is a lease. A member reads its partitions through
 * one message stream, so when any lease's reader falls behind the stream is
 * paused (the client stops fetching) until every lease has room again.
 * Offsets are committed only through `PartitionLease.commit` (autocommit is
 * off). A rebalance revokes the leases of partitions the member lost; a
 * partition it keeps keeps its lease, and records the client fetches again
 * from the committed offset are skipped there.
 */
import { createLease, type BrokerConsumer, type BrokerProducer, type LeaseHandle, type OutboundRecord, type PartitionLease } from '@xmachines/broker';
import type { ConsumeOptions, Consumer, Message, MessagesStream, Producer } from '@platformatic/kafka';

type Bytes = Buffer;

export interface KafkaConsumerOptions {
  /** The injected group consumer (default Buffer deserialisers). */
  readonly consumer: Consumer<Bytes, Bytes, Bytes, Bytes>;
  /** Records buffered per partition before reading pauses (default 1000). */
  readonly highWaterMark?: number;
  /** More `consume` options (`sessionTimeout`, `heartbeatInterval`, `maxWaitTime`, ...). */
  readonly consume?: Partial<ConsumeOptions<Bytes, Bytes, Bytes, Bytes>>;
  /** Close the client with the broker consumer (default true; false leaves it to its owner). */
  readonly closeClient?: boolean;
}

interface Held {
  readonly handle: LeaseHandle;
  /** The next offset to deliver: anything before it was delivered already. */
  next: bigint;
  epoch: number;
}

const text = (b: Bytes) => b.toString('utf8');

function headers(map: Map<Bytes, Bytes> | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of map ?? []) {
    const name = text(k);
    if (!(name in out)) out[name] = text(v);
  }
  return out;
}

/** A group member reading through an injected `@platformatic/kafka` consumer. */
export function kafkaConsumer(options: KafkaConsumerOptions): BrokerConsumer {
  const { consumer } = options;
  const held = new Map<string, Held>();
  /** Leases whose readers are behind. */
  const behind = new Set<string>();
  let stream: MessagesStream<Bytes, Bytes, Bytes, Bytes> | undefined;
  let controller: ReadableStreamDefaultController<PartitionLease> | undefined;
  let closing = false;

  const assigned = (topic: string, partition: number) =>
    consumer.assignments?.some((a) => a.topic === topic && a.partitions.includes(partition)) ?? false;

  const release = (id: string) => {
    behind.delete(id);
    if (behind.size === 0 && stream?.isPaused()) stream.resume();
  };

  const revoke = (id: string) => {
    held.get(id)?.handle.revoke();
    held.delete(id);
    release(id);
  };

  const onJoin = () => {
    for (const [id, h] of held) if (!assigned(h.handle.lease.topic, h.handle.lease.partition)) revoke(id);
  };

  const lease = (topic: string, partition: number): Held => {
    const id = `${topic}:${partition}`;
    const existing = held.get(id);
    if (existing) return existing;
    const entry: Held = {
      next: -1n,
      epoch: -1,
      handle: createLease(
        topic,
        partition,
        {
          pause: () => {
            behind.add(id);
            stream?.pause();
          },
          resume: () => release(id),
          commit: async (offset) => {
            if (held.get(id) !== entry) throw new Error(`partition ${topic}/${partition} is no longer held`);
            await consumer.commit({
              offsets: [{ topic, partition, offset: BigInt(offset) + 1n, leaderEpoch: entry.epoch }],
            });
          },
        },
        { highWaterMark: options.highWaterMark },
      ),
    };
    held.set(id, entry);
    controller?.enqueue(entry.handle.lease);
    return entry;
  };

  const onMessage = (m: Message<Bytes, Bytes, Bytes, Bytes>) => {
    if (closing || !assigned(m.topic, m.partition)) return;
    const entry = lease(m.topic, m.partition);
    // After a rebalance the client fetches again from the committed offset.
    if (m.offset < entry.next) return;
    entry.next = m.offset + 1n;
    entry.epoch = m.leaderEpoch;
    entry.handle.push({
      topic: m.topic,
      partition: m.partition,
      offset: m.offset.toString(),
      key: m.key === undefined || m.key === null ? null : text(m.key),
      value: m.value ?? null,
      headers: headers(m.headers),
    });
  };

  return {
    leases(topics) {
      return new ReadableStream<PartitionLease>({
        start: async (c) => {
          controller = c;
          consumer.on('consumer:group:join', onJoin);
          stream = await consumer.consume({
            mode: 'committed',
            fallbackMode: 'earliest',
            ...options.consume,
            topics: [...topics],
            autocommit: false,
          });
          stream.on('data', onMessage);
          stream.on('error', (error: Error) => {
            if (!closing) c.error(error);
          });
        },
        cancel: () => {
          controller = undefined;
        },
      });
    },
    async close() {
      closing = true;
      for (const id of [...held.keys()]) revoke(id);
      try {
        controller?.close();
      } catch {
        // Already closed or cancelled.
      }
      controller = undefined;
      consumer.removeListener('consumer:group:join', onJoin);
      await stream?.close();
      if (options.closeClient ?? true) await consumer.close();
    },
  };
}

export interface KafkaProducerOptions {
  /** The injected producer (default Buffer serialisers). */
  readonly producer: Producer<Bytes, Bytes, Bytes, Bytes>;
  /** Acknowledgements per send: -1 (all in-sync replicas, the default), 1 or 0. */
  readonly acks?: number;
  /** Close the client with the broker producer (default true). */
  readonly closeClient?: boolean;
}

const bytes = (v: Uint8Array | string) => (typeof v === 'string' ? Buffer.from(v) : Buffer.from(v.buffer, v.byteOffset, v.byteLength));

/** Sends through an injected `@platformatic/kafka` producer. */
export function kafkaProducer(options: KafkaProducerOptions): BrokerProducer {
  const { producer } = options;
  return {
    async send(records: readonly OutboundRecord[]) {
      if (records.length === 0) return;
      await producer.send({
        acks: options.acks ?? -1,
        messages: records.map((r) => ({
          topic: r.topic,
          ...(r.key !== null && { key: bytes(r.key) }),
          ...(r.value !== null && { value: bytes(r.value) }),
          ...(r.headers && {
            headers: new Map(Object.entries(r.headers).map(([k, v]) => [Buffer.from(k), Buffer.from(v)])),
          }),
        })),
      });
    },
    async close() {
      if (options.closeClient ?? true) await producer.close();
    },
  };
}
