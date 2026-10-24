/**
 * `@xmachines/broker-kafka`: `BrokerConsumer` and `BrokerProducer` for
 * Kafka-API brokers through a KafkaJS-style client: KafkaJS itself, or the
 * KafkaJS-compatible API of `@confluentinc/kafka-javascript` (librdkafka).
 *
 * Each assigned partition is a lease; when its reader falls behind, the
 * partition is paused (`consumer.pause`) and resumed when the reader catches
 * up. Offsets are committed only through `PartitionLease.commit` (auto-commit
 * is off). Every rebalance revokes the member's leases (KafkaJS rebalances
 * eagerly) and partitions come back as new leases from the committed
 * offsets.
 */
import { createLease, type BrokerConsumer, type BrokerProducer, type LeaseHandle, type OutboundRecord, type PartitionLease } from '@xmachines/broker';

type Bytes = Uint8Array;
type HeaderValue = Bytes | string | (Bytes | string)[] | undefined;

/** The message shape KafkaJS hands to `eachMessage`. */
export interface KafkaMessage {
  readonly key: Bytes | null;
  readonly value: Bytes | null;
  readonly offset: string;
  readonly headers?: Readonly<Record<string, HeaderValue>>;
}

export interface TopicPartitions {
  topic: string;
  partitions?: number[];
}

/** The consumer methods this package uses (KafkaJS's `Consumer`). */
export interface ConsumerLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(subscription: { topics: string[]; fromBeginning?: boolean }): Promise<void>;
  run(config: {
    autoCommit?: boolean;
    partitionsConsumedConcurrently?: number;
    eachMessage(payload: { topic: string; partition: number; message: KafkaMessage }): Promise<void>;
  }): Promise<void>;
  pause(topics: TopicPartitions[]): void;
  resume(topics: TopicPartitions[]): void;
  commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void>;
  on(event: string, listener: (event: { payload?: unknown }) => void): unknown;
  readonly events: { readonly GROUP_JOIN: string; readonly REBALANCING?: string; readonly CRASH?: string };
}

/** The producer methods this package uses (KafkaJS's `Producer`). */
export interface ProducerLike {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  sendBatch(batch: {
    topicMessages: { topic: string; messages: { key: string | null; value: Bytes | string | null; headers?: Record<string, string> }[] }[];
    acks?: number;
  }): Promise<unknown>;
}

/** A KafkaJS-style client (`new Kafka({ brokers })`). */
export interface KafkaLike {
  consumer(config: { groupId: string } & Record<string, unknown>): ConsumerLike;
  producer(config?: Record<string, unknown>): ProducerLike;
}

export interface KafkaConsumerOptions {
  readonly groupId: string;
  /** Read partitions without a committed offset from the beginning (default true). */
  readonly fromBeginning?: boolean;
  /** Records buffered per partition before it is paused (default 1000). */
  readonly highWaterMark?: number;
  /** Partitions handed to readers at the same time (default 16). */
  readonly partitionsConsumedConcurrently?: number;
  /** More consumer settings (`sessionTimeout`, `heartbeatInterval`, ...). */
  readonly config?: Record<string, unknown>;
}

const text = (v: Bytes | string) => (typeof v === 'string' ? v : new TextDecoder().decode(v));

function headers(h: KafkaMessage['headers']): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(h ?? {})) {
    const first = Array.isArray(value) ? value[0] : value;
    if (first !== undefined) out[name] = text(first);
  }
  return out;
}

/** A group member reading through `kafka`. */
export function kafkaConsumer(kafka: KafkaLike, options: KafkaConsumerOptions): BrokerConsumer {
  const consumer = kafka.consumer({ ...options.config, groupId: options.groupId });
  const held = new Map<string, LeaseHandle>();
  let controller: ReadableStreamDefaultController<PartitionLease> | undefined;
  let closing = false;

  const revokeAll = () => {
    for (const h of held.values()) h.revoke();
    held.clear();
  };

  const lease = (topic: string, partition: number): LeaseHandle => {
    const id = `${topic}/${partition}`;
    let handle = held.get(id);
    if (handle) return handle;
    const created: LeaseHandle = createLease(
      topic,
      partition,
      {
        pause: () => consumer.pause([{ topic, partitions: [partition] }]),
        resume: () => {
          if (held.get(id) === created) consumer.resume([{ topic, partitions: [partition] }]);
        },
        commit: async (offset) => {
          if (held.get(id) !== created) throw new Error(`partition ${id} is no longer held`);
          await consumer.commitOffsets([{ topic, partition, offset: (BigInt(offset) + 1n).toString() }]);
        },
      },
      { highWaterMark: options.highWaterMark },
    );
    handle = created;
    held.set(id, handle);
    controller?.enqueue(handle.lease);
    return handle;
  };

  return {
    leases(topics) {
      return new ReadableStream<PartitionLease>({
        start: async (c) => {
          controller = c;
          // A rebalance revokes every lease: KafkaJS's protocol is eager,
          // and it fetches again from the committed offsets.
          consumer.on(consumer.events.GROUP_JOIN, revokeAll);
          if (consumer.events.REBALANCING) consumer.on(consumer.events.REBALANCING, revokeAll);
          if (consumer.events.CRASH) {
            consumer.on(consumer.events.CRASH, (e) => {
              const payload = e.payload as { error?: unknown; restart?: boolean } | undefined;
              if (!payload?.restart && !closing) c.error(payload?.error ?? new Error('the Kafka consumer crashed'));
            });
          }
          await consumer.connect();
          await consumer.subscribe({ topics: [...topics], fromBeginning: options.fromBeginning ?? true });
          await consumer.run({
            autoCommit: false,
            partitionsConsumedConcurrently: options.partitionsConsumedConcurrently ?? 16,
            eachMessage: async ({ topic, partition, message }) => {
              if (closing) return;
              // A full lease pauses its partition; the rest of this batch
              // is not delivered and is fetched again after the resume.
              lease(topic, partition).push({
                topic,
                partition,
                offset: message.offset,
                key: message.key === null ? null : text(message.key),
                value: message.value,
                headers: headers(message.headers),
              });
            },
          });
        },
        cancel: () => {
          controller = undefined;
        },
      });
    },
    async close() {
      closing = true;
      revokeAll();
      try {
        controller?.close();
      } catch {
        // Already closed or cancelled.
      }
      controller = undefined;
      await consumer.disconnect();
    },
  };
}

export interface KafkaProducerOptions {
  /** Producer settings (`idempotent`, `maxInFlightRequests`, ...). */
  readonly config?: Record<string, unknown>;
  /** Acknowledgements per send: -1 (all in-sync replicas, the default), 1 or 0. */
  readonly acks?: number;
}

/** Sends through `kafka`; connects on first use. */
export function kafkaProducer(kafka: KafkaLike, options: KafkaProducerOptions = {}): BrokerProducer {
  const producer = kafka.producer(options.config);
  let connected: Promise<void> | undefined;
  return {
    async send(records: readonly OutboundRecord[]) {
      if (records.length === 0) return;
      await (connected ??= producer.connect());
      const byTopic = new Map<string, { key: string | null; value: Bytes | string | null; headers?: Record<string, string> }[]>();
      for (const r of records) {
        let messages = byTopic.get(r.topic);
        if (!messages) byTopic.set(r.topic, (messages = []));
        messages.push({ key: r.key, value: r.value, ...(r.headers ? { headers: { ...r.headers } } : {}) });
      }
      await producer.sendBatch({
        topicMessages: [...byTopic].map(([topic, messages]) => ({ topic, messages })),
        acks: options.acks ?? -1,
      });
    },
    async close() {
      if (connected) await producer.disconnect();
    },
  };
}
