/**
 * An in-memory broker with Kafka's model (partitioned logs, consumer groups
 * with committed offsets, one holder per partition), for tests and local
 * runs.
 */
import { createLease, type LeaseHandle, type LeaseOptions } from './lease.ts';
import type { BrokerConsumer, BrokerProducer, BrokerRecord, OutboundRecord, PartitionLease } from './types.ts';

const encoder = new TextEncoder();

interface Group {
  readonly committed: Map<string, bigint>;
  member?: MemoryConsumer;
}

export class MemoryBroker {
  readonly #logs = new Map<string, BrokerRecord[][]>();
  readonly #groups = new Map<string, Group>();
  readonly #waiting = new Set<() => void>();

  /** `topics` maps each topic to its partition count. */
  constructor(topics: Readonly<Record<string, number>>) {
    for (const [topic, partitions] of Object.entries(topics)) {
      this.#logs.set(
        topic,
        Array.from({ length: partitions }, () => []),
      );
    }
  }

  /** The partition a key goes to (FNV-1a; keyless records go to partition 0). */
  partitionOf(topic: string, key: string | null): number {
    const n = this.#log(topic).length;
    if (key === null) return 0;
    let h = 0x811c9dc5;
    for (const b of encoder.encode(key)) h = Math.imul(h ^ b, 0x01000193) >>> 0;
    return h % n;
  }

  /** Appends a record. */
  append(record: OutboundRecord): BrokerRecord {
    const partition = this.partitionOf(record.topic, record.key);
    const log = this.#log(record.topic)[partition]!;
    const value = typeof record.value === 'string' ? encoder.encode(record.value) : record.value;
    const stored: BrokerRecord = {
      topic: record.topic,
      partition,
      offset: String(log.length),
      key: record.key,
      value,
      headers: { ...record.headers },
    };
    log.push(stored);
    for (const wake of this.#waiting) wake();
    return stored;
  }

  /** A topic's records, all partitions, in partition then offset order. */
  records(topic: string): BrokerRecord[] {
    return this.#log(topic).flat();
  }

  /** A group's committed offset (the next record to read) of a partition. */
  committed(groupId: string, topic: string, partition: number): bigint | undefined {
    return this.#groups.get(groupId)?.committed.get(`${topic}/${partition}`);
  }

  producer(): BrokerProducer {
    return {
      send: async (records) => {
        for (const r of records) this.append(r);
      },
      close: async () => {},
    };
  }

  /** A member of `groupId`. A group has one member at a time: a new one takes every partition over. */
  consumer(groupId: string, options: LeaseOptions = {}): MemoryConsumer {
    let group = this.#groups.get(groupId);
    if (!group) {
      group = { committed: new Map() };
      this.#groups.set(groupId, group);
    }
    return new MemoryConsumer(this, group, options);
  }

  /** Revokes the group member's leases; it gets them back as new leases, from the committed offsets. */
  rebalance(groupId: string): void {
    this.#groups.get(groupId)?.member?.reassign();
  }

  /** @internal */
  log(topic: string): BrokerRecord[][] {
    return this.#log(topic);
  }

  /** @internal */
  wait(wake: () => void): () => void {
    this.#waiting.add(wake);
    return () => this.#waiting.delete(wake);
  }

  /** @internal */
  join(group: Group, member: MemoryConsumer): void {
    const previous = group.member;
    group.member = member;
    previous?.revokeAll();
  }

  #log(topic: string): BrokerRecord[][] {
    const log = this.#logs.get(topic);
    if (!log) throw new Error(`unknown topic "${topic}"`);
    return log;
  }
}

interface Held {
  readonly handle: LeaseHandle;
  position: number;
}

export class MemoryConsumer implements BrokerConsumer {
  readonly #broker: MemoryBroker;
  readonly #group: Group;
  readonly #options: LeaseOptions;
  readonly #held = new Map<string, Held>();
  #topics: readonly string[] = [];
  #controller: ReadableStreamDefaultController<PartitionLease> | undefined;
  #unwait: (() => void) | undefined;
  #scheduled = false;
  /** Times a partition was paused because its reader was behind. */
  pauses = 0;

  constructor(broker: MemoryBroker, group: Group, options: LeaseOptions) {
    this.#broker = broker;
    this.#group = group;
    this.#options = options;
  }

  leases(topics: readonly string[]): ReadableStream<PartitionLease> {
    this.#topics = topics;
    return new ReadableStream<PartitionLease>({
      start: (controller) => {
        this.#controller = controller;
        this.#broker.join(this.#group, this);
        this.#unwait = this.#broker.wait(() => this.#schedule());
        this.#assign();
      },
      cancel: () => {
        this.#controller = undefined;
      },
    });
  }

  async close(): Promise<void> {
    this.#unwait?.();
    for (const held of this.#held.values()) held.handle.revoke();
    this.#held.clear();
    try {
      this.#controller?.close();
    } catch {
      // Already cancelled.
    }
    this.#controller = undefined;
    if (this.#group.member === this) this.#group.member = undefined;
  }

  /** @internal */
  revokeAll(): void {
    for (const held of this.#held.values()) held.handle.revoke();
    this.#held.clear();
  }

  /** @internal */
  reassign(): void {
    this.revokeAll();
    this.#assign();
  }

  #assign() {
    const controller = this.#controller;
    if (!controller) return;
    for (const topic of this.#topics) {
      this.#broker.log(topic).forEach((_, partition) => {
        const id = `${topic}/${partition}`;
        const held: Held = {
          position: Number(this.#group.committed.get(id) ?? 0n),
          handle: createLease(
            topic,
            partition,
            {
              pause: () => {
                this.pauses++;
              },
              resume: () => this.#schedule(),
              commit: async (offset) => {
                if (this.#held.get(id) !== held) throw new Error(`partition ${id} is not held`);
                this.#group.committed.set(id, BigInt(offset) + 1n);
              },
            },
            this.#options,
          ),
        };
        this.#held.set(id, held);
        controller.enqueue(held.handle.lease);
      });
    }
    this.#schedule();
  }

  /** Delivers records asynchronously, as a fetch would. */
  #schedule() {
    if (this.#scheduled) return;
    this.#scheduled = true;
    setTimeout(() => {
      this.#scheduled = false;
      this.#fetch();
    }, 0);
  }

  #fetch() {
    for (const held of this.#held.values()) {
      const { topic, partition } = held.handle.lease;
      const log = this.#broker.log(topic)[partition]!;
      while (!held.handle.paused && held.position < log.length) {
        held.handle.push(log[held.position++]!);
      }
    }
  }
}
