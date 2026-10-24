/**
 * The broker interfaces: what a transport (Kafka, Redpanda, memory) gives
 * the runner. A consumer hands out partition leases, each a stream of
 * records in offset order held by one consumer at a time; a producer sends
 * records.
 */

/** A record read from a partition. */
export interface BrokerRecord {
  readonly topic: string;
  readonly partition: number;
  /** The record's offset, as a decimal string (Kafka offsets are 64-bit). */
  readonly offset: string;
  readonly key: string | null;
  readonly value: Uint8Array | null;
  /** Header values as text (the first value of a repeated header). */
  readonly headers: Readonly<Record<string, string>>;
}

/** A record to send. */
export interface OutboundRecord {
  readonly topic: string;
  /** Records with the same key go to the same partition, in order. */
  readonly key: string | null;
  readonly value: Uint8Array | string | null;
  readonly headers?: Readonly<Record<string, string>>;
}

/**
 * A partition this consumer holds, until it is revoked (a rebalance moved it
 * to another consumer of the group) or the consumer closes. Only the lease
 * holder reads the partition: the single-writer guarantee for the runs keyed
 * to it.
 */
export interface PartitionLease {
  readonly topic: string;
  readonly partition: number;
  /**
   * The partition's records in offset order, from the group's committed
   * offset. Reading slower than records arrive pauses fetching this
   * partition until the reader catches up (backpressure). The stream errors
   * when the lease is revoked, discarding what it buffered: the next holder
   * reads those records again.
   */
  readonly records: ReadableStream<BrokerRecord>;
  /**
   * Commits the partition up to and including `offset`: the group's next
   * holder starts after it. Call it only once every record up to `offset`
   * is processed (at-least-once delivery).
   */
  commit(offset: string): Promise<void>;
  /** Aborted when the lease is revoked or the consumer closes. */
  readonly signal: AbortSignal;
}

/** Reads topics as a member of a consumer group. */
export interface BrokerConsumer {
  /**
   * Joins the group for `topics`. Every partition assigned to this member
   * becomes a lease; a partition assigned again after a revocation is a new
   * lease.
   */
  leases(topics: readonly string[]): ReadableStream<PartitionLease>;
  /** Leaves the group. Leases still held are revoked. */
  close(): Promise<void>;
}

/** Writes records. */
export interface BrokerProducer {
  /** Resolves once the broker acknowledged every record. */
  send(records: readonly OutboundRecord[]): Promise<void>;
  close(): Promise<void>;
}

/** The error a revoked lease's records stream fails with. */
export class LeaseRevokedError extends Error {
  constructor(topic: string, partition: number) {
    super(`partition ${topic}/${partition} was revoked`);
    this.name = 'LeaseRevokedError';
  }
}
