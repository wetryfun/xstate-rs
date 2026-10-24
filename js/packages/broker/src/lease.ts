import { LeaseRevokedError, type BrokerRecord, type PartitionLease } from './types.ts';

/** How a transport fetches, pauses and commits one partition. */
export interface LeaseTransport {
  /** Stop fetching the partition (its reader is behind). */
  pause(): void;
  /** Fetch it again. */
  resume(): void;
  /** Commits up to and including `offset`. */
  commit(offset: string): Promise<void>;
}

/** A lease as a transport drives it. */
export interface LeaseHandle {
  readonly lease: PartitionLease;
  /**
   * Adds a fetched record. Returns false once the reader is behind: the
   * transport was asked to pause the partition, and resumes it when the
   * reader catches up. Records pushed after revocation are dropped.
   */
  push(record: BrokerRecord): boolean;
  /** The partition was revoked: buffered records are discarded. */
  revoke(): void;
  /** No more records will come (the consumer is closing); buffered ones stay readable. */
  end(): void;
  readonly paused: boolean;
}

export interface LeaseOptions {
  /** Records buffered before the partition is paused (default 1000). */
  readonly highWaterMark?: number;
}

/**
 * Builds a lease for a transport: a records stream that pauses the
 * partition while its reader is behind (more than `highWaterMark` records
 * buffered) and resumes it when the reader pulls again.
 */
export function createLease(
  topic: string,
  partition: number,
  transport: LeaseTransport,
  options: LeaseOptions = {},
): LeaseHandle {
  const abort = new AbortController();
  let controller!: ReadableStreamDefaultController<BrokerRecord>;
  let paused = false;
  let open = true;
  const records = new ReadableStream<BrokerRecord>(
    {
      start(c) {
        controller = c;
      },
      pull() {
        if (paused && open) {
          paused = false;
          transport.resume();
        }
      },
      cancel() {
        open = false;
      },
    },
    new CountQueuingStrategy({ highWaterMark: options.highWaterMark ?? 1000 }),
  );
  const lease: PartitionLease = {
    topic,
    partition,
    records,
    signal: abort.signal,
    commit: (offset) => transport.commit(offset),
  };
  return {
    lease,
    get paused() {
      return paused;
    },
    push(record) {
      if (!open) return false;
      controller.enqueue(record);
      if (!paused && (controller.desiredSize ?? 0) <= 0) {
        paused = true;
        transport.pause();
      }
      return !paused;
    },
    revoke() {
      if (open) {
        open = false;
        controller.error(new LeaseRevokedError(topic, partition));
      }
      abort.abort(new LeaseRevokedError(topic, partition));
    },
    end() {
      if (open) {
        open = false;
        controller.close();
      }
    },
  };
}
