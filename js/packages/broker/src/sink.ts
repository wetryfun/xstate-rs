import type { BrokerProducer, OutboundRecord } from './types.ts';

export interface SinkOptions {
  /** How long a batch waits for more records (default 5 ms). */
  readonly lingerMs?: number;
  /** Records per send, at most (default 500). */
  readonly maxBatch?: number;
  /** Sends in flight before writes wait (default 4). */
  readonly maxInFlight?: number;
}

/** A producer as a writable stream, batching records into sends. */
export interface ProducerSink {
  readonly writable: WritableStream<OutboundRecord>;
  /**
   * Sends what is batched and waits for every send so far; rejects with the
   * first failed send.
   */
  flush(): Promise<void>;
}

/**
 * Batches the records written to it into `producer.send` calls. A write
 * resolves once its record is batched; while `maxInFlight` sends are
 * unacknowledged, writes wait (backpressure from the broker).
 */
export function producerSink(producer: BrokerProducer, options: SinkOptions = {}): ProducerSink {
  const lingerMs = options.lingerMs ?? 5;
  const maxBatch = options.maxBatch ?? 500;
  const maxInFlight = options.maxInFlight ?? 4;
  let batch: OutboundRecord[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  const inFlight = new Set<Promise<void>>();
  let failure: { error: unknown } | undefined;

  const send = () => {
    clearTimeout(timer);
    timer = undefined;
    if (batch.length === 0) return;
    const records = batch;
    batch = [];
    const sent: Promise<void> = producer
      .send(records)
      .catch((error: unknown) => {
        failure ??= { error };
      })
      .finally(() => inFlight.delete(sent));
    inFlight.add(sent);
  };

  const writable = new WritableStream<OutboundRecord>({
    async write(record) {
      if (failure) throw failure.error;
      batch.push(record);
      if (batch.length >= maxBatch) send();
      else timer ??= setTimeout(send, lingerMs);
      while (inFlight.size >= maxInFlight) await Promise.race(inFlight);
    },
    async close() {
      send();
      await Promise.all(inFlight);
      if (failure) throw failure.error;
    },
  });

  return {
    writable,
    async flush() {
      send();
      await Promise.all(inFlight);
      if (failure) throw failure.error;
    },
  };
}
