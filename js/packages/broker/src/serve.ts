import { producerSink, type SinkOptions } from './sink.ts';
import type { BrokerConsumer, BrokerProducer, BrokerRecord, OutboundRecord, PartitionLease } from './types.ts';

/**
 * A run: one machine instance (actor), fed the events of its run id in
 * partition order and producing results.
 */
export interface Run<In = unknown, Out = unknown> {
  /**
   * Inputs. A write resolves once the input is processed, its outputs are
   * enqueued on `out` and its state is as durable as the run makes it: the
   * input's offset may be committed after that. A rejected write sends the
   * record to the dead-letter topic and ends the run (the next input for its
   * id starts a new one).
   */
  readonly in: WritableStream<In>;
  /** Outputs, produced to the output topic under the run id. */
  readonly out: ReadableStream<Out>;
}

export interface Topics {
  readonly input: string;
  readonly output: string;
  /** Records that cannot be processed, with a `reason` header. */
  readonly deadLetter?: string;
}

export interface ServeOptions<In, Out> {
  readonly consumer: BrokerConsumer;
  readonly producer: BrokerProducer;
  readonly topics: Topics;
  /**
   * Instantiates (or restores) the run for `runId`. Runs live while their
   * partition's lease does; restoring state from a durability backend is the
   * run's concern. A rejection stops serving (a store that is down should
   * not turn every record into a dead letter).
   */
  createRun(runId: string, lease: PartitionLease): Run<In, Out> | Promise<Run<In, Out>>;
  /** The run a record is for (default: its key; `null` is a dead letter). */
  runId?(record: BrokerRecord): string | null;
  /** The input of a record (default: its value as JSON). Throwing makes it a dead letter. */
  decode?(record: BrokerRecord): In;
  /** The output record of a run's output (default: JSON). */
  encode?(output: Out, runId: string): Pick<OutboundRecord, 'value' | 'headers'>;
  /** How often processed offsets are committed (default 100 ms). */
  readonly commitIntervalMs?: number;
  /** Output batching. */
  readonly sink?: SinkOptions;
  /** Reports errors that do not stop serving (dead letters without a topic, failed commits). */
  onError?(error: unknown): void;
}

/** A running broker loop. */
export interface Serving {
  /** Settles when serving stops: rejects with the error that stopped it. */
  readonly done: Promise<void>;
  /**
   * Stops: no new records are read, inputs in flight finish, their offsets
   * are committed, runs are closed, and the consumer and producer close.
   */
  close(): Promise<void>;
}

const decoder = new TextDecoder();

function defaultDecode(record: BrokerRecord): unknown {
  if (record.value === null) throw new Error('no value');
  return JSON.parse(decoder.decode(record.value));
}

function defaultEncode(output: unknown): Pick<OutboundRecord, 'value' | 'headers'> {
  return { value: JSON.stringify(output), headers: { 'content-type': 'application/json' } };
}

/** Lets every pending microtask run (stream reads resolve in microtasks). */
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

const message = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * Serves runs from the input topic: each assigned partition is read in
 * order, each record goes to the run of its key (created on its first
 * record), and the runs' outputs go to the output topic under their run id.
 *
 * - Ordering: a partition's records reach each run in offset order.
 * - Backpressure: a record waits until its run takes inputs again
 *   (`in`'s writer is ready), which stops reading the partition, so its
 *   lease pauses fetching; outputs wait for the producer the same way.
 * - At-least-once: an offset is committed once every record up to it was
 *   processed and the outputs enqueued by then were acknowledged by the
 *   broker. Records after the last commit are delivered again to the next
 *   holder of the partition, so runs should tolerate redelivery.
 */
export function serve<In = unknown, Out = unknown>(options: ServeOptions<In, Out>): Serving {
  const sink = producerSink(options.producer, options.sink);
  const writer = sink.writable.getWriter();
  const decode = (options.decode ?? defaultDecode) as (record: BrokerRecord) => In;
  const encode = (options.encode ?? defaultEncode) as (output: Out, runId: string) => Pick<OutboundRecord, 'value' | 'headers'>;
  const runIdOf = options.runId ?? ((record: BrokerRecord) => record.key);
  const commitIntervalMs = options.commitIntervalMs ?? 100;
  const report = options.onError ?? ((error: unknown) => console.error('@xmachines/broker:', error));
  const partitions = new Set<PartitionTask>();
  let stopping = false;
  let leaseReader: ReadableStreamDefaultReader<PartitionLease> | undefined;

  const deadLetter = async (record: BrokerRecord, reason: string) => {
    const topic = options.topics.deadLetter;
    if (!topic) {
      report(new Error(`dropped ${record.topic}/${record.partition}@${record.offset}: ${reason}`));
      return;
    }
    await writer.write({
      topic,
      key: record.key,
      value: record.value,
      headers: { ...record.headers, reason },
    });
  };

  interface RunState {
    readonly id: string;
    readonly writer: WritableStreamDefaultWriter<In>;
    /** Reading `out` or producing what it read. */
    busy: boolean;
    readonly pumped: Promise<void>;
  }

  /** One leased partition being served. */
  class PartitionTask {
    readonly lease: PartitionLease;
    readonly runs = new Map<string, RunState>();
    readonly reader: ReadableStreamDefaultReader<BrokerRecord>;
    /** Offsets read, in order, with whether each is processed. */
    readonly offsets: { offset: string; done: boolean }[] = [];
    /** Inputs written to runs and not yet processed. */
    readonly writes = new Set<Promise<void>>();
    committing: Promise<void> = Promise.resolve();
    timer: ReturnType<typeof setTimeout> | undefined;
    readonly done: Promise<void>;

    constructor(lease: PartitionLease) {
      this.lease = lease;
      this.reader = lease.records.getReader();
      this.done = this.serve();
    }

    async serve() {
      try {
        await this.read();
        // Stopping, or the records ended: finish what is in flight and commit it.
        await Promise.allSettled(this.writes);
        clearTimeout(this.timer);
        await this.commit().catch(report);
      } catch (error) {
        if (!this.lease.signal.aborted) throw error;
        // Revoked: the next holder takes over from the last commit.
        clearTimeout(this.timer);
        await Promise.allSettled(this.writes);
      } finally {
        await this.closeRuns();
      }
    }

    async read() {
      for (;;) {
        const next = await this.reader.read();
        if (next.done) return;
        await this.handle(next.value);
        this.scheduleCommit();
      }
    }

    /** Hands a record to its run; resolves once the run takes it (backpressure). */
    async handle(record: BrokerRecord) {
      const entry = { offset: record.offset, done: false };
      this.offsets.push(entry);
      const runId = runIdOf(record);
      if (runId === null) {
        await deadLetter(record, 'no run id (the record has no key)');
        entry.done = true;
        return;
      }
      let input: In;
      try {
        input = decode(record);
      } catch (error) {
        await deadLetter(record, `undecodable: ${message(error)}`);
        entry.done = true;
        return;
      }
      let run: RunState;
      for (;;) {
        run = this.runs.get(runId) ?? (await this.open(runId));
        try {
          // A run that is behind stops the partition's reading here.
          await run.writer.ready;
          break;
        } catch {
          // The run failed on an earlier input: this one goes to a new run.
          if (this.runs.get(runId) === run) this.runs.delete(runId);
        }
      }
      const write = run.writer.write(input).then(
        () => {
          entry.done = true;
        },
        async (error: unknown) => {
          if (this.runs.get(runId) === run) {
            this.runs.delete(runId);
            run.writer.abort(error).catch(() => {});
          }
          await deadLetter(record, `run failed: ${message(error)}`).catch(report);
          entry.done = true;
        },
      );
      this.writes.add(write);
      void write.finally(() => this.writes.delete(write));
    }

    async open(runId: string): Promise<RunState> {
      const run = await options.createRun(runId, this.lease);
      const state: RunState = {
        id: runId,
        writer: run.in.getWriter(),
        busy: false,
        pumped: Promise.resolve(),
      };
      (state as { pumped: Promise<void> }).pumped = this.pump(state, run.out);
      this.runs.set(runId, state);
      return state;
    }

    /** Produces a run's outputs. */
    async pump(state: RunState, out: ReadableStream<Out>) {
      const reader = out.getReader();
      try {
        for (;;) {
          state.busy = false;
          const next = await reader.read();
          state.busy = true;
          if (next.done) return;
          const { value, headers } = encode(next.value, state.id);
          await writer.write({ topic: options.topics.output, key: state.id, value, headers });
        }
      } catch (error) {
        report(error);
      } finally {
        state.busy = false;
        reader.releaseLock();
      }
    }

    scheduleCommit() {
      this.timer ??= setTimeout(() => {
        this.timer = undefined;
        this.commit().catch(report);
      }, commitIntervalMs);
    }

    /** Commits the processed prefix, once its outputs are acknowledged. */
    commit(): Promise<void> {
      const commit = this.committing.then(async () => {
        let last: string | undefined;
        while (this.offsets[0]?.done) last = this.offsets.shift()!.offset;
        if (last === undefined) return;
        // Every output enqueued by a processed input is read by its pump
        // and batched once the pumps are idle (reads resolve in microtasks).
        do await tick();
        while ([...this.runs.values()].some((r) => r.busy));
        await sink.flush();
        if (this.lease.signal.aborted) return;
        await this.lease.commit(last);
      });
      this.committing = commit.catch((error) => {
        if (!this.lease.signal.aborted) report(error);
      });
      return commit;
    }

    async closeRuns() {
      const runs = [...this.runs.values()];
      this.runs.clear();
      await Promise.allSettled(runs.map((r) => r.writer.close()));
      await Promise.allSettled(runs.map((r) => r.pumped));
    }

    stop() {
      this.reader.cancel().catch(() => {});
    }
  }

  const done = (async () => {
    const reader = options.consumer.leases([options.topics.input]).getReader();
    leaseReader = reader;
    const failures: unknown[] = [];
    for (;;) {
      const next = await reader.read().catch((error: unknown) => {
        if (!stopping) failures.push(error);
        return { done: true as const, value: undefined };
      });
      if (next.done) break;
      const task = new PartitionTask(next.value);
      partitions.add(task);
      task.done
        .catch((error: unknown) => {
          failures.push(error);
          void stop();
        })
        .finally(() => partitions.delete(task));
    }
    await Promise.allSettled([...partitions].map((p) => p.done));
    if (failures.length > 0) throw failures[0];
  })();

  let stopped: Promise<void> | undefined;
  const stop = () => {
    stopped ??= (async () => {
      stopping = true;
      leaseReader?.cancel().catch(() => {});
      for (const p of partitions) p.stop();
      await Promise.allSettled([...partitions].map((p) => p.done));
      await writer.close().catch(report);
      await options.consumer.close().catch(report);
      await options.producer.close().catch(report);
    })();
    return stopped;
  };

  return {
    done: done.then(
      () => stop(),
      async (error: unknown) => {
        await stop();
        throw error;
      },
    ),
    close: async () => {
      await stop();
      await done.catch(() => {});
    },
  };
}
