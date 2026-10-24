import type { Event, Input, RunError, RunSnapshot } from '@xmachines/run';
import { producerSink, type SinkOptions } from './sink.ts';
import type { BrokerConsumer, BrokerProducer, BrokerRecord, OutboundRecord, PartitionLease } from './types.ts';

/**
 * A run as the broker drives it: the Run RFC's ports (`@xmachines/run`'s
 * `Run` and `HostedRun` are ones). A write to `in` resolves once the input is
 * applied (or rejected onto `err`) and its outputs are enqueued on `out`:
 * the record's offset may be committed after that.
 */
export interface BrokerRun {
  readonly in: WritableStream<Input>;
  /** Produced to the output topic, keyed by the record key. */
  readonly out: ReadableStream<Event>;
  /** Produced to the dead-letter topic, keyed by the record key. */
  readonly err: ReadableStream<RunError>;
  /** Produced to the snapshot topic (latest per key, for a compacted topic) when there is one. */
  readonly snapshot?: ReadableStream<RunSnapshot>;
  /** Ends the run (default: closing `in`). */
  close?(): Promise<void>;
}

export interface Topics {
  readonly input: string;
  readonly output: string;
  /** Records that cannot be processed and errors runs report, with a `reason` header. */
  readonly deadLetter?: string;
  /** Run snapshots, keyed by record key: a compacted topic of every run's latest state. */
  readonly snapshots?: string;
  /**
   * External work runs wait for (invoked async actors their host does not run
   * itself), each published once, keyed by record key, as `PendingWork` plus
   * `run`. Whoever does it sends the result to the input topic under the same
   * key: `{ "type": "xstate.system.effect.result", "effectId", "event" }`.
   */
  readonly work?: string;
}

export interface ServeOptions {
  readonly consumer: BrokerConsumer;
  readonly producer: BrokerProducer;
  readonly topics: Topics;
  /**
   * Opens the run of a record key: the partition key maps to a run (with
   * `@xmachines/run`'s `runner`, the fork of the machine named by the key,
   * restored from its journal). Runs live while their partition's lease
   * does. A rejection stops serving (a store that is down should not turn
   * every record into a dead letter).
   */
  open(key: string, lease: PartitionLease): BrokerRun | Promise<BrokerRun>;
  /** The input of a record (default: its value as JSON). Throwing makes it a dead letter. */
  decode?(record: BrokerRecord): Input;
  /** The output record of a run's output (default: JSON). */
  encode?(output: Event, key: string): Pick<OutboundRecord, 'value' | 'headers'>;
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

function defaultDecode(record: BrokerRecord): Input {
  if (record.value === null) throw new Error('no value');
  return JSON.parse(decoder.decode(record.value)) as Input;
}

function defaultEncode(output: unknown): Pick<OutboundRecord, 'value' | 'headers'> {
  return { value: JSON.stringify(output), headers: { 'content-type': 'application/json' } };
}

/** Lets every pending microtask run (stream reads resolve in microtasks). */
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

const message = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * Serves runs from the input topic: each assigned partition is read in
 * order, each record goes to the run of its key (opened on its first
 * record), and the runs' outputs go to the output topic and their errors to
 * the dead-letter topic, under the key.
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
export function serve(options: ServeOptions): Serving {
  const sink = producerSink(options.producer, options.sink);
  const writer = sink.writable.getWriter();
  const decode = options.decode ?? defaultDecode;
  const encode = options.encode ?? defaultEncode;
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
    readonly key: string;
    readonly run: BrokerRun;
    readonly writer: WritableStreamDefaultWriter<Input>;
    /** Pumps reading a port or producing what they read. */
    busy: number;
    readonly pumped: Promise<unknown>;
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
      const key = record.key;
      if (key === null) {
        await deadLetter(record, 'no key (the key names the run)');
        entry.done = true;
        return;
      }
      let input: Input;
      try {
        input = decode(record);
      } catch (error) {
        await deadLetter(record, `undecodable: ${message(error)}`);
        entry.done = true;
        return;
      }
      let run: RunState;
      for (;;) {
        run = this.runs.get(key) ?? (await this.open(key));
        try {
          // A run that is behind stops the partition's reading here.
          await run.writer.ready;
          break;
        } catch {
          // The run broke on an earlier input: this one goes to a new run.
          if (this.runs.get(key) === run) this.runs.delete(key);
        }
      }
      const write = run.writer.write(input).then(
        () => {
          entry.done = true;
        },
        async (error: unknown) => {
          // The run could not take it (its journal failed, ...): it is
          // broken; the next record reopens it from its durable state.
          if (this.runs.get(key) === run) {
            this.runs.delete(key);
            run.writer.abort(error).catch(() => {});
          }
          await deadLetter(record, `run failed: ${message(error)}`).catch(report);
          entry.done = true;
        },
      );
      this.writes.add(write);
      void write.finally(() => this.writes.delete(write));
    }

    async open(key: string): Promise<RunState> {
      const run = await options.open(key, this.lease);
      const state: RunState = { key, run, writer: run.in.getWriter(), busy: 0, pumped: Promise.resolve() };
      const { snapshots, work } = options.topics;
      // Work published and still pending.
      const published = new Set<string>();
      const fromSnapshot = (s: RunSnapshot): OutboundRecord[] => {
        const records: OutboundRecord[] = [];
        const headers = { 'content-type': 'application/json' };
        if (snapshots) records.push({ topic: snapshots, key, value: JSON.stringify(s), headers });
        if (work) {
          const pending = new Set<string>();
          for (const w of s.work ?? []) {
            pending.add(w.id);
            if (published.has(w.id)) continue;
            published.add(w.id);
            records.push({ topic: work, key, value: JSON.stringify({ ...w, run: s.run }), headers });
          }
          for (const id of published) if (!pending.has(id)) published.delete(id);
        }
        return records;
      };
      (state as { pumped: Promise<unknown> }).pumped = Promise.all([
        this.pump(state, run.out, (output) => ({ topic: options.topics.output, key, ...encode(output, key) })),
        this.pump(state, run.err, (error) => ({
          topic: options.topics.deadLetter!,
          key,
          value: JSON.stringify(error),
          headers: { 'content-type': 'application/json', reason: error.message },
        }), !options.topics.deadLetter),
        run.snapshot && (snapshots || work ? this.pump(state, run.snapshot, fromSnapshot) : run.snapshot.cancel()),
      ]);
      this.runs.set(key, state);
      return state;
    }

    /** Produces what a port gives (or reports it, when it has no topic). */
    async pump<T>(
      state: RunState,
      port: ReadableStream<T>,
      record: (chunk: T) => OutboundRecord | readonly OutboundRecord[],
      report_ = false,
    ) {
      const reader = port.getReader();
      try {
        for (;;) {
          const next = await reader.read();
          if (next.done) return;
          state.busy++;
          try {
            if (report_) report(new Error(`run ${state.key}: ${(next.value as RunError).message}`));
            else {
              const records = record(next.value);
              for (const r of Array.isArray(records) ? records : [records]) await writer.write(r);
            }
          } finally {
            state.busy--;
          }
        }
      } catch (error) {
        report(error);
      } finally {
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
        while ([...this.runs.values()].some((r) => r.busy > 0));
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
      await Promise.allSettled(
        runs.map((r) => {
          r.writer.releaseLock();
          return r.run.close ? r.run.close() : r.run.in.close();
        }),
      );
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
