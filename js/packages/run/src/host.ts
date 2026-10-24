/**
 * Hosting a run live: the host owns the clock and durability. Every input
 * chunk is stamped with the time (a leading time event, in the same atomic
 * batch), appended to the run's journal, then applied; when a timer is due,
 * the host sends a time event the same way. The journal therefore holds
 * every input the run consumed, and replaying it reproduces the run.
 */
import type { Engine, EngineMachine, Event } from './engine.ts';
import { forkObjects, machineObjects, type ForkOptions } from './ids.ts';
import { chunks } from '@xmachines/streams';
import { Latest } from './ports.ts';
import { createRun, time, type Input, type Run, type RunSnapshot } from './run.ts';

/** A run's durability backend: its input batches and snapshots, append-only. */
export interface Journal {
  /** Appends an input batch; resolves once it is durable (with the entry's id, when the journal has ids). */
  append(batch: readonly Event[]): Promise<unknown>;
  /** Records a snapshot, taken after `snapshot.seq` batches. */
  snapshot(snapshot: RunSnapshot): Promise<void>;
  /** The latest snapshot (if any) and every batch after it. */
  restore(): Promise<{ readonly snapshot?: RunSnapshot; readonly batches: ReadableStream<readonly Event[]> }>;
}

/** Journals of many runs. */
export interface Journals {
  /** The journal of the run `runId` of `definition` (forked with `fork`). */
  open(runId: string, definition: object, fork: ForkOptions): Journal | Promise<Journal>;
}

/** A journal in memory. */
export class MemoryJournal implements Journal {
  readonly batches: (readonly Event[])[] = [];
  readonly snapshots: RunSnapshot[] = [];

  async append(batch: readonly Event[]): Promise<void> {
    this.batches.push(structuredClone(batch));
  }

  async snapshot(snapshot: RunSnapshot): Promise<void> {
    this.snapshots.push(structuredClone(snapshot));
  }

  async restore() {
    const snapshot = this.snapshots.at(-1);
    const tail = this.batches.slice(snapshot?.seq ?? 0).map((b) => structuredClone(b));
    return {
      ...(snapshot && { snapshot: structuredClone(snapshot) }),
      batches: new ReadableStream<readonly Event[]>({
        pull(controller) {
          const next = tail.shift();
          if (next) controller.enqueue(next);
          else controller.close();
        },
      }),
    };
  }
}

/** Memory journals by run id. */
export class MemoryJournals implements Journals {
  readonly journals = new Map<string, MemoryJournal>();
  open(runId: string): MemoryJournal {
    let journal = this.journals.get(runId);
    if (!journal) this.journals.set(runId, (journal = new MemoryJournal()));
    return journal;
  }
}

export interface HostOptions {
  readonly id: string;
  readonly machine: EngineMachine;
  readonly input?: unknown;
  readonly journal?: Journal;
  /** Milliseconds (default `Date.now`). */
  readonly clock?: () => number;
  /** Batches between journal snapshots (default 1: after every batch). */
  readonly snapshotEvery?: number;
  /** Outputs and errors buffered before writes wait (default 64). */
  readonly highWaterMark?: number;
  log?(args: readonly unknown[]): void;
}

/** A live run. Its `in` takes events (or batches) without time events: the host stamps them. */
export interface HostedRun extends Run {
  /** Stops timers, finishes what is in flight, journals a final snapshot and ends the run. */
  close(): Promise<void>;
}

/** Restores a run from its journal (if any) and hosts it. */
export async function host(options: HostOptions): Promise<HostedRun> {
  const clock = options.clock ?? Date.now;
  const journal = options.journal;
  const every = Math.max(1, options.snapshotEvery ?? 1);
  const restored = journal ? await journal.restore() : undefined;
  const run = createRun({
    id: options.id,
    machine: options.machine,
    input: options.input,
    highWaterMark: options.highWaterMark,
    ...(restored?.snapshot && { from: restored.snapshot }),
    ...(restored && { replay: restored.batches }),
    ...(options.log && { log: options.log }),
  });
  const writer = run.in.getWriter();
  let last = restored?.snapshot?.time ?? 0;
  let chain: Promise<unknown> = writer.ready;
  let closed = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  /** Stamps, journals and applies a batch, after everything submitted before it. */
  const submit = (events: readonly Event[]) => {
    const done = chain.then(async () => {
      last = Math.max(last, clock());
      const batch = [time(last), ...events];
      await journal?.append(batch);
      await writer.write(batch);
    });
    chain = done.catch(() => {});
    return done;
  };

  // Follow the snapshots: schedule the next timer, journal snapshots, and
  // pass them on (latest only).
  let latest: RunSnapshot | undefined;
  let journaled = restored?.snapshot?.seq ?? -1;
  const snapshots = new Latest<RunSnapshot>();
  const followed = (async () => {
    for await (const s of chunks(run.snapshot)) {
      latest = s;
      clearTimeout(timer);
      timer = undefined;
      if (s.deadline !== undefined && !closed) {
        timer = setTimeout(() => {
          timer = undefined;
          if (!closed) submit([]).catch(() => {});
        }, Math.max(0, s.deadline - clock()));
      }
      if (journal && s.status !== 'pending' && s.seq - journaled >= every) {
        journaled = s.seq;
        await journal.snapshot(s);
      }
      snapshots.publish(s);
    }
    if (journal && latest && latest.status !== 'pending' && latest.seq > journaled) await journal.snapshot(latest);
    snapshots.close();
  })();

  let ended: Promise<void> | undefined;
  const close = () => {
    ended ??= (async () => {
      closed = true;
      clearTimeout(timer);
      await chain;
      await writer.close().catch(() => {});
      await followed;
    })();
    return ended;
  };

  const inputs = new WritableStream<Input>({
    write: (chunk) => submit((Array.isArray(chunk) ? chunk : [chunk]) as readonly Event[]),
    close,
    abort: close,
  });

  return { id: run.id, in: inputs, out: run.out, err: run.err, snapshot: snapshots.readable, close };
}

export interface RunnerOptions {
  readonly engine: Engine;
  /** The machine definition (`MachineJSON`). */
  readonly machine: object;
  /** Where runs keep their journals; without it runs are not durable. */
  readonly journals?: Journals;
  /** The machine's input for a run. */
  input?(name: string): unknown;
  readonly clock?: () => number;
  readonly snapshotEvery?: number;
  readonly highWaterMark?: number;
}

/**
 * Opens runs by name: the run of `name` is the fork of the machine with that
 * name, so its id (a Git commit id) is the same everywhere, with or without
 * Git, and needs no lookup table. Opening restores it from its journal.
 */
export function runner(options: RunnerOptions): (name: string) => Promise<HostedRun> {
  const loaded = Promise.resolve(options.engine.load(options.machine));
  const objects = machineObjects(options.machine);
  return async (name) => {
    const input = options.input?.(name);
    const fork: ForkOptions = { name, ...(input !== undefined && { input }) };
    const id = (await forkObjects(await objects, fork)).id;
    const journal = await options.journals?.open(id, options.machine, fork);
    return host({
      id,
      machine: await loaded,
      input,
      ...(journal && { journal }),
      ...(options.clock && { clock: options.clock }),
      ...(options.snapshotEvery !== undefined && { snapshotEvery: options.snapshotEvery }),
      ...(options.highWaterMark !== undefined && { highWaterMark: options.highWaterMark }),
    });
  };
}
