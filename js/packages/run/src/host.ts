/**
 * Hosting a run live: the host owns the clock and durability. Every input
 * chunk is stamped with the time (a leading time event, in the same atomic
 * batch), appended to the run's journal, then applied; when a timer is due,
 * the host sends a time event the same way, and when registered work (an
 * invoked async actor's) completes, its effect result. The journal
 * therefore holds every input the run consumed, and replaying it reproduces
 * the run.
 */
import type { Engine, EngineMachine, Event } from './engine.ts';
import { forkObjects, machineObjects, type ForkOptions } from './ids.ts';
import { chunks } from '@xmachines/streams';
import { Latest } from './ports.ts';
import { createRun, rejected, resolved, time, type Input, type Run, type RunSnapshot } from './run.ts';

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
  /** Implementations of external work, by invoke `src` (see `Work`). */
  readonly work?: Readonly<Record<string, Work>>;
  log?(args: readonly unknown[]): void;
}

/** What a `Work` implementation gets besides the actor's input. */
export interface WorkContext {
  /** The effect's id: work with side effects should be idempotent by it (work restarts after a restore). */
  readonly id: string;
  /** The address of the actor whose work it is. */
  readonly actor: string;
  /** Aborted when the work is no longer wanted (its invoking state was left) or the run closes. */
  readonly signal: AbortSignal;
}

/**
 * An invoked async actor's work, run by the host: its result (or what it
 * throws) completes the actor as `xstate.async.resolve` (or `reject`), sent
 * to the run as an effect result, journaled like any input. It runs again
 * when a run is restored with it still pending (at least once).
 */
export type Work = (input: unknown, context: WorkContext) => unknown;

/** What a rejection carries: an error's name and message, or the thrown value. */
const rejection = (error: unknown) => (error instanceof Error ? { name: error.name, message: error.message } : error);

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

  /**
   * Stamps, journals and applies a batch, after everything submitted before
   * it; `events` may be computed then (nothing is applied when it gives none).
   */
  const submit = (events: readonly Event[] | (() => Promise<readonly Event[] | undefined>)) => {
    const done = chain.then(async () => {
      const content = typeof events === 'function' ? await events() : events;
      if (!content) return;
      last = Math.max(last, clock());
      const batch = [time(last), ...content];
      await journal?.append(batch);
      await writer.write(batch);
    });
    chain = done.catch(() => {});
    return done;
  };

  /** Work in progress (or whose result is submitted), by effect id, until its effect is gone. */
  const running = new Map<string, { readonly abort: AbortController }>();

  /** Starts registered work that is pending, and aborts work that no longer is. */
  const reconcile = (s: RunSnapshot) => {
    const pending = new Set((s.work ?? []).map((w) => w.id));
    for (const [id, job] of running) {
      if (pending.has(id)) continue;
      job.abort.abort(new Error('the work is no longer pending'));
      running.delete(id);
    }
    if (closed) return;
    for (const w of s.work ?? []) {
      const work = options.work?.[w.src];
      if (!work || running.has(w.id)) continue;
      const job = { abort: new AbortController() };
      running.set(w.id, job);
      const finish = (result: Event) =>
        submit(async () => {
          // Let the snapshots of what was applied meanwhile be followed: the
          // work may have been cancelled.
          await new Promise((r) => setTimeout(r, 0));
          if (closed || job.abort.signal.aborted || running.get(w.id) !== job) return undefined;
          return [result];
        }).catch(() => {});
      Promise.resolve()
        .then(() => work(w.input, { id: w.id, actor: w.actor, signal: job.abort.signal }))
        .then(
          (data) => finish(resolved(w.id, data)),
          (error: unknown) => finish(rejected(w.id, rejection(error))),
        );
    }
  };

  // Follow the snapshots: schedule the next timer, run work, journal
  // snapshots, and pass them on (latest only).
  let latest: RunSnapshot | undefined;
  let journaled = restored?.snapshot?.seq ?? -1;
  const snapshots = new Latest<RunSnapshot>();
  const followed = (async () => {
    for await (const s of chunks(run.snapshot)) {
      latest = s;
      reconcile(s);
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
      for (const job of running.values()) job.abort.abort(new Error('the run closed'));
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
  /** Implementations of external work, by invoke `src`. */
  readonly work?: Readonly<Record<string, Work>>;
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
      ...(options.work && { work: options.work }),
    });
  };
}
