/**
 * A run: one execution lineage of a machine, driven only by ordered input
 * batches. Its ports are Web Streams: `in` takes events, `out` gives what the
 * machine emits, `err` the errors, `snapshot` the materialized state.
 *
 * A run never reads a clock: time is an input. `{ type: "xmachines.time",
 * now }` advances the run's clock (firing due timers), and every other event
 * applies at the current clock. Given the same machine and the same inputs,
 * a run always ends in the same state with the same outputs.
 */
import { errorMessage, type EngineInstance, type EngineMachine, type Event, type RootState, type Step } from './engine.ts';
import { chunks } from '@xmachines/streams';
import { Latest, Port } from './ports.ts';

export const TIME = 'xmachines.time';

/** Advances a run's clock to `now` (milliseconds). Time never moves back: an earlier `now` is ignored. */
export interface TimeEvent extends Event {
  readonly type: typeof TIME;
  readonly now: number;
}

export const time = (now: number): TimeEvent => ({ type: TIME, now });

/** An input chunk: one event, or a batch applied atomically (all or nothing). */
export type Input = Event | readonly Event[];

export type RunStatus = 'pending' | RootState['status'];

/** The materialized state of a run, after `seq` input chunks. */
export interface RunSnapshot {
  readonly run: string;
  /** Input chunks consumed (applied or rejected). */
  readonly seq: number;
  /** The run's clock. */
  readonly time: number;
  /** `pending` until the first input starts the machine. */
  readonly status: RunStatus;
  readonly value?: unknown;
  readonly context?: unknown;
  readonly output?: unknown;
  readonly error?: unknown;
  /** When the next timer is due: write a time event then. */
  readonly deadline?: number;
  /** The engine's persisted state (XState's `SystemSnapshot` JSON), for restoring on any engine. */
  readonly state?: unknown;
}

/** A rejected input chunk. The run's state is as before it. */
export interface RunError {
  readonly run: string;
  /** The chunk's sequence number (1 for the first). */
  readonly seq: number;
  readonly batch: readonly unknown[];
  readonly message: string;
}

export interface Run {
  readonly id: string;
  /**
   * Events in. A write resolves once the chunk is applied (or rejected) and
   * its outputs are enqueued on `out` (or its error on `err`). Concurrent
   * senders serialize here; closing it ends the run (not the machine).
   */
  readonly in: WritableStream<Input>;
  readonly out: ReadableStream<Event>;
  readonly err: ReadableStream<RunError>;
  /** The latest snapshot (a reader that falls behind skips to the latest). */
  readonly snapshot: ReadableStream<RunSnapshot>;
}

export interface RunOptions {
  /** The run's id (`runId(...)` for a Git-compatible one); also the root actor's id. */
  readonly id: string;
  readonly machine: EngineMachine;
  readonly input?: unknown;
  /** Continue from a snapshot. */
  readonly from?: RunSnapshot;
  /**
   * Inputs the run already consumed after `from` (a journal's tail): applied
   * before `in` takes writes, without emitting outputs or errors again.
   */
  readonly replay?: ReadableStream<Input> | Iterable<Input>;
  /** Start the machine now, at this time; otherwise the first input starts it, at its time. */
  readonly startAt?: number;
  /** Outputs and errors buffered before `in` waits for readers (default 64). Cancel a port to drop it. */
  readonly highWaterMark?: number;
  /** Receives the arguments of the machine's `@xstate.log` actions. */
  log?(args: readonly unknown[]): void;
}

const isEvent = (e: unknown): e is Event =>
  typeof e === 'object' && e !== null && !Array.isArray(e) && typeof (e as { type?: unknown }).type === 'string';

/** Creates a run of a loaded machine. */
export function createRun(options: RunOptions): Run {
  const { id, machine } = options;
  const hwm = options.highWaterMark ?? 64;
  const out = new Port<Event>(hwm);
  const err = new Port<RunError>(hwm);
  const snapshots = new Latest<RunSnapshot>();

  let instance: EngineInstance | undefined;
  /** The instance's state after the last applied chunk (for rollback and snapshots). */
  let state: unknown;
  let clock = options.from?.time ?? 0;
  let seq = options.from?.seq ?? 0;

  if (options.from?.state !== undefined) {
    instance = machine.restore(options.from.state);
    state = options.from.state;
  } else if (options.startAt !== undefined) {
    clock = options.startAt;
    instance = machine.start({ id, input: options.input, time: clock });
    state = instance.state();
  }

  const snapshot = (): RunSnapshot => {
    if (!instance) return { run: id, seq, time: clock, status: 'pending' };
    const root = instance.root();
    const deadline = instance.deadline();
    return {
      run: id,
      seq,
      time: clock,
      status: root.status,
      value: root.value,
      context: root.context,
      ...(root.output !== undefined && { output: root.output }),
      ...(root.error !== undefined && { error: root.error }),
      ...(deadline !== undefined && { deadline }),
      state,
    };
  };

  /** Applies a batch, or throws with the run as before it. */
  const apply = (events: readonly Event[]): Step => {
    let current = instance;
    let now = clock;
    let i = 0;
    try {
      if (!current) {
        const first = events[0];
        if (first?.type === TIME && Number.isFinite(first.now)) {
          now = Math.max(now, Number(first.now));
          i = 1;
        }
        current = machine.start({ id, input: options.input, time: now });
      }
      for (; i < events.length; i++) {
        const event = events[i]!;
        if (event.type === TIME) {
          const at = Number(event.now);
          if (!(at > now)) continue;
          current.advance(at);
          now = at;
        } else {
          current.send(event, now);
        }
      }
      const step = current.take();
      const root = current.root();
      if (root.status === 'error') throw new Error(`the machine failed: ${JSON.stringify(root.error ?? null)}`);
      instance = current;
      clock = now;
      state = current.state();
      return step;
    } catch (error) {
      if (current && current !== instance) current.dispose?.();
      else if (instance) {
        instance.dispose?.();
        instance = machine.restore(state);
      }
      throw error;
    }
  };

  /** Consumes one chunk; `quiet` while replaying. */
  const consume = (chunk: Input, quiet: boolean): void => {
    seq++;
    const batch = (Array.isArray(chunk) ? chunk : [chunk]) as readonly unknown[];
    try {
      if (!batch.every(isEvent)) throw new Error('an input must be an event (an object with a string "type") or an array of events');
      const step = apply(batch as readonly Event[]);
      if (!quiet) {
        for (const e of step.emitted) out.push(e);
        for (const args of step.logs) options.log?.(args);
      }
    } catch (error) {
      if (!quiet) err.push({ run: id, seq, batch, message: errorMessage(error) });
    }
    snapshots.publish(snapshot());
  };

  snapshots.publish(snapshot());

  const replay = async () => {
    const source = options.replay;
    if (!source) return;
    if (source instanceof ReadableStream) {
      for await (const chunk of chunks(source)) consume(chunk, true);
    } else {
      for (const chunk of source) consume(chunk, true);
    }
  };

  const end = () => {
    out.close();
    err.close();
    snapshots.close();
    instance?.dispose?.();
  };

  const inputs = new WritableStream<Input>({
    start: replay,
    async write(chunk) {
      consume(chunk, false);
      await Promise.all([out.room(), err.room()]);
    },
    close: end,
    abort: end,
  });

  return { id, in: inputs, out: out.readable, err: err.readable, snapshot: snapshots.readable };
}
