/**
 * Runs backed by XState actors: each run is an actor of a machine
 * (`MachineJSON`, revived with `machineFromJSON`, or a machine), fed the
 * run's events; its emitted events are the run's outputs. With a
 * `SnapshotStore`, a run restores its actor's persisted snapshot and saves it
 * after every change, before an input counts as processed.
 */
import { createActor, machineFromJSON, type AnyStateMachine } from 'xstate';
import type { Run } from './serve.ts';
import type { SnapshotStore } from './store.ts';

export { MemorySnapshotStore, type SnapshotStore } from './store.ts';

export interface XStateRunsOptions {
  /** A machine, or `MachineJSON` with the sources `machineFromJSON` needs. */
  readonly machine: AnyStateMachine | object;
  readonly sources?: Parameters<typeof machineFromJSON>[1];
  readonly store?: SnapshotStore;
  /** The actor's input for a new run. */
  input?(runId: string): unknown;
}

type Event = { readonly type: string };

const isMachine = (m: object): m is AnyStateMachine => typeof (m as { transition?: unknown }).transition === 'function';

/**
 * Creates runs (for `serve`'s `createRun`) as XState actors. An input is
 * sent to the actor; the write fails when the actor is in error. Emitted
 * events go to `out`.
 */
export function xstateRuns(options: XStateRunsOptions): (runId: string) => Promise<Run<Event, Event>> {
  const machine = isMachine(options.machine)
    ? options.machine
    : machineFromJSON(options.machine as Parameters<typeof machineFromJSON>[0], options.sources);
  const store = options.store;
  return async (runId) => {
    const snapshot = (await store?.load(runId)) ?? undefined;
    const actor = createActor(machine, {
      ...(snapshot === undefined ? { input: options.input?.(runId) } : { snapshot: snapshot as never }),
    });
    let out!: ReadableStreamDefaultController<Event>;
    const outputs = new ReadableStream<Event>({
      start(controller) {
        out = controller;
      },
    });
    actor.on('*', (event: Event) => out.enqueue(event));
    // Saves run one after another; an input waits for the saves its
    // processing caused.
    let saved: Promise<void> = Promise.resolve();
    let saveError: { error: unknown } | undefined;
    const subscription =
      store &&
      actor.subscribe({
        next: () => {
          const persisted = actor.getPersistedSnapshot();
          saved = saved
            .then(() => store.save(runId, persisted))
            .catch((error: unknown) => {
              saveError ??= { error };
            });
        },
        error: () => {},
      });
    actor.start();
    let ended = false;
    const end = () => {
      if (ended) return;
      ended = true;
      // The stored snapshot stays the running one, for the next holder.
      subscription?.unsubscribe();
      actor.stop();
      out.close();
    };
    const inputs = new WritableStream<Event>({
      async write(event) {
        actor.send(event);
        const s = actor.getSnapshot() as { status: string; error?: unknown };
        if (s.status === 'error') throw s.error instanceof Error ? s.error : new Error(String(s.error));
        await saved;
        if (saveError) throw saveError.error;
      },
      close: end,
      abort: end,
    });
    return { in: inputs, out: outputs };
  };
}
