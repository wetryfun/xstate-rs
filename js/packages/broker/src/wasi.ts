/**
 * Runs backed by the xstate-rs runtime compiled to a WASI component
 * (`crates/xstate-wasi`, world `xstate:runtime/runtime`), transpiled for
 * JavaScript with jco (`jco transpile xstate_wasi.wasm --instantiation
 * async`). The machine runs in Rust; this module drives it: inputs are
 * sent with the host's clock, timers fire at the runtime's next deadline,
 * emitted events are the run's outputs, and the persisted system snapshot
 * (XState's `SystemSnapshot` JSON) goes to the `SnapshotStore`.
 *
 * Async work an invoked actor requests (`pending-effects`) is not handled:
 * use machines without async invokes here, or drive the actor directly.
 */
import type { Run } from './serve.ts';
import type { SnapshotStore } from './store.ts';

/** An actor of the component (jco's binding of the `actor` resource). */
export interface ComponentActor {
  send(event: string, now: bigint): void;
  advance(now: bigint): void;
  nextDeadline(): bigint | undefined;
  snapshot(): string;
  persist(): string;
  takeEmitted(): string[];
}

/** The component's `actors` export, as jco's `instantiate` returns it. */
export interface ComponentActors {
  readonly Actor: {
    start(definition: string, options: string, now: bigint): ComponentActor;
    restore(definition: string, options: string, snapshot: string): ComponentActor;
  };
}

/** The component's `implementations` import: what a definition references by name. */
export interface Implementations {
  guard?(name: string, params: string | undefined, context: string, event: string): boolean;
  action?(name: string, params: string | undefined): void;
  delay?(name: string, context: string, event: string): bigint;
  expr?(name: string, scope: string): string;
  contextExpr?(name: string, input: string | undefined): string;
}

/** The parts of `@bytecodealliance/preview2-shim` the component imports. */
export interface Preview2Shim {
  readonly cli: Record<string, unknown>;
  readonly clocks: Record<string, unknown>;
  readonly io: Record<string, unknown>;
  readonly random: Record<string, unknown>;
}

const missing = (what: string) => (name: string) => {
  throw new Error(`the definition references ${what} "${name}", which no implementation provides`);
};

/**
 * The imports for jco's `instantiate(getCoreModule, imports)`: WASI from
 * `shim` (pass `await import('@bytecodealliance/preview2-shim')`) and the
 * machine's named implementations.
 */
export function componentImports(shim: Preview2Shim, implementations: Implementations = {}): Record<string, unknown> {
  const { cli, clocks, io, random } = shim;
  return {
    'wasi:cli/environment': cli.environment,
    'wasi:cli/exit': cli.exit,
    'wasi:cli/stderr': cli.stderr,
    'wasi:cli/stdin': cli.stdin,
    'wasi:cli/stdout': cli.stdout,
    'wasi:cli/terminal-input': cli.terminalInput,
    'wasi:cli/terminal-output': cli.terminalOutput,
    'wasi:cli/terminal-stderr': cli.terminalStderr,
    'wasi:cli/terminal-stdin': cli.terminalStdin,
    'wasi:cli/terminal-stdout': cli.terminalStdout,
    'wasi:clocks/monotonic-clock': clocks.monotonicClock,
    'wasi:io/error': io.error,
    'wasi:io/poll': io.poll,
    'wasi:io/streams': io.streams,
    'wasi:random/insecure-seed': random.insecureSeed,
    'xstate:runtime/implementations': {
      guard: implementations.guard ?? missing('guard'),
      action: implementations.action ?? missing('action'),
      delay: implementations.delay ?? missing('delay'),
      expr: implementations.expr ?? missing('expression'),
      contextExpr: implementations.contextExpr ?? missing('context expression'),
    },
  };
}

export interface WasiRunsOptions {
  /** The instantiated component's `actors` export. */
  readonly actors: ComponentActors;
  /** The machine (`MachineJSON`, as an object or JSON text). */
  readonly machine: object | string;
  /** Start options besides `input` (`children`, `maxSteps`). */
  readonly options?: Readonly<Record<string, unknown>>;
  readonly store?: SnapshotStore;
  /** The root's input for a new run. */
  input?(runId: string): unknown;
  /** The clock in milliseconds (default `Date.now`). */
  now?(): number;
}

type Event = { readonly type: string };

/** The component's errors carry their message as `payload`. */
const failure = (error: unknown) =>
  new Error(String((error as { payload?: unknown })?.payload ?? (error instanceof Error ? error.message : error)));

/** Creates runs (for `serve`'s `createRun`) on the xstate-rs runtime. */
export function wasiRuns(options: WasiRunsOptions): (runId: string) => Promise<Run<Event, Event>> {
  const definition = typeof options.machine === 'string' ? options.machine : JSON.stringify(options.machine);
  const now = options.now ?? Date.now;
  const store = options.store;
  return async (runId) => {
    const stored = await store?.load(runId);
    let actor: ComponentActor;
    try {
      actor =
        stored === undefined
          ? options.actors.Actor.start(
              definition,
              JSON.stringify({ ...options.options, input: options.input?.(runId) }),
              BigInt(now()),
            )
          : options.actors.Actor.restore(definition, JSON.stringify(options.options ?? {}), JSON.stringify(stored));
    } catch (error) {
      throw failure(error);
    }
    let out!: ReadableStreamDefaultController<Event>;
    const outputs = new ReadableStream<Event>({
      start(controller) {
        out = controller;
      },
    });
    let timer: ReturnType<typeof setTimeout> | undefined;
    let ended = false;
    let saved: Promise<void> = Promise.resolve();
    let saveError: { error: unknown } | undefined;

    /** Emits what the last call produced, saves, and schedules the next timer. */
    const settle = () => {
      for (const e of actor.takeEmitted()) out.enqueue(JSON.parse(e) as Event);
      if (store) {
        const snapshot = JSON.parse(actor.persist()) as unknown;
        saved = saved
          .then(() => store.save(runId, snapshot))
          .catch((error: unknown) => {
            saveError ??= { error };
          });
      }
      clearTimeout(timer);
      timer = undefined;
      const due = actor.nextDeadline();
      if (due !== undefined && !ended) {
        timer = setTimeout(fire, Math.max(0, Number(due) - now()));
      }
    };
    const fire = () => {
      timer = undefined;
      if (ended) return;
      try {
        actor.advance(BigInt(now()));
      } catch (error) {
        out.error(failure(error));
        return;
      }
      settle();
    };
    settle();

    const end = () => {
      ended = true;
      clearTimeout(timer);
      out.close();
    };
    const inputs = new WritableStream<Event>({
      async write(event) {
        try {
          actor.send(JSON.stringify(event), BigInt(now()));
        } catch (error) {
          throw failure(error);
        }
        const snapshot = JSON.parse(actor.snapshot()) as { status: string; error?: unknown };
        settle();
        if (snapshot.status === 'error') throw new Error(`the actor failed: ${JSON.stringify(snapshot.error)}`);
        await saved;
        if (saveError) throw saveError.error;
      },
      close: end,
      abort: end,
    });
    return { in: inputs, out: outputs };
  };
}
