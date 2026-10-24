/**
 * The xstate-rs runtime's actor, as both the WASI component (through jco)
 * and the native addon expose it: one adapter for both engines.
 */
import { errorMessage, type EngineInstance, type EngineMachine, type Event, type RootState, type Step } from '../engine.ts';

/** The runtime's actor, with times as `T` (`bigint` from WASI, `number` natively). */
export interface RuntimeActor<T> {
  send(event: string, now: T): void;
  advance(now: T): void;
  resolveEffect(id: string, event?: string): void;
  nextDeadline(): T | undefined | null;
  snapshot(): string;
  persist(): string;
  takeEmitted(): string[];
  takeLogs(): string[];
}

export interface RuntimeBinding<T> {
  start(definition: string, options: string, now: T): RuntimeActor<T>;
  restore(definition: string, options: string, snapshot: string): RuntimeActor<T>;
  time(ms: number): T;
}

/** Calls the runtime, turning its errors (a `payload` from jco, an `Error` natively) into `Error`s. */
function call<R>(f: () => R): R {
  try {
    return f();
  } catch (error) {
    throw new Error(errorMessage(error));
  }
}

class RuntimeInstance<T> implements EngineInstance {
  readonly #actor: RuntimeActor<T>;
  readonly #time: (ms: number) => T;

  constructor(actor: RuntimeActor<T>, time: (ms: number) => T) {
    this.#actor = actor;
    this.#time = time;
  }

  send(event: Event, time: number): void {
    call(() => this.#actor.send(JSON.stringify(event), this.#time(time)));
  }

  advance(time: number): void {
    call(() => this.#actor.advance(this.#time(time)));
  }

  resolve(effectId: string, event: Event | undefined, time: number): void {
    // The runtime's resolve takes no time: move the clock first.
    this.advance(time);
    call(() => (event ? this.#actor.resolveEffect(effectId, JSON.stringify(event)) : this.#actor.resolveEffect(effectId)));
  }

  take(): Step {
    return {
      emitted: this.#actor.takeEmitted().map((e) => JSON.parse(e) as Event),
      logs: this.#actor.takeLogs().map((l) => JSON.parse(l) as unknown[]),
    };
  }

  deadline(): number | undefined {
    const due = this.#actor.nextDeadline();
    return due === undefined || due === null ? undefined : Number(due);
  }

  root(): RootState {
    return JSON.parse(this.#actor.snapshot()) as RootState;
  }

  state(): unknown {
    return JSON.parse(this.#actor.persist());
  }

  dispose(): void {
    (this.#actor as { [Symbol.dispose]?: () => void })[Symbol.dispose]?.();
  }
}

/** A machine on the runtime: `definition` loads at start and restore (the runtime keeps no machine handle). */
export function runtimeMachine<T>(binding: RuntimeBinding<T>, definition: object, options: object = {}): EngineMachine {
  const text = JSON.stringify(definition);
  const restoreOptions = JSON.stringify(options);
  return {
    start: ({ id, input, time }) =>
      new RuntimeInstance(
        call(() => binding.start(text, JSON.stringify({ ...options, id, ...(input !== undefined && { input }) }), binding.time(time))),
        binding.time,
      ),
    restore: (state) => new RuntimeInstance(call(() => binding.restore(text, restoreOptions, JSON.stringify(state))), binding.time),
  };
}
