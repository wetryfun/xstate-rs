/**
 * Engines: what executes a machine. A run drives an engine; the engine is
 * pluggable: XState itself (`@xmachines/run/xstate`), the xstate-rs runtime
 * as a native addon (`/native`) or as a WASI component (`/wasi`). All of them
 * implement XState's pure system semantics, never read a clock, and persist
 * XState's `SystemSnapshot` JSON, so a run's state moves between engines.
 */

/** An event: an object with a string `type`. */
export interface Event {
  readonly type: string;
  readonly [key: string]: unknown;
}

/** The root actor's state. */
export interface RootState {
  readonly status: 'active' | 'done' | 'error' | 'stopped';
  readonly value: unknown;
  readonly context: unknown;
  readonly output?: unknown;
  readonly error?: unknown;
}

/** What steps since the last `take` produced. */
export interface Step {
  /** Events the root emitted. */
  readonly emitted: readonly Event[];
  /** Argument lists of the root's `@xstate.log` actions. */
  readonly logs: readonly (readonly unknown[])[];
}

/** One running system. Methods throw when a step fails. */
export interface EngineInstance {
  /** Sends `event` to the root at `time` (timers due by then fire first). */
  send(event: Event, time: number): void;
  /** Fires every timer due by `time`. */
  advance(time: number): void;
  /**
   * Completes pending external work (an `@xstate.effect`, such as an invoked
   * async actor's) at `time`, with its result event (`xstate.async.resolve`
   * or `xstate.async.reject` with `data`); without one, it only acknowledges
   * it. Throws when the effect is not pending.
   */
  resolve(effectId: string, event: Event | undefined, time: number): void;
  /** What the steps since the last call produced. */
  take(): Step;
  /** When the next timer is due. */
  deadline(): number | undefined;
  root(): RootState;
  /** The persisted state (XState's `SystemSnapshot` JSON): `restore` takes it, on any engine. */
  state(): unknown;
  /** Frees what the instance holds outside JavaScript's heap. */
  dispose?(): void;
}

export interface StartOptions {
  /** The root actor's id. */
  readonly id: string;
  readonly input?: unknown;
  /** The time of the initial transition, in milliseconds. */
  readonly time: number;
}

/** A machine an engine loaded. */
export interface EngineMachine {
  start(options: StartOptions): EngineInstance;
  restore(state: unknown): EngineInstance;
}

export interface Engine {
  readonly name: string;
  /** Loads a machine definition (`MachineJSON`). */
  load(definition: object): EngineMachine | Promise<EngineMachine>;
}

/** The input event that completes external work: XState's effect result. */
export const EFFECT_RESULT = 'xstate.system.effect.result';

/** External work a run waits for: an `@xstate.effect` of an actor in its system. */
export interface PendingWork {
  /** The effect's id (`system:N`): what the result names. */
  readonly id: string;
  /** The address of the actor whose work it is (`<root>/<invoke id>`). */
  readonly actor: string;
  /** The actor's logic: the invoke's `src`. */
  readonly src: string;
  readonly input?: unknown;
}

/**
 * The pending work in a persisted system (XState's `SystemSnapshot` JSON,
 * which every engine writes), in effect order.
 */
export function pendingWork(state: unknown): PendingWork[] {
  const s = state as {
    externalEffects?: Record<string, { id: string; type: string; source: { $actor: string } }>;
    actors?: Record<string, { src?: string; input?: unknown }>;
  } | null;
  const out: PendingWork[] = [];
  for (const effect of Object.values(s?.externalEffects ?? {})) {
    if (effect.type !== '@xstate.effect') continue;
    const actor = s?.actors?.[effect.source.$actor];
    // Logic is named by path (`root/actors/<src>`).
    const path = actor?.src ?? '';
    const src = path.includes('/actors/') ? path.slice(path.lastIndexOf('/actors/') + '/actors/'.length) : path;
    out.push({ id: effect.id, actor: effect.source.$actor, src, ...(actor?.input !== undefined && { input: actor.input }) });
  }
  return out.sort((a, b) => Number(a.id.split(':')[1]) - Number(b.id.split(':')[1]));
}

/** The message of an error from any engine. */
export function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  const payload = (error as { payload?: unknown } | null)?.payload;
  return typeof payload === 'string' ? payload : String(error);
}
