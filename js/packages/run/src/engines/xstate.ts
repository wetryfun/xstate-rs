/**
 * The JavaScript engine: upstream XState's pure system API
 * (`initialSystemTransition`, `systemTransition`, `advanceSystemTime`), the
 * semantics the xstate-rs runtime ports. The system snapshot is a value: a
 * step returns a new one, so a failed batch costs nothing to roll back.
 *
 * External effects are handled the way the Rust runtime's `Actor` does: the
 * root's emits are collected, `@xstate.log` arguments too, custom actions run
 * through `actions`, and all of these (with cancellations and cleanups) are
 * acknowledged at once. Async work (`@xstate.effect`) stays pending until an
 * effect result completes it (`resolve`); invoked sources the definition's
 * `sources` do not provide are async logic whose work is that external work.
 */
import {
  advanceSystemTime,
  createAsyncLogic,
  initialSystemTransition,
  machineFromJSON,
  systemTransition,
  type AnyStateMachine,
} from 'xstate';
import type { Engine, EngineInstance, EngineMachine, Event, RootState, StartOptions, Step } from '../engine.ts';
import { jsEvaluators } from './evaluate.ts';

type Sources = Parameters<typeof machineFromJSON>[1];

export interface XStateEngineOptions {
  /**
   * What `machineFromJSON` needs for names the definition does not define
   * (guards, delays, actors, evaluators). Evaluators for `js` and `ts` are
   * there unless given: XState's own convention (`jsEvaluators`).
   */
  readonly sources?: Sources;
  /** Custom actions, by name: called with the action's params when the effect runs. */
  readonly actions?: Readonly<Record<string, (params: unknown) => void>>;
}

interface Effect {
  readonly id: string;
  readonly source: { readonly $actor: string };
  readonly kind: string;
  readonly type: string;
  readonly params?: unknown;
  readonly args?: readonly unknown[];
  readonly event?: Event;
}

interface SystemSnapshot {
  readonly root: string;
  readonly now: number;
  readonly actors: Record<string, { readonly snapshot: RootState }>;
  readonly timers: readonly { readonly dueAt: number }[];
}

type System = { root: AnyStateMachine };
type Transition = readonly [unknown, readonly unknown[]];

/** Every `invoke.src` a definition names. */
function invokeSources(node: unknown, out = new Set<string>()): Set<string> {
  if (Array.isArray(node)) for (const n of node) invokeSources(n, out);
  else if (node && typeof node === 'object') {
    for (const [key, value] of Object.entries(node)) {
      if (key === 'invoke') {
        for (const invoke of Array.isArray(value) ? value : [value]) {
          const src = (invoke as { src?: unknown } | null)?.src;
          if (typeof src === 'string') out.add(src);
        }
      }
      invokeSources(value, out);
    }
  }
  return out;
}

/**
 * The sources with async logic for every invoked source they lack: its work
 * is external (`@xstate.effect`), done by the host and completed with an
 * effect result, as the xstate-rs runtime does for sources without a child
 * machine.
 */
function withWork(definition: object, sources: Sources): Sources {
  const given = sources as { actors?: Record<string, unknown>; evaluators?: Record<string, unknown> } | undefined;
  const actors: Record<string, unknown> = { ...given?.actors };
  for (const src of invokeSources(definition)) actors[src] ??= createAsyncLogic({ run: async () => undefined });
  return { ...sources, actors, evaluators: { ...jsEvaluators(), ...given?.evaluators } } as Sources;
}

const isMachine = (m: object): m is AnyStateMachine => typeof (m as { transition?: unknown }).transition === 'function';

class XStateInstance implements EngineInstance {
  #system: System;
  #snapshot: SystemSnapshot;
  #actions: XStateEngineOptions['actions'];
  #emitted: Event[] = [];
  #logs: (readonly unknown[])[] = [];

  constructor(system: System, snapshot: unknown, actions: XStateEngineOptions['actions'], effects: readonly unknown[] = []) {
    this.#system = system;
    this.#snapshot = snapshot as SystemSnapshot;
    this.#actions = actions;
    this.#settle(effects);
  }

  /** Takes a transition's result and handles its effects (acknowledging them may produce more). */
  #apply([snapshot, effects]: Transition) {
    this.#snapshot = snapshot as SystemSnapshot;
    this.#settle(effects);
  }

  #settle(effects: readonly unknown[]) {
    for (const e of effects as readonly Effect[]) {
      if (e.kind === 'action' && e.type === '@xstate.effect') continue;
      if (e.kind === 'emit') {
        if (e.source.$actor === this.#snapshot.root && e.event) this.#emitted.push(e.event);
      } else if (e.type === '@xstate.log') {
        if (e.source.$actor === this.#snapshot.root) this.#logs.push(e.args ?? []);
      } else if (e.kind === 'action' && !e.type.startsWith('@xstate.')) {
        // Custom actions are named by path (`root/actions/<name>`).
        const name = e.type.slice(e.type.lastIndexOf('/') + 1);
        const run = this.#actions?.[name];
        if (!run) throw new Error(`the action "${name}" has no implementation`);
        run(e.params);
      }
      // Acknowledge, as the Rust runtime does for these effects.
      if (!(e.id in ((this.#snapshot as unknown as { externalEffects: object }).externalEffects ?? {}))) continue;
      this.#apply(
        systemTransition(this.#system, this.#snapshot as never, e.source.$actor, {
          type: 'xstate.system.effect.result',
          effectId: e.id,
        } as never) as unknown as Transition,
      );
    }
  }

  advance(time: number): void {
    if (time > this.#snapshot.now) this.#apply(advanceSystemTime(this.#system, this.#snapshot as never, { time }) as unknown as Transition);
  }

  send(event: Event, time: number): void {
    this.advance(time);
    this.#apply(systemTransition(this.#system, this.#snapshot as never, this.#snapshot.root, event as never) as unknown as Transition);
  }

  resolve(effectId: string, event: Event | undefined, time: number): void {
    this.advance(time);
    const effects = (this.#snapshot as unknown as { externalEffects?: Record<string, Effect> }).externalEffects ?? {};
    const effect = effects[effectId];
    if (!effect) throw new Error(`Unknown external effect '${effectId}'.`);
    this.#apply(
      systemTransition(this.#system, this.#snapshot as never, effect.source.$actor, {
        type: 'xstate.system.effect.result',
        effectId,
        ...(event && { event }),
      } as never) as unknown as Transition,
    );
  }

  take(): Step {
    const step = { emitted: this.#emitted, logs: this.#logs };
    this.#emitted = [];
    this.#logs = [];
    return step;
  }

  deadline(): number | undefined {
    let due: number | undefined;
    for (const t of this.#snapshot.timers) if (due === undefined || t.dueAt < due) due = t.dueAt;
    return due;
  }

  root(): RootState {
    const s = this.#snapshot.actors[this.#snapshot.root]?.snapshot;
    if (!s) return { status: 'stopped', value: undefined, context: undefined };
    return {
      status: s.status,
      value: s.value,
      context: s.context,
      ...(s.output !== undefined && { output: s.output }),
      ...(s.error !== undefined && { error: s.error instanceof Error ? s.error.message : s.error }),
    };
  }

  state(): unknown {
    // A JSON copy: what any engine restores, and safe to store.
    return JSON.parse(JSON.stringify(this.#snapshot));
  }
}

/** Upstream XState as an engine. */
export function xstateEngine(options: XStateEngineOptions = {}): Engine {
  return {
    name: 'xstate',
    load(definition: object): EngineMachine {
      const machine = isMachine(definition) ? definition : machineFromJSON(definition as never, withWork(definition, options.sources));
      const system: System = { root: machine };
      return {
        start: ({ id, input, time }: StartOptions) => {
          const [snapshot, effects] = initialSystemTransition(system, {
            time,
            id,
            ...(input !== undefined && { input }),
          } as never) as unknown as Transition;
          return new XStateInstance(system, snapshot, options.actions, effects);
        },
        restore: (state: unknown) => new XStateInstance(system, structuredClone(state), options.actions),
      };
    },
  };
}
