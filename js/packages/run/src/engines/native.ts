/**
 * The xstate-rs runtime as a native Node.js addon (`crates/xstate-node`,
 * napi-rs). The addon is injected: build it (`crates/xstate-node/build.sh`)
 * and pass the loaded module (`createRequire(import.meta.url)(path)`).
 */
import type { Engine } from '../engine.ts';
import { runtimeMachine, type RuntimeActor } from './component.ts';
import { jsEvaluate } from './evaluate.ts';

export { jsEvaluate } from './evaluate.ts';

/** What the machine's named references call (values as JSON text), like the WASI component's imports. */
export interface NativeImplementations {
  guard?(name: string, params: string | undefined, context: string, event: string): boolean;
  action?(name: string, params: string | undefined): void;
  delay?(name: string, context: string, event: string): number;
  expr?(name: string, scope: string): string;
  contextExpr?(name: string, input: string | undefined): string;
  /** Code nodes in languages other than `rust` (default: `js` and `ts`, see `jsEvaluate`). */
  evaluate?(code: string, scope: string): string;
}

/** The addon's exports. */
export interface NativeAddon {
  readonly Actor: {
    start(definition: string, options: string, now: number, implementations?: NativeImplementations): RuntimeActor<number>;
    restore(definition: string, options: string, snapshot: string, implementations?: NativeImplementations): RuntimeActor<number>;
  };
}

export interface NativeEngineOptions {
  /** The loaded addon. */
  readonly addon: NativeAddon;
  readonly implementations?: NativeImplementations;
  /** Start options besides `id` and `input` (`children`, `maxSteps`). */
  readonly options?: Readonly<Record<string, unknown>>;
}

/** The xstate-rs runtime, natively, as an engine. */
export function nativeEngine(options: NativeEngineOptions): Engine {
  const { Actor } = options.addon;
  const impl: NativeImplementations = { evaluate: jsEvaluate(), ...options.implementations };
  const binding = {
    start: (d: string, o: string, now: number) => Actor.start(d, o, now, impl),
    restore: (d: string, o: string, s: string) => Actor.restore(d, o, s, impl),
    time: (ms: number) => Math.floor(ms),
  };
  return { name: 'native', load: (definition) => runtimeMachine(binding, definition, options.options) };
}
