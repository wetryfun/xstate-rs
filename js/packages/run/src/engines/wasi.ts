/**
 * The xstate-rs runtime as a WASI component (`crates/xstate-wasi`, world
 * `xstate:runtime/runtime`), transpiled for JavaScript with jco
 * (`jco transpile xstate_wasi.wasm --instantiation async`). The component is
 * injected: instantiate it with `componentImports` and pass its `actors`
 * export.
 */
import type { Engine } from '../engine.ts';
import { runtimeMachine, type RuntimeActor } from './component.ts';
import { jsEvaluate } from './evaluate.ts';

export { jsEvaluate } from './evaluate.ts';

/** The component's `actors` export, as jco's `instantiate` returns it. */
export interface ComponentActors {
  readonly Actor: {
    start(definition: string, options: string, now: bigint): RuntimeActor<bigint>;
    restore(definition: string, options: string, snapshot: string): RuntimeActor<bigint>;
  };
}

/** The component's `implementations` import: what a definition references by name (values as JSON text). */
export interface Implementations {
  guard?(name: string, params: string | undefined, context: string, event: string): boolean;
  action?(name: string, params: string | undefined): void;
  delay?(name: string, context: string, event: string): bigint;
  expr?(name: string, scope: string): string;
  contextExpr?(name: string, input: string | undefined): string;
  /** Code nodes in languages other than `rust` (default: `js` and `ts`, see `jsEvaluate`). */
  evaluate?(code: string, scope: string): string;
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
      evaluate: implementations.evaluate ?? jsEvaluate(),
    },
  };
}

export interface WasiEngineOptions {
  /** The instantiated component's `actors` export. */
  readonly actors: ComponentActors;
  /** Start options besides `id` and `input` (`children`, `maxSteps`). */
  readonly options?: Readonly<Record<string, unknown>>;
}

/** The xstate-rs runtime, through its WASI component, as an engine. */
export function wasiEngine(options: WasiEngineOptions): Engine {
  const { Actor } = options.actors;
  const binding = {
    start: (d: string, o: string, now: bigint) => Actor.start(d, o, now),
    restore: (d: string, o: string, s: string) => Actor.restore(d, o, s),
    time: (ms: number) => BigInt(Math.floor(ms)),
  };
  return { name: 'wasi', load: (definition) => runtimeMachine(binding, definition, options.options) };
}
