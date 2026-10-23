// Loads the xstate-rs core (crates/xstate-wasm) and exposes it to XState's
// stateUtils. Installed into packages/core/src by tools/xstate-suite.
//
// The wasm ABI is integers only. Rust calls back into the current host
// through the `xs` imports; hosts form a stack because calls re-enter (a
// guard may run another actor). A hook that throws stashes its exception and
// returns -1; Rust unwinds and the export returns -1, and we rethrow here.
import { readFileSync } from 'node:fs';

export interface CoreResolved {
  targets: number[] | undefined;
  reenter: boolean;
}

export type RegionDescriptor = [kind: 0 | 1 | 2, region: number, child: number | undefined];

export interface CoreHost {
  config(): number[];
  history(h: number): number[] | undefined;
  staticInitial(n: number): number[];
  beginSelection(): void;
  evalCandidate(t: number, node: number, eventless: boolean): boolean;
  resolve(resolver: 0 | 1 | 2, tref: number): CoreResolved;
  beginMicrostep(): void;
  exitState(n: number): void;
  transitionContent(trefs: number[]): void;
  beginEntry(active: number[], entered: Array<[number, number[]]>): void;
  enterState(n: number, defaultEntry: boolean, historyDefaults: number[]): void;
  doneCompound(parent: number, finalChild: number): void;
  doneParallel(parallel: number, regions: RegionDescriptor[]): void;
  machineDone(completion: number): void;
  endEntry(): void;
  isDone(): boolean;
  exitOnDone(nodes: number[]): void;
  applyMicrostep(
    result:
      | {
          filtered: number[];
          nodes: number[];
          structureChanged: boolean;
          history: Array<[number, number[]]>;
        }
      | undefined,
    enabled: number[]
  ): void;
  isActive(): boolean;
  checkIteration(n: number): void;
  snapshotToken(): number;
  microstepToken(): number;
  drainRaised(): void;
  queueIsEmpty(): boolean;
  dequeue(): string | undefined;
}

export const TREF_INITIAL_MICROSTEP = 0xffffffff;
export const TREF_INITIAL = 0x80000000;
export const TREF_HISTORY_DEFAULT = 0x40000000;
export const TREF_MASK = 0x3fffffff;

const wasmPath = process.env.XSTATE_RS_WASM;

export function rsEnabled(): boolean {
  return !!wasmPath;
}

const hosts: CoreHost[] = [];
let pending: { error: unknown } | undefined;
let exports: any;
let memory: WebAssembly.Memory;
let u32Base = 0;
let byteBase = 0;
let bufLen = 0;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function u32(): Uint32Array {
  return new Uint32Array(memory.buffer);
}
function bytes(): Uint8Array {
  return new Uint8Array(memory.buffer);
}
function readU32s(ptr: number, len: number): number[] {
  return Array.from(u32().subarray(ptr >>> 2, (ptr >>> 2) + len));
}
function writeOut(values: readonly number[]): number {
  if (values.length > bufLen) {
    throw new Error('xstate-rs exchange buffer overflow');
  }
  const view = u32();
  const base = u32Base >>> 2;
  for (let i = 0; i < values.length; i++) {
    view[base + i] = values[i] >>> 0;
  }
  return values.length;
}
function writeString(s: string): number {
  const encoded = encoder.encode(s);
  if (encoded.length > bufLen) {
    throw new Error('xstate-rs exchange buffer overflow');
  }
  bytes().set(encoded, byteBase);
  return encoded.length;
}

function host(): CoreHost {
  return hosts[hosts.length - 1];
}

function hook<A extends unknown[]>(
  fn: (h: CoreHost, ...args: A) => number
): (...args: A) => number {
  return (...args: A) => {
    try {
      return fn(host(), ...args);
    } catch (error) {
      pending = { error };
      return -1;
    }
  };
}

const ok = () => 0;

const imports = {
  xs: {
    config: hook((h) => writeOut(h.config())),
    history: (hId: number) => {
      const recorded = host().history(hId);
      return recorded === undefined ? -1 : writeOut(recorded);
    },
    static_initial: (n: number) => writeOut(host().staticInitial(n)),
    begin_selection: hook((h) => (h.beginSelection(), ok())),
    eval_candidate: hook((h, t: number, node: number, eventless: number) =>
      h.evalCandidate(t, node, eventless === 1) ? 1 : 0
    ),
    resolve: hook((h, r: number, tref: number) => {
      const res = h.resolve(r as 0 | 1 | 2, tref >>> 0);
      const targets = res.targets ?? [];
      writeOut([res.targets ? 1 : 0, res.reenter ? 1 : 0, targets.length, ...targets]);
      return 0;
    }),
    begin_microstep: hook((h) => (h.beginMicrostep(), ok())),
    exit_state: hook((h, n: number) => (h.exitState(n), ok())),
    transition_content: hook((h, ptr: number, len: number) => {
      h.transitionContent(readU32s(ptr, len));
      return 0;
    }),
    begin_entry: hook(
      (h, aPtr: number, aLen: number, ePtr: number, eLen: number) => {
        const active = readU32s(aPtr, aLen);
        const flat = readU32s(ePtr, eLen);
        const entered: Array<[number, number[]]> = [];
        for (let i = 0; i < flat.length; ) {
          const tref = flat[i];
          const n = flat[i + 1];
          entered.push([tref, flat.slice(i + 2, i + 2 + n)]);
          i += 2 + n;
        }
        h.beginEntry(active, entered);
        return 0;
      }
    ),
    enter_state: hook(
      (h, n: number, def: number, ptr: number, len: number) => {
        h.enterState(n, def === 1, readU32s(ptr, len));
        return 0;
      }
    ),
    done_compound: hook((h, p: number, f: number) => (h.doneCompound(p, f), ok())),
    done_parallel: hook((h, p: number, ptr: number, len: number) => {
      const flat = readU32s(ptr, len);
      const regions: RegionDescriptor[] = [];
      for (let i = 0; i < flat.length; i += 3) {
        regions.push([
          flat[i] as 0 | 1 | 2,
          flat[i + 1],
          flat[i + 2] === 0 ? undefined : flat[i + 2] - 1
        ]);
      }
      h.doneParallel(p, regions);
      return 0;
    }),
    machine_done: hook((h, c: number) => (h.machineDone(c), ok())),
    end_entry: hook((h) => (h.endEntry(), ok())),
    is_done: () => (host().isDone() ? 1 : 0),
    exit_on_done: hook((h, ptr: number, len: number) => {
      h.exitOnDone(readU32s(ptr, len));
      return 0;
    }),
    apply_microstep: hook(
      (
        h,
        has: number,
        fPtr: number,
        fLen: number,
        nPtr: number,
        nLen: number,
        changed: number,
        hPtr: number,
        hLen: number,
        enPtr: number,
        enLen: number
      ) => {
        const enabled = readU32s(enPtr, enLen);
        if (has === 0) {
          h.applyMicrostep(undefined, enabled);
          return 0;
        }
        const flat = readU32s(hPtr, hLen);
        const history: Array<[number, number[]]> = [];
        for (let i = 0; i < flat.length; ) {
          const n = flat[i + 1];
          history.push([flat[i], flat.slice(i + 2, i + 2 + n)]);
          i += 2 + n;
        }
        h.applyMicrostep(
          {
            filtered: readU32s(fPtr, fLen),
            nodes: readU32s(nPtr, nLen),
            structureChanged: changed === 1,
            history
          },
          enabled
        );
        return 0;
      }
    ),
    is_active: () => (host().isActive() ? 1 : 0),
    check_iteration: hook((h, n: number) => (h.checkIteration(n), ok())),
    snapshot_token: () => host().snapshotToken(),
    microstep_token: () => host().microstepToken(),
    drain_raised: hook((h) => (h.drainRaised(), ok())),
    queue_is_empty: () => (host().queueIsEmpty() ? 1 : 0),
    dequeue: hook((h) => {
      const type = h.dequeue();
      return type === undefined ? -2 : writeString(type);
    })
  }
};

function load() {
  if (exports) {
    return;
  }
  const module = new WebAssembly.Module(readFileSync(wasmPath!));
  const instance = new WebAssembly.Instance(module, imports as any);
  exports = instance.exports;
  memory = exports.memory;
  u32Base = exports.xs_u32buf();
  byteBase = exports.xs_bytebuf();
  bufLen = exports.xs_buf_len();
}

function call<T>(h: CoreHost, fn: () => number, read: (n: number) => T): T {
  hosts.push(h);
  let n: number;
  try {
    n = fn();
  } finally {
    hosts.pop();
  }
  if (n === -1) {
    const error = pending;
    pending = undefined;
    if (error) {
      throw error.error;
    }
    throw new Error('xstate-rs core failed without a JavaScript error');
  }
  return read(n);
}

function alloc(data: Uint8Array): number {
  const ptr = exports.xs_alloc(data.length);
  bytes().set(data, ptr);
  return ptr;
}

/** Registers a machine structure (see `decode_tree` in xstate-wasm). */
export function registerTree(words: number[], strings: string[]): number {
  load();
  const encodedStrings = strings.map((s) => encoder.encode(s));
  const total = encodedStrings.reduce((n, s) => n + s.length, 0);
  const blob = new Uint8Array(total);
  const table = [strings.length];
  let offset = 0;
  for (const s of encodedStrings) {
    blob.set(s, offset);
    table.push(offset, s.length);
    offset += s.length;
  }
  const w = alloc(new Uint8Array(new Uint32Array(words).buffer));
  const t = alloc(new Uint8Array(new Uint32Array(table).buffer));
  const b = alloc(blob.length ? blob : new Uint8Array(1));
  const handle = exports.xs_tree_new(w, words.length, t, table.length, b, blob.length);
  exports.xs_free(w, words.length * 4);
  exports.xs_free(t, table.length * 4);
  exports.xs_free(b, Math.max(blob.length, 1));
  if (!handle) {
    throw new Error('xstate-rs rejected a machine structure');
  }
  return handle;
}

const readOut = (n: number) => readU32s(u32Base, n);

export function select(handle: number, h: CoreHost, eventType: string): number[] {
  return call(
    h,
    () => exports.xs_select(handle, writeString(eventType)),
    readOut
  );
}

export function selectEventless(handle: number, h: CoreHost): number[] {
  return call(h, () => exports.xs_select_eventless(handle), readOut);
}

export function microstep(handle: number, h: CoreHost, trefs: number[]): void {
  const data = new Uint32Array(trefs.map((t) => t >>> 0));
  const ptr = data.length ? alloc(new Uint8Array(data.buffer)) : 0;
  try {
    call(h, () => exports.xs_microstep(handle, ptr, data.length), () => undefined);
  } finally {
    if (ptr) {
      exports.xs_free(ptr, data.length * 4);
    }
  }
}

export function initialMicrostep(handle: number, h: CoreHost): void {
  call(h, () => exports.xs_initial_microstep(handle), () => undefined);
}

export function initialTargets(handle: number, h: CoreHost): number[] {
  return call(h, () => exports.xs_initial_targets(handle), readOut);
}

export function runToCompletion(handle: number, h: CoreHost): void {
  call(h, () => exports.xs_run_to_completion(handle), () => undefined);
}
