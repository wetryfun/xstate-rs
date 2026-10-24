/**
 * JavaScript code nodes for the xstate-rs runtime's engines (the native addon
 * and the WASI component), as XState's own evaluators run them: a `@code`
 * node is a function called as `fn(scope, scope.enq)`, an `@expr` node an
 * expression over the scope's names (`with (scope) { ... }`). The XState
 * engine gets the same through `sources.evaluators` (`jsEvaluators`).
 *
 * The runtime hands each node over as JSON text and replays what `enq`
 * recorded (see the WIT `evaluate` import), so `enq.spawn` and the root
 * context's `spawn`, which need ids from the runtime while the code runs, are
 * not available there.
 */

/** XState's evaluator arguments (`sources.evaluators[lang]`). */
export interface EvaluatorArgs {
  readonly source: string;
  readonly kind: 'code' | 'expr';
  readonly scope: Record<string, unknown>;
}

type Compiled = (scope: Record<string, unknown>) => unknown;

/** Compiles a code node as XState's tests do. */
function compile(kind: string, source: string): Compiled {
  if (kind === 'code') {
    const fn = Function(`return (${source});`)() as (scope: unknown, enq: unknown) => unknown;
    return (scope) => fn(scope, scope.enq);
  }
  return Function('scope', `with (scope) { return (${source}); }`) as Compiled;
}

/** XState's evaluator for JavaScript (and TypeScript without types). */
export function jsEvaluator(): (args: EvaluatorArgs) => unknown {
  const cache = new Map<string, Compiled>();
  return ({ source, kind, scope }) => {
    const key = `${kind}:${source}`;
    let fn = cache.get(key);
    if (!fn) cache.set(key, (fn = compile(kind, source)));
    return fn(scope);
  };
}

/** `sources.evaluators` for the XState engine: `js` and `ts`. */
export function jsEvaluators(): Record<'js' | 'ts', (args: EvaluatorArgs) => unknown> {
  const js = jsEvaluator();
  return { js, ts: js };
}

// An actor reference as XState persists it, with its id as a non-enumerable
// property, as live references have.
type Ref = { $actor: string; incarnation: string; id?: string };
const isRef = (v: unknown): v is Ref =>
  typeof v === 'object' && v !== null && typeof (v as Ref).$actor === 'string' && 'incarnation' in v && Object.keys(v).length === 2;
function revive<T>(v: T): T {
  if (Array.isArray(v)) v.forEach(revive);
  else if (isRef(v)) {
    const segment = v.$actor.slice(v.$actor.lastIndexOf('/') + 1);
    Object.defineProperty(v, 'id', { value: decodeURIComponent(segment), enumerable: false });
  } else if (typeof v === 'object' && v !== null) for (const value of Object.values(v)) revive(value);
  return v;
}

// What the runtime needs of a target: its reference and id.
const target = (t: unknown) => (typeof t === 'object' && t !== null ? { ...(t as Ref), id: (t as Ref).id } : t);

// `actions.name` and `actors.name` are the names themselves.
const names = new Proxy({}, { get: (_, key) => (typeof key === 'string' ? key : undefined) });

/** An `enq` that records its calls as `[op, ...args]`. */
function recorder(calls: unknown[][]) {
  const enq = (action: unknown, params?: unknown) => {
    if (typeof action !== 'string') throw new TypeError('enq(action, params): name a registered action as actions.<name>');
    calls.push(['action', action, params]);
  };
  return Object.assign(enq, {
    raise: (event: unknown, options?: unknown) => void calls.push(['raise', event, options]),
    emit: (event: unknown) => void calls.push(['emit', event]),
    cancel: (id: unknown) => void calls.push(['cancel', id]),
    log: (...args: unknown[]) => void calls.push(['log', args]),
    sendTo: (to: unknown, event: unknown, options?: unknown) => void calls.push(['sendTo', target(to), event, options]),
    stop: (actor: unknown) => void calls.push(['stop', target(actor)]),
    spawn: () => {
      throw new Error('enq.spawn is not available to code nodes run by the host (use the XState engine)');
    },
    listen: () => {
      throw new Error('enq.listen is not supported');
    },
    subscribeTo: () => {
      throw new Error('enq.subscribeTo is not supported');
    },
  });
}

/**
 * The runtime's `evaluate` (WIT import, addon callback) for `js` and `ts`
 * code nodes: `(code, scope)` as JSON text in, `{ value?, enq?, error? }` out.
 */
export function jsEvaluate(): (code: string, scope: string) => string {
  const evaluate = jsEvaluator();
  return (codeText, scopeText) => {
    const code = JSON.parse(codeText) as { lang: string; source: string; kind: 'code' | 'expr'; slot: string };
    if (code.lang !== 'js' && code.lang !== 'ts') {
      return JSON.stringify({ error: `no evaluator for code in language '${code.lang}'` });
    }
    const raw = JSON.parse(scopeText) as Record<string, unknown>;
    const calls: unknown[][] = [];
    const scope: Record<string, unknown> = { actions: names, actors: names };
    if (code.slot === 'context') {
      scope.input = raw.input;
      scope.self = revive(raw.self);
      scope.spawn = () => {
        throw new Error('spawn is not available to code nodes run by the host (use the XState engine)');
      };
    } else {
      const self = revive(raw.self) as object;
      if (raw.parent) Object.defineProperty(self, '_parent', { value: revive(raw.parent), enumerable: false });
      Object.assign(scope, {
        context: revive(raw.context),
        event: raw.event,
        input: raw.input,
        params: raw.params,
        self,
        children: revive(raw.children ?? {}),
      });
      if (raw.enq) scope.enq = recorder(calls);
    }
    try {
      const value = evaluate({ source: code.source, kind: code.kind, scope });
      const out: { value?: unknown; enq?: unknown[][] } = {};
      if (value !== undefined) out.value = typeof value === 'function' || typeof value === 'symbol' ? true : value;
      if (calls.length) out.enq = calls;
      return JSON.stringify(out);
    } catch (error) {
      return JSON.stringify({ error: error instanceof Error ? error.message : String(error) });
    }
  };
}
