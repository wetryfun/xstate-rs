// The JavaScript side of xstate-js's evaluator: compiles code nodes as
// XState's own evaluators do and calls them with XState's scope. Values
// cross to Rust as JSON text; `enq` and `spawn` call Rust through the bridge.
(() => {
  const bridge = globalThis.__xstate_bridge;
  delete globalThis.__xstate_bridge;

  const call = (op, ...args) => {
    const out = bridge(op, JSON.stringify(args));
    return out === undefined ? undefined : JSON.parse(out);
  };

  // An actor reference as XState persists it ({ $actor, incarnation }),
  // with the actor's id as a non-enumerable `id`, as live references have.
  const isRef = (v) =>
    v !== null && typeof v === 'object' && typeof v.$actor === 'string' && 'incarnation' in v && Object.keys(v).length === 2;
  const revive = (v) => {
    if (Array.isArray(v)) v.forEach(revive);
    else if (isRef(v)) {
      const segment = v.$actor.slice(v.$actor.lastIndexOf('/') + 1);
      Object.defineProperty(v, 'id', { value: decodeURIComponent(segment), enumerable: false });
    } else if (v !== null && typeof v === 'object') for (const key of Object.keys(v)) revive(v[key]);
    return v;
  };
  // What the bridge needs of a target: its ref and id.
  const target = (t) =>
    t !== null && typeof t === 'object' ? { $actor: t.$actor, incarnation: t.incarnation, id: t.id } : t;

  const enq = (action, params) => {
    if (typeof action !== 'string') {
      throw new TypeError('enq(action, params): name a registered action as actions.<name>');
    }
    call('action', action, params);
  };
  Object.assign(enq, {
    raise: (event, options) => void call('raise', event, options),
    emit: (event) => void call('emit', event),
    cancel: (id) => void call('cancel', id),
    log: (...args) => void call('log', args),
    sendTo: (to, event, options) => void call('sendTo', target(to), event, options),
    stop: (actor) => void call('stop', target(actor)),
    spawn: (src, options) => ({ id: call('spawn', src, options) }),
    listen: () => {
      throw new Error('enq.listen is not supported by xstate-rs');
    },
    subscribeTo: () => {
      throw new Error('enq.subscribeTo is not supported by xstate-rs');
    }
  });

  // `actions.name` and `actors.name` are the names themselves.
  const names = new Proxy({}, { get: (_, key) => (typeof key === 'string' ? key : undefined) });

  // The result as JSON text (`undefined` for undefined). A function or
  // symbol has no JSON form; as a result only its truthiness can matter.
  const result = (value) => {
    if (value === undefined) return undefined;
    if (typeof value === 'function' || typeof value === 'symbol') return 'true';
    return JSON.stringify(value);
  };

  // XState's scope, its values parsed from JSON text when first read (code
  // usually reads one or two): `context`, `event`, `input`, `params`,
  // `self` (with `_parent`), `children`, `actions`, `actors`, and `enq`
  // where XState has it. The values are getters on the prototype, so a
  // spread (`{ ...scope }`) copies only `enq`.
  const TEXTS = Symbol('texts');
  const VALUES = Symbol('values');
  const UNREAD = Symbol('unread');
  // References only need reviving where the text has one.
  const parse = (text) => (text.includes('"$actor"') ? revive(JSON.parse(text)) : JSON.parse(text));
  const value = (scope, i) => {
    const values = scope[VALUES];
    const v = values[i];
    if (v !== UNREAD) return v;
    const text = scope[TEXTS][i];
    return (values[i] = text === undefined || text === null ? undefined : parse(text));
  };
  class Scope {
    constructor(texts, withEnq) {
      this[TEXTS] = texts;
      this[VALUES] = [UNREAD, UNREAD, UNREAD, UNREAD, UNREAD];
      if (withEnq) this.enq = enq;
    }
    get context() {
      return value(this, 0);
    }
    get event() {
      return value(this, 1);
    }
    get input() {
      return value(this, 2);
    }
    get params() {
      return value(this, 3);
    }
    // `self`, its parent and the children, fetched when first read: { self, parent?, children? }.
    get self() {
      const refs = this.#refs();
      if (refs.parent && !Object.hasOwn(refs.self, '_parent')) {
        Object.defineProperty(refs.self, '_parent', { value: refs.parent, enumerable: false });
      }
      return refs.self;
    }
    get children() {
      return this.#refs().children ?? {};
    }
    #refs() {
      const values = this[VALUES];
      return values[4] !== UNREAD ? values[4] : (values[4] = revive(call('refs')));
    }
    get actions() {
      return names;
    }
    get actors() {
      return names;
    }
  }
  // The root context's scope (XState's context factory): `input`, `self`,
  // `actors` and `spawn`.
  class ContextScope {
    constructor(texts) {
      this[TEXTS] = texts;
      this[VALUES] = [UNREAD, UNREAD];
    }
    get input() {
      return value(this, 0);
    }
    get self() {
      return value(this, 1);
    }
    get actors() {
      return names;
    }
    spawn(src, options) {
      return revive(call('spawnContext', src, options));
    }
  }

  const fns = new Map();
  // `mode`: 1 for `@code` (a function), plus 2 with `enq`.
  const invoke = (id, mode, scope) => {
    const fn = fns.get(id);
    return result(mode & 1 ? fn(scope, scope.enq) : fn(scope));
  };
  globalThis.__xstate = {
    compile(id, kind, source) {
      fns.set(id, kind === 'code' ? Function(`return (${source});`)() : Function('scope', `with (scope) { return (${source}); }`));
    },
    run: (id, mode, context, event, input, params) => invoke(id, mode, new Scope([context, event, input, params], mode & 2)),
    runContext: (id, mode, input, self) => invoke(id, mode, new ContextScope([input, self]))
  };
})();
