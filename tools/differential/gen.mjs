// Random machine definitions and event sequences for differential fuzzing
// (fuzz.mjs, fuzz-system.mjs). Seeded, so a corpus is reproducible.

// mulberry32
export function prng(a) {
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Options: `id` (the machine id), `atLeast` (whether the ctxAtLeast stub
// guard exists), `invoke` ({ sources, chance }: invoke these actor sources
// with completion, error, snapshot and timeout transitions), `depth`.
export function generator(rand, { id: machineId = 'm', atLeast = true, invoke = null, depth: maxDepth = 3 } = {}) {
  const chance = (p) => rand() < p;
  const int = (n) => Math.floor(rand() * n);
  const pick = (xs) => xs[int(xs.length)];
  const EVENTS = ['a', 'b', 'c', 'x.y', 'x.z'];
  const DESCRIPTORS = [...EVENTS, 'x.*', '*'];

  let counter = 0;
  const nodes = []; // { id, path, kind, parentId }

  function guard(depth = 0) {
    const r = rand();
    if (r < 0.2) return { type: pick(['yes', 'no']) };
    if (r < 0.45) return { type: 'ctxEquals', params: { key: pick(['x', 'y']), value: int(3) } };
    if (r < 0.6) return { type: atLeast ? 'ctxAtLeast' : 'ctxEquals', params: { key: pick(['x', 'y']), value: int(3) } };
    if (r < 0.8) return { type: 'eventEquals', params: { key: 'n', value: int(3) } };
    if (r < 0.9 && depth < 2) return { type: '@xstate.not', params: { guard: guard(depth + 1) } };
    return { type: '@xstate.stateIn', params: { stateId: `#${pick(nodes).id}` } };
  }

  function actionList(tag) {
    const list = [];
    const n = int(4);
    for (let i = 0; i < n; i++) {
      const r = rand();
      if (r < 0.4) list.push({ type: '@xstate.emit', event: { type: 'trace', tag: `${tag}${i}` } });
      else if (r < 0.67) list.push({ type: '@xstate.assign', context: { [pick(['x', 'y'])]: int(3) } });
      else if (r < 0.75) list.push({ type: '@xstate.raise', event: { type: pick(EVENTS), n: int(3) } });
      else if (r < 0.85) {
        list.push({ type: '@xstate.raise', event: { type: pick(EVENTS), n: int(3) }, id: pick(['r1', 'r2']), delay: pick([50, 150]) });
      } else if (r < 0.92) list.push({ type: '@xstate.cancel', id: pick(['r1', 'r2']) });
      else list.push({ type: 'noop' });
    }
    return list;
  }

  function transition(source, tag) {
    const t = {};
    if (chance(0.75)) {
      const target = pick(nodes);
      t.target = `#${target.id}`;
      if (chance(0.1)) t.reenter = true;
    }
    if (chance(0.45)) t.guard = guard();
    if (chance(0.15)) t.context = { [pick(['x', 'y'])]: int(3) };
    if (chance(0.15)) t.matches = { n: int(3) };
    const actions = actionList(tag);
    if (actions.length) t.actions = actions;
    return t;
  }

  function transitions(source, tag) {
    if (chance(0.7)) return transition(source, tag);
    return Array.from({ length: 1 + int(3) }, (_, i) => transition(source, `${tag}.${i}`));
  }

  // Structure first (targets may point anywhere), then behavior.
  function build(path, depth, kind) {
    const key = `s${counter++}`;
    // The root's id is the machine id; descendants extend their parent's.
    const id = path === null ? machineId : `${path}.${key}`;
    const node = { key, id, kind, children: [], json: {} };
    nodes.push(node);
    if (kind === 'compound' || kind === 'parallel') {
      const n = 2 + int(2);
      for (let i = 0; i < n; i++) {
        let childKind;
        if (kind === 'parallel') childKind = depth < maxDepth && chance(0.7) ? 'compound' : 'atomic';
        else {
          const r = rand();
          childKind =
            depth >= maxDepth ? (r < 0.8 ? 'atomic' : 'final') : r < 0.55 ? 'atomic' : r < 0.75 ? 'compound' : r < 0.85 ? 'parallel' : r < 0.95 ? 'final' : 'atomic';
          if (i === 0 && childKind === 'final') childKind = 'atomic';
        }
        node.children.push(build(id, depth + 1, childKind));
      }
      if (kind === 'compound' && chance(0.2)) {
        const hkey = `s${counter++}`;
        const h = { key: hkey, id: `${id}.${hkey}`, kind: 'history', children: [], json: {} };
        h.json = { type: 'history', history: pick(['shallow', 'deep']), target: node.children[0].key };
        nodes.push(h);
        node.children.push(h);
      }
    }
    return node;
  }

  function behave(node) {
    const json = node.json;
    if (node.kind === 'history') return;
    if (node.kind === 'parallel') json.type = 'parallel';
    if (node.kind === 'final') {
      json.type = 'final';
      if (chance(0.5)) json.output = { v: int(3) };
    }
    if (node.kind === 'compound') json.initial = node.children[0].key;
    if (chance(0.4)) json.entry = actionList(`${node.key}.entry`);
    if (chance(0.3)) json.exit = actionList(`${node.key}.exit`);
    if (node.kind !== 'final') {
      const on = {};
      for (const d of DESCRIPTORS) {
        if (chance(d.includes('*') ? 0.08 : 0.3)) on[d] = transitions(node, `${node.key}.${d}`);
      }
      if ((node.kind === 'compound' || node.kind === 'parallel') && chance(0.5)) {
        on['xstate.done.state'] = { ...transition(node, `${node.key}.done`), matches: { stateId: node.id } };
      }
      if (Object.keys(on).length) json.on = on;
      if (chance(0.08)) {
        const t = transition(node, `${node.key}.always`);
        t.guard ??= { type: 'ctxEquals', params: { key: 'x', value: int(3) } };
        json.always = t;
      }
      if (chance(0.2)) {
        json.after = { [pick([50, 100, 'short', 'long', '200ms'])]: transition(node, `${node.key}.after`) };
      }
      if (invoke && chance(invoke.chance)) {
        const inv = { src: pick(invoke.sources) };
        if (chance(0.7)) inv.id = `c${int(3)}`;
        if (chance(0.6)) inv.onDone = transitions(node, `${node.key}.done.actor`);
        if (chance(0.5)) inv.onError = transitions(node, `${node.key}.error.actor`);
        if (chance(0.15)) inv.onSnapshot = { actions: actionList(`${node.key}.snapshot`) };
        if (chance(0.15)) {
          inv.timeout = pick([100, 300, '1s']);
          inv.onTimeout = transition(node, `${node.key}.timeout.actor`);
        }
        json.invoke = inv;
      }
      if (invoke && chance(0.05)) {
        json.timeout = pick([150, 'short', '2s']);
        json.onTimeout = transition(node, `${node.key}.timeout.state`);
      }
    }
    if (node.children.length) {
      json.states = Object.fromEntries(node.children.map((c) => [c.key, (behave(c), c.json)]));
    }
  }

  return function machine() {
    counter = 0;
    nodes.length = 0;
    const root = build(null, 0, 'compound');
    behave(root);
    const def = { id: machineId, context: { x: 0, y: 0 }, ...root.json };
    if (chance(0.2)) def.output = { done: true };
    return def;
  };
}

export function steps(rand) {
  const EVENTS = ['a', 'b', 'c', 'x.y', 'x.z'];
  const n = 4 + Math.floor(rand() * 12);
  let at = 0;
  const list = [];
  for (let i = 0; i < n; i++) {
    at += [0, 0, 30, 60, 120, 400, 1100][Math.floor(rand() * 7)];
    list.push(rand() < 0.85 ? { at, send: { type: EVENTS[Math.floor(rand() * 5)], n: Math.floor(rand() * 3) } } : { at });
  }
  return list;
}

