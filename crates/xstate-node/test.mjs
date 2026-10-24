// Drives the xstate-node addon from Node, mirroring the WASI component's
// tests (crates/xstate-wasi-host/tests/component.rs) with JavaScript
// callbacks as the host.
//
//   crates/xstate-node/build.sh && node --test crates/xstate-node/test.mjs
//
// XSTATE_NODE_ADDON overrides the addon path (default:
// target/release/xstate_node.node under the repository root).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const addon = process.env.XSTATE_NODE_ADDON ?? path.join(root, 'target/release/xstate_node.node');
const { Actor } = createRequire(import.meta.url)(path.resolve(addon));

const json = JSON.stringify;
const parse = JSON.parse;
const code = (name) => ({ '@code': name, '@lang': 'rust' });

/// Implementations recording what the runtime asked for, like the Wasmtime
/// host of the WASI tests.
function host() {
  const h = {
    actions: [],
    guardCalls: [],
    guard(name, params, context, event) {
      params = params === undefined ? undefined : parse(params);
      context = parse(context);
      event = parse(event);
      this.guardCalls.push([name, params, context, event]);
      switch (name) {
        case 'atLeast':
          return event.n >= params.min;
        case 'retryable':
          return event.error?.retryable === true;
        default:
          throw new Error(`unexpected guard ${name}`);
      }
    },
    action(name, params) {
      this.actions.push([name, params === undefined ? undefined : parse(params)]);
    },
    delay(name, context) {
      assert.equal(name, 'backoff');
      return 100 * (1 + (parse(context).tries ?? 0));
    },
    expr(name, scope) {
      scope = parse(scope);
      switch (name) {
        case 'nextTries':
          return json(scope.context.tries + 1);
        case 'carEvent':
          return json({ type: 'seen', n: scope.event.n, label: scope.params.label });
        default:
          throw new Error(`unexpected expression ${name}`);
      }
    },
    contextExpr(name, input) {
      assert.equal(name, 'fromInput');
      return json({ tries: (input === undefined ? {} : parse(input)).tries ?? 0 });
    },
  };
  return h;
}

const light = () => ({
  id: 'light',
  initial: 'green',
  context: { tries: 0 },
  states: {
    green: {
      on: {
        car: [
          { target: 'yellow', guard: { type: 'atLeast', params: { min: 3 } } },
          { actions: [{ type: 'honk', params: 'beep' }, { type: 'count' }] },
        ],
      },
    },
    yellow: {
      entry: [{ type: '@xstate.log', args: ['yellow'] }],
      after: { backoff: { target: 'red', context: { tries: 1 } } },
    },
    red: {
      entry: [{ type: '@xstate.emit', event: { type: 'stopped', at: 'red' } }],
      on: { go: { target: 'done' } },
    },
    done: { type: 'final', output: { ok: true } },
  },
});

/// A machine without implementations: emits, an `after` timer, a final state.
const plain = () => ({
  id: 'plain',
  initial: 'idle',
  context: { n: 0 },
  states: {
    idle: {
      on: {
        ping: {
          target: 'waiting',
          actions: [
            { type: '@xstate.assign', context: { n: 1 } },
            { type: '@xstate.emit', event: { type: 'pong' } },
          ],
        },
      },
    },
    waiting: { after: { 500: { target: 'done' } } },
    done: { type: 'final', output: { finished: true } },
  },
});

test('start, send and emitted events', () => {
  const a = Actor.start(json(plain()), '', 0);
  assert.deepEqual(parse(a.snapshot()), { status: 'active', value: 'idle', context: { n: 0 } });
  a.send(json({ type: 'ping' }), 10);
  assert.deepEqual(parse(a.snapshot()), { status: 'active', value: 'waiting', context: { n: 1 } });
  assert.deepEqual(a.takeEmitted().map(parse), [{ type: 'pong' }]);
  assert.deepEqual(a.takeEmitted(), []);
  assert.deepEqual(a.takeLogs(), []);
  assert.deepEqual(a.pendingEffects(), []);
});

test('after timers through nextDeadline and advance', () => {
  const a = Actor.start(json(plain()), '{}', 0);
  assert.equal(a.nextDeadline(), null);
  a.send(json({ type: 'ping' }), 10);
  assert.equal(a.nextDeadline(), 510);
  a.advance(509);
  assert.equal(parse(a.snapshot()).value, 'waiting');
  a.advance(510);
  assert.deepEqual(parse(a.snapshot()), {
    status: 'done',
    value: 'done',
    context: { n: 1 },
    output: { finished: true },
  });
  assert.equal(a.nextDeadline(), null);
});

test('persist and restore round trip', () => {
  const a = Actor.start(json(plain()), '', 0);
  a.send(json({ type: 'ping' }), 10);
  const saved = a.persist();
  const system = parse(saved);
  assert.ok(system.actors, saved);

  const b = Actor.restore(json(plain()), '', saved);
  assert.equal(b.persist(), saved);
  assert.deepEqual(parse(b.snapshot()), parse(a.snapshot()));
  assert.equal(b.nextDeadline(), 510);
  b.advance(510);
  assert.equal(parse(b.snapshot()).status, 'done');
  // The original is unaffected.
  assert.equal(parse(a.snapshot()).value, 'waiting');
});

test('errors carry the same messages as the WASI component', () => {
  const a = Actor.start(json(plain()), '', 0);
  assert.throws(() => a.send('{nope', 0), /^Error: invalid event JSON: /);
  assert.throws(() => a.send(json({ no: 'type' }), 0), {
    message: 'an event must be an object with a string "type"',
  });
  assert.throws(() => a.sendTo('plain/nobody', json({ type: 'x' }), 0));
  assert.throws(() => a.resolveEffect('system:999', null));
  assert.throws(() => a.send(json({ type: 'ping' }), -1), {
    message: 'now must be a non-negative number of milliseconds',
  });
  assert.throws(() => Actor.start(json(plain()), json({ bogus: 1 }), 0), { message: 'unknown option "bogus"' });
  assert.throws(() => Actor.start('{', '', 0), /^Error: invalid definition JSON: /);
  assert.throws(() => Actor.restore(json(plain()), '', 'nope'), /^Error: invalid snapshot JSON: /);
  const ts = { id: 'x', initial: 'a', states: { a: { entry: { '@code': '() => {}', '@lang': 'ts' } } } };
  assert.throws(() => Actor.start(json(ts), '', 0), /@code/);

  const looping = {
    id: 'loop',
    initial: 'a',
    states: { a: { on: { go: { actions: [{ type: '@xstate.raise', event: { type: 'go' } }] } } } },
  };
  const b = Actor.start(json(looping), '', 0);
  assert.throws(() => b.send(json({ type: 'go' }), 0), /infinite loop/);
  assert.equal(parse(b.snapshot()).status, 'error');
  // The actor is still usable after a runtime error.
  b.stop();
});

test('guards, actions and delays come from JavaScript', () => {
  const h = host();
  const a = Actor.start(json(light()), '{}', 0, h);
  assert.deepEqual(parse(a.snapshot()), { status: 'active', value: 'green', context: { tries: 0 } });

  a.send(json({ type: 'car', n: 1 }), 10);
  assert.deepEqual(h.actions, [
    ['honk', 'beep'],
    ['count', undefined],
  ]);
  assert.deepEqual(h.guardCalls[0], ['atLeast', { min: 3 }, { tries: 0 }, { type: 'car', n: 1 }]);

  a.send(json({ type: 'car', n: 5 }), 20);
  assert.equal(parse(a.snapshot()).value, 'yellow');
  assert.deepEqual(a.takeLogs().map(parse), [['yellow']]);
  // `backoff` came from JavaScript: 100 * (1 + tries).
  assert.equal(a.nextDeadline(), 120);
  a.advance(119);
  assert.equal(parse(a.snapshot()).value, 'yellow');
  a.advance(120);
  assert.deepEqual(parse(a.snapshot()).context, { tries: 1 });
  assert.equal(parse(a.snapshot()).value, 'red');
  assert.deepEqual(a.takeEmitted().map(parse), [{ type: 'stopped', at: 'red' }]);

  a.send(json({ type: 'go' }), 200);
  assert.deepEqual(parse(a.snapshot()), {
    status: 'done',
    value: 'done',
    context: { tries: 1 },
    output: { ok: true },
  });
});

test('expressions come from JavaScript', () => {
  const definition = {
    id: 'counted',
    context: code('fromInput'),
    actions: { report: [{ type: '@xstate.emit', event: code('carEvent') }] },
    initial: 'a',
    states: {
      a: {
        on: {
          car: {
            context: { tries: code('nextTries') },
            actions: [{ type: 'report', params: { label: 'car' } }],
          },
        },
      },
    },
  };
  const a = Actor.start(json(definition), json({ input: { tries: 4 } }), 0, host());
  assert.deepEqual(parse(a.snapshot()).context, { tries: 4 });
  a.send(json({ type: 'car', n: 2 }), 1);
  assert.deepEqual(parse(a.snapshot()).context, { tries: 5 });
  assert.deepEqual(a.takeEmitted().map(parse), [{ type: 'seen', n: 2, label: 'car' }]);
});

test('persist and restore with implementations', () => {
  const a = Actor.start(json(light()), '{}', 0, host());
  a.send(json({ type: 'car', n: 9 }), 50);
  const saved = a.persist();

  const b = Actor.restore(json(light()), '{}', saved, host());
  assert.equal(parse(b.snapshot()).value, 'yellow');
  assert.equal(b.nextDeadline(), 150);
  assert.equal(b.persist(), saved);
  b.advance(150);
  assert.equal(parse(b.snapshot()).value, 'red');
});

test('invoked work is done by JavaScript or by child machines', () => {
  const definition = {
    id: 'order',
    initial: 'charging',
    context: { tries: 0 },
    states: {
      charging: {
        invoke: {
          src: 'charge',
          onDone: { target: 'shipping' },
          onError: [{ target: 'charging', reenter: true, guard: { type: 'retryable' } }, { target: 'failed' }],
        },
      },
      shipping: { invoke: { id: 'courier', src: 'courier', onDone: { target: 'delivered' } } },
      delivered: { type: 'final' },
      failed: { type: 'final' },
    },
  };
  const courier = {
    id: 'courier',
    initial: 'driving',
    states: {
      driving: { on: { arrive: { target: 'arrived' } } },
      arrived: { type: 'final', entry: [{ type: 'notify', params: { to: 'customer' } }] },
    },
  };
  const h = host();
  const a = Actor.start(json(definition), json({ children: { courier } }), 0, h);

  let effects = a.pendingEffects().map(parse);
  assert.equal(effects.length, 1);
  assert.equal(effects[0].type, '@xstate.effect');
  a.resolveEffect(effects[0].id, json({ type: 'xstate.async.reject', data: { retryable: true } }));
  assert.equal(parse(a.snapshot()).value, 'charging', 'retried');

  const id = parse(a.pendingEffects()[0]).id;
  a.resolveEffect(id, json({ type: 'xstate.async.resolve', data: { receipt: 1 } }));
  assert.equal(parse(a.snapshot()).value, 'shipping');

  assert.ok(parse(a.persist()).actors['order/courier']);
  a.sendTo('order/courier', json({ type: 'arrive' }), 10);
  assert.deepEqual(h.actions, [['notify', { to: 'customer' }]]);
  assert.deepEqual(parse(a.snapshot()), { status: 'done', value: 'delivered', context: { tries: 0 } });
  a.stop();
});

test('a missing implementation fails at start', () => {
  assert.throws(() => Actor.start(json(light()), '', 0), {
    message: 'the definition references guard "atLeast" but implementations.guard is missing',
  });
  assert.throws(() => Actor.start(json(light()), '', 0, { guard: () => false, delay: () => 1 }), {
    message: 'the definition references action "honk" but implementations.action is missing',
  });
  assert.throws(() => Actor.start(json(light()), '', 0, { guard: 1 }), {
    message: 'implementations.guard must be a function',
  });
});

test('a callback that throws fails the call and retires the actor', () => {
  const h = host();
  const failure = new Error('guard broke');
  const a = Actor.start(json(light()), '', 0, {
    ...h,
    guard() {
      throw failure;
    },
  });
  assert.throws(
    () => a.send(json({ type: 'car', n: 1 }), 1),
    (e) => e === failure,
  );
  assert.throws(() => a.snapshot(), /^Error: the actor is unusable: .*guard broke/);

  // A bad return value is reported the same way.
  const b = Actor.start(json(light()), '', 0, { ...h, delay: () => 'soon' });
  assert.throws(() => b.send(json({ type: 'car', n: 5 }), 1), {
    message: 'delay "backoff" must return a non-negative number of milliseconds',
  });
});

test('a callback cannot re-enter its own actor, but may use another', () => {
  const other = Actor.start(json(plain()), '', 0);
  let seen;
  const a = Actor.start(json(light()), '', 0, {
    ...host(),
    guard() {
      other.send(json({ type: 'ping' }), 1);
      seen = parse(other.snapshot()).value;
      try {
        a.snapshot();
      } catch (e) {
        seen += ` / ${e.message}`;
      }
      return true;
    },
  });
  a.send(json({ type: 'car', n: 1 }), 1);
  assert.equal(seen, 'waiting / the actor is busy: an implementation called back into it');
  assert.equal(parse(a.snapshot()).value, 'yellow');
});
