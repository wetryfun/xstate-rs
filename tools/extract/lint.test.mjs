import assert from 'node:assert/strict';
import { test } from 'node:test';
import { lintCode } from './lint.mjs';

const lint = (node, options) => lintCode({ '@exprLang': 'js', context: { v: node } }, options);
const reads = (node, options) => lint(node, options).map((e) => /reads (.*?)(?:, which| from its scope)/.exec(e)[1]);

test('names a function declares, binds or takes are not free', () => {
  const code = (source) => ({ '@code': source });
  assert.deepEqual(lint(code('({ context: { a, b: [c, ...d] = [] }, event }, enq) => a + c + d.length + event.n + enq.x')), []);
  assert.deepEqual(lint(code('function named(x) { return named.length + x + arguments.length; }')), []);
  assert.deepEqual(
    lint(code('(s) => { var v = 1; { let w = 2; } function inner() { return v; } class K {} try { throw 1; } catch ({ message }) { return message; } return inner() + w + new K(); }')),
    []
  );
  assert.deepEqual(lint(code('({ context }) => ({ [context.k]: Math.max(1, Number.NaN), ...context, ok: true })')), []);
  assert.deepEqual(lint(code('(x = DEFAULT) => x')).length, 1);
  assert.deepEqual(reads(code('() => { label: for (;;) { break label; } return LIMIT; }')), ['`LIMIT`']);
  assert.deepEqual(reads(code('({ context }) => helper(context) + context.helper')), ['`helper`']);
  assert.deepEqual(reads(code('() => { undeclared = 1; }')), ['`undeclared`']);
});

test('expressions see the scope names; host globals need --globals', () => {
  assert.deepEqual(lint({ '@expr': 'context.n + event.by + (input?.x ?? 0) + scope.params' }), []);
  assert.deepEqual(reads({ '@expr': 'console.log(context)' }), ['`console`']);
  assert.deepEqual(lint({ '@expr': 'console.log(context)' }, { globals: ['console'] }), []);
  assert.deepEqual(reads({ '@expr': 'Intl.NumberFormat' }), ['`Intl`']);
});

test('guards and delays from the scope are reported, data named so is not', () => {
  assert.deepEqual(reads({ '@code': '(args) => args.guards.ok(args)' }), ['`guards`']);
  assert.deepEqual(reads({ '@code': '({ delays }) => delays.d' }), ['`delays`']);
  assert.deepEqual(reads({ '@expr': 'guards.ok(scope)' }), ['`guards`']);
  assert.deepEqual(lint({ '@code': '({ context }) => context.guards + context.delays' }), []);
});

test('only JavaScript is checked, and what does not parse is reported', () => {
  assert.deepEqual(lintCode({ context: { v: { '@code': 'anything at all', '@lang': 'rust' } } }), []);
  assert.match(lint({ '@code': '() => {' })[0], /does not parse/);
  assert.match(lint({ '@expr': 'a b' })[0], /does not parse/);
});
