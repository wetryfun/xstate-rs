// Checks for what a machine loses on its way to JSON, so it fails here
// instead of at run time:
//
// - JavaScript code nodes (`js`, or `ts` once stripped) that read names
//   nothing defines where they run: a serialized function is only its text,
//   so constants and helpers from its module are gone (XState's own revived
//   machine fails the same way). Allowed are the scope's names (`@expr`),
//   the ECMAScript globals, and `--globals`.
// - Code that calls `guards.*` or `delays.*`: XState's scope has its
//   sources there, but serialization drops functions and xstate-rs code
//   nodes have no such scope members.
// - `setup()` implementations serialization drops (functions are not data):
//   the host must supply them, or the JSON must define them.
// - Invokes whose `src` is inline actor logic, which `machineToJSON` drops
//   with the whole invoke.
import { parseExpressionAt } from 'acorn';
import { runInNewContext } from 'node:vm';

/**
 * The ECMAScript globals every evaluator has: a fresh V8 context's (no host
 * globals such as `process`) less those QuickJS (xstate-js) lacks.
 */
const ECMASCRIPT_GLOBALS = new Set(
  runInNewContext('Object.getOwnPropertyNames(globalThis)').filter((n) => !['console', 'Intl', 'WebAssembly'].includes(n))
);

/** What an `@expr` node can name besides globals (XState's scope, `with (scope)`). */
const SCOPE_NAMES = new Set(['scope', 'context', 'event', 'input', 'params', 'self', 'children', 'actions', 'actors', 'enq', 'spawn']);

/** Scope members XState has that xstate-rs code nodes do not. */
const MISSING_MEMBERS = new Set(['guards', 'delays']);

const SKIP = new Set(['type', 'start', 'end', 'loc', 'range']);

/** Names a pattern binds. */
function bindPattern(p, names) {
  switch (p?.type) {
    case 'Identifier':
      names.add(p.name);
      break;
    case 'ObjectPattern':
      for (const prop of p.properties) bindPattern(prop.type === 'RestElement' ? prop.argument : prop.value, names);
      break;
    case 'ArrayPattern':
      for (const e of p.elements) if (e) bindPattern(e, names);
      break;
    case 'AssignmentPattern':
      bindPattern(p.left, names);
      break;
    case 'RestElement':
      bindPattern(p.argument, names);
      break;
  }
}

/**
 * Declarations a function body makes, wherever they are (blocks included,
 * nested functions excluded): a slight over-approximation of JavaScript's
 * scoping that never reports a declared name as free.
 */
function hoist(node, names) {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) return node.forEach((n) => hoist(n, names));
  switch (node.type) {
    case 'VariableDeclaration':
      for (const d of node.declarations) bindPattern(d.id, names);
      break;
    case 'FunctionDeclaration':
      if (node.id) names.add(node.id.name);
      return;
    case 'ClassDeclaration':
      if (node.id) names.add(node.id.name);
      break;
    case 'CatchClause':
      if (node.param) bindPattern(node.param, names);
      break;
    case 'FunctionExpression':
    case 'ArrowFunctionExpression':
      return;
  }
  for (const [k, v] of Object.entries(node)) if (!SKIP.has(k)) hoist(v, names);
}

/**
 * The free names of an expression, and the members of `scopeName` it reads
 * that xstate-rs's scope does not have (`args.guards`).
 */
export function analyze(ast, scopeName) {
  const free = new Set();
  const members = new Set();
  const scopes = [];
  const declared = (name) => scopes.some((s) => s.has(name));

  // Default values and computed keys in a binding pattern are expressions.
  const patternExpressions = (p) => {
    switch (p?.type) {
      case 'ObjectPattern':
        for (const prop of p.properties) {
          if (prop.type === 'RestElement') patternExpressions(prop.argument);
          else {
            if (prop.computed) visit(prop.key);
            patternExpressions(prop.value);
          }
        }
        break;
      case 'ArrayPattern':
        for (const e of p.elements) if (e) patternExpressions(e);
        break;
      case 'AssignmentPattern':
        patternExpressions(p.left);
        visit(p.right);
        break;
      case 'RestElement':
        patternExpressions(p.argument);
        break;
    }
  };

  const visitFunction = (fn) => {
    const names = new Set();
    if (fn.type === 'FunctionExpression' && fn.id) names.add(fn.id.name);
    if (fn.type !== 'ArrowFunctionExpression') names.add('arguments');
    for (const p of fn.params) bindPattern(p, names);
    hoist(fn.body, names);
    scopes.push(names);
    for (const p of fn.params) patternExpressions(p);
    visit(fn.body);
    scopes.pop();
  };

  function visit(node) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) return node.forEach(visit);
    switch (node.type) {
      case 'Identifier':
        if (!declared(node.name)) free.add(node.name);
        return;
      case 'FunctionExpression':
      case 'ArrowFunctionExpression':
      case 'FunctionDeclaration':
        return visitFunction(node);
      case 'MemberExpression':
        visit(node.object);
        if (node.computed) visit(node.property);
        else if (
          scopeName &&
          node.object.type === 'Identifier' &&
          node.object.name === scopeName &&
          MISSING_MEMBERS.has(node.property.name)
        ) {
          members.add(node.property.name);
        }
        return;
      case 'Property':
      case 'MethodDefinition':
      case 'PropertyDefinition':
        if (node.computed) visit(node.key);
        return visit(node.value);
      case 'VariableDeclarator':
        patternExpressions(node.id);
        return visit(node.init);
      case 'CatchClause':
        patternExpressions(node.param);
        return visit(node.body);
      case 'ClassDeclaration':
      case 'ClassExpression':
        visit(node.superClass);
        return visit(node.body);
      case 'LabeledStatement':
        return visit(node.body);
      case 'BreakStatement':
      case 'ContinueStatement':
      case 'MetaProperty':
        return;
    }
    for (const [k, v] of Object.entries(node)) if (!SKIP.has(k)) visit(v);
  }

  scopes.push(new Set());
  hoist(ast, scopes[0]);
  visit(ast);
  return { free, members };
}

/** Parses a code node as XState's JavaScript evaluators compile it. */
function parse(source, kind) {
  const text = kind === 'code' ? `(${source})` : source;
  let ast = parseExpressionAt(text, 0, { ecmaVersion: 'latest', preserveParens: true });
  if (text.slice(ast.end).trim()) throw new Error(`unexpected input after the expression: ${text.slice(ast.end).trim().slice(0, 20)}`);
  while (ast.type === 'ParenthesizedExpression') ast = ast.expression;
  return ast;
}

/**
 * Checks the JavaScript code nodes of a (type-stripped) definition. Returns
 * error messages.
 */
export function lintCode(machine, { globals = [] } = {}) {
  const errors = [];
  const allowed = new Set([...ECMASCRIPT_GLOBALS, ...globals]);
  const walk = (value, path, lang) => {
    if (Array.isArray(value)) return value.forEach((v, i) => walk(v, `${path}[${i}]`, lang));
    if (!value || typeof value !== 'object') return;
    const key = ['@code', '@expr'].find((k) => k in value);
    if (key) {
      const valueLang = value['@lang'] ?? lang;
      if (valueLang !== 'js' && valueLang !== 'ts') return;
      let ast;
      try {
        ast = parse(value[key], key === '@code' ? 'code' : 'expr');
      } catch (e) {
        return errors.push(`${path}: ${key} does not parse: ${e.message}`);
      }
      // The scope: a function's first parameter, or `scope` in an expression.
      const first = key === '@code' && /Function/.test(ast.type) ? ast.params[0] : undefined;
      const result = analyze(ast, key === '@expr' ? 'scope' : first?.type === 'Identifier' ? first.name : undefined);
      if (first?.type === 'ObjectPattern') {
        for (const prop of first.properties) {
          const name = prop.type === 'Property' && !prop.computed ? (prop.key.name ?? prop.key.value) : undefined;
          if (MISSING_MEMBERS.has(name)) result.members.add(name);
        }
      }
      if (key === '@expr') {
        for (const name of MISSING_MEMBERS) {
          if (result.free.delete(name)) result.members.add(name);
        }
      }
      const names = [...result.free].filter((n) => !allowed.has(n) && !(key === '@expr' && SCOPE_NAMES.has(n)));
      if (names.length) {
        errors.push(
          `${path}: ${key} reads ${names.map((n) => `\`${n}\``).join(', ')}, which nothing defines where it runs ` +
            `(a serialized function keeps only its text, not its module's names): pass the value in through ` +
            `input, context or the event, write it inline, or allow host globals with --globals`
        );
      }
      if (result.members.size) {
        errors.push(
          `${path}: ${key} reads ${[...result.members].map((m) => `\`${m}\``).join(', ')} from its scope, which ` +
            `xstate-rs code nodes do not have: reference a named guard ({ type }) or delay in the JSON instead`
        );
      }
      return;
    }
    for (const [k, v] of Object.entries(value)) walk(v, `${path}.${k}`, lang);
  };
  walk(machine, '$', machine['@exprLang']);
  return errors;
}

/**
 * Checks what `machineToJSON` drops from a machine built with `setup()` /
 * `createMachine()`. Returns `{ errors, warnings }`.
 */
export function lintMachine(stateMachine, json) {
  const errors = [];
  const warnings = [];
  const statePath = (node) => ['$', ...node.path.map((k) => `states.${k}`)].join('.');
  const visit = (node) => {
    (node.invoke ?? []).forEach((inv, i) => {
      if (typeof inv.src !== 'string') {
        errors.push(
          `${statePath(node)}: invoke ${i} (id "${inv.id}") has inline actor logic, which machineToJSON drops with ` +
            `the whole invoke: give the logic a name (setup({ actors: { name } }) and src: "name") and supply it from the host`
        );
      }
    });
    for (const child of Object.values(node.states ?? {})) visit(child);
  };
  visit(stateMachine.root);

  const sources = stateMachine.sources ?? {};
  const host = { actions: 'an action', guards: 'a guard', delays: 'a delay' };
  for (const [kind, what] of Object.entries(host)) {
    for (const name of Object.keys(sources[kind] ?? {})) {
      if (json[kind]?.[name] !== undefined) continue;
      warnings.push(
        `setup ${kind}.${name} is a function, which serialization drops: the host must implement ${what} ` +
          `"${name}" (Registry, codegen) or the JSON must define it`
      );
    }
  }
  for (const name of Object.keys(sources.actors ?? {})) {
    warnings.push(
      `setup actors.${name} is actor logic, which serialization drops: the host supplies it ` +
        `(Registry::actor, a child machine, or async work completed by effect results)`
    );
  }
  return { errors, warnings };
}
