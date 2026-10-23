// Expressions for `{ "@code": name, "@lang": "rust" }` nodes, as XState's
// evaluator for the `rust` language. Keep in sync with
// crates/xstate-rt/tests/common/exprs.rs.
export const exprs = {
  countPlusOne: ({ context }) => context.count + 1,
  totalPlusAmount: ({ context, event }) => context.total + event.amount,
  attemptsPlusOne: ({ context }) => context.attempts + 1,
  eventValue: ({ event }) => event.value,
  eventType: ({ event }) => event.type,
  paramsValue: ({ params }) => params.value,
  countIsEven: ({ context }) => context.count % 2 === 0,
  incrementCount: ({ context }) => ({ context: { count: context.count + 1 } }),
  appendItem: ({ context, event }) => [...context.items, event.item],
  backoff: ({ context }) => 100 * 2 ** context.attempts,
  eventOutput: ({ event }) => event.output,
  totalFromOutput: ({ context, event }) => context.total + event.output.total,
  summary: ({ context }) => ({ count: context.count, total: context.total }),
  // Transition functions (`undefined`: not enabled) and `enq`.
  pickTarget: ({ context, event }) =>
    event.value === undefined
      ? undefined
      : { target: event.value > 0 ? 'positive' : '#fn.negative', context: { count: context.count + 1 } },
  pickReenter: ({ context }) => ({ target: '.inner', reenter: context.count > 1 }),
  enqueueOnly: ({ context, enq }) => {
    enq.emit({ type: 'enqueued', count: context.count });
  },
  raiseLater: ({ context, enq }) => {
    enq.raise({ type: 'LATER', value: context.count }, { delay: 50, id: 'later' });
    enq.raise({ type: 'NOW' });
    return { context: { count: context.count + 100 } };
  },
  cancelLater: ({ enq }) => {
    enq.cancel('later');
    return {};
  },
  logAndCall: ({ context, enq, actions }) => {
    enq.log('count', context.count);
    enq(actions.noop, { value: context.count });
  },
  emitTwice: ({ context, enq }) => {
    enq.emit({ type: 'one' });
    enq.emit({ type: 'two', count: context.count });
    return { context: { count: context.count + 10 } };
  },
  routeIfEven: ({ context }) => (context.count % 2 === 0 ? { context: { routed: context.count } } : false),
  routeAlways: () => true,
  choosePick: ({ context }) => (context.count % 2 === 0 ? { target: 'even' } : { target: 'odd', context: { picked: context.count } }),
  pickEvenAlways: ({ context }) => (context.count % 2 === 0 ? { target: 'even' } : undefined),
  doneToEven: ({ event }) => ({ target: 'even', context: { result: event.output } }),
  // State input.
  stateInput: ({ input }) => input ?? null,
  hasInput: ({ input }) => input !== undefined,
  toBWithInput: ({ context }) => ({ target: 'b', input: { fromFn: context.count } }),
  // examples/pattern-retry-policy (fixtures/system/ex_retry_policy*).
  canRetry: ({ context }) => context.attempts + 1 < 5,
  contextName: ({ context }) => context.name,
  errorMessage: ({ event }) => String(event.error.message),
  outputRecordId: ({ event }) => event.output.recordId,
  retryMessage: ({ context }) =>
    `attempt ${context.attempts} failed (${context.error}); retrying in ${100 * 2 ** context.attempts}ms`,
  storedOutput: ({ context }) => ({ ok: true, recordId: context.recordId }),
  gaveUpOutput: ({ context }) => ({ ok: false, attempts: context.attempts }),
  retryContext: ({ input }) => ({ name: input.name, attempts: 0, recordId: null, error: null }),
  // Root `context` (scope: the actor's input).
  contextFromInput: ({ input }) => ({ count: input?.start ?? 0, total: 0, attempts: 0, items: [] })
};

export const evaluators = {
  rust: ({ source, scope }) => {
    const f = exprs[source];
    if (!f) throw new Error(`no expression '${source}'`);
    return f(scope);
  }
};
