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
