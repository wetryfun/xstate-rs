import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { Engine, EngineInstance, Event } from '@xmachines/run';
import { engines } from './engines.ts';

/** A machine computed by JavaScript code nodes (as `machineToJSON` writes inline functions). */
const machine = {
  id: 'counter',
  '@exprLang': 'js',
  context: { '@code': '({ input }) => ({ count: input?.start ?? 0 })' },
  initial: 'counting',
  states: {
    counting: {
      on: {
        inc: {
          guard: { '@expr': 'context.count < 3' },
          context: { count: { '@expr': 'context.count + event.by' } },
          actions: [
            {
              '@code':
                "({ context, self }, enq) => { enq.emit({ type: 'counted', count: context.count, by: self.id }); enq.raise({ type: 'check' }, { delay: 100, id: 'c' }); }",
            },
          ],
        },
        check: { '@code': "({ context }) => (context.count >= 3 ? { target: 'done' } : undefined)" },
      },
    },
    done: { type: 'final', output: { total: { '@expr': 'context.count' } } },
  },
};

type Step = { send: Event; at: number } | { advance: number };
const steps: Step[] = [
  { send: { type: 'inc', by: 1 }, at: 0 },
  { advance: 100 },
  { send: { type: 'inc', by: 1 }, at: 150 },
  { advance: 250 },
];

/** Applies steps, returning what each produced. */
function play(instance: EngineInstance, from: number, to: number): unknown[] {
  const trace: unknown[] = [];
  for (const step of steps.slice(from, to)) {
    if ('send' in step) instance.send(step.send, step.at);
    else instance.advance(step.advance);
    trace.push({ ...instance.take(), root: instance.root(), deadline: instance.deadline() });
  }
  return trace;
}

async function start(engine: Engine): Promise<EngineInstance> {
  return (await engine.load(machine)).start({ id: 'counter', input: { start: 1 }, time: 0 });
}

test('JavaScript code nodes run alike on every engine', async () => {
  const all = Object.entries(await engines());
  const traces: Record<string, unknown[]> = {};
  for (const [name, engine] of all) {
    const instance = await start(engine);
    assert.deepEqual(instance.root().context, { count: 1 }, name);
    traces[name] = play(instance, 0, steps.length);
    assert.deepEqual(instance.root().output, { total: 3 }, name);
    instance.dispose?.();
  }
  const [first, ...rest] = Object.keys(traces);
  assert.deepEqual(
    (traces[first!]![0] as { emitted: unknown }).emitted,
    [{ type: 'counted', count: 2, by: 'counter' }],
  );
  for (const name of rest) assert.deepEqual(traces[name], traces[first!], `${name} differs from ${first}`);
});

test('a run with JavaScript code nodes moves between engines', async () => {
  const all = Object.entries(await engines());
  const reference = play(await start(all[0]![1]), 0, steps.length);
  for (const [from, a] of all) {
    for (const [to, b] of all) {
      const first = await start(a);
      const head = play(first, 0, 2);
      const second = (await b.load(machine)).restore(first.state());
      const tail = play(second, 2, steps.length);
      assert.deepEqual([...head, ...tail], reference, `${from} -> ${to}`);
    }
  }
});
