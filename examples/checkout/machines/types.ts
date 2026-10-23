// Exercises the schema-to-Rust mapping; see tests/types.rs.
import { z } from 'zod';
import type { MachineJSON } from 'xstate';

const Money = z.object({ amount: z.number().int(), currency: z.string() }).meta({ id: 'Money' });
const Category = z.object({
  name: z.string(),
  get children() {
    return z.array(Category);
  }
});

export const schemas = {
  context: z.object({
    prices: z.record(z.string(), Money),
    total: Money,
    point: z.tuple([z.number(), z.number()]),
    note: z.string().nullable(),
    type: z.string().default('basic'),
    tree: Category,
    anything: z.unknown(),
    flags: z.array(z.boolean()),
    kind: z.literal('demo')
  }),
  events: {
    set: z.object({ value: z.union([z.object({ op: z.literal('add'), by: z.number() }), z.object({ op: z.literal('reset') })]) })
  },
  input: z.object({ seed: z.number().int().nullable().optional() }),
  output: z.string(),
  actions: { record: { params: z.array(z.string()) } },
  guards: { atLeast: { params: z.number() } }
};

export const machine = {
  id: 'types',
  initial: 'idle',
  context: {
    prices: { a: { amount: 100, currency: 'SEK' } },
    total: { amount: 0, currency: 'SEK' },
    point: [1, 2],
    note: null,
    type: 'basic',
    tree: { name: 'root', children: [{ name: 'leaf', children: [] }] },
    anything: { free: ['form'] },
    flags: [],
    kind: 'demo'
  },
  states: {
    idle: {
      on: {
        set: { target: 'done', guard: { type: 'atLeast', params: 2 }, actions: [{ type: 'record', params: ['a', 'b'] }] }
      }
    },
    done: { type: 'final', output: 'finished' }
  }
} satisfies MachineJSON;
