// A checkout flow written for xstate-rs: a JSON-portable MachineJSON
// definition (transitions as objects, guards and actions referenced by name)
// plus the Zod schemas codegen turns into Rust types.
//
// Regenerate the bundle with
//   node ../../tools/extract/extract.mjs machines/checkout.ts
import { z } from 'zod';
import type { MachineJSON } from 'xstate';

const Line = z.object({ sku: z.string(), quantity: z.number().int().min(1) });

export const schemas = {
  context: z.object({
    status: z.enum(['open', 'paying', 'retrying', 'paid', 'failed']),
    lines: z.array(Line),
    coupon: z.string().optional().describe('A coupon code, once one is applied.')
  }),
  events: {
    'cart.add': z.object({ line: Line }),
    checkout: z.object({ total: z.number() }),
    'payment.method': z.object({
      method: z.discriminatedUnion('kind', [
        z.object({ kind: z.literal('card'), last4: z.string() }),
        z.object({ kind: z.literal('invoice'), reference: z.string().optional() })
      ])
    }),
    cancel: z.object({})
  },
  internalEvents: { retry: z.object({}) },
  emitted: {
    'order.paid': z.object({ channel: z.enum(['email', 'sms']) }),
    'order.failed': z.object({})
  },
  output: z.object({ paid: z.boolean() }),
  actions: {
    notify: { params: z.object({ channel: z.enum(['email', 'sms']), template: z.string() }) },
    audit: {}
  },
  guards: {
    withinLimit: { params: z.object({ max: z.number() }) },
    retryable: {}
  }
};

export const machine = {
  id: 'checkout',
  initial: 'cart',
  context: { status: 'open', lines: [] },
  states: {
    cart: {
      on: {
        'cart.add': { actions: [{ type: 'audit' }] },
        'payment.method': { actions: [{ type: 'audit' }] },
        checkout: [
          { target: 'paying', guard: { type: 'withinLimit', params: { max: 1000 } } },
          { actions: [{ type: 'notify', params: { channel: 'email', template: 'over-limit' } }] }
        ]
      }
    },
    paying: {
      entry: [{ type: '@xstate.assign', context: { status: 'paying' } }],
      invoke: {
        src: 'chargeCard',
        timeout: '5s',
        onTimeout: { target: 'retrying' },
        onDone: { target: 'paid' },
        onError: [{ target: 'retrying', guard: { type: 'retryable' } }, { target: 'failed' }]
      },
      on: { cancel: { target: 'failed' } }
    },
    retrying: {
      entry: [{ type: '@xstate.assign', context: { status: 'retrying' } }],
      after: { backoff: { actions: [{ type: '@xstate.raise', event: { type: 'retry' } }] } },
      on: { retry: { target: 'paying' }, cancel: { target: 'failed' } }
    },
    paid: {
      type: 'final',
      entry: [
        { type: '@xstate.assign', context: { status: 'paid' } },
        { type: 'notify', params: { channel: 'sms', template: 'paid' } },
        { type: '@xstate.emit', event: { type: 'order.paid', channel: 'sms' } }
      ],
      output: { paid: true }
    },
    failed: {
      type: 'final',
      entry: [
        { type: '@xstate.assign', context: { status: 'failed' } },
        { type: '@xstate.emit', event: { type: 'order.failed' } }
      ],
      output: { paid: false }
    }
  }
} satisfies MachineJSON;
