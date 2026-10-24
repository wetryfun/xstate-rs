/** The machine the browser test runs, in the page and on the server. */
const emit = (type: string) => [{ type: '@xstate.emit', event: { type } }];

export const door = {
  id: 'door',
  initial: 'closed',
  context: { opened: 0 },
  states: {
    closed: { on: { open: { target: 'opened', context: { last: 'open' }, actions: emit('opened') } } },
    opened: {
      on: { close: { target: 'closed', context: { last: 'close' }, actions: emit('closed') } },
      after: { 60000: { target: 'closed', actions: emit('auto-closed') } },
    },
  },
};
