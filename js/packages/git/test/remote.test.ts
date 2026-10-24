import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import http from 'isomorphic-git/http/node';
import { GitStore, pull, push } from '@xmachines/git';
import { smartHttp } from '@xmachines/git/http';
import { runner, type HostedRun } from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';
import { listen } from './node-http.ts';

const emit = (type: string) => [{ type: '@xstate.emit', event: { type } }];
const door = {
  id: 'door',
  initial: 'closed',
  states: {
    closed: { on: { open: { target: 'opened', actions: emit('opened') } } },
    opened: { on: { close: { target: 'closed', actions: emit('closed') } } },
  },
};

async function send(run: HostedRun, ...types: string[]) {
  const out: string[] = [];
  const reading = (async () => {
    for await (const e of run.out) out.push(e.type);
  })();
  const w = run.in.getWriter();
  for (const type of types) await w.write({ type });
  w.releaseLock();
  await run.close();
  await reading;
  return out;
}

test('runs replicate between repositories with push and pull, append-only', async () => {
  const dir = (n: string) => mkdtempSync(join(tmpdir(), `xmachines-${n}-`));
  const server = new GitStore({ fs, gitdir: dir('server') });
  await server.ready();
  const api = await listen(smartHttp({ fs, resolve: () => ({ gitdir: server.gitdir }), receive: true }));
  const remote = { http, url: `${api.url}/runs.git` };
  try {
    const a = new GitStore({ fs, gitdir: dir('a') });
    const b = new GitStore({ fs, gitdir: dir('b') });
    const openA = runner({ engine: xstateEngine(), machine: door, journals: a });
    const openB = runner({ engine: xstateEngine(), machine: door, journals: b });
    assert.deepEqual(await pull(b, remote), { updated: [], diverged: [] }, 'nothing to pull yet');

    assert.deepEqual(await send(await openA('front'), 'open'), ['opened']);
    const pushed = await push(a, remote);
    assert.equal(pushed.pushed.length, 2);

    // b takes the run and goes on from where a left it.
    const pulled = await pull(b, remote);
    assert.equal(pulled.updated.length, 2);
    assert.deepEqual(await send(await openB('front'), 'close'), ['closed']);
    assert.deepEqual((await push(b, remote)).rejected, []);

    // a goes on without pulling: its history and the server's diverge, and
    // the server refuses the rewrite.
    assert.deepEqual(await send(await openA('front'), 'close'), ['closed'], 'its own copy is still open');
    const refused = await push(a, remote);
    assert.equal(refused.rejected.length, 1);
    assert.match(refused.rejected[0]!.reason, /fast-forward|append-only|fetch first/);
    assert.equal((await pull(a, remote)).diverged.length, 1, 'and a pull leaves both as they are');
  } finally {
    await api.close();
  }
});
