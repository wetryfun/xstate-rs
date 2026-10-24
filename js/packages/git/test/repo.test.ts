import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import * as fs from 'node:fs';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { GitStore, runRef } from '@xmachines/git';
import { host, runId, runner, time, type Event } from '@xmachines/run';
import { replay } from '@xmachines/streams';
import { xstateEngine } from '@xmachines/run/xstate';

const emit = (type: string) => [{ type: '@xstate.emit', event: { type } }];
const machine = {
  id: 'door',
  initial: 'closed',
  states: {
    closed: { on: { open: { target: 'opened', actions: emit('opened') } } },
    opened: { on: { close: { target: 'closed', actions: emit('closed') } } },
  },
};
const git = (gitdir: string, ...args: string[]) => execFileSync('git', ['--git-dir', gitdir, ...args]).toString().trim();

async function collect(stream: ReadableStream<Event>) {
  const out: string[] = [];
  for await (const e of stream) out.push(e.type);
  return out;
}

test('machines are orphan branches, runs are forks, batches and snapshots are commits', async () => {
  const gitdir = mkdtempSync(join(tmpdir(), 'xmachines-git-'));
  const store = new GitStore({ fs, gitdir });
  const fork = await store.fork(machine, { name: 'front' });
  assert.equal(fork.id, await runId(machine, { name: 'front' }), 'the same id with and without Git');
  const journal = await store.journal(fork.id);
  const run = await host({ id: fork.id, machine: await xstateEngine().load(machine), journal, clock: () => 60_000 });
  const outputs = collect(run.out);
  const w = run.in.getWriter();
  await w.write({ type: 'open' });
  await w.write([{ type: 'close' }, { type: 'open' }]);
  w.releaseLock();
  await run.close();
  assert.deepEqual(await outputs, ['opened', 'closed', 'opened']);

  // Plain git reads it: the machine's orphan line, the run's linear history.
  const machineTip = git(gitdir, 'rev-parse', 'refs/heads/machines/door');
  assert.equal(git(gitdir, 'rev-list', '--count', 'refs/heads/machines/door'), '1');
  assert.equal(git(gitdir, 'rev-parse', `${fork.id}^`), machineTip);
  const log = git(gitdir, 'log', '--format=%s', runRef(fork.id)).split('\n');
  assert.ok(log.includes('events 1') && log.includes('events 2') && log.at(-2) === 'run front' && log.at(-1) === 'machine door', log.join(','));
  assert.equal(git(gitdir, 'show', `${runRef(fork.id)}:machine.json`), JSON.stringify(machine, null, 2));
  const events = git(gitdir, 'log', '-p', '--format=%s', runRef(fork.id), '--', 'events.ndjson');
  assert.match(events, /\+\{"type":"xmachines\.time","now":60000\}\n\+\{"type":"close"\}\n\+\{"type":"open"\}/);
  assert.match(git(gitdir, 'show', `${runRef(fork.id)}:snapshot.json`), /"value": "opened"/);
  git(gitdir, 'fsck', '--strict');

  // The run's batches are a log to replay.
  const batches: unknown[] = [];
  for await (const b of replay(journal)) batches.push(b);
  assert.deepEqual(batches, [
    [time(60_000), { type: 'open' }],
    [time(60_000), { type: 'close' }, { type: 'open' }],
  ]);
  // Another definition under the same machine name is refused.
  await assert.rejects(store.machine({ ...machine, initial: 'opened' }), /holds another definition/);
});

test('a runner on Git restores runs from their history', async () => {
  const gitdir = mkdtempSync(join(tmpdir(), 'xmachines-git-'));
  const store = new GitStore({ fs, gitdir });
  const open = runner({ engine: xstateEngine(), machine, journals: store, clock: () => 1000, snapshotEvery: 2 });
  let run = await open('back');
  let w = run.in.getWriter();
  await w.write({ type: 'open' });
  await w.write({ type: 'close' });
  await w.write({ type: 'open' });
  w.releaseLock();
  const first = collect(run.out);
  await run.close();
  assert.deepEqual(await first, ['opened', 'closed', 'opened']);
  // A second store over the same repository (another process) continues the run.
  run = await runner({ engine: xstateEngine(), machine, journals: new GitStore({ fs, gitdir }), clock: () => 2000 })('back');
  const second = collect(run.out);
  w = run.in.getWriter();
  await w.write({ type: 'close' });
  w.releaseLock();
  await run.close();
  assert.deepEqual(await second, ['closed']);
  assert.deepEqual(await store.runs(), [run.id]);
  // A writer that lost its place cannot rewrite history.
  const stale = await store.journal(run.id);
  await stale.append([time(3000), { type: 'close' }]);
  const other = await new GitStore({ fs, gitdir }).journal(run.id);
  await other.append([time(3000), { type: 'open' }]);
  await assert.rejects(stale.append([time(3000), { type: 'close' }]), /append-only/);
});
