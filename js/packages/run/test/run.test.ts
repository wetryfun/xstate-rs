import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { test } from 'node:test';
import {
  createRun,
  forkObjects,
  host,
  machineObjects,
  MemoryJournal,
  MemoryJournals,
  objectId,
  runId,
  runner,
  time,
  type Event,
  type Input,
  type Run,
  type RunError,
  type RunSnapshot,
} from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';
import { engines } from './engines.ts';

const emit = (type: string) => [{ type: '@xstate.emit', event: { type } }];
export const machine = {
  id: 'job',
  initial: 'idle',
  context: { pings: 0 },
  states: {
    idle: { on: { start: { target: 'running', actions: emit('started') } } },
    running: {
      after: { 100: { target: 'done', actions: emit('finished') } },
      on: {
        ping: { context: { pinged: true }, actions: emit('pong') },
        loop: { target: 'a' },
      },
    },
    a: { always: { target: 'b' } },
    b: { always: { target: 'a' } },
    done: { on: { start: { target: 'running', actions: emit('restarted') } } },
  },
};

/** Writes inputs and collects every port. */
async function drive(run: Run, inputs: readonly Input[]) {
  const out: Event[] = [];
  const err: RunError[] = [];
  let snapshot: RunSnapshot | undefined;
  const reading = Promise.all([
    (async () => {
      for await (const e of run.out) out.push(e);
    })(),
    (async () => {
      for await (const e of run.err) err.push(e);
    })(),
    (async () => {
      for await (const s of run.snapshot) snapshot = s;
    })(),
  ]);
  const writer = run.in.getWriter();
  for (const input of inputs) await writer.write(input);
  await writer.close();
  await reading;
  return { out, err, snapshot: snapshot! };
}

const script: Input[] = [
  [time(1000), { type: 'start' }],
  { type: 'ping' },
  [{ type: 'ping' }, 'not an event' as never],
  time(1050),
  time(1100),
  { type: 'start' },
  [time(1150), { type: 'loop' }],
  { type: 'ping' },
];

test('ids are Git object ids', async () => {
  const git = (args: string[], input: string) => execFileSync('git', args, { input }).toString().trim();
  assert.equal(await objectId('blob', new TextEncoder().encode('hello')), git(['hash-object', '--stdin'], 'hello'));
  const m = await machineObjects(machine);
  const text = new TextDecoder().decode(m.commit.content);
  assert.equal(m.id, git(['hash-object', '-t', 'commit', '--stdin', '--literally'], text));
  assert.match(text, /^tree [0-9a-f]{40}\nauthor xmachines <xmachines@localhost> 0 \+0000\n/);
  const run = await forkObjects(m, { name: 'order-1' });
  assert.equal(run.id, await runId(machine, { name: 'order-1' }));
  assert.notEqual(run.id, await runId(machine, { name: 'order-2' }));
  assert.match(new TextDecoder().decode(run.commit.content), new RegExp(`\nparent ${m.id}\n`));
});

test('a run is deterministic, applies batches atomically, and reports errors', async () => {
  const m = await xstateEngine().load(machine);
  const a = await drive(createRun({ id: 'r', machine: m }), script);
  const b = await drive(createRun({ id: 'r', machine: m }), script);
  assert.deepEqual(a, b);
  assert.deepEqual(
    a.out.map((e) => e.type),
    ['started', 'pong', 'finished', 'restarted', 'pong'],
  );
  // The invalid batch was rejected as a whole (its ping did not apply), as
  // was the batch whose event looped forever.
  assert.deepEqual(
    a.err.map((e) => e.seq),
    [3, 7],
  );
  assert.equal(a.snapshot.seq, 8);
  assert.equal(a.snapshot.value, 'running');
  assert.equal(a.snapshot.time, 1100, 'the rejected batch took its time event with it');
  assert.equal(a.snapshot.deadline, 1200);
});

test('every engine runs the script the same way', async () => {
  const all = await engines();
  const results: Record<string, unknown> = {};
  for (const [name, engine] of Object.entries(all)) {
    const { out, err, snapshot } = await drive(createRun({ id: 'r', machine: await engine.load(machine) }), script);
    results[name] = {
      out,
      errors: err.map((e) => e.seq),
      value: snapshot.value,
      context: snapshot.context,
      status: snapshot.status,
      deadline: snapshot.deadline,
    };
  }
  for (const name of Object.keys(all)) assert.deepEqual(results[name], results.xstate, name);
});

test("a run's state moves between engines", async () => {
  const all = Object.entries(await engines());
  const first = script.slice(0, 2);
  const rest: Input[] = [{ type: 'ping' }, time(1100), { type: 'start' }, { type: 'ping' }, time(1250)];
  const expected = await drive(createRun({ id: 'r', machine: await all[0]![1].load(machine) }), [...first, ...rest]);
  for (const [from, a] of all) {
    for (const [to, b] of all) {
      const half = await drive(createRun({ id: 'r', machine: await a.load(machine) }), first);
      const moved = JSON.parse(JSON.stringify(half.snapshot)) as RunSnapshot;
      const end = await drive(createRun({ id: 'r', machine: await b.load(machine), from: moved }), rest);
      assert.deepEqual([...half.out, ...end.out], expected.out, `${from} -> ${to}`);
      assert.equal(end.snapshot.value, expected.snapshot.value, `${from} -> ${to}`);
      assert.deepEqual(end.snapshot.context, expected.snapshot.context, `${from} -> ${to}`);
    }
  }
});

test('a hosted run stamps time, fires timers, journals, and restores', async () => {
  let now = 5000;
  const journal = new MemoryJournal();
  const m = await xstateEngine().load(machine);
  const hosted = await host({ id: 'h', machine: m, journal, clock: () => now });
  const out: string[] = [];
  const reading = (async () => {
    for await (const e of hosted.out) out.push(e.type);
  })();
  const writer = hosted.in.getWriter();
  await writer.write({ type: 'start' });
  now = 5200;
  // The timer was due at 5100: the host sends the time event itself.
  const deadline = Date.now() + 3000;
  while (!out.includes('finished')) {
    assert.ok(Date.now() < deadline, 'the timer fired');
    await new Promise((r) => setTimeout(r, 10));
  }
  writer.releaseLock();
  await hosted.close();
  await reading;
  assert.deepEqual(out, ['started', 'finished']);
  assert.deepEqual(journal.batches[0], [time(5000), { type: 'start' }]);
  assert.equal(journal.batches[1]![0]!.type, 'xmachines.time');
  assert.equal(journal.snapshots.at(-1)!.value, 'done');

  // Restored from the journal: from its snapshot, or by replaying every batch.
  const copy = (snapshots: boolean) =>
    Object.assign(new MemoryJournal(), {
      batches: structuredClone(journal.batches),
      snapshots: snapshots ? structuredClone(journal.snapshots) : [],
    });
  for (const j of [copy(true), copy(false)]) {
    const again = await host({ id: 'h', machine: m, journal: j, clock: () => 6000 });
    const result = await drive(again, [{ type: 'start' }]);
    assert.deepEqual(
      result.out.map((e) => e.type),
      ['restarted'],
      'replayed batches emit nothing again',
    );
    assert.equal(result.snapshot.value, 'running');
  }
});

test('a runner opens runs by name, with stable ids, from their journals', async () => {
  const journals = new MemoryJournals();
  const open = runner({ engine: xstateEngine(), machine, journals, clock: () => 1 });
  const first = await open('order-7');
  assert.equal(first.id, await runId(machine, { name: 'order-7' }));
  await drive(first, [{ type: 'start' }]);
  const second = await open('order-7');
  const result = await drive(second, [{ type: 'ping' }]);
  assert.deepEqual(
    result.out.map((e) => e.type),
    ['pong'],
  );
  assert.equal(journals.journals.get(first.id)!.batches.length, 2);
});
