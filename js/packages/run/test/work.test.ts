import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  createRun,
  host,
  MemoryJournal,
  rejected,
  resolved,
  time,
  type Engine,
  type Event,
  type Input,
  type Run,
  type RunError,
  type RunSnapshot,
  type WorkContext,
} from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';
import { engines } from './engines.ts';

const emit = (type: string) => [{ type: '@xstate.emit', event: { type } }];
/** Loads a user through an invoked async actor; `cancel` leaves the loading state. */
const machine = {
  id: 'profile',
  initial: 'idle',
  states: {
    idle: { on: { load: { target: 'loading' } } },
    loading: {
      invoke: {
        src: 'fetchUser',
        id: 'fetch',
        input: { id: 7 },
        onDone: { target: 'loaded', actions: emit('loaded') },
        onError: { target: 'failed', actions: emit('failed') },
      },
      on: { cancel: { target: 'idle', actions: emit('cancelled') } },
    },
    loaded: { on: { load: { target: 'loading' } } },
    failed: { on: { load: { target: 'loading' } } },
  },
};

/** A run whose ports are read as it goes. */
function open(run: Run) {
  const out: string[] = [];
  const err: RunError[] = [];
  let snapshot: RunSnapshot | undefined;
  const reading = Promise.all([
    (async () => {
      for await (const e of run.out) out.push(e.type);
    })(),
    (async () => {
      for await (const e of run.err) err.push(e);
    })(),
    (async () => {
      for await (const s of run.snapshot) snapshot = s;
    })(),
  ]);
  const writer = run.in.getWriter();
  return {
    out,
    err,
    snapshot: () => snapshot!,
    write: async (input: Input) => {
      await writer.write(input);
      await new Promise((r) => setTimeout(r, 0));
    },
    close: async () => {
      await writer.close();
      await reading;
    },
  };
}

test('every engine completes async work with effect results', async () => {
  for (const [name, engine] of Object.entries(await engines())) {
    const r = open(createRun({ id: 'r', machine: await engine.load(machine) }));
    await r.write([time(1000), { type: 'load' }]);
    const [work] = r.snapshot().work!;
    assert.deepEqual({ ...work, id: undefined }, { id: undefined, actor: 'r/fetch', src: 'fetchUser', input: { id: 7 } }, name);
    await r.write(resolved(work!.id, { name: 'Ada' }));
    assert.equal(r.snapshot().value, 'loaded', name);
    assert.equal(r.snapshot().work, undefined, name);
    // A result for work that is no longer pending is rejected.
    await r.write(resolved(work!.id, { name: 'Ada' }));
    assert.equal(r.err.length, 1, name);
    await r.write({ type: 'load' });
    await r.write(rejected(r.snapshot().work![0]!.id, 'offline'));
    assert.equal(r.snapshot().value, 'failed', name);
    await r.close();
    assert.deepEqual(r.out, ['loaded', 'failed'], name);
  }
});

test('a run waiting for work moves between engines and completes there', async () => {
  const all = Object.entries(await engines());
  for (const [from, a] of all) {
    for (const [to, b] of all) {
      const first = open(createRun({ id: 'r', machine: await a.load(machine) }));
      await first.write([time(1000), { type: 'load' }]);
      const moved = JSON.parse(JSON.stringify(first.snapshot())) as RunSnapshot;
      await first.close();
      const second = open(createRun({ id: 'r', machine: await b.load(machine), from: moved }));
      await second.write(resolved(moved.work![0]!.id, { name: 'Ada' }));
      await second.close();
      assert.deepEqual(second.out, ['loaded'], `${from} -> ${to}`);
      assert.equal(second.snapshot().value, 'loaded', `${from} -> ${to}`);
    }
  }
});

/** Work whose calls the test controls. */
function controlled() {
  const calls: { input: unknown; context: WorkContext; resolve(v: unknown): void; reject(e: unknown): void }[] = [];
  const fetchUser = (input: unknown, context: WorkContext) =>
    new Promise((resolve, reject) => calls.push({ input, context, resolve, reject }));
  return { calls, work: { fetchUser } };
}

async function until(check: () => boolean) {
  const deadline = Date.now() + 3000;
  while (!check()) {
    assert.ok(Date.now() < deadline, 'timed out');
    await new Promise((r) => setTimeout(r, 2));
  }
}

for (const name of ['xstate', 'native', 'wasi']) {
  test(`a host runs registered work and journals its results (${name})`, async (t) => {
    const engine: Engine | undefined = (await engines())[name];
    if (!engine) return t.skip(`no ${name} engine`);
    const m = await engine.load(machine);
    const journal = new MemoryJournal();
    const { calls, work } = controlled();
    const run = await host({ id: 'h', machine: m, journal, clock: () => 5000, work });
    const r = open(run);
    await r.write({ type: 'load' });
    await until(() => calls.length === 1);
    assert.deepEqual(calls[0]!.input, { id: 7 });
    assert.equal(calls[0]!.context.actor, 'h/fetch');
    calls[0]!.resolve({ name: 'Ada' });
    await until(() => r.out.includes('loaded'));

    // A failure rejects the actor.
    await r.write({ type: 'load' });
    await until(() => calls.length === 2);
    calls[1]!.reject(new Error('offline'));
    await until(() => r.out.includes('failed'));

    // Leaving the invoking state aborts the work; its late result is dropped.
    await r.write({ type: 'load' });
    await until(() => calls.length === 3);
    await r.write({ type: 'cancel' });
    await until(() => calls[2]!.context.signal.aborted);
    calls[2]!.resolve({ name: 'late' });
    await new Promise((resolve) => setTimeout(resolve, 20));

    // Work pending when the run closes runs again after a restore.
    await r.write({ type: 'load' });
    await until(() => calls.length === 4);
    await run.close();
    await r.close().catch(() => {});
    assert.ok(calls[3]!.context.signal.aborted, 'closing aborts the work');
    assert.deepEqual(r.out, ['loaded', 'failed', 'cancelled']);
    assert.deepEqual(r.err, []);
    const results = journal.batches.flat().filter((e) => e.type === 'xstate.system.effect.result');
    assert.deepEqual(
      results.map((e) => (e.event as Event).type),
      ['xstate.async.resolve', 'xstate.async.reject'],
    );
    assert.deepEqual((results[1]!.event as { data: unknown }).data, { name: 'Error', message: 'offline' });

    const again = controlled();
    const restored = open(await host({ id: 'h', machine: m, journal, clock: () => 6000, work: again.work }));
    await until(() => again.calls.length === 1);
    again.calls[0]!.resolve({ name: 'Grace' });
    await until(() => restored.out.includes('loaded'));
    await restored.close();
    assert.deepEqual(restored.out, ['loaded']);
  });
}

test('a hosted run replays its journaled work results without running the work', async () => {
  const journal = new MemoryJournal();
  const m = await xstateEngine().load(machine);
  const first = await host({ id: 'h', machine: m, journal, clock: () => 1, work: { fetchUser: async () => ({ name: 'Ada' }) } });
  const r = open(first);
  await r.write({ type: 'load' });
  await until(() => r.out.includes('loaded'));
  await first.close();
  await r.close().catch(() => {});
  let ran = 0;
  const replayed = { batches: structuredClone(journal.batches), snapshots: [] } as unknown as MemoryJournal;
  const second = open(
    await host({ id: 'h', machine: m, journal: Object.assign(new MemoryJournal(), replayed), clock: () => 2, work: { fetchUser: () => ran++ } }),
  );
  await second.write({ type: 'load' });
  await until(() => second.out.includes('loaded'));
  await second.close();
  assert.equal(ran, 1, 'only the new load ran the work, not the replayed one');
});

test('work a journal snapshot has pending but its replay completes does not run again', async () => {
  const m = await xstateEngine().load(machine);
  // A journal as a crash leaves it: a snapshot while the work was pending,
  // then the batch with its result, and no snapshot after that.
  const r = open(createRun({ id: 'h', machine: m }));
  const load = [time(1), { type: 'load' }];
  await r.write(load);
  const pending = r.snapshot();
  const result = [time(2), resolved(pending.work![0]!.id, {})];
  await r.close();
  const crashed = Object.assign(new MemoryJournal(), { batches: [load, result], snapshots: [pending] });
  let ran = 0;
  const second = open(await host({ id: 'h', machine: m, journal: crashed, clock: () => 3, work: { fetchUser: () => ran++ } }));
  await new Promise((resolve) => setTimeout(resolve, 30));
  await second.close();
  assert.equal(second.snapshot().value, 'loaded');
  assert.deepEqual(second.out, [], 'replayed outputs are not emitted again');
  assert.equal(ran, 0);
});
