import assert from 'node:assert/strict';
import { test } from 'node:test';
import { chunks, durable, durableWritable, fanIn, filter, json, MemoryLog, ndjson, project, replay, sse, through } from '@xmachines/streams';

const from = <T>(chunks: T[]) =>
  new ReadableStream<T>({
    start(c) {
      for (const x of chunks) c.enqueue(x);
      c.close();
    },
  });
const all = async <T>(s: ReadableStream<T>) => {
  const out: T[] = [];
  for await (const x of s) out.push(x);
  return out;
};

test('NDJSON frames records across chunk boundaries', async () => {
  const records = [{ a: 1 }, { b: 'x\ny' }, [1, 2]];
  const text = (await all(from(records).pipeThrough(ndjson.encode()))).join('');
  assert.equal(text, '{"a":1}\n{"b":"x\\ny"}\n[1,2]\n');
  const bytes = new TextEncoder().encode(`${text}\r\n{"tail":true}`);
  const chunks = [bytes.slice(0, 3), bytes.slice(3, 11), bytes.slice(11)];
  assert.deepEqual(await all(from(chunks).pipeThrough(ndjson.decode())), [...records, { tail: true }]);
});

test('SSE frames records with event names and ids', async () => {
  const text = (
    await all(from([{ type: 'a' }, { type: 'b' }]).pipeThrough(sse.encode<{ type: string }>({ event: (r) => r.type, id: (r) => r.type + '1' })))
  ).join('');
  assert.equal(text, 'event: a\nid: a1\ndata: {"type":"a"}\n\nevent: b\nid: b1\ndata: {"type":"b"}\n\n');
  const back = await all(from([`: comment\n${text.slice(0, 20)}`, text.slice(20)]).pipeThrough(sse.decode()));
  assert.deepEqual(back, [
    { data: { type: 'a' }, event: 'a', id: 'a1' },
    { data: { type: 'b' }, event: 'b', id: 'b1' },
  ]);
});

test('JSON reads one value', async () => {
  assert.deepEqual(await json.read(from(['{"a":', '[1,2]}'])), { a: [1, 2] });
  assert.deepEqual(await all(from([{ x: 1 }]).pipeThrough(json.encode())), ['{"x":1}']);
});

test('durability is a transform; replay reads it back', async () => {
  const log = new MemoryLog<number>();
  const passed = await all(from([1, 2, 3]).pipeThrough(durable(log)).pipeThrough(project((n: number) => n * 10)));
  assert.deepEqual(passed, [10, 20, 30]);
  assert.deepEqual(await all(replay(log)), [1, 2, 3]);
  assert.deepEqual(await all(replay(log, { after: '0' }).pipeThrough(filter((n: number) => n > 2))), [3]);
});

test('write-through stages acknowledge end to end; senders fan in in order', async () => {
  const seen: string[] = [];
  const sink = new WritableStream<string>({
    async write(s) {
      await new Promise((r) => setTimeout(r, 2));
      seen.push(s);
    },
  });
  const log = new MemoryLog<string>();
  const w = through(durableWritable(sink, log), (n: number) => `#${n}`).getWriter();
  await w.write(1);
  assert.deepEqual(seen, ['#1'], 'acknowledged after the sink took it');
  await w.close();
  assert.deepEqual(log.entries.map((e) => e.value), ['#1']);

  const merged: number[] = [];
  const fan = fanIn(new WritableStream<number>({ write: (n) => void merged.push(n) }));
  const [a, b] = [fan.writable().getWriter(), fan.writable().getWriter()];
  await Promise.all([a.write(1), b.write(2), a.write(3), b.write(4)]);
  await fan.close();
  assert.deepEqual(merged, [1, 2, 3, 4]);
});

test('chunks reads a stream without async iteration and cancels it when left early', async () => {
  let cancelled = false;
  const stream = new ReadableStream<number>({
    start(c) {
      for (const n of [1, 2, 3]) c.enqueue(n);
    },
    cancel() {
      cancelled = true;
    },
  });
  const seen: number[] = [];
  for await (const n of chunks(stream)) {
    seen.push(n);
    if (n === 2) break;
  }
  assert.deepEqual(seen, [1, 2]);
  assert.ok(cancelled);
  assert.ok(!stream.locked);
  const all: number[] = [];
  for await (const n of chunks(from([4, 5]))) all.push(n);
  assert.deepEqual(all, [4, 5]);
});
