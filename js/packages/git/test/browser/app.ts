/**
 * The page side of the browser test: a run on upstream XState, journaled in
 * Git in IndexedDB (LightningFS), fed from a server-sent event stream, and
 * replicated to the server's Git over Smart HTTP (cross-origin), all with Web
 * Streams. Bundled with esbuild; the test drives it through `window.app`.
 */
import { GitStore, pull, push, type GitJournal } from '@xmachines/git/browser';
import LightningFS from '@isomorphic-git/lightning-fs';
import http from 'isomorphic-git/http/web';
import { runId, runner, type Event } from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';
import { chunks, ndjson, project, replay, sse, type SseMessage } from '@xmachines/streams';
import { door } from './door.ts';

const fs = new LightningFS('xmachines-test', { wipe: true } as ConstructorParameters<typeof LightningFS>[1]);
const store = new GitStore({ fs, gitdir: '/runs.git' });
const open = runner({ engine: xstateEngine(), machine: door, journals: store });

/** Runs `name` on the inputs `body` streams (SSE), returning its outputs as NDJSON text. */
async function serve(name: string, body: ReadableStream<Uint8Array>) {
  const run = await open(name);
  const outputs = (async () => {
    let text = '';
    for await (const line of chunks(run.out.pipeThrough(ndjson.encode<Event>()))) text += line;
    return text;
  })();
  await body
    .pipeThrough(sse.decode<Event>())
    .pipeThrough(project((m: SseMessage<Event>) => m.data))
    .pipeTo(run.in);
  return { id: run.id, outputs: await outputs };
}

async function batches(id: string) {
  const out: unknown[] = [];
  for await (const b of chunks(replay(await store.journal(id) as GitJournal))) out.push(b.map((e) => e.type));
  return out;
}

declare global {
  interface Window {
    app: Record<string, (server: string) => Promise<unknown>>;
  }
}

window.app = {
  async phase1(server) {
    const { id, outputs } = await serve('front', (await fetch(`${server}/feed?events=open,close,open`)).body!);
    const remote = { http, url: `${server}/runs.git` };
    return {
      id,
      expected: await runId(door, { name: 'front' }),
      outputs,
      batches: await batches(id),
      pushed: await push(store, remote),
      secure: window.isSecureContext,
      asyncIterable: Symbol.asyncIterator in ReadableStream.prototype,
    };
  },
  async phase2(server) {
    const remote = { http, url: `${server}/runs.git` };
    const pulled = await pull(store, remote);
    const { id, outputs } = await serve('front', (await fetch(`${server}/feed?events=open`)).body!);
    return { id, pulled, outputs, batches: await batches(id), pushed: await push(store, remote) };
  },
};
