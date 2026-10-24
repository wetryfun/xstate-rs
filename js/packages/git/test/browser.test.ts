/**
 * `@xmachines/git/browser` with the streams in a real browser (Playwright's
 * Chromium): the page (bundled by esbuild) runs a machine on upstream XState,
 * journals it in Git on IndexedDB (LightningFS), reads its inputs from a
 * server-sent event stream, and pushes and pulls the run over Smart HTTP to a
 * Node server on another origin (CORS), which continues the same run in
 * between. Skipped without Playwright.
 */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import * as fs from 'node:fs';
import { mkdtempSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { GitStore, runRef } from '@xmachines/git';
import { smartHttp } from '@xmachines/git/http';
import { runner, type Event } from '@xmachines/run';
import { xstateEngine } from '@xmachines/run/xstate';
import { sse } from '@xmachines/streams';
import { door } from './browser/door.ts';
import { listen, type Listening } from './node-http.ts';

// Playwright is installed globally here, without types in the workspace.
interface Page {
  on(event: 'pageerror', f: (e: Error) => void): void;
  on(event: 'console', f: (m: { type(): string; text(): string }) => void): void;
  goto(url: string): Promise<unknown>;
  addInitScript(f: () => void): Promise<unknown>;
  waitForFunction(f: () => boolean): Promise<unknown>;
  evaluate<A>(f: (arg: A) => Promise<unknown>, arg: A): Promise<unknown>;
}
type Playwright = { chromium: { launch(): Promise<{ newPage(): Promise<Page>; close(): Promise<void> }> } };
let playwright: Playwright | undefined;
try {
  playwright = createRequire(import.meta.url)('playwright') as Playwright;
} catch {
  // Not installed: the test is skipped.
}
const skip = playwright ? false : 'Playwright is not installed';

const gitdir = mkdtempSync(join(tmpdir(), 'xmachines-browser-'));
let api: Listening;
let page: Listening;

before(async () => {
  if (skip) return;
  await new GitStore({ fs, gitdir }).ready();
  const bundle = await build({
    entryPoints: [fileURLToPath(new URL('./browser/app.ts', import.meta.url))],
    bundle: true,
    write: false,
    format: 'esm',
    platform: 'browser',
    target: 'es2022',
    conditions: ['source'],
    logLevel: 'silent',
  });
  const script = bundle.outputFiles[0]!.text;
  // The page's origin.
  page = await listen(async (request) => {
    const path = new URL(request.url).pathname;
    if (path === '/app.js') return new Response(script, { headers: { 'Content-Type': 'text/javascript' } });
    return new Response('<!doctype html><script type="module" src="/app.js"></script>', {
      headers: { 'Content-Type': 'text/html' },
    });
  });
  // The server, on another origin: Git over Smart HTTP, and an event feed.
  const git = smartHttp({ fs, resolve: (name) => (name === 'runs' ? { gitdir } : undefined), receive: true, cors: (o) => o === page.url });
  api = await listen(async (request) => {
    const url = new URL(request.url);
    if (url.pathname !== '/feed') return git(request);
    const events = (url.searchParams.get('events') ?? '').split(',').map((type) => ({ type }));
    const body = new ReadableStream<Event>({
      start(c) {
        for (const e of events) c.enqueue(e);
        c.close();
      },
    })
      .pipeThrough(sse.encode<Event>({ event: (e) => e.type }))
      .pipeThrough(new TextEncoderStream());
    return new Response(body, {
      headers: { 'Content-Type': 'text/event-stream', 'Access-Control-Allow-Origin': page.url },
    });
  });
});

after(async () => {
  await api?.close();
  await page?.close();
});

const sh = (...args: string[]) => execFileSync('git', ['--git-dir', gitdir, ...args]).toString().trim();
const types = (ndjson: string) => ndjson.split('\n').filter(Boolean).map((l) => (JSON.parse(l) as Event).type);

test('a browser runs, journals in Git on IndexedDB, and replicates over Smart HTTP', { skip, timeout: 60_000 }, async () => {
  const browser = await playwright!.chromium.launch();
  try {
    const tab = await browser.newPage();
    const errors: string[] = [];
    tab.on('pageerror', (e) => errors.push(String(e)));
    tab.on('console', (m) => m.type() === 'error' && errors.push(m.text()));
    // As in Safari, which cannot iterate streams with `for await`.
    await tab.addInitScript(() => {
      delete (ReadableStream.prototype as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator];
    });
    await tab.goto(page.url);
    await tab.waitForFunction(() => 'app' in window);

    type Phase1 = {
      id: string;
      expected: string;
      outputs: string;
      batches: string[][];
      pushed: { pushed: string[]; rejected: unknown[] };
      secure: boolean;
      asyncIterable: boolean;
    };
    const one = (await tab.evaluate((server: string) => (window as unknown as { app: Record<string, (s: string) => Promise<unknown>> }).app.phase1!(server), api.url)) as Phase1;
    assert.deepEqual(errors, []);
    assert.ok(one.secure, 'Web Crypto needs a secure context; 127.0.0.1 is one');
    assert.equal(one.asyncIterable, false, 'streams were read without async iteration');
    assert.equal(one.id, one.expected, 'the run id is the same in the browser');
    assert.deepEqual(types(one.outputs), ['opened', 'closed', 'opened']);
    assert.deepEqual(one.batches, [
      ['xmachines.time', 'open'],
      ['xmachines.time', 'close'],
      ['xmachines.time', 'open'],
    ]);
    assert.deepEqual(one.pushed, { pushed: ['machines/door', `runs/${one.id}`], rejected: [] });

    // The server has the run, as plain Git: machine, fork, batches, snapshots.
    sh('fsck', '--strict');
    const log = sh('log', '--format=%s', runRef(one.id)).split('\n');
    assert.deepEqual(log.slice(-2), ['run front', 'machine door']);
    assert.equal(log.filter((s) => s.startsWith('events')).length, 3);

    // The server continues the run from the history the browser pushed.
    const run = await runner({ engine: xstateEngine(), machine: door, journals: new GitStore({ fs, gitdir }) })('front');
    const outputs: string[] = [];
    const reading = (async () => {
      for await (const e of run.out) outputs.push(e.type);
    })();
    const w = run.in.getWriter();
    await w.write({ type: 'close' });
    w.releaseLock();
    await run.close();
    await reading;
    assert.deepEqual(outputs, ['closed']);

    // The browser pulls that, restores the run in the state the server left
    // it, goes on, and pushes again (a fast-forward).
    type Phase2 = {
      id: string;
      pulled: { updated: string[]; diverged: string[] };
      outputs: string;
      batches: string[][];
      pushed: { pushed: string[]; rejected: unknown[] };
    };
    const two = (await tab.evaluate((server: string) => (window as unknown as { app: Record<string, (s: string) => Promise<unknown>> }).app.phase2!(server), api.url)) as Phase2;
    assert.deepEqual(errors, []);
    assert.deepEqual(two.pulled, { updated: [`runs/${one.id}`], diverged: [] });
    assert.deepEqual(types(two.outputs), ['opened'], 'the run was closed by the server, so it opens again');
    assert.equal(two.batches.length, 5);
    assert.deepEqual(two.pushed.rejected, []);
    assert.equal(sh('log', '--format=%s', '-1', runRef(one.id)), 'snapshot 5');
    sh('fsck', '--strict');
  } finally {
    await browser.close();
  }
});
