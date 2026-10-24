import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import fs from 'node:fs';
import { mkdtemp, readFile, rm, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, describe, test } from 'node:test';
import * as igit from 'isomorphic-git';
import igitHttp from 'isomorphic-git/http/node';
import { smartHttp } from '@xmachines/git/http';
import { listen, type Listening } from './node-http.ts';

let root: string;
let env: NodeJS.ProcessEnv;
const repos = new Map<string, string>();
const seen: Array<{ path: string; encoding: string | null; protocol: string | null; packObjects?: number }> = [];
let rw: Listening;
let ro: Listening;

/** Runs the git CLI; resolves with stdout, rejects with stderr. */
function git(cwd: string, ...args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile('git', args, { cwd, env, maxBuffer: 64 << 20 }, (err, stdout, stderr) => {
      if (err) reject(Object.assign(new Error(`git ${args.join(' ')}: ${stderr}`), { stderr }));
      else resolve(stdout.trim());
    });
  });
}

/** Runs a git command that must fail; resolves with its stderr. */
async function gitFails(cwd: string, ...args: string[]): Promise<string> {
  try {
    await git(cwd, ...args);
  } catch (err) {
    return (err as { stderr: string }).stderr;
  }
  assert.fail(`git ${args.join(' ')} succeeded`);
}

async function commitFile(dir: string, path: string, content: string, message = `write ${path}`): Promise<string> {
  const file = join(dir, path);
  await mkdir(join(file, '..'), { recursive: true });
  await writeFile(file, content);
  await git(dir, 'add', '-A');
  await git(dir, 'commit', '-q', '-m', message);
  return git(dir, 'rev-parse', 'HEAD');
}

/** Creates a bare server repository `<name>` holding every ref of `work`. */
async function serverRepo(name: string, work?: string): Promise<string> {
  const gitdir = join(root, 'server', `${name}.git`);
  await mkdir(gitdir, { recursive: true });
  await git(gitdir, 'init', '-q', '--bare', '-b', 'main');
  if (work) await git(work, 'push', '-q', `file://${gitdir}`, 'refs/*:refs/*');
  repos.set(name, gitdir);
  return gitdir;
}

async function workRepo(name: string): Promise<string> {
  const dir = join(root, 'work', name);
  await mkdir(dir, { recursive: true });
  await git(dir, 'init', '-q', '-b', 'main');
  return dir;
}

function packObjectCount(body: Uint8Array): number | undefined {
  const text = Buffer.from(body);
  const at = text.indexOf('PACK');
  return at < 0 ? undefined : text.readUInt32BE(at + 8);
}

before(async () => {
  root = await mkdtemp(join(tmpdir(), 'xmachines-git-http-'));
  const home = join(root, 'home');
  await mkdir(home);
  env = {
    PATH: process.env.PATH,
    HOME: home,
    XDG_CONFIG_HOME: home,
    GIT_CONFIG_NOSYSTEM: '1',
    GIT_TERMINAL_PROMPT: '0',
    GIT_AUTHOR_NAME: 'Test',
    GIT_AUTHOR_EMAIL: 'test@example.com',
    GIT_COMMITTER_NAME: 'Test',
    GIT_COMMITTER_EMAIL: 'test@example.com',
    LANG: 'C',
    LC_ALL: 'C',
  };
  const resolve = (name: string) => {
    const gitdir = repos.get(name);
    return gitdir ? { gitdir } : undefined;
  };
  const write = smartHttp({ fs, resolve, receive: true });
  rw = await listen(async (request) => {
    const url = new URL(request.url);
    const entry: (typeof seen)[number] = {
      path: url.pathname,
      encoding: request.headers.get('content-encoding'),
      protocol: request.headers.get('git-protocol'),
    };
    seen.push(entry);
    const response = await write(request);
    if (url.pathname.endsWith('/git-upload-pack')) {
      const body = new Uint8Array(await response.clone().arrayBuffer());
      entry.packObjects = packObjectCount(body);
    }
    return response;
  });
  ro = await listen(smartHttp({ fs, resolve, prefix: '/git/' }));
});

after(async () => {
  await rw?.close();
  await ro?.close();
  await rm(root, { recursive: true, force: true });
});

describe('git CLI', () => {
  let seed: string;
  let gitdir: string;
  const tips: Record<string, string> = {};

  before(async () => {
    seed = await workRepo('seed');
    await commitFile(seed, 'README.md', '# seed\n');
    await commitFile(seed, 'src/a.txt', 'a1\n');
    tips.main = await commitFile(seed, 'src/b.txt', 'b1\n');
    await git(seed, 'tag', '-a', 'v1', '-m', 'version 1');
    await git(seed, 'checkout', '-q', '-b', 'feature');
    await commitFile(seed, 'feature.txt', 'f1\n');
    tips.feature = await commitFile(seed, 'src/a.txt', 'a2\n');
    await git(seed, 'checkout', '-q', '-b', 'runs/x', 'main');
    tips['runs/x'] = await commitFile(seed, 'runs/x/events/0001.json', '{"type":"start"}\n');
    await git(seed, 'checkout', '-q', '-b', 'machines/m', 'main');
    tips['machines/m'] = await commitFile(seed, 'machine.json', '{}\n');
    await git(seed, 'checkout', '-q', 'main');
    gitdir = await serverRepo('flow', seed);
  });

  test('clone over HTTP serves every ref and the same objects', async () => {
    const dir = join(root, 'clone1');
    const before = seen.length;
    await git(root, 'clone', '-q', `${rw.url}/flow.git`, dir);
    // git 2.43 asks for protocol v2 and falls back to the v0 answer.
    assert.ok(seen.slice(before).some((s) => s.protocol === 'version=2'));
    assert.equal(await readFile(join(dir, 'src/b.txt'), 'utf8'), 'b1\n');
    assert.equal(await git(dir, 'rev-parse', 'HEAD'), tips.main);
    for (const [branch, oid] of Object.entries(tips)) {
      assert.equal(await git(dir, 'rev-parse', `origin/${branch}`), oid);
    }
    assert.equal(await git(dir, 'rev-parse', 'v1^{commit}'), tips.main);
    await git(dir, 'fsck', '--strict');

    const remote = await git(root, 'ls-remote', `${rw.url}/flow.git`);
    const local = await git(gitdir, 'show-ref', '--head', '-d');
    const normalize = (s: string) => s.split('\n').map((l) => l.split(/\s+/).join(' ')).sort();
    assert.deepEqual(normalize(remote), normalize(local));

    // A mirror clone holds exactly the server's refs.
    const mirror = join(root, 'mirror1.git');
    await git(root, 'clone', '-q', '--mirror', `${rw.url}/flow.git`, mirror);
    assert.equal(await git(mirror, 'for-each-ref'), await git(gitdir, 'for-each-ref'));
    assert.equal(await git(mirror, 'show', 'runs/x:runs/x/events/0001.json'), '{"type":"start"}');
  });

  test('fetch is incremental (haves) after new commits', async () => {
    const dir = join(root, 'clone2');
    await git(root, 'clone', '-q', `${rw.url}/flow.git`, dir);
    await git(seed, 'checkout', '-q', 'runs/x');
    const next = await commitFile(seed, 'runs/x/events/0002.json', '{"type":"next"}\n');
    await git(seed, 'checkout', '-q', 'main');
    await git(seed, 'push', '-q', `file://${gitdir}`, 'runs/x');

    const before = seen.length;
    await git(dir, 'fetch', '-q', 'origin');
    assert.equal(await git(dir, 'rev-parse', 'origin/runs/x'), next);
    assert.equal(await git(dir, 'show', 'origin/runs/x:runs/x/events/0002.json'), '{"type":"next"}');
    const posts = seen.slice(before).filter((s) => s.path.endsWith('/git-upload-pack'));
    const packs = posts.map((s) => s.packObjects).filter((n) => n !== undefined);
    // One commit, its root tree, runs/, runs/x/, runs/x/events/ and the new blob.
    assert.deepEqual(packs, [6]);
    await git(dir, 'fsck', '--strict');
  });

  test('fetch with many local commits negotiates over several rounds (gzip requests)', async () => {
    const dir = join(root, 'clone3');
    await git(root, 'clone', '-q', `${rw.url}/flow.git`, dir);
    await git(dir, 'checkout', '-q', '-b', 'local');
    for (let i = 0; i < 80; i++) await commitFile(dir, `local/${i}.txt`, `${i}\n`);
    await git(seed, 'checkout', '-q', 'main');
    const next = await commitFile(seed, 'main.txt', 'more\n');
    await git(seed, 'push', '-q', `file://${gitdir}`, 'main');

    const before = seen.length;
    await git(dir, 'fetch', '-q', 'origin');
    assert.equal(await git(dir, 'rev-parse', 'origin/main'), next);
    const posts = seen.slice(before).filter((s) => s.path.endsWith('/git-upload-pack'));
    assert.ok(posts.length > 1, `negotiation rounds: ${posts.length}`);
    assert.ok(posts.some((s) => s.encoding === 'gzip'));
    assert.deepEqual(posts.map((s) => s.packObjects).filter((n) => n !== undefined), [3]);
    await git(dir, 'fsck', '--strict');
  });

  test('push fast-forwards and new refs', async () => {
    const dir = join(root, 'clone4');
    await git(root, 'clone', '-q', `${rw.url}/flow.git`, dir);
    await git(dir, 'checkout', '-q', 'runs/x');
    const oid = await commitFile(dir, 'runs/x/events/0003.json', '{"type":"pushed"}\n');
    await git(dir, 'push', '-q', 'origin', 'runs/x');
    assert.equal(await git(gitdir, 'rev-parse', 'refs/heads/runs/x'), oid);
    assert.match(await git(gitdir, 'log', '--format=%s', '-1', 'runs/x'), /0003\.json/);

    await git(dir, 'checkout', '-q', '-b', 'runs/y');
    const y = await commitFile(dir, 'runs/y/events/0001.json', '{"type":"start"}\n');
    await git(dir, 'tag', '-a', 'run-y', '-m', 'run y');
    await git(dir, 'push', '-q', 'origin', 'runs/y', 'run-y');
    assert.equal(await git(gitdir, 'rev-parse', 'refs/heads/runs/y'), y);
    assert.equal(await git(gitdir, 'rev-parse', 'run-y^{commit}'), y);
    await git(gitdir, 'fsck', '--strict');

    // The pushed history is served back.
    const again = join(root, 'clone4b');
    await git(root, 'clone', '-q', '-b', 'runs/y', `${rw.url}/flow.git`, again);
    assert.equal(await git(again, 'rev-parse', 'HEAD'), y);
  });

  test('non-fast-forward and delete pushes are rejected with the reason', async () => {
    const dir = join(root, 'clone5');
    await git(root, 'clone', '-q', `${rw.url}/flow.git`, dir);
    await git(dir, 'checkout', '-q', 'feature');
    const before = await git(gitdir, 'rev-parse', 'refs/heads/feature');
    await git(dir, 'commit', '-q', '--amend', '-m', 'rewritten');
    const stderr = await gitFails(dir, 'push', '--force', 'origin', 'feature');
    assert.match(stderr, /\[remote rejected\] +feature -> feature \(non-fast-forward: history is append-only\)/);
    assert.equal(await git(gitdir, 'rev-parse', 'refs/heads/feature'), before);

    const deleted = await gitFails(dir, 'push', 'origin', ':refs/heads/feature');
    // No `delete-refs` is advertised, so the client refuses the delete itself.
    assert.match(deleted, /\[remote rejected\] +feature/);
    assert.equal(await git(gitdir, 'rev-parse', 'refs/heads/feature'), before);

    // One rejected ref does not block the others.
    await git(dir, 'checkout', '-q', '-b', 'runs/z', 'origin/main');
    const z = await commitFile(dir, 'z.txt', 'z\n');
    const mixed = await gitFails(dir, 'push', '--force', 'origin', 'feature', 'runs/z');
    assert.match(mixed, /non-fast-forward: history is append-only/);
    assert.equal(await git(gitdir, 'rev-parse', 'refs/heads/runs/z'), z);
  });

  test('pushes are refused when receive is off', async () => {
    const dir = join(root, 'clone6');
    await git(root, 'clone', '-q', `${ro.url}/git/flow.git`, dir);
    assert.equal(await git(dir, 'rev-parse', 'origin/main'), await git(gitdir, 'rev-parse', 'main'));
    await commitFile(dir, 'ro.txt', 'ro\n');
    const stderr = await gitFails(dir, 'push', 'origin', 'main');
    assert.match(stderr, /403/);
    const res = await fetch(`${ro.url}/git/flow.git/git-receive-pack`, { method: 'POST', body: '0000' });
    assert.equal(res.status, 403);
  });

  test('empty repositories can be cloned and pushed to', async () => {
    const empty = await serverRepo('empty');
    const dir = join(root, 'clone7');
    await git(root, 'clone', '-q', `${rw.url}/empty.git`, dir);
    const oid = await commitFile(dir, 'first.txt', 'first\n');
    await git(dir, 'push', '-q', 'origin', 'HEAD:refs/heads/main');
    assert.equal(await git(empty, 'rev-parse', 'main'), oid);
    await git(empty, 'fsck', '--strict');
  });
});

describe('isomorphic-git client', () => {
  test('clones, fetches and pushes', async () => {
    const seed = await workRepo('iso-seed');
    await commitFile(seed, 'a.txt', 'a\n');
    const main = await commitFile(seed, 'dir/b.txt', 'b\n');
    await git(seed, 'branch', 'runs/one');
    const gitdir = await serverRepo('iso', seed);
    const url = `${rw.url}/iso.git`;

    const dir = join(root, 'iso-clone');
    await igit.clone({ fs, http: igitHttp, dir, url });
    assert.equal(await readFile(join(dir, 'dir/b.txt'), 'utf8'), 'b\n');
    assert.equal(await igit.resolveRef({ fs, dir, ref: 'refs/remotes/origin/runs/one' }), main);

    await writeFile(join(dir, 'c.txt'), 'c\n');
    await igit.add({ fs, dir, filepath: 'c.txt' });
    const author = { name: 'Iso', email: 'iso@example.com', timestamp: 1_700_000_000, timezoneOffset: 0 };
    const oid = await igit.commit({ fs, dir, message: 'from isomorphic-git', author });
    const pushed = await igit.push({ fs, http: igitHttp, dir, remote: 'origin', ref: 'main' });
    assert.equal(pushed.ok, true);
    assert.equal(await git(gitdir, 'rev-parse', 'main'), oid);
    assert.equal(await git(gitdir, 'log', '--format=%s', '-1', 'main'), 'from isomorphic-git');
    await git(gitdir, 'fsck', '--strict');

    // A new ref, then a fetch of a server-side update.
    await igit.branch({ fs, dir, ref: 'runs/two', checkout: false });
    await igit.push({ fs, http: igitHttp, dir, remote: 'origin', ref: 'runs/two' });
    assert.equal(await git(gitdir, 'rev-parse', 'runs/two'), oid);
    await git(seed, 'pull', '-q', `file://${gitdir}`, 'main');
    const next = await commitFile(seed, 'd.txt', 'd\n');
    await git(seed, 'push', '-q', `file://${gitdir}`, 'main');
    await igit.fetch({ fs, http: igitHttp, dir, ref: 'main', singleBranch: true });
    assert.equal(await igit.resolveRef({ fs, dir, ref: 'refs/remotes/origin/main' }), next);
    const { blob } = await igit.readBlob({ fs, dir, oid: next, filepath: 'd.txt' });
    assert.equal(Buffer.from(blob).toString(), 'd\n');

    // A non-fast-forward push is rejected by the server even when forced.
    await writeFile(join(dir, 'e.txt'), 'e\n');
    await igit.add({ fs, dir, filepath: 'e.txt' });
    await igit.commit({ fs, dir, message: 'diverged', author });
    await assert.rejects(
      igit.push({ fs, http: igitHttp, dir, remote: 'origin', ref: 'main', force: true }),
      /non-fast-forward: history is append-only/,
    );
    assert.equal(await git(gitdir, 'rev-parse', 'main'), next);

    // isomorphic-git sends deletes even without `delete-refs`; the server refuses them.
    await assert.rejects(
      igit.push({ fs, http: igitHttp, dir, remote: 'origin', ref: 'runs/two', delete: true }),
      /deletion prohibited: history is append-only/,
    );
    assert.equal(await git(gitdir, 'rev-parse', 'runs/two'), oid);
  });
});

describe('routing', () => {
  test('unknown repositories and paths are 404, wrong methods 405', async () => {
    const handler = smartHttp({ fs, resolve: () => undefined });
    const get = (path: string, init?: RequestInit) => handler(new Request(`http://x${path}`, init));
    assert.equal((await get('/nope.git/info/refs?service=git-upload-pack')).status, 404);
    assert.equal((await fetch(`${rw.url}/nope.git/info/refs?service=git-upload-pack`)).status, 404);
    assert.match(await gitFails(root, 'ls-remote', `${rw.url}/nope.git`), /not found|404/i);

    const known = smartHttp({ fs, resolve: () => ({ gitdir: join(root, 'server', 'flow.git') }) });
    const req = (path: string, init?: RequestInit) => known(new Request(`http://x${path}`, init));
    assert.equal((await req('/flow.git/info/refs')).status, 404); // dumb protocol
    assert.equal((await req('/flow.git/HEAD')).status, 404);
    assert.equal((await req('/flow/info/refs?service=git-upload-pack')).status, 404);
    assert.equal((await req('/flow.git/info/refs?service=git-upload-pack', { method: 'POST' })).status, 405);
    assert.equal((await req('/flow.git/git-upload-pack')).status, 405);
    assert.equal((await req('/flow.git/git-receive-pack', { method: 'POST', body: '0000' })).status, 403);
    const ok = await req('/flow.git/info/refs?service=git-upload-pack');
    assert.equal(ok.status, 200);
    assert.equal(ok.headers.get('content-type'), 'application/x-git-upload-pack-advertisement');
    assert.match(ok.headers.get('cache-control') ?? '', /no-cache/);
    const body = await ok.text();
    assert.ok(body.startsWith('001e# service=git-upload-pack\n0000'));
    assert.match(body, /\0[^\n]*symref=HEAD:refs\/heads\/main/);
    assert.match(body, /refs\/heads\/runs\/x\n/);
    assert.match(body, /refs\/tags\/v1\^\{\}\n/);
  });

  test('receive-pack reports each rejected command', async () => {
    const gitdir = repos.get('flow')!;
    const handler = smartHttp({ fs, resolve: () => ({ gitdir }), receive: true });
    const main = await git(gitdir, 'rev-parse', 'main');
    const zero = '0'.repeat(40);
    const pkt = (s: string) => (Buffer.byteLength(s) + 4).toString(16).padStart(4, '0') + s;
    const body =
      pkt(`${zero} ${main} refs/heads/main\0report-status\n`) +
      pkt(`${main} ${zero} refs/heads/main\n`) +
      pkt(`${zero} ${main} refs/heads/a..b\n`) +
      pkt(`${zero} ${'ab'.repeat(20)} refs/heads/missing\n`) +
      pkt(`${zero} ${main} refs/heads/runs/raw\n`) +
      '0000';
    const res = await handler(new Request('http://x/flow.git/git-receive-pack', { method: 'POST', body }));
    assert.equal(res.headers.get('content-type'), 'application/x-git-receive-pack-result');
    assert.equal(
      await res.text(),
      pkt('unpack ok\n') +
        pkt('ng refs/heads/main fetch first: the ref has changed\n') +
        pkt('ng refs/heads/main deletion prohibited: history is append-only\n') +
        pkt('ng refs/heads/a..b funny refname\n') +
        pkt('ng refs/heads/missing missing necessary objects\n') +
        pkt('ok refs/heads/runs/raw\n') +
        '0000',
    );
    assert.equal(await git(gitdir, 'rev-parse', 'refs/heads/runs/raw'), main);
  });

  test('gzip request bodies are accepted', async () => {
    const gitdir = repos.get('flow')!;
    const handler = smartHttp({ fs, resolve: () => ({ gitdir }) });
    const want = await git(gitdir, 'rev-parse', 'main');
    const pkt = (s: string) => (s.length + 4).toString(16).padStart(4, '0') + s;
    const plain = `${pkt(`want ${want} side-band-64k\n`)}0000${pkt('done\n')}`;
    const gzipped = await new Response(new Blob([plain]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer();
    const res = await handler(
      new Request('http://x/flow.git/git-upload-pack', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-git-upload-pack-request', 'Content-Encoding': 'gzip' },
        body: gzipped,
      }),
    );
    assert.equal(res.status, 200);
    assert.equal(res.headers.get('content-type'), 'application/x-git-upload-pack-result');
    const body = Buffer.from(await res.arrayBuffer());
    assert.equal(body.subarray(0, 8).toString(), '0008NAK\n');
    assert.ok((packObjectCount(body) ?? 0) > 0);
  });
});
