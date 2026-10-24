/**
 * `@xmachines/git/http`: a Git Smart HTTP server as a WHATWG fetch handler.
 *
 * It serves `GET /<repo>.git/info/refs?service=...`, `POST /<repo>.git/git-upload-pack` and
 * `POST /<repo>.git/git-receive-pack` the way `git http-backend` does (protocol v0/v1, stateless
 * RPC), with isomorphic-git doing the object storage, so it runs wherever a `Request` and an
 * isomorphic-git `fs` do: Node, Deno, Bun, workers.
 *
 * Git is an append-only durable stream here: pushes may only create refs or fast-forward them.
 * Deletes and non-fast-forward updates are rejected with a reason the client prints.
 *
 * Protocol scope:
 * - Protocol v0 (and v1 clients); a `Git-Protocol: version=2` request is answered in v0, which
 *   clients accept as a fallback.
 * - upload-pack: `multi_ack_detailed` (without `ready` / `no-done`), `side-band` and
 *   `side-band-64k`, `no-progress`. No shallow or partial clones, no `include-tag`, no
 *   `thin-pack` / `ofs-delta`: packs hold every object whole (no deltas). Any existing object may
 *   be wanted (like `uploadpack.allowAnySHA1InWant`).
 * - receive-pack: `report-status`, `side-band-64k`, `no-thin` (clients send self-contained packs,
 *   which keeps the repository readable by the git CLI). No `delete-refs`, `atomic` or push
 *   options. A received pack is checked (trailer checksum, every object indexed, every object it
 *   references present) before any ref moves.
 * - Ref updates are serialized per repository within this process (one `smartHttp` module
 *   instance); isomorphic-git has no cross-process ref locking.
 * - Request and response bodies are buffered in memory.
 */
import {
  indexPack,
  isDescendent,
  listRefs,
  packObjects,
  readCommit,
  readObject,
  readTag,
  readTree,
  resolveRef,
  writeRef,
  type CallbackFsClient,
  type CommitObject,
  type PromiseFsClient,
  type TagObject,
  type TreeObject,
} from 'isomorphic-git';

export interface SmartHttpOptions {
  /** The fs isomorphic-git uses (Node's `fs`, LightningFS, ...). */
  readonly fs: PromiseFsClient | CallbackFsClient;
  /** Maps a repository name (the path before `.git`, may contain `/`) to its gitdir, or undefined for 404. */
  resolve(name: string): { gitdir: string } | undefined | Promise<{ gitdir: string } | undefined>;
  /** Accept pushes (default false: read-only). */
  readonly receive?: boolean;
  /** Path prefix to strip before matching (default ''). */
  readonly prefix?: string;
  /**
   * Cross-origin access, for browsers (isomorphic-git's `http/web` client) on
   * other origins: `true` for any origin, an origin, a list of them, or a
   * predicate. Preflight requests are answered, and responses carry the CORS
   * headers. Off by default.
   */
  readonly cors?: boolean | string | readonly string[] | ((origin: string) => boolean);
}

type Service = 'git-upload-pack' | 'git-receive-pack';

const ZERO_OID = '0'.repeat(40);
const OID = /^[0-9a-f]{40}$/;
const AGENT = 'agent=xmachines';
const UPLOAD_CAPS = ['multi_ack_detailed', 'side-band', 'side-band-64k', 'no-progress', 'object-format=sha1'];
const RECEIVE_CAPS = ['report-status', 'side-band-64k', 'no-thin', 'object-format=sha1', AGENT];
const NO_CACHE = {
  'Cache-Control': 'no-cache, max-age=0, must-revalidate',
  Expires: 'Fri, 01 Jan 1980 00:00:00 GMT',
  Pragma: 'no-cache',
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Creates the fetch handler. */
export function smartHttp(options: SmartHttpOptions): (request: Request) => Promise<Response> {
  const prefix = (options.prefix ?? '').replace(/\/+$/, '');
  const fs = wrapFs(options.fs);
  const allowed = corsPolicy(options.cors);
  return async (request) => {
    const origin = request.headers.get('Origin');
    const cors = origin !== null && allowed(origin) ? corsHeaders(origin) : undefined;
    if (cors && request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });
    let response: Response;
    try {
      response = await handle(request);
    } catch (err) {
      response = err instanceof HttpError ? text(err.status, err.message, err.headers) : text(500, 'Internal Server Error');
    }
    if (cors) for (const [k, v] of Object.entries(cors)) response.headers.set(k, v);
    return response;
  };

  async function handle(request: Request): Promise<Response> {
    const url = new URL(request.url);
    let path = url.pathname;
    if (prefix) {
      if (path !== prefix && !path.startsWith(`${prefix}/`)) return notFound();
      path = path.slice(prefix.length);
    }
    const match = /^\/(.+)\.git\/(info\/refs|git-upload-pack|git-receive-pack)$/.exec(path);
    if (!match) return notFound();
    const [, rawName = '', route] = match;
    let name: string;
    try {
      name = decodeURIComponent(rawName);
    } catch {
      return notFound();
    }
    if (name.includes('\0') || name.split('/').some((s) => s === '' || s === '.' || s === '..')) return notFound();

    let service: Service;
    if (route === 'info/refs') {
      if (request.method !== 'GET' && request.method !== 'HEAD') return methodNotAllowed('GET, HEAD');
      const param = url.searchParams.get('service');
      // Only the smart protocol is served; dumb clients get a 404.
      if (param !== 'git-upload-pack' && param !== 'git-receive-pack') return notFound();
      service = param;
    } else {
      if (request.method !== 'POST') return methodNotAllowed('POST');
      service = route as Service;
    }

    const repo = await options.resolve(name);
    if (!repo) return notFound();
    if (service === 'git-receive-pack' && !options.receive) return text(403, 'Pushes are not accepted');
    const ctx: Ctx = { fs, gitfs: options.fs, gitdir: trimSlashes(repo.gitdir), cache: {} };

    if (route === 'info/refs') {
      const body = concat([
        pkt(`# service=${service}\n`),
        FLUSH,
        ...(await advertise(ctx, service)),
        FLUSH,
      ]);
      return new Response(request.method === 'HEAD' ? null : body, {
        headers: { 'Content-Type': `application/x-${service}-advertisement`, ...NO_CACHE },
      });
    }
    const body = await readBody(request);
    const result = service === 'git-upload-pack' ? await uploadPack(ctx, body) : await receivePack(ctx, body);
    return new Response(result, {
      headers: { 'Content-Type': `application/x-${service}-result`, ...NO_CACHE },
    });
  }
}

/** Whether an origin may call the server. */
function corsPolicy(cors: SmartHttpOptions['cors']): (origin: string) => boolean {
  if (cors === undefined || cors === false) return () => false;
  if (cors === true) return () => true;
  if (typeof cors === 'string') return (o) => o === cors;
  if (typeof cors === 'function') return cors;
  return (o) => cors.includes(o);
}

function corsHeaders(origin: string): Record<string, string> {
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'GET, HEAD, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Content-Encoding, Git-Protocol, Authorization, Accept',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Expose-Headers': 'Content-Type',
    'Access-Control-Max-Age': '600',
    Vary: 'Origin',
  };
}

// ---------------------------------------------------------------------------------------------
// Ref advertisement

interface Ref {
  readonly name: string;
  readonly oid: string;
}

async function listAllRefs(ctx: Ctx): Promise<Ref[]> {
  const names = await listRefs({ fs: ctx.gitfs, gitdir: ctx.gitdir, filepath: 'refs' });
  const refs: Ref[] = [];
  for (const short of names) {
    const name = `refs/${short}`;
    if (!validRefName(name)) continue;
    try {
      const oid = await resolveRef({ fs: ctx.gitfs, gitdir: ctx.gitdir, ref: name });
      if (OID.test(oid)) refs.push({ name, oid });
    } catch {
      // A broken or vanished ref is not advertised.
    }
  }
  refs.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  return refs;
}

async function advertise(ctx: Ctx, service: Service): Promise<Uint8Array[]> {
  const refs = await listAllRefs(ctx);
  const lines: Array<{ name: string; oid: string }> = [];
  const caps = service === 'git-upload-pack' ? [...UPLOAD_CAPS] : [...RECEIVE_CAPS];
  if (service === 'git-upload-pack') {
    try {
      const target = await resolveRef({ fs: ctx.gitfs, gitdir: ctx.gitdir, ref: 'HEAD', depth: 2 });
      const oid = OID.test(target) ? target : await resolveRef({ fs: ctx.gitfs, gitdir: ctx.gitdir, ref: target });
      lines.push({ name: 'HEAD', oid });
      if (!OID.test(target)) caps.push(`symref=HEAD:${target}`);
    } catch {
      // Unborn or dangling HEAD: not advertised.
    }
    caps.push(AGENT);
    for (const ref of refs) {
      lines.push(ref);
      if (ref.name.startsWith('refs/tags/')) {
        const peeled = await peel(ctx, ref.oid);
        if (peeled !== ref.oid) lines.push({ name: `${ref.name}^{}`, oid: peeled });
      }
    }
  } else {
    lines.push(...refs);
  }
  if (lines.length === 0) lines.push({ name: 'capabilities^{}', oid: ZERO_OID });
  return lines.map((line, i) => pkt(i === 0 ? `${line.oid} ${line.name}\0${caps.join(' ')}\n` : `${line.oid} ${line.name}\n`));
}

/** Follows annotated tags to the object they finally point at. */
async function peel(ctx: Ctx, oid: string): Promise<string> {
  for (let i = 0; i < 64; i++) {
    try {
      const { tag } = await readTag({ fs: ctx.gitfs, gitdir: ctx.gitdir, oid, cache: ctx.cache });
      oid = tag.object;
    } catch {
      return oid;
    }
  }
  return oid;
}

// ---------------------------------------------------------------------------------------------
// upload-pack (fetch / clone)

async function uploadPack(ctx: Ctx, body: Uint8Array): Promise<Uint8Array<ArrayBuffer>> {
  const { packets } = parsePackets(body);
  const wants: string[] = [];
  const haves: string[] = [];
  let caps = new Set<string>();
  let done = false;
  let section: 'wants' | 'haves' = 'wants';
  for (const packet of packets) {
    if (packet === null) {
      section = 'haves';
      continue;
    }
    const line = decoder.decode(packet).replace(/\n$/, '');
    if (section === 'wants' && line.startsWith('want ')) {
      const [, oid = '', ...rest] = line.split(' ');
      if (!OID.test(oid)) throw new HttpError(400, `Malformed want line: ${line}`);
      if (wants.length === 0) caps = new Set(rest);
      wants.push(oid);
    } else if (line.startsWith('have ')) {
      const oid = line.slice(5).trim();
      if (!OID.test(oid)) throw new HttpError(400, `Malformed have line: ${line}`);
      haves.push(oid);
    } else if (line === 'done') {
      done = true;
      break;
    } else if (/^(shallow|deepen|deepen-since|deepen-not|filter) /.test(line)) {
      return concat([pkt(`ERR upload-pack: shallow and partial clones are not supported\n`)]);
    } else {
      throw new HttpError(400, `Unexpected line in upload-pack request: ${line}`);
    }
  }
  if (wants.length === 0) return new Uint8Array(0);

  const out: Uint8Array[] = [];
  const objects = new Objects(ctx);
  for (const want of wants) {
    if (!(await objects.has(want))) return concat([pkt(`ERR upload-pack: not our ref ${want}\n`)]);
  }

  // Negotiation, as git's upload-pack answers it (get_common_commits), without `ready`.
  const detailed = caps.has('multi_ack_detailed');
  const common: string[] = [];
  for (const oid of new Set(haves)) {
    if (!(await objects.has(oid))) continue;
    common.push(oid);
    if (detailed) out.push(pkt(`ACK ${oid} common\n`));
    else if (common.length === 1) out.push(pkt(`ACK ${oid}\n`));
  }
  if (!done) {
    if (detailed || common.length === 0) out.push(pkt('NAK\n'));
    return concat(out);
  }
  const last = common.at(-1);
  if (last === undefined) out.push(pkt('NAK\n'));
  else if (detailed) out.push(pkt(`ACK ${last}\n`));

  const band = caps.has('side-band-64k') ? 65515 : caps.has('side-band') ? 995 : 0;
  let packfile: Uint8Array;
  try {
    const oids = await objectsToSend(ctx, wants, common);
    const result = await packObjects({ fs: ctx.gitfs, gitdir: ctx.gitdir, oids, cache: ctx.cache });
    packfile = result.packfile ?? new Uint8Array(0);
  } catch (err) {
    const message = `upload-pack: ${err instanceof Error ? err.message : String(err)}\n`;
    out.push(band ? sideband(3, encoder.encode(message), band) : pkt(`ERR ${message}`));
    return concat(out);
  }
  if (band) {
    out.push(sideband(1, packfile, band), FLUSH);
  } else {
    out.push(packfile);
  }
  return concat(out);
}

const UNINTERESTING = 1;
const SEEN = 2;
const PROPAGATED = 4;

interface CommitNode {
  readonly oid: string;
  readonly time: number;
  readonly tree: string;
  readonly parents: readonly string[];
  flags: number;
  queued: number;
}

/**
 * Every object reachable from `wants` and not from `haves`, as git's revision walk finds it:
 * commits newest first, stopping once only uninteresting commits are left to walk (so a long
 * shared history is not read), then the trees and blobs of the new commits, minus everything in
 * the trees of the uninteresting commits at the boundary. Clock skew can only add objects the
 * client already has, never leave one out.
 */
async function objectsToSend(ctx: Ctx, wants: readonly string[], haves: readonly string[]): Promise<string[]> {
  const out: string[] = [];
  const added = new Set<string>();
  const add = (oid: string) => {
    if (!added.has(oid)) {
      added.add(oid);
      out.push(oid);
    }
  };

  const nodes = new Map<string, CommitNode>();
  const load = async (oid: string): Promise<CommitNode> => {
    let node = nodes.get(oid);
    if (!node) {
      const { commit } = await readCommit({ fs: ctx.gitfs, gitdir: ctx.gitdir, oid, cache: ctx.cache });
      node = { oid, time: commit.committer.timestamp, tree: commit.tree, parents: commit.parent, flags: 0, queued: 0 };
      nodes.set(oid, node);
    }
    return node;
  };

  // Peel wanted tags (the tag objects are sent too) and sort the rest by type.
  const wantCommits: CommitNode[] = [];
  const wantTrees: string[] = [];
  for (const want of wants) {
    let oid = want;
    for (let depth = 0; ; depth++) {
      const type = await objectType(ctx, oid);
      if (type.type === 'tag' && depth < 64) {
        add(oid);
        oid = type.tag.object;
        continue;
      }
      if (type.type === 'commit') wantCommits.push(await load(oid));
      else if (type.type === 'tree') wantTrees.push(oid);
      else add(oid);
      break;
    }
  }
  const excluded = new Set<string>();
  const haveCommits: CommitNode[] = [];
  for (const have of haves) {
    let oid = have;
    let type = await objectType(ctx, oid);
    for (let depth = 0; type.type === 'tag' && depth < 64; depth++) {
      excluded.add(oid);
      oid = type.tag.object;
      type = await objectType(ctx, oid);
    }
    if (type.type === 'commit') haveCommits.push(await load(oid));
    else if (type.type === 'tree') await walkTree(ctx, oid, excluded, new Set());
    else excluded.add(oid);
  }

  // The commit walk: newest first, ties in insertion order (as git's prio_queue), so the
  // interesting and uninteresting frontiers advance together through commits of the same second.
  const queue = new Heap<{ node: CommitNode; seq: number }>((a, b) =>
    a.node.time !== b.node.time ? a.node.time > b.node.time : a.seq < b.seq,
  );
  let seq = 0;
  let interesting = 0;
  const push = (node: CommitNode) => {
    node.queued++;
    if (!(node.flags & UNINTERESTING)) interesting++;
    queue.push({ node, seq: seq++ });
  };
  const markUninteresting = (node: CommitNode) => {
    if (!(node.flags & UNINTERESTING)) {
      node.flags |= UNINTERESTING;
      interesting -= node.queued;
    }
  };
  /** Pops one entry: an uninteresting commit marks its parents, an interesting one is walked. */
  const step = async (walked: CommitNode[]) => {
    const { node } = queue.pop()!;
    node.queued--;
    if (node.flags & UNINTERESTING) {
      if (node.flags & PROPAGATED) return;
      node.flags |= PROPAGATED;
      for (const parent of node.parents) {
        const p = await load(parent);
        markUninteresting(p);
        // Walked commits that turn out to be uninteresting pass it on to their parents too.
        if (p.queued === 0 && !(p.flags & PROPAGATED)) push(p);
      }
    } else {
      interesting--;
      if (node.flags & SEEN) return;
      node.flags |= SEEN;
      walked.push(node);
      for (const parent of node.parents) {
        const p = await load(parent);
        if (p.queued === 0 && !(p.flags & (SEEN | UNINTERESTING))) push(p);
      }
    }
  };
  for (const node of haveCommits) {
    markUninteresting(node);
    push(node);
  }
  for (const node of wantCommits) if (node.queued === 0 && !(node.flags & UNINTERESTING)) push(node);

  const walked: CommitNode[] = [];
  while (interesting > 0) await step(walked);
  // Slop (git's limit_list has one too): the interesting side may have run ahead through
  // commits of the same time; let the uninteresting side catch up for a bounded number of steps
  // while it is still at or after the oldest walked commit.
  let oldest = Infinity;
  for (const node of walked) if (!(node.flags & UNINTERESTING)) oldest = Math.min(oldest, node.time);
  for (let budget = walked.length + 32; budget > 0; budget--) {
    const top = queue.peek();
    if (!top || top.node.time < oldest) break;
    await step(walked);
  }
  const commits = walked.filter((node) => !(node.flags & UNINTERESTING));

  // Trees at the boundary: the client has everything in them.
  const boundary = new Set<string>(haveCommits.map((node) => node.tree));
  for (const node of commits) {
    for (const parent of node.parents) {
      const p = nodes.get(parent);
      if (p && p.flags & UNINTERESTING) boundary.add(p.tree);
    }
  }
  for (const tree of boundary) await walkTree(ctx, tree, excluded, new Set());

  for (const node of commits) add(node.oid);
  for (const node of commits) await walkTree(ctx, node.tree, added, excluded, out);
  for (const tree of wantTrees) await walkTree(ctx, tree, added, excluded, out);
  return out;
}

type ObjectType = { type: 'commit' } | { type: 'tree' } | { type: 'blob' } | { type: 'tag'; tag: TagObject };

async function objectType(ctx: Ctx, oid: string): Promise<ObjectType> {
  const result = await readObject({ fs: ctx.gitfs, gitdir: ctx.gitdir, oid, format: 'parsed', cache: ctx.cache });
  switch (result.type) {
    case 'tag':
      return { type: 'tag', tag: result.object as TagObject };
    case 'commit':
    case 'tree':
    case 'blob':
      return { type: result.type };
    default:
      throw new Error(`Unexpected object type ${String(result.type)} for ${oid}`);
  }
}

/** Adds every tree and blob under `root` to `into` (and `list`), skipping what `skip` holds. */
async function walkTree(ctx: Ctx, root: string, into: Set<string>, skip: Set<string>, list?: string[]): Promise<void> {
  const stack = [root];
  while (stack.length > 0) {
    const oid = stack.pop()!;
    if (into.has(oid) || skip.has(oid)) continue;
    into.add(oid);
    list?.push(oid);
    const { tree } = await readTree({ fs: ctx.gitfs, gitdir: ctx.gitdir, oid, cache: ctx.cache });
    for (const entry of tree) {
      if (entry.type === 'tree') stack.push(entry.oid);
      else if (entry.type === 'blob' && !into.has(entry.oid) && !skip.has(entry.oid)) {
        into.add(entry.oid);
        list?.push(entry.oid);
      }
      // Gitlinks (submodule commits) are not part of this repository.
    }
  }
}

// ---------------------------------------------------------------------------------------------
// receive-pack (push)

interface Command {
  readonly old: string;
  readonly new: string;
  readonly ref: string;
}

const refLocks = new WeakMap<object, Map<string, Promise<unknown>>>();

/** Runs `fn` after every earlier `withLock` call for the same fs and gitdir has finished. */
async function withLock<T>(ctx: Ctx, fn: () => Promise<T>): Promise<T> {
  let locks = refLocks.get(ctx.gitfs);
  if (!locks) refLocks.set(ctx.gitfs, (locks = new Map()));
  const previous = locks.get(ctx.gitdir) ?? Promise.resolve();
  const run = previous.then(fn, fn);
  const tail = run.then(
    () => undefined,
    () => undefined,
  );
  locks.set(ctx.gitdir, tail);
  try {
    return await run;
  } finally {
    if (locks.get(ctx.gitdir) === tail) locks.delete(ctx.gitdir);
  }
}

async function receivePack(ctx: Ctx, body: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
  const { packets, rest } = parsePackets(body, true);
  const commands: Command[] = [];
  let caps = new Set<string>();
  for (const packet of packets) {
    if (packet === null) break;
    let line = decoder.decode(packet).replace(/\n$/, '');
    const nul = line.indexOf('\0');
    if (nul >= 0) {
      if (commands.length === 0) caps = new Set(line.slice(nul + 1).split(' ').filter(Boolean));
      line = line.slice(0, nul);
    }
    if (line.startsWith('shallow ')) continue;
    const [old = '', next = '', ref = ''] = line.split(' ');
    if (!OID.test(old) || !OID.test(next) || !ref) throw new HttpError(400, `Malformed command: ${line}`);
    commands.push({ old, new: next, ref });
  }
  // A request without commands is git's probe ("0000") before a large push.
  if (commands.length === 0) return new Uint8Array(0);

  const pack = body.subarray(rest);
  const objects = new Objects(ctx);
  let unpack = 'ok';
  let packOids = new Set<string>();
  try {
    packOids = await storePack(ctx, pack, objects);
  } catch (err) {
    unpack = (err instanceof Error ? err.message : String(err)).replace(/\s+/g, ' ');
  }

  const results = new Map<Command, string | undefined>();
  if (unpack !== 'ok') {
    for (const command of commands) results.set(command, 'unpacker error');
  } else {
    await withLock(ctx, async () => {
      for (const command of commands) results.set(command, await update(ctx, objects, packOids, command));
      await adoptHead(ctx, commands.filter((c) => results.get(c) === undefined && c.old === ZERO_OID));
    });
  }

  if (!caps.has('report-status')) return new Uint8Array(0);
  const report = [pkt(`unpack ${unpack}\n`)];
  for (const command of commands) {
    const error = results.get(command);
    report.push(pkt(error === undefined ? `ok ${command.ref}\n` : `ng ${command.ref} ${error}\n`));
  }
  report.push(FLUSH);
  const status = concat(report);
  return caps.has('side-band-64k') ? concat([sideband(1, status, 65515), FLUSH]) : status;
}

/**
 * Validates, stores and indexes a received pack, and checks that every object it references is
 * present, so the repository stays connected. Returns the ids of the objects in it.
 */
async function storePack(ctx: Ctx, pack: Uint8Array<ArrayBuffer>, objects: Objects): Promise<Set<string>> {
  if (pack.byteLength === 0) return new Set();
  if (pack.byteLength < 32 || decoder.decode(pack.subarray(0, 4)) !== 'PACK') throw new Error('bad pack header');
  const view = new DataView(pack.buffer, pack.byteOffset, pack.byteLength);
  const version = view.getUint32(4);
  if (version !== 2 && version !== 3) throw new Error(`unsupported pack version ${version}`);
  const count = view.getUint32(8);
  const trailer = pack.subarray(pack.byteLength - 20);
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-1', pack.subarray(0, pack.byteLength - 20)));
  if (!equalBytes(digest, trailer)) throw new Error('pack checksum mismatch');
  if (count === 0) return new Set();

  const name = `pack-${hex(trailer)}`;
  const dir = `${ctx.gitdir}/objects/pack`;
  await ctx.fs.mkdirp(dir);
  const packPath = `${dir}/${name}.pack`;
  const idxPath = `${dir}/${name}.idx`;
  await ctx.fs.writeFile(packPath, pack);
  const discard = async () => {
    await ctx.fs.unlink(idxPath).catch(() => {});
    await ctx.fs.unlink(packPath).catch(() => {});
    objects.reset();
  };
  let oids: string[];
  try {
    ({ oids } = await indexPack({ fs: ctx.gitfs, dir: ctx.gitdir, gitdir: ctx.gitdir, filepath: `objects/pack/${name}.pack`, cache: ctx.cache }));
  } catch (err) {
    await discard();
    throw new Error(`index-pack failed: ${err instanceof Error ? err.message : String(err)}`);
  }
  objects.reset();
  const inPack = new Set(oids);
  if (inPack.size !== count) {
    await discard();
    throw new Error(`index-pack failed: resolved ${inPack.size} of ${count} objects`);
  }
  // Connectivity: everything a new object refers to must be in the pack or already stored.
  const missing = async (oid: string) => !inPack.has(oid) && !(await objects.has(oid));
  for (const oid of oids) {
    const result = await readObject({ fs: ctx.gitfs, gitdir: ctx.gitdir, oid, format: 'parsed', cache: ctx.cache });
    const refs: string[] = [];
    if (result.type === 'commit') {
      const commit = result.object as CommitObject;
      refs.push(commit.tree, ...commit.parent);
    } else if (result.type === 'tree') {
      for (const entry of result.object as TreeObject) if (entry.type !== 'commit') refs.push(entry.oid);
    } else if (result.type === 'tag') {
      refs.push((result.object as TagObject).object);
    }
    for (const ref of refs) {
      if (await missing(ref)) {
        await discard();
        throw new Error(`missing object ${ref} referenced by ${oid}`);
      }
    }
  }
  return inPack;
}

/** Applies one command if it is a creation or a fast-forward; returns the rejection reason otherwise. */
async function update(ctx: Ctx, objects: Objects, packOids: Set<string>, command: Command): Promise<string | undefined> {
  const { ref } = command;
  if (!ref.startsWith('refs/') || !validRefName(ref)) return 'funny refname';
  if (command.new === ZERO_OID) return 'deletion prohibited: history is append-only';
  if (!packOids.has(command.new) && !(await objects.has(command.new))) return 'missing necessary objects';

  let current = ZERO_OID;
  try {
    current = await resolveRef({ fs: ctx.gitfs, gitdir: ctx.gitdir, ref });
  } catch {
    // The ref does not exist yet.
  }
  if (current !== command.old) return 'fetch first: the ref has changed';
  if (command.new === current) return undefined;

  const type = (await objectType(ctx, command.new)).type;
  if (type !== 'commit' && !(ref.startsWith('refs/tags/') && type === 'tag')) return `not a commit: ${type}`;
  if (current !== ZERO_OID) {
    let fastForward = false;
    try {
      fastForward =
        type === 'commit' &&
        (await isDescendent({ fs: ctx.gitfs, gitdir: ctx.gitdir, oid: command.new, ancestor: current, depth: -1, cache: ctx.cache }));
    } catch {
      fastForward = false;
    }
    if (!fastForward) return 'non-fast-forward: history is append-only';
  }
  try {
    await writeRef({ fs: ctx.gitfs, gitdir: ctx.gitdir, ref, value: command.new, force: true });
  } catch {
    return 'failed to update ref';
  }
  return undefined;
}

/**
 * Points an unborn HEAD at the first branch a push created, as Git hosting
 * does: clients (isomorphic-git's fetch among them) need a HEAD to resolve.
 */
async function adoptHead(ctx: Ctx, created: readonly Command[]): Promise<void> {
  const branch = created.find((c) => c.ref.startsWith('refs/heads/'));
  if (!branch) return;
  try {
    await resolveRef({ fs: ctx.gitfs, gitdir: ctx.gitdir, ref: 'HEAD' });
  } catch {
    await writeRef({ fs: ctx.gitfs, gitdir: ctx.gitdir, ref: 'HEAD', value: branch.ref, symbolic: true, force: true });
  }
}

/** Git's check-ref-format rules for a full ref name. */
function validRefName(name: string): boolean {
  if (name === '@' || name.endsWith('/') || name.endsWith('.') || name.includes('//')) return false;
  if (name.includes('..') || name.includes('@{')) return false;
  // eslint-disable-next-line no-control-regex
  if (/[\x00-\x20\x7f~^:?*[\\]/.test(name)) return false;
  return name.split('/').every((part) => part !== '' && !part.startsWith('.') && !part.endsWith('.lock'));
}

// ---------------------------------------------------------------------------------------------
// Object existence without inflating objects: loose files and pack indexes.

class Objects {
  readonly #ctx: Ctx;
  #indexes: Uint8Array[] | undefined;

  constructor(ctx: Ctx) {
    this.#ctx = ctx;
  }

  /** Forgets the pack indexes read so far (after a pack was added or removed). */
  reset(): void {
    this.#indexes = undefined;
  }

  async has(oid: string): Promise<boolean> {
    const { fs, gitdir } = this.#ctx;
    if (await fs.exists(`${gitdir}/objects/${oid.slice(0, 2)}/${oid.slice(2)}`)) return true;
    const key = fromHex(oid);
    for (const idx of await this.#packIndexes()) if (idxHas(idx, key)) return true;
    return false;
  }

  async #packIndexes(): Promise<Uint8Array[]> {
    if (!this.#indexes) {
      const { fs, gitdir } = this.#ctx;
      const dir = `${gitdir}/objects/pack`;
      let names: string[] = [];
      try {
        names = await fs.readdir(dir);
      } catch {
        names = [];
      }
      const indexes: Uint8Array[] = [];
      for (const name of names) {
        if (!name.endsWith('.idx')) continue;
        try {
          indexes.push(await fs.readFile(`${dir}/${name}`));
        } catch {
          // Removed meanwhile.
        }
      }
      this.#indexes = indexes;
    }
    return this.#indexes;
  }
}

/** Binary search in a pack index (version 1 or 2). */
function idxHas(idx: Uint8Array, key: Uint8Array): boolean {
  const view = new DataView(idx.buffer, idx.byteOffset, idx.byteLength);
  const v2 = idx.byteLength >= 8 && view.getUint32(0) === 0xff744f63;
  const fanout = v2 ? 8 : 0;
  if (idx.byteLength < fanout + 1024) return false;
  const first = key[0]!;
  let lo = first === 0 ? 0 : view.getUint32(fanout + (first - 1) * 4);
  let hi = view.getUint32(fanout + first * 4);
  const base = fanout + 1024;
  const stride = v2 ? 20 : 24;
  const skip = v2 ? 0 : 4;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const at = base + mid * stride + skip;
    if (at + 20 > idx.byteLength) return false;
    let cmp = 0;
    for (let i = 0; i < 20 && cmp === 0; i++) cmp = idx[at + i]! - key[i]!;
    if (cmp === 0) return true;
    if (cmp < 0) lo = mid + 1;
    else hi = mid;
  }
  return false;
}

// ---------------------------------------------------------------------------------------------
// fs access beyond what isomorphic-git exports

interface Ctx {
  readonly fs: Fs;
  readonly gitfs: PromiseFsClient | CallbackFsClient;
  readonly gitdir: string;
  /** isomorphic-git's cache, per request. */
  readonly cache: object;
}

interface Fs {
  readFile(path: string): Promise<Uint8Array>;
  writeFile(path: string, data: Uint8Array): Promise<void>;
  readdir(path: string): Promise<string[]>;
  unlink(path: string): Promise<void>;
  exists(path: string): Promise<boolean>;
  mkdirp(path: string): Promise<void>;
}

type Call = (...args: unknown[]) => Promise<unknown>;

function wrapFs(fs: PromiseFsClient | CallbackFsClient): Fs {
  const promises = (fs as Partial<PromiseFsClient>).promises;
  const method = (name: 'readFile' | 'writeFile' | 'readdir' | 'unlink' | 'stat' | 'mkdir'): Call => {
    if (promises && typeof promises[name] === 'function') {
      const fn = promises[name];
      return (...args) => fn.apply(promises, args) as Promise<unknown>;
    }
    const fn = (fs as CallbackFsClient)[name];
    return (...args) =>
      new Promise((resolve, reject) => {
        fn.call(fs, ...args, (err: unknown, value: unknown) => (err ? reject(err) : resolve(value)));
      });
  };
  const readFile = method('readFile');
  const writeFile = method('writeFile');
  const readdir = method('readdir');
  const unlink = method('unlink');
  const stat = method('stat');
  const mkdir = method('mkdir');
  const exists = async (path: string) => {
    try {
      await stat(path);
      return true;
    } catch {
      return false;
    }
  };
  return {
    readFile: async (path) => (await readFile(path)) as Uint8Array,
    writeFile: async (path, data) => {
      await writeFile(path, data);
    },
    readdir: async (path) => (await readdir(path)) as string[],
    unlink: async (path) => {
      await unlink(path);
    },
    exists,
    async mkdirp(path) {
      if (await exists(path)) return;
      const parent = path.slice(0, path.lastIndexOf('/'));
      if (parent && parent !== path) await this.mkdirp(parent);
      try {
        await mkdir(path);
      } catch (err) {
        if (!(await exists(path))) throw err;
      }
    },
  };
}

// ---------------------------------------------------------------------------------------------
// pkt-lines and bytes

const FLUSH = encoder.encode('0000');

function pkt(data: string | Uint8Array): Uint8Array {
  const bytes = typeof data === 'string' ? encoder.encode(data) : data;
  const out = new Uint8Array(bytes.byteLength + 4);
  out.set(encoder.encode((bytes.byteLength + 4).toString(16).padStart(4, '0')));
  out.set(bytes, 4);
  return out;
}

/** `data` in side-band packets on `band`, at most `max` payload bytes each. */
function sideband(band: number, data: Uint8Array, max: number): Uint8Array {
  const parts: Uint8Array[] = [];
  for (let at = 0; at < data.byteLength; at += max) {
    const chunk = data.subarray(at, at + max);
    const payload = new Uint8Array(chunk.byteLength + 1);
    payload[0] = band;
    payload.set(chunk, 1);
    parts.push(pkt(payload));
  }
  return concat(parts);
}

/** Reads pkt-lines (`null` for a flush); with `untilFlush`, stops after the first flush. */
function parsePackets(body: Uint8Array, untilFlush = false): { packets: Array<Uint8Array | null>; rest: number } {
  const packets: Array<Uint8Array | null> = [];
  let at = 0;
  while (at + 4 <= body.byteLength) {
    const length = Number.parseInt(decoder.decode(body.subarray(at, at + 4)), 16);
    if (!Number.isInteger(length) || (length > 0 && length < 4) || at + Math.max(length, 4) > body.byteLength) {
      throw new HttpError(400, 'Malformed pkt-line');
    }
    if (length === 0) {
      packets.push(null);
      at += 4;
      if (untilFlush) return { packets, rest: at };
      continue;
    }
    packets.push(body.subarray(at + 4, at + length));
    at += length;
  }
  if (at !== body.byteLength) throw new HttpError(400, 'Truncated pkt-line');
  return { packets, rest: at };
}

function concat(parts: readonly Uint8Array[]): Uint8Array<ArrayBuffer> {
  let length = 0;
  for (const part of parts) length += part.byteLength;
  const out = new Uint8Array(length);
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.byteLength;
  }
  return out;
}

function hex(bytes: Uint8Array): string {
  let s = '';
  for (const b of bytes) s += b.toString(16).padStart(2, '0');
  return s;
}

function fromHex(oid: string): Uint8Array {
  const out = new Uint8Array(20);
  for (let i = 0; i < 20; i++) out[i] = Number.parseInt(oid.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
  return true;
}

function trimSlashes(path: string): string {
  const trimmed = path.replace(/\/+$/, '');
  return trimmed === '' ? path : trimmed;
}

/** A binary max-heap by `before`. */
class Heap<T> {
  readonly #items: T[] = [];
  readonly #before: (a: T, b: T) => boolean;

  constructor(before: (a: T, b: T) => boolean) {
    this.#before = before;
  }

  push(item: T): void {
    const items = this.#items;
    items.push(item);
    let i = items.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (!this.#before(items[i]!, items[parent]!)) break;
      [items[i], items[parent]] = [items[parent]!, items[i]!];
      i = parent;
    }
  }

  peek(): T | undefined {
    return this.#items[0];
  }

  pop(): T | undefined {
    const items = this.#items;
    const top = items[0];
    const last = items.pop();
    if (items.length > 0 && last !== undefined) {
      items[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let best = i;
        if (l < items.length && this.#before(items[l]!, items[best]!)) best = l;
        if (r < items.length && this.#before(items[r]!, items[best]!)) best = r;
        if (best === i) break;
        [items[i], items[best]] = [items[best]!, items[i]!];
        i = best;
      }
    }
    return top;
  }
}

// ---------------------------------------------------------------------------------------------
// HTTP plumbing

class HttpError extends Error {
  readonly status: number;
  readonly headers: Record<string, string> | undefined;

  constructor(status: number, message: string, headers?: Record<string, string>) {
    super(message);
    this.status = status;
    this.headers = headers;
  }
}

async function readBody(request: Request): Promise<Uint8Array<ArrayBuffer>> {
  if (!request.body) return new Uint8Array(0);
  const encoding = request.headers.get('content-encoding')?.trim().toLowerCase();
  let stream: ReadableStream<Uint8Array> = request.body;
  if (encoding === 'gzip' || encoding === 'x-gzip') {
    stream = stream.pipeThrough(new DecompressionStream('gzip') as unknown as TransformStream<Uint8Array, Uint8Array>);
  } else if (encoding && encoding !== 'identity') {
    throw new HttpError(415, `Unsupported Content-Encoding: ${encoding}`);
  }
  try {
    return new Uint8Array(await new Response(stream).arrayBuffer());
  } catch {
    throw new HttpError(400, 'Unreadable request body');
  }
}

function text(status: number, message: string, headers?: Record<string, string>): Response {
  return new Response(`${message}\n`, { status, headers: { 'Content-Type': 'text/plain; charset=utf-8', ...NO_CACHE, ...headers } });
}

function notFound(): Response {
  return text(404, 'Not Found');
}

function methodNotAllowed(allow: string): Response {
  return text(405, 'Method Not Allowed', { Allow: allow });
}
