/**
 * Replication between repositories with isomorphic-git's HTTP client (the
 * client is injected: `isomorphic-git/http/web` in browsers,
 * `isomorphic-git/http/node` in Node), against any Git Smart HTTP server
 * (`@xmachines/git/http`, or plain Git hosting).
 *
 * History is append-only both ways: `push` moves remote refs only forward
 * (a server built with `smartHttp` refuses anything else), and `pull` takes
 * a remote ref only when it extends the local one. Runs whose local and
 * remote histories diverged are reported, never merged.
 */
import git from 'isomorphic-git';
import type { GitStore } from './repo.ts';

type Http = Parameters<typeof git.push>[0]['http'];

export interface Remote {
  /** isomorphic-git's HTTP client. */
  readonly http: Http;
  /** The repository's URL, like `https://example.com/runs.git`. */
  readonly url: string;
  readonly headers?: Record<string, string>;
  /** A CORS proxy, for servers that do not answer cross-origin requests. */
  readonly corsProxy?: string;
  onAuth?: Parameters<typeof git.push>[0]['onAuth'];
}

export interface RefsOptions {
  /** Which branches (`machines/...`, `runs/...`), default every machine and run. */
  readonly branches?: readonly string[];
}

export interface PushResult {
  /** Branches the remote took. */
  readonly pushed: string[];
  /** Branches it refused, with its reason. */
  readonly rejected: { readonly branch: string; readonly reason: string }[];
}

export interface PullResult {
  /** Local branches created or moved forward. */
  readonly updated: string[];
  /** Branches whose local and remote histories diverged (left as they are). */
  readonly diverged: string[];
}

const ours = (b: string) => b.startsWith('machines/') || b.startsWith('runs/');

const common = (remote: Remote) => ({
  http: remote.http,
  url: remote.url,
  ...(remote.headers && { headers: remote.headers }),
  ...(remote.corsProxy && { corsProxy: remote.corsProxy }),
  ...(remote.onAuth && { onAuth: remote.onAuth }),
});

/** Pushes machines and runs (machines first, so a run's machine is there before it). */
export async function push(store: GitStore, remote: Remote, options: RefsOptions = {}): Promise<PushResult> {
  await store.ready();
  const { fs, gitdir, cache } = store;
  const branches = options.branches ?? (await git.listBranches({ fs, gitdir })).filter(ours);
  const sorted = [...branches].sort((a, b) => Number(a.startsWith('runs/')) - Number(b.startsWith('runs/')));
  const result: PushResult = { pushed: [], rejected: [] };
  for (const branch of sorted) {
    const ref = `refs/heads/${branch}`;
    try {
      const r = await git.push({ fs, gitdir, cache, ...common(remote), ref, remoteRef: ref });
      const status = r.refs?.[ref];
      if (r.ok && (!status || status.ok)) result.pushed.push(branch);
      else result.rejected.push({ branch, reason: status?.error ?? r.error ?? 'rejected' });
    } catch (error) {
      result.rejected.push({ branch, reason: error instanceof Error ? error.message : String(error) });
    }
  }
  return result;
}

/** Fetches the remote's machines and runs and moves local branches forward to them. */
export async function pull(store: GitStore, remote: Remote, options: RefsOptions = {}): Promise<PullResult> {
  await store.ready();
  const { fs, gitdir, cache } = store;
  const name = 'xmachines';
  // fetch resolves `ref` (HEAD by default) on the server, which may have none
  // (a fresh repository): name one of the branches it has.
  const offered = (await git.listServerRefs({ ...common(remote), prefix: 'refs/heads/' }))
    .map((r) => r.ref.slice('refs/heads/'.length))
    .filter((b) => ours(b) && (!options.branches || options.branches.includes(b)));
  if (offered.length === 0) return { updated: [], diverged: [] };
  // Its fetch refspec (`refs/heads/*` to `refs/remotes/xmachines/*`).
  await git.addRemote({ fs, gitdir, remote: name, url: remote.url, force: true });
  // fetch needs the server's HEAD to resolve (`GitStore` and `smartHttp`
  // repositories have one as soon as they hold a branch).
  await git.fetch({
    fs,
    gitdir,
    cache,
    ...common(remote),
    remote: name,
    ref: offered[0],
    singleBranch: false,
    tags: false,
    prune: false,
  });
  const remoteBranches = (await git.listBranches({ fs, gitdir, remote: name })).filter(
    (b) => ours(b) && (!options.branches || options.branches.includes(b)),
  );
  const result: PullResult = { updated: [], diverged: [] };
  for (const branch of remoteBranches) {
    const theirs = await git.resolveRef({ fs, gitdir, ref: `refs/remotes/${name}/${branch}` });
    const ref = `refs/heads/${branch}`;
    const local = await store.tip(ref);
    if (local === theirs) continue;
    const ahead =
      local === undefined || (await git.isDescendent({ fs, gitdir, cache, oid: theirs, ancestor: local, depth: -1 }));
    if (ahead) {
      await git.writeRef({ fs, gitdir, ref, value: theirs, force: true });
      result.updated.push(branch);
    } else if (!(await git.isDescendent({ fs, gitdir, cache, oid: local, ancestor: theirs, depth: -1 }))) {
      result.diverged.push(branch);
    }
  }
  return result;
}
