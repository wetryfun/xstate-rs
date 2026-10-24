/**
 * Git as the durable, append-only stream of runs (isomorphic-git, pure
 * JavaScript):
 *
 * - a machine is an orphan branch, `refs/heads/machines/<name>`, whose
 *   commit holds `machine.json`; its commit id is the machine id;
 * - a run is a fork of it, `refs/heads/runs/<run id>`, starting with the
 *   commit that adds `run.json` on top of the machine commit; that commit's
 *   id is the run id (the same `@xmachines/run` computes without Git);
 * - each input batch is a commit on the run with `events.ndjson`, and each
 *   snapshot a commit with `snapshot.json`.
 *
 * Refs only move forward: every commit's parent is the previous tip, and a
 * ref that moved elsewhere fails the write. The fs is injected (Node's `fs`,
 * LightningFS in a browser, ...).
 */
import git from 'isomorphic-git';
import {
  blob,
  commit,
  forkObjects,
  jsonText,
  machineObjects,
  tree,
  XMACHINES,
  TIME,
  type Event,
  type ForkOptions,
  type GitObject,
  type Journal,
  type Journals,
  type MachineObjects,
  type RunObjects,
  type RunSnapshot,
  type TreeEntry,
} from '@xmachines/run';
import type { Log, LogEntry } from '@xmachines/streams';

type Fs = Parameters<typeof git.init>[0]['fs'];

export interface GitStoreOptions {
  /** The fs isomorphic-git uses. */
  readonly fs: Fs;
  /** The repository's git directory (a bare repository is created there if there is none). */
  readonly gitdir: string;
}

const decoder = new TextDecoder();

/** A run's ref. */
export const runRef = (runId: string) => `refs/heads/runs/${runId}`;

/** A machine's ref: its name, sanitized for a ref (`id` or `id@version` by default). */
export function machineRef(name: string): string {
  return `refs/heads/machines/${name.replace(/[^A-Za-z0-9._@-]/g, '_').replace(/^\./, '_').replace(/\.lock$/, '_lock')}`;
}

/** The default name of a machine: its `id`, with `@version` when it has one. */
export function machineName(definition: object): string {
  const { id, version } = definition as { id?: unknown; version?: unknown };
  const base = typeof id === 'string' && id !== '' ? id : 'machine';
  return typeof version === 'string' && version !== '' ? `${base}@${version}` : base;
}

/** Machines, runs and their journals in one repository. */
export class GitStore implements Journals {
  readonly fs: Fs;
  readonly gitdir: string;
  readonly cache = {};
  #ready: Promise<void> | undefined;

  constructor(options: GitStoreOptions) {
    this.fs = options.fs;
    this.gitdir = options.gitdir;
  }

  /** Creates the (bare) repository if needed. */
  ready(): Promise<void> {
    this.#ready ??= (async () => {
      try {
        await git.resolveRef({ fs: this.fs, gitdir: this.gitdir, ref: 'HEAD', depth: 1 });
      } catch {
        await git.init({ fs: this.fs, gitdir: this.gitdir, bare: true, defaultBranch: 'main' });
      }
    })();
    return this.#ready;
  }

  /** @internal Stores an object; its id must be the one computed without Git. */
  async write(object: GitObject): Promise<void> {
    const oid = await git.writeObject({
      fs: this.fs,
      gitdir: this.gitdir,
      type: object.type,
      object: object.content,
      format: 'content',
    });
    if (oid !== object.oid) throw new Error(`object id mismatch: ${oid} !== ${object.oid}`);
  }

  /** @internal The commit a ref points to, if it exists. */
  async tip(ref: string): Promise<string | undefined> {
    try {
      return await git.resolveRef({ fs: this.fs, gitdir: this.gitdir, ref });
    } catch {
      return undefined;
    }
  }

  /** @internal Moves `ref` from `from` (undefined: it must not exist) to `to`. */
  async advance(ref: string, from: string | undefined, to: string): Promise<void> {
    const current = await this.tip(ref);
    if (current !== from) throw new Error(`${ref} moved (expected ${from ?? 'none'}, found ${current ?? 'none'}): history is append-only`);
    await git.writeRef({ fs: this.fs, gitdir: this.gitdir, ref, value: to, force: true });
  }

  /**
   * Stores a machine on its orphan branch and returns its objects. A name
   * holds one definition: storing another one under it is an error (give the
   * new definition a `version`).
   */
  async machine(definition: object, name = machineName(definition)): Promise<MachineObjects> {
    await this.ready();
    const objects = await machineObjects(definition);
    const ref = machineRef(name);
    const tip = await this.tip(ref);
    if (tip === objects.id) return objects;
    if (tip !== undefined) throw new Error(`${ref} holds another definition (${tip}): give the new one a version`);
    for (const o of [objects.definition, objects.tree, objects.commit]) await this.write(o);
    await this.advance(ref, undefined, objects.id);
    // Give an unborn HEAD a branch: clients need one to resolve (the first
    // machine's, as a default branch).
    if ((await this.tip('HEAD')) === undefined) {
      await git.writeRef({ fs: this.fs, gitdir: this.gitdir, ref: 'HEAD', value: ref, symbolic: true, force: true });
    }
    return objects;
  }

  /** Forks a run of `definition` (storing the machine if needed) and returns its objects. */
  async fork(definition: object, options: ForkOptions = {}): Promise<RunObjects> {
    const machine = await this.machine(definition);
    const run = await forkObjects(machine, options);
    const ref = runRef(run.id);
    if ((await this.tip(ref)) === undefined) {
      for (const o of [run.runFile, run.tree, run.commit]) await this.write(o);
      await this.advance(ref, undefined, run.id);
    }
    return run;
  }

  /** The journal of an existing run. */
  async journal(runId: string): Promise<GitJournal> {
    await this.ready();
    if ((await this.tip(runRef(runId))) === undefined) throw new Error(`no run ${runId}`);
    return new GitJournal(this, runId);
  }

  /** `Journals`: forks the run if it does not exist yet, then opens its journal. */
  async open(runId: string, definition: object, fork: ForkOptions): Promise<GitJournal> {
    if ((await this.tip(runRef(runId))) === undefined) {
      const run = await this.fork(definition, fork);
      if (run.id !== runId) throw new Error(`run ${runId} is not the fork of this machine with these options (${run.id})`);
    }
    return new GitJournal(this, runId);
  }

  /** The runs in the repository. */
  async runs(): Promise<string[]> {
    await this.ready();
    const branches = await git.listBranches({ fs: this.fs, gitdir: this.gitdir });
    return branches.filter((b) => b.startsWith('runs/')).map((b) => b.slice('runs/'.length));
  }
}

interface RunCommit {
  readonly oid: string;
  readonly kind: 'fork' | 'events' | 'snapshot';
}

/**
 * A run's journal: batches and snapshots as commits on its branch. It is also
 * a `Log` of batches (`@xmachines/streams`' `durable` and `replay`).
 */
export class GitJournal implements Journal, Log<readonly Event[]> {
  readonly store: GitStore;
  readonly runId: string;
  #tip: string | undefined;
  #base: TreeEntry[] | undefined;
  /** Batches so far, once known. */
  #count: number | undefined;
  #time = 0;
  #chain: Promise<unknown> = Promise.resolve();

  constructor(store: GitStore, runId: string) {
    this.store = store;
    this.runId = runId;
  }

  get ref(): string {
    return runRef(this.runId);
  }

  /** The run's commits, oldest first (first parents from the tip to the fork). */
  async #history(): Promise<RunCommit[]> {
    const { fs, gitdir, cache } = this.store;
    const log = await git.log({ fs, gitdir, ref: this.ref, cache });
    const out: RunCommit[] = [];
    for (const entry of log) {
      const kind = entry.commit.message.startsWith('snapshot')
        ? 'snapshot'
        : entry.commit.message.startsWith('events')
          ? 'events'
          : 'fork';
      out.push({ oid: entry.oid, kind });
      if (kind === 'fork') break;
    }
    return out.reverse();
  }

  /** The fork's files (`machine.json`, `run.json`), which every commit keeps. */
  async #baseEntries(): Promise<TreeEntry[]> {
    if (this.#base) return this.#base;
    const history = await this.#history();
    const fork = history[0];
    if (!fork || fork.kind !== 'fork') throw new Error(`${this.ref} has no fork commit`);
    const { fs, gitdir, cache } = this.store;
    const { tree: entries } = await git.readTree({ fs, gitdir, oid: fork.oid, cache });
    this.#base = entries.filter((e) => e.type === 'blob').map((e) => ({ mode: '100644' as const, path: e.path, oid: e.oid }));
    this.#count = history.filter((c) => c.kind === 'events').length;
    this.#tip = history.at(-1)!.oid;
    return this.#base;
  }

  /** Commits a file on the run (one commit at a time). */
  #commit(path: string, text: string, message: (n: number) => string, events: boolean): Promise<string> {
    const done = this.#chain.then(async () => {
      const base = await this.#baseEntries();
      const file = await blob(text);
      const t = await tree([...base, { mode: '100644', path, oid: file.oid }]);
      const n = this.#count! + (events ? 1 : 0);
      const c = await commit({
        tree: t.oid,
        parents: [this.#tip!],
        author: { ...XMACHINES, timestamp: Math.floor(this.#time / 1000) },
        message: message(n),
      });
      for (const o of [file, t, c]) await this.store.write(o);
      await this.store.advance(this.ref, this.#tip, c.oid);
      this.#tip = c.oid;
      this.#count = n;
      return c.oid;
    });
    this.#chain = done.catch(() => {
      // Forget what we knew: the next write starts from the ref.
      this.#base = undefined;
    });
    return done;
  }

  /** Appends a batch as a commit with `events.ndjson`; resolves with the commit id. */
  append(batch: readonly Event[]): Promise<string> {
    const first = batch[0];
    if (first?.type === TIME && typeof first.now === 'number') this.#time = Math.max(this.#time, first.now);
    const text = batch.map((e) => `${JSON.stringify(e)}\n`).join('');
    return this.#commit('events.ndjson', text, (n) => `events ${n}`, true);
  }

  /** Commits a snapshot as `snapshot.json`. */
  async snapshot(snapshot: RunSnapshot): Promise<void> {
    this.#time = Math.max(this.#time, snapshot.time);
    await this.#commit('snapshot.json', jsonText(snapshot), () => `snapshot ${snapshot.seq}`, false);
  }

  async #read(oid: string, path: string): Promise<string> {
    const { fs, gitdir, cache } = this.store;
    const { blob: bytes } = await git.readBlob({ fs, gitdir, oid, filepath: path, cache });
    return decoder.decode(bytes);
  }

  #batch(text: string): Event[] {
    return text
      .split('\n')
      .filter((l) => l !== '')
      .map((l) => JSON.parse(l) as Event);
  }

  /** The latest snapshot and the batches after it. */
  async restore(): Promise<{ snapshot?: RunSnapshot; batches: ReadableStream<readonly Event[]> }> {
    await this.#chain;
    const history = await this.#history();
    let from = 0;
    let snapshot: RunSnapshot | undefined;
    for (let i = history.length - 1; i >= 0; i--) {
      if (history[i]!.kind === 'snapshot') {
        snapshot = JSON.parse(await this.#read(history[i]!.oid, 'snapshot.json')) as RunSnapshot;
        from = i + 1;
        break;
      }
    }
    const tail = history.slice(from).filter((c) => c.kind === 'events');
    return {
      ...(snapshot && { snapshot }),
      batches: new ReadableStream<readonly Event[]>({
        pull: async (controller) => {
          const next = tail.shift();
          if (next) controller.enqueue(this.#batch(await this.#read(next.oid, 'events.ndjson')));
          else controller.close();
        },
      }),
    };
  }

  /** `Log`: every batch, by commit id, oldest first (after the commit `after`). */
  read(options: { readonly after?: string } = {}): ReadableStream<LogEntry<readonly Event[]>> {
    let pending: RunCommit[] | undefined;
    return new ReadableStream({
      pull: async (controller) => {
        if (!pending) {
          const events = (await this.#history()).filter((c) => c.kind === 'events');
          const at = options.after === undefined ? -1 : events.findIndex((c) => c.oid === options.after);
          pending = events.slice(at + 1);
        }
        const next = pending.shift();
        if (!next) return controller.close();
        controller.enqueue({ id: next.oid, value: this.#batch(await this.#read(next.oid, 'events.ndjson')) });
      },
    });
  }
}

