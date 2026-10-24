/**
 * Identity: Git object ids, computed the same way with or without Git. A
 * machine is the commit of its definition (`machine.json`) on its own
 * orphan line; a run is the commit that forks it; events and snapshots are
 * commits on the run. Without Git these are computed here (SHA-1 of Git's
 * object encoding, through Web Crypto); with Git (`@xmachines/git`) the same
 * bytes are stored, so the ids are the commit hashes.
 */

export type ObjectType = 'blob' | 'tree' | 'commit';

/** A Git object: its type, its content (without the header) and its id. */
export interface GitObject {
  readonly type: ObjectType;
  readonly content: Uint8Array;
  readonly oid: string;
}

const encoder = new TextEncoder();

const hex = (bytes: ArrayBuffer) => Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, '0')).join('');

/** Git's object id: SHA-1 of `<type> <length>\0<content>`. */
export async function objectId(type: ObjectType, content: Uint8Array): Promise<string> {
  const header = encoder.encode(`${type} ${content.length}\0`);
  const all = new Uint8Array(header.length + content.length);
  all.set(header);
  all.set(content, header.length);
  return hex(await crypto.subtle.digest('SHA-1', all));
}

export async function blob(data: Uint8Array | string): Promise<GitObject> {
  const content = typeof data === 'string' ? encoder.encode(data) : data;
  return { type: 'blob', content, oid: await objectId('blob', content) };
}

export interface TreeEntry {
  /** `100644` for files, `40000` for trees. */
  readonly mode: '100644' | '100755' | '40000';
  readonly path: string;
  readonly oid: string;
}

/** Git orders tree entries by name, a tree's name as if it ended in `/`. */
const sortKey = (e: TreeEntry) => (e.mode === '40000' ? `${e.path}/` : e.path);

export async function tree(entries: readonly TreeEntry[]): Promise<GitObject> {
  const sorted = [...entries].sort((a, b) => {
    const [x, y] = [encoder.encode(sortKey(a)), encoder.encode(sortKey(b))];
    for (let i = 0; i < Math.min(x.length, y.length); i++) if (x[i] !== y[i]) return x[i]! - y[i]!;
    return x.length - y.length;
  });
  const parts: Uint8Array[] = [];
  for (const e of sorted) {
    parts.push(encoder.encode(`${e.mode} ${e.path}\0`));
    parts.push(Uint8Array.from(e.oid.match(/../g)!.map((b) => parseInt(b, 16))));
  }
  const content = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    content.set(p, at);
    at += p.length;
  }
  return { type: 'tree', content, oid: await objectId('tree', content) };
}

/** Who wrote a commit, and when (seconds since the epoch, UTC). */
export interface Signature {
  readonly name: string;
  readonly email: string;
  readonly timestamp: number;
}

export const XMACHINES: Omit<Signature, 'timestamp'> = { name: 'xmachines', email: 'xmachines@localhost' };

export interface CommitFields {
  readonly tree: string;
  readonly parents: readonly string[];
  readonly author: Signature;
  readonly committer?: Signature;
  readonly message: string;
}

const signature = (s: Signature) => `${s.name} <${s.email}> ${Math.floor(s.timestamp)} +0000`;

export async function commit(fields: CommitFields): Promise<GitObject> {
  let text = `tree ${fields.tree}\n`;
  for (const p of fields.parents) text += `parent ${p}\n`;
  text += `author ${signature(fields.author)}\ncommitter ${signature(fields.committer ?? fields.author)}\n\n`;
  text += fields.message.endsWith('\n') ? fields.message : `${fields.message}\n`;
  const content = encoder.encode(text);
  return { type: 'commit', content, oid: await objectId('commit', content) };
}

/** A machine's objects: `machine.json`, its tree, and its commit. */
export interface MachineObjects {
  readonly id: string;
  readonly definition: GitObject;
  readonly tree: GitObject;
  readonly commit: GitObject;
}

/** JSON text as stored: two-space indentation and a final newline (key order is kept: it matters to machines). */
export const jsonText = (value: unknown) => `${JSON.stringify(value, null, 2)}\n`;

/**
 * A machine version's objects. Its id is the commit: the root of the
 * machine's orphan line, or a child of `parent` (the previous version).
 */
export async function machineObjects(definition: unknown, options: { readonly parent?: string } = {}): Promise<MachineObjects> {
  const file = await blob(jsonText(definition));
  const t = await tree([{ mode: '100644', path: 'machine.json', oid: file.oid }]);
  const id = (definition as { id?: unknown } | null)?.id;
  const c = await commit({
    tree: t.oid,
    parents: options.parent ? [options.parent] : [],
    author: { ...XMACHINES, timestamp: 0 },
    message: `machine${typeof id === 'string' ? ` ${id}` : ''}`,
  });
  return { id: c.oid, definition: file, tree: t, commit: c };
}

/** What a run is forked with. */
export interface ForkOptions {
  /** A name that tells runs of the same machine apart (a record key, an order id, ...). */
  readonly name?: string;
  /** The machine's input. */
  readonly input?: unknown;
  /** When the run was forked, in milliseconds (default 0: the id depends only on the machine and the options). */
  readonly time?: number;
}

/** A run's creation objects: `run.json` next to `machine.json`, and the fork commit, whose id is the run id. */
export interface RunObjects {
  readonly id: string;
  readonly runFile: GitObject;
  readonly tree: GitObject;
  readonly commit: GitObject;
}

/** The fork of `machine` that creates a run: deterministic in the machine and the options. */
export async function forkObjects(machine: MachineObjects, options: ForkOptions = {}): Promise<RunObjects> {
  const runFile = await blob(
    jsonText({ ...(options.name !== undefined && { name: options.name }), ...(options.input !== undefined && { input: options.input }) }),
  );
  const t = await tree([
    { mode: '100644', path: 'machine.json', oid: machine.definition.oid },
    { mode: '100644', path: 'run.json', oid: runFile.oid },
  ]);
  const c = await commit({
    tree: t.oid,
    parents: [machine.id],
    author: { ...XMACHINES, timestamp: Math.floor((options.time ?? 0) / 1000) },
    message: `run${options.name !== undefined ? ` ${options.name}` : ''}`,
  });
  return { id: c.oid, runFile, tree: t, commit: c };
}

/** The id of the run of `definition` forked with `options`. */
export async function runId(definition: unknown, options: ForkOptions = {}): Promise<string> {
  return (await forkObjects(await machineObjects(definition), options)).id;
}
