/**
 * `@xmachines/git`: Git as a durable, append-only stream for XMachines, in
 * pure JavaScript (isomorphic-git). Machines are orphan branches, runs are
 * forks, events and snapshots are commits; commit ids are the canonical ids
 * (`@xmachines/run` computes the same without Git). Git is optional: it is
 * one `Journal` among others. `@xmachines/git/http` serves repositories over
 * Git's Smart HTTP protocol, so runs replicate with plain `git`, and `push` /
 * `pull` replicate through isomorphic-git's HTTP client. In browsers, import
 * `@xmachines/git/browser`.
 */
export { GitJournal, GitStore, machineName, machineRef, runRef } from './repo.ts';
export type { GitStoreOptions } from './repo.ts';
export { pull, push } from './remote.ts';
export type { PullResult, PushResult, Remote, RefsOptions } from './remote.ts';
