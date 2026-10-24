/** Where runs keep their actors' persisted snapshots (a durability backend). */
export interface SnapshotStore {
  load(runId: string): unknown | Promise<unknown>;
  save(runId: string, snapshot: unknown): void | Promise<void>;
}

/** Snapshots in a map, for tests and local runs. */
export class MemorySnapshotStore implements SnapshotStore {
  readonly snapshots = new Map<string, unknown>();
  load(runId: string): unknown {
    const s = this.snapshots.get(runId);
    return s === undefined ? undefined : structuredClone(s);
  }
  save(runId: string, snapshot: unknown): void {
    this.snapshots.set(runId, structuredClone(snapshot));
  }
}
