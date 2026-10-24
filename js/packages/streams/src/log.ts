/**
 * Durability as transforms: a log is append-only, ordered and replayable;
 * `durable(log)` appends every chunk before passing it on, and `replay(log)`
 * reads it back as a stream to pipe anywhere.
 */

/** An entry of a log. */
export interface LogEntry<T> {
  /** The entry's id in its log (a commit id, a sequence number, ...). */
  readonly id: string;
  readonly value: T;
}

/** An append-only, ordered log: the durability backend contract. */
export interface Log<T> {
  /** Appends `value` after every entry so far; resolves with its id once it is durable. */
  append(value: T): Promise<string>;
  /** Entries in append order, from the start or after the entry `after`. */
  read(options?: { readonly after?: string }): ReadableStream<LogEntry<T>>;
}

/** A log in memory, for tests and local runs. */
export class MemoryLog<T> implements Log<T> {
  readonly entries: LogEntry<T>[] = [];

  async append(value: T): Promise<string> {
    const id = String(this.entries.length);
    this.entries.push({ id, value: structuredClone(value) });
    return id;
  }

  read(options: { readonly after?: string } = {}): ReadableStream<LogEntry<T>> {
    let next = options.after === undefined ? 0 : Number(options.after) + 1;
    return new ReadableStream({
      pull: (controller) => {
        const entry = this.entries[next++];
        if (entry) controller.enqueue(structuredClone(entry));
        else controller.close();
      },
    });
  }
}

/**
 * A transform that appends each chunk to `log` and passes it on once it is
 * durable: ordering and backpressure are preserved, and a failed append
 * errors the stream (nothing after it is passed on).
 */
export function durable<T>(log: Log<T>): TransformStream<T, T> {
  return new TransformStream({
    async transform(chunk, controller) {
      await log.append(chunk);
      controller.enqueue(chunk);
    },
  });
}

/** Reads a log back as a stream of its values: pipe it into a fresh run to replay it. */
export function replay<T>(log: Log<T>, options?: { readonly after?: string }): ReadableStream<T> {
  return log.read(options).pipeThrough(project((entry: LogEntry<T>) => entry.value));
}

/** Projection: each chunk mapped to another representation, in order. */
export function project<I, O>(map: (chunk: I) => O): TransformStream<I, O> {
  return new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(map(chunk));
    },
  });
}

/** Keeps the chunks `keep` accepts, in order. */
export function filter<T>(keep: (chunk: T) => boolean): TransformStream<T, T> {
  return new TransformStream({
    transform(chunk, controller) {
      if (keep(chunk)) controller.enqueue(chunk);
    },
  });
}
