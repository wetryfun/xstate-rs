/**
 * Writable compositions that keep acknowledgements end to end: a write
 * resolves when the target's write resolves, which piping through a
 * `TransformStream` does not give.
 */
import type { Log } from './log.ts';

/**
 * A writable that maps each chunk with `map` (which may be async: append it
 * to a log, stamp it, ...) and writes the result to `target`. A write
 * resolves once `target` accepted the result, so whoever writes learns when
 * the chunk was processed; closing and aborting propagate to `target`.
 */
export function through<I, O>(target: WritableStream<O>, map: (chunk: I) => O | Promise<O>): WritableStream<I> {
  const writer = target.getWriter();
  return new WritableStream<I>({
    async write(chunk) {
      await writer.write(await map(chunk));
    },
    close: () => writer.close(),
    abort: (reason) => writer.abort(reason),
  });
}

/** `durable(log)` in front of `target`, acknowledged end to end: each chunk is appended, then written. */
export function durableWritable<T>(target: WritableStream<T>, log: Log<T>): WritableStream<T> {
  return through(target, async (chunk: T) => {
    await log.append(chunk);
    return chunk;
  });
}

/** Many senders into one writable. */
export interface FanIn<T> {
  /** A new writable for one sender; its writes are serialized with every other sender's. */
  writable(): WritableStream<T>;
  /** Closes the target once the writes so far are done. */
  close(): Promise<void>;
}

/**
 * Serializes concurrent senders into `target`, in the order their writes
 * arrive. Each sender's write resolves when `target` took it.
 */
export function fanIn<T>(target: WritableStream<T>): FanIn<T> {
  const writer = target.getWriter();
  let chain: Promise<unknown> = Promise.resolve();
  const write = (chunk: T) => {
    const written = chain.then(() => writer.write(chunk));
    chain = written.catch(() => {});
    return written;
  };
  return {
    writable: () => new WritableStream<T>({ write }),
    close: async () => {
      await chain;
      await writer.close();
    },
  };
}
