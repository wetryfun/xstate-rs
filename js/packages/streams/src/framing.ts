/**
 * Framing transforms: how records become text and back. Framing never
 * changes what a stream means: one logical record per chunk, in order.
 */

/** Text or bytes as they arrive from a socket, a file or a response body. */
export type TextChunk = string | Uint8Array;

/** Splits text into lines across chunk boundaries (`\n`, optionally `\r\n`). */
function lines(): TransformStream<TextChunk, string> {
  const decoder = new TextDecoder();
  let rest = '';
  return new TransformStream({
    transform(chunk, controller) {
      rest += typeof chunk === 'string' ? chunk : decoder.decode(chunk, { stream: true });
      let newline: number;
      while ((newline = rest.indexOf('\n')) >= 0) {
        controller.enqueue(rest.slice(0, newline).replace(/\r$/, ''));
        rest = rest.slice(newline + 1);
      }
    },
    flush(controller) {
      rest += decoder.decode();
      if (rest !== '') controller.enqueue(rest.replace(/\r$/, ''));
    },
  });
}

/** Newline-delimited JSON: logs and bulk streaming. */
export const ndjson = {
  /** Each record as one line of JSON. */
  encode<T>(): TransformStream<T, string> {
    return new TransformStream({
      transform(record, controller) {
        controller.enqueue(`${JSON.stringify(record)}\n`);
      },
    });
  },
  /** One record per non-empty line. A line that is not JSON errors the stream. */
  decode<T>(): TransformStream<TextChunk, T> {
    const split = lines();
    const parse = new TransformStream<string, T>({
      transform(line, controller) {
        if (line.trim() !== '') controller.enqueue(JSON.parse(line) as T);
      },
    });
    return { writable: split.writable, readable: split.readable.pipeThrough(parse) } as TransformStream<TextChunk, T>;
  },
};

export interface SseOptions<T> {
  /** The `event:` field of a record (none by default). */
  event?(record: T): string | undefined;
  /** The `id:` field of a record (none by default). */
  id?(record: T): string | undefined;
}

/** A server-sent event as read back. */
export interface SseMessage<T> {
  readonly data: T;
  readonly event?: string;
  readonly id?: string;
}

/** Server-sent events (`text/event-stream`): browser-friendly framing, no protocol semantics. */
export const sse = {
  /** Each record as one event with its JSON as `data`. */
  encode<T>(options: SseOptions<T> = {}): TransformStream<T, string> {
    return new TransformStream({
      transform(record, controller) {
        let text = '';
        const event = options.event?.(record);
        const id = options.id?.(record);
        if (event !== undefined) text += `event: ${event}\n`;
        if (id !== undefined) text += `id: ${id}\n`;
        for (const line of JSON.stringify(record).split('\n')) text += `data: ${line}\n`;
        controller.enqueue(`${text}\n`);
      },
    });
  },
  /** Events back into records (comments and retry fields are skipped). */
  decode<T>(): TransformStream<TextChunk, SseMessage<T>> {
    const split = lines();
    let data: string[] = [];
    let event: string | undefined;
    let id: string | undefined;
    const parse = new TransformStream<string, SseMessage<T>>({
      transform(line, controller) {
        if (line === '') {
          if (data.length > 0) {
            controller.enqueue({
              data: JSON.parse(data.join('\n')) as T,
              ...(event !== undefined && { event }),
              ...(id !== undefined && { id }),
            });
          }
          data = [];
          event = id = undefined;
          return;
        }
        if (line.startsWith(':')) return;
        const colon = line.indexOf(':');
        const field = colon < 0 ? line : line.slice(0, colon);
        const value = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '');
        if (field === 'data') data.push(value);
        else if (field === 'event') event = value;
        else if (field === 'id') id = value;
      },
    });
    return { writable: split.writable, readable: split.readable.pipeThrough(parse) } as TransformStream<
      TextChunk,
      SseMessage<T>
    >;
  },
};

/** A single JSON value (snapshots, acknowledgements): not a stream format. */
export const json = {
  /** Each chunk as one JSON text, unframed: for streams of one value. */
  encode<T>(): TransformStream<T, string> {
    return new TransformStream({
      transform(value, controller) {
        controller.enqueue(JSON.stringify(value));
      },
    });
  },
  /** Reads a whole stream of text as one JSON value. */
  async read<T>(stream: ReadableStream<TextChunk>): Promise<T> {
    const decoder = new TextDecoder();
    let text = '';
    for await (const chunk of stream) text += typeof chunk === 'string' ? chunk : decoder.decode(chunk, { stream: true });
    return JSON.parse(text + decoder.decode()) as T;
  },
};
