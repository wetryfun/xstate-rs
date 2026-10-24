/** Readables with a producer side: bounded with backpressure, or latest-value only. */

/** A readable whose producer can wait for room. */
export class Port<T> {
  readonly readable: ReadableStream<T>;
  #controller!: ReadableStreamDefaultController<T>;
  #cancelled = false;
  #room: (() => void) | undefined;

  constructor(highWaterMark: number) {
    this.readable = new ReadableStream<T>(
      {
        start: (c) => {
          this.#controller = c;
        },
        pull: () => {
          this.#room?.();
          this.#room = undefined;
        },
        cancel: () => {
          this.#cancelled = true;
          this.#room?.();
        },
      },
      new CountQueuingStrategy({ highWaterMark }),
    );
  }

  push(chunk: T) {
    if (!this.#cancelled) this.#controller.enqueue(chunk);
  }

  /** Waits while the queue is full and someone may still read it. */
  async room() {
    while (!this.#cancelled && (this.#controller.desiredSize ?? 1) <= 0) {
      await new Promise<void>((resolve) => (this.#room = resolve));
    }
  }

  close() {
    if (!this.#cancelled) {
      try {
        this.#controller.close();
      } catch {
        // Already closed.
      }
    }
  }
}

/** A readable of the latest value only. */
export class Latest<T> {
  readonly readable: ReadableStream<T>;
  #latest: { value: T } | undefined;
  #wake: (() => void) | undefined;
  #closed = false;
  #cancelled = false;

  constructor() {
    this.readable = new ReadableStream<T>(
      {
        pull: async (controller) => {
          while (!this.#latest && !this.#closed) await new Promise<void>((resolve) => (this.#wake = resolve));
          if (this.#latest) {
            controller.enqueue(this.#latest.value);
            this.#latest = undefined;
          } else controller.close();
        },
        cancel: () => {
          this.#cancelled = true;
        },
      },
      { highWaterMark: 0 },
    );
  }

  publish(value: T) {
    if (this.#cancelled) return;
    this.#latest = { value };
    this.#wake?.();
  }

  close() {
    this.#closed = true;
    this.#wake?.();
  }
}
