import type { Dialect } from "./dialect.ts";
import type { Job } from "./job.ts";

/**
 * A concrete job class that declares its wire key as a type-only brand:
 * `declare readonly __jobKey: "SendMail";`. The brand is the only compile-time link
 * between a class and its registry key. It has no runtime field.
 */
type JobClass = abstract new (payload: never) => Job<unknown> & { readonly __jobKey: string };

/** Registry: wire key to job class. */
export type JobRegistry = { readonly [key: string]: JobClass };

/** Rejects any entry whose class brand does not equal its registry key. */
type BrandedRegistry<R> = {
  readonly [K in keyof R]: R[K] extends abstract new (payload: never) => { readonly __jobKey: K } ? R[K] : never;
};

export interface QueueOptions<R extends JobRegistry> {
  readonly jobs: R & BrandedRegistry<R>;
  /** Injected clock, epoch ms. Defaults to `Date.now`. */
  readonly now?: () => number;
  /** Queue name passed to every dialect call. Defaults to `default`. */
  readonly queue?: string;
}

export interface EnqueueOptions {
  /** Relative delay in ms. Omitted or 0 means available now. */
  readonly delay?: number;
}

const MAX_PAYLOAD_BYTES = 262144;

/**
 * Typed client that enqueues registered jobs. Only enqueue lives here; dequeue,
 * acknowledge, release, count, and close are out of scope (TS-90 and later).
 */
export class Queue<R extends JobRegistry> {
  private readonly keys = new Map<object, string>();
  private readonly now: () => number;
  private readonly name: string;

  constructor(
    private readonly dialect: Dialect,
    options: QueueOptions<R>,
  ) {
    for (const [key, cls] of Object.entries(options.jobs)) this.keys.set(cls, key);
    this.now = options.now ?? (() => Date.now());
    this.name = options.queue ?? "default";
  }

  async enqueue(job: InstanceType<R[keyof R]>, options: EnqueueOptions = {}): Promise<string> {
    const key = this.keys.get(job.constructor);
    if (key === undefined) throw new Error(`Job class ${job.constructor.name} is not registered`);

    const delay = options.delay ?? 0;
    if (delay > this.dialect.capabilities.maxDelayMs) {
      throw new Error(`Delay ${delay}ms exceeds maxDelayMs ${this.dialect.capabilities.maxDelayMs}`);
    }

    const serialized = JSON.stringify({ job: key, payload: job.payload });
    const bytes = new TextEncoder().encode(serialized).byteLength;
    if (bytes > MAX_PAYLOAD_BYTES) {
      throw new Error(`Payload is ${bytes} bytes, over the ${MAX_PAYLOAD_BYTES} byte limit`);
    }

    const now = this.now();
    return this.dialect.enqueue(this.name, serialized, { availableAt: now + delay, now });
  }
}
