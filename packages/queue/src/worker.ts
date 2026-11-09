import type { Dialect } from "./dialect.ts";
import type { Job, JobContext } from "./job.ts";
import type { BrandedRegistry, JobRegistry } from "./queue.ts";

/** The context type a job class expects, read from its `Job<Payload, Ctx>` base. */
type ContextOf<C> = C extends abstract new (payload: never) => Job<unknown, infer X> ? X : never;

/** Union-to-intersection: `U` becomes `A & B` when `U` is `A | B`. */
type UnionToIntersection<U> = (U extends unknown ? (arg: U) => void : never) extends (arg: infer I) => void ? I : never;

/**
 * The context every registered job needs together: the intersection of every job's `Ctx`.
 * A union here would let a context satisfy one job while missing another job's field.
 */
export type RegistryContext<R extends JobRegistry> = UnionToIntersection<ContextOf<R[keyof R]>>;

export interface WorkerOptions<R extends JobRegistry> {
  readonly dialect: Dialect;
  readonly jobs: R & BrandedRegistry<R>;
  /** Passed to every job run, merged with a fresh `signal`. */
  readonly context: RegistryContext<R>;
  /** Queue name. Defaults to `default`. */
  readonly queue?: string;
  /** Lease length in ms. Defaults to 30_000. */
  readonly lease?: number;
  /** Injected clock, epoch ms. Defaults to `Date.now`. */
  readonly now?: () => number;
  /** Total attempts per message, including the first. Integer >= 1. Defaults to 3. */
  readonly tries?: number;
  /** Delay in ms after the first failed attempt. Finite, > 0. Defaults to 1_000. */
  readonly backoffBase?: number;
  /** Upper bound in ms on any retry delay. Finite, >= `backoffBase`. Defaults to 60_000. */
  readonly backoffCap?: number;
  /** Idle wait in ms between empty dequeues. Finite, > 0. Defaults to 1_000. */
  readonly pollInterval?: number;
  /** Injected idle sleep. Defaults to a `setTimeout`-based sleep. */
  readonly sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_POLL_INTERVAL_MS = 1_000;
const DEFAULT_TRIES = 3;
const DEFAULT_BACKOFF_BASE_MS = 1_000;
const DEFAULT_BACKOFF_CAP_MS = 60_000;

/** Runs one job per `processOne` call, in dequeue order. */
export class Worker<R extends JobRegistry> {
  private readonly dialect: Dialect;
  private readonly jobs: R;
  private readonly context: RegistryContext<R>;
  private readonly name: string;
  private readonly lease: number;
  private readonly now: () => number;
  private readonly tries: number;
  private readonly backoffBase: number;
  private readonly backoffCap: number;
  private readonly pollInterval: number;
  private readonly injectedSleep: ((ms: number) => Promise<void>) | undefined;
  private readonly stopped = Promise.withResolvers<void>();
  private stopRequested = false;
  private running = false;
  private dequeued = false;
  private wake: (() => void) | undefined;

  constructor(options: WorkerOptions<R>) {
    this.dialect = options.dialect;
    this.jobs = options.jobs;
    this.context = options.context;
    this.name = options.queue ?? "default";
    this.lease = options.lease ?? DEFAULT_LEASE_MS;
    this.now = options.now ?? (() => Date.now());
    this.tries = options.tries ?? DEFAULT_TRIES;
    this.backoffBase = options.backoffBase ?? DEFAULT_BACKOFF_BASE_MS;
    this.backoffCap = options.backoffCap ?? DEFAULT_BACKOFF_CAP_MS;
    this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL_MS;
    this.injectedSleep = options.sleep;

    if (!Number.isInteger(this.tries) || this.tries < 1) {
      throw new Error(`Invalid tries ${this.tries}: must be an integer >= 1`);
    }
    if (!Number.isFinite(this.backoffBase) || this.backoffBase <= 0) {
      throw new Error(`Invalid backoffBase ${this.backoffBase}: must be a finite number of ms > 0`);
    }
    if (!Number.isFinite(this.backoffCap) || this.backoffCap < this.backoffBase) {
      throw new Error(`Invalid backoffCap ${this.backoffCap}: must be a finite number of ms >= backoffBase`);
    }
    if (!Number.isFinite(this.pollInterval) || this.pollInterval <= 0) {
      throw new Error(`Invalid pollInterval ${this.pollInterval}: must be a finite number of ms > 0`);
    }

    // Same class under two keys would run the wrong key's mapping. Mirrors Queue's runtime check.
    const keys = new Map<object, string>();
    for (const [key, cls] of Object.entries(options.jobs)) {
      const existing = keys.get(cls);
      if (existing !== undefined) {
        throw new Error(`Job class ${cls.name} is registered under two keys: "${existing}" and "${key}"`);
      }
      keys.set(cls, key);
    }
  }

  /**
   * Dequeues and runs one job. Resolves `false` when nothing is ready. Acknowledges on success.
   * On any failure after dequeue (envelope, unknown key, or handle): releases with capped exponential backoff
   * while attempts < tries, else acknowledges (drops). Always rethrows the original error.
   */
  async processOne(): Promise<boolean> {
    const now = this.now();
    this.dequeued = false;
    const message = await this.dialect.dequeue(this.name, { now, leaseUntil: now + this.lease });
    this.dequeued = true;
    if (message === null) return false;

    try {
      // Envelope and lookup failures take the same retry path as handle failures (TS-78 release or ack).
      const { job, payload } = parseEnvelope(message.payload);
      const Cls = this.lookup(job);
      const instance = new Cls(payload);

      const controller = new AbortController();
      const ctx = Object.assign(Object.create(null), this.context, { signal: controller.signal }) as JobContext<
        RegistryContext<R>
      >;
      await instance.handle(ctx);
    } catch (error) {
      // The original error always propagates. A stale receipt (false) is ignored as before.
      // Any rejection during bookkeeping, including the injected clock, is discarded so it cannot replace the handle error.
      try {
        const failedAt = this.now();
        if (message.attempts < this.tries) {
          await this.dialect.release(this.name, message.receipt, {
            availableAt: failedAt + this.backoffDelay(message.attempts),
            now: failedAt,
          });
        } else {
          await this.dialect.acknowledge(this.name, message.receipt, failedAt);
        }
      } catch {
        // Intentionally discarded: the handle error is the one the caller needs.
      }
      throw error;
    }

    await this.dialect.acknowledge(this.name, message.receipt, this.now());
    return true;
  }

  /**
   * Loops `processOne()` until `stop()`. A job that ran is followed by an immediate dequeue; an empty dequeue
   * waits `pollInterval`. A handler error is caught and the loop continues, so run() never rejects for it.
   * Known gap until TS-92: handler errors are dropped here, with no onError hook.
   * Rejects if run() is already active. A stop() before run() resolves it without dequeuing.
   */
  async run(): Promise<void> {
    if (this.running) throw new Error("Worker.run() is already running");
    this.running = true;
    try {
      while (!this.stopRequested) {
        let ran: boolean;
        try {
          ran = await this.processOne();
        } catch {
          // Known gap until TS-92: the error is dropped. processOne has already released or acknowledged the message.
          // A dequeue failure (no message yet) falls through to the idle wait so a dead dialect cannot hot-loop.
          ran = this.dequeued;
        }
        if (!ran) await this.idle();
      }
    } finally {
      this.running = false;
    }
  }

  /** Requests a stop. The in-flight job finishes, and an idle wait ends at once. Idempotent. */
  stop(): void {
    this.stopRequested = true;
    this.stopped.resolve();
    this.wake?.();
  }

  /** Idle wait that ends early when stop() is called. The default timer is cleared on stop so it does not keep the process alive. */
  private idle(): Promise<void> {
    if (this.injectedSleep) return Promise.race([this.injectedSleep(this.pollInterval), this.stopped.promise]);
    return new Promise<void>((resolve) => {
      const done = (): void => {
        clearTimeout(timer);
        this.wake = undefined;
        resolve();
      };
      this.wake = done;
      const timer = setTimeout(done, this.pollInterval);
    });
  }

  /** Delay after the failed `attempt` (1-based): min(cap, base * 2^(attempt - 1)). No jitter. */
  private backoffDelay(attempt: number): number {
    return Math.min(this.backoffCap, this.backoffBase * 2 ** (attempt - 1));
  }

  private lookup(key: string): new (payload: unknown) => Job<unknown, RegistryContext<R>> {
    if (!Object.hasOwn(this.jobs, key)) throw new Error(`Job key "${key}" is not registered`);
    return this.jobs[key] as unknown as new (
      payload: unknown,
    ) => Job<unknown, RegistryContext<R>>;
  }
}

/** Parses `{ job, payload }`. Throws without acknowledging anything when the envelope is malformed. */
function parseEnvelope(raw: string): { job: string; payload: unknown } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("Malformed job envelope: not valid JSON");
  }
  if (typeof parsed !== "object" || parsed === null || !("job" in parsed) || !("payload" in parsed)) {
    throw new Error("Malformed job envelope: expected { job, payload }");
  }
  if (typeof parsed.job !== "string") throw new Error("Malformed job envelope: job must be a string");
  return { job: parsed.job, payload: parsed.payload };
}
