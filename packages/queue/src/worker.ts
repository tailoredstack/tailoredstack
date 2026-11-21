import type { Dialect, Message } from "./dialect.ts";
import type { Job, JobContext } from "./job.ts";
import type { BrandedRegistry, JobRegistry } from "./queue.ts";

type ContextOf<C> = C extends abstract new (payload: never) => Job<unknown, infer X> ? X : never;

type UnionToIntersection<U> = (U extends unknown ? (arg: U) => void : never) extends (arg: infer I) => void ? I : never;

export type RegistryContext<R extends JobRegistry> = UnionToIntersection<ContextOf<R[keyof R]>>;

export interface WorkerOptions<R extends JobRegistry> {
  readonly dialect: Dialect;
  readonly jobs: R & BrandedRegistry<R>;
  readonly context: RegistryContext<R>;
  readonly queue?: string;
  readonly lease?: number;
  readonly timeout?: number;
  readonly now?: () => number;
  readonly tries?: number;
  readonly backoffBase?: number;
  readonly backoffCap?: number;
  readonly pollInterval?: number;
  readonly sleep?: (ms: number) => Promise<void>;
  readonly onError?: (error: unknown) => void;
}

export class JobTimeoutError extends Error {
  readonly timeoutMs: number;

  constructor(timeoutMs: number) {
    super(`Job timed out after ${timeoutMs} ms`);
    this.name = "JobTimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_TIMEOUT_CAP_MS = 25_000;
const DEFAULT_POLL_INTERVAL_MS = 1_000;
const DEFAULT_TRIES = 3;
const DEFAULT_BACKOFF_BASE_MS = 1_000;
const DEFAULT_BACKOFF_CAP_MS = 60_000;

export class Worker<R extends JobRegistry> {
  private readonly dialect: Dialect;
  private readonly jobs: R;
  private readonly context: RegistryContext<R>;
  private readonly name: string;
  private readonly lease: number;
  private readonly timeout: number;
  private readonly now: () => number;
  private readonly tries: number;
  private readonly backoffBase: number;
  private readonly backoffCap: number;
  private readonly pollInterval: number;
  private readonly injectedSleep: ((ms: number) => Promise<void>) | undefined;
  private readonly onError: (error: unknown) => void;
  private readonly stopped = Promise.withResolvers<void>();
  private stopRequested = false;
  private running = false;
  private dequeued = false;
  private dialectFailure: { error: unknown } | undefined;
  private wake: (() => void) | undefined;

  constructor(options: WorkerOptions<R>) {
    this.dialect = options.dialect;
    this.jobs = options.jobs;
    this.context = options.context;
    this.name = options.queue ?? "default";
    this.lease = options.lease ?? DEFAULT_LEASE_MS;
    this.timeout = options.timeout ?? Math.min(DEFAULT_TIMEOUT_CAP_MS, Math.floor(this.lease / 2));
    this.now = options.now ?? (() => Date.now());
    this.tries = options.tries ?? DEFAULT_TRIES;
    this.backoffBase = options.backoffBase ?? DEFAULT_BACKOFF_BASE_MS;
    this.backoffCap = options.backoffCap ?? DEFAULT_BACKOFF_CAP_MS;
    this.pollInterval = options.pollInterval ?? DEFAULT_POLL_INTERVAL_MS;
    this.injectedSleep = options.sleep;
    this.onError = options.onError ?? defaultOnError;

    if (!Number.isInteger(this.tries) || this.tries < 1) {
      throw new Error(`Invalid tries ${this.tries}: must be an integer >= 1`);
    }
    if (!Number.isFinite(this.lease) || this.lease <= 0) {
      throw new Error(`Invalid lease ${this.lease}: must be a finite number of ms > 0`);
    }
    if (!Number.isInteger(this.timeout) || this.timeout <= 0) {
      throw new Error(`Invalid timeout ${this.timeout}: must be an integer number of ms > 0`);
    }
    if (this.timeout >= this.lease) {
      throw new Error(`Invalid timeout ${this.timeout}: must be less than lease ${this.lease}`);
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

    const keys = new Map<object, string>();
    for (const [key, cls] of Object.entries(options.jobs)) {
      const existing = keys.get(cls);
      if (existing !== undefined) {
        throw new Error(`Job class ${cls.name} is registered under two keys: "${existing}" and "${key}"`);
      }
      keys.set(cls, key);
    }
  }

  async processOne(): Promise<boolean> {
    this.dialectFailure = undefined;
    const now = this.now();
    this.dequeued = false;
    let message: Message | null;
    try {
      message = await this.dialect.dequeue(this.name, { now, leaseUntil: now + this.lease });
    } catch (error) {
      this.dialectFailure = { error };
      throw error;
    }
    this.dequeued = true;
    if (message === null) return false;

    try {
      const { job, payload } = parseEnvelope(message.payload);
      const Cls = this.lookup(job);
      const instance = new Cls(payload);

      const controller = new AbortController();
      const ctx = Object.assign(Object.create(null), this.context, { signal: controller.signal }) as JobContext<
        RegistryContext<R>
      >;
      await this.runWithTimeout(instance, ctx, controller);
    } catch (error) {
      let failedAt: number | undefined;
      try {
        failedAt = this.now();
      } catch {}
      if (failedAt !== undefined) {
        try {
          if (message.attempts < this.tries) {
            await this.dialect.release(this.name, message.receipt, {
              availableAt: failedAt + this.backoffDelay(message.attempts),
              now: failedAt,
            });
          } else {
            await this.dialect.acknowledge(this.name, message.receipt, failedAt);
          }
        } catch (dialectError) {
          this.dialectFailure = { error: dialectError };
        }
      }
      throw error;
    }

    const ackAt = this.now();
    try {
      await this.dialect.acknowledge(this.name, message.receipt, ackAt);
    } catch (error) {
      this.dialectFailure = { error };
      throw error;
    }
    return true;
  }

  async run(): Promise<void> {
    if (this.running) throw new Error("Worker.run() is already running");
    this.running = true;
    try {
      while (!this.stopRequested) {
        let ran: boolean;
        this.dequeued = false;
        try {
          ran = await this.processOne();
        } catch {
          if (this.dialectFailure !== undefined) this.reportDialectError(this.dialectFailure.error);
          ran = this.dequeued;
        }
        if (!ran) await this.idle();
      }
    } finally {
      this.running = false;
    }
  }

  stop(): void {
    this.stopRequested = true;
    this.stopped.resolve();
    this.wake?.();
  }

  private idle(): Promise<void> {
    if (this.stopRequested) return Promise.resolve();
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

  private reportDialectError(error: unknown): void {
    try {
      this.onError(error);
    } catch (reporterError) {
      console.error("Worker onError threw:", reporterError);
    }
  }

  private async runWithTimeout(
    instance: Job<unknown, RegistryContext<R>>,
    ctx: JobContext<RegistryContext<R>>,
    controller: AbortController,
  ): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const error = new JobTimeoutError(this.timeout);
        controller.abort(error);
        reject(error);
      }, this.timeout);
    });
    try {
      await Promise.race([instance.handle(ctx), expired]);
    } finally {
      clearTimeout(timer);
    }
  }

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

function defaultOnError(error: unknown): void {
  console.error("Worker dialect error:", error);
}

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
