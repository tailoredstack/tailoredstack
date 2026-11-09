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
}

const DEFAULT_LEASE_MS = 30_000;

/** Runs one job per `processOne` call, in dequeue order. */
export class Worker<R extends JobRegistry> {
  private readonly dialect: Dialect;
  private readonly jobs: R;
  private readonly context: RegistryContext<R>;
  private readonly name: string;
  private readonly lease: number;
  private readonly now: () => number;

  constructor(options: WorkerOptions<R>) {
    this.dialect = options.dialect;
    this.jobs = options.jobs;
    this.context = options.context;
    this.name = options.queue ?? "default";
    this.lease = options.lease ?? DEFAULT_LEASE_MS;
    this.now = options.now ?? (() => Date.now());

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

  /** Dequeues and runs one job. Resolves `false` when nothing is ready. Acknowledges only on success. */
  async processOne(): Promise<boolean> {
    const now = this.now();
    const message = await this.dialect.dequeue(this.name, { now, leaseUntil: now + this.lease });
    if (message === null) return false;

    const { job, payload } = parseEnvelope(message.payload);
    const Cls = this.lookup(job);
    const instance = new Cls(payload);

    const controller = new AbortController();
    const ctx = Object.assign(Object.create(null), this.context, { signal: controller.signal }) as JobContext<
      RegistryContext<R>
    >;
    await instance.handle(ctx);

    await this.dialect.acknowledge(this.name, message.receipt, this.now());
    return true;
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
