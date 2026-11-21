import type { Dialect, Message, QueueCount, Receipt } from "./dialect.ts";
import { InvalidArgumentError, LimitError } from "./errors.ts";
import type { Job } from "./job.ts";

type JobClass = abstract new (payload: never) => Job<unknown, unknown> & { readonly __jobKey: string };

export type JobRegistry = { readonly [key: string]: JobClass };

export type BrandedRegistry<R> = {
  readonly [K in keyof R]: R[K] extends abstract new (payload: never) => { readonly __jobKey: K } ? R[K] : never;
};

export interface QueueOptions<R extends JobRegistry> {
  readonly jobs: R & BrandedRegistry<R>;
  readonly now?: () => number;
  readonly queue?: string;
}

const DEFAULT_LEASE_MS = 30_000;

export interface ReleaseOptions {
  readonly delay?: number;
}

export interface EnqueueOptions {
  readonly delay?: number;
}

const MAX_PAYLOAD_BYTES = 262144;

function isPlainStructure(value: object): boolean {
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === null || Object.getPrototypeOf(proto) === null;
}

function isPlainArray(value: unknown[]): boolean {
  const proto: unknown = Object.getPrototypeOf(value);
  if (proto === null) return false;
  return (proto as { constructor: { name?: unknown } }).constructor.name === "Array";
}

function assertRoundTrips(value: unknown, path: string, ancestors: Set<object> = new Set()): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new InvalidArgumentError(`${path} is not a finite number`);
    if (Object.is(value, -0)) throw new InvalidArgumentError(`${path} is -0, which JSON turns into 0`);
    return;
  }
  if (typeof value !== "object") throw new InvalidArgumentError(`${path} has unserializable type ${typeof value}`);

  if (ancestors.has(value)) throw new InvalidArgumentError(`${path} is a cycle`);
  ancestors.add(value);

  if (Array.isArray(value)) {
    if (!isPlainArray(value)) throw new InvalidArgumentError(`${path} is not a plain array`);
    if (Reflect.ownKeys(value).length !== value.length + 1) {
      throw new InvalidArgumentError(`${path} is sparse or has extra properties`);
    }
    for (const [index, item] of value.entries()) assertRoundTrips(item, `${path}.${index}`, ancestors);
  } else {
    if (Object.getOwnPropertySymbols(value).length > 0) throw new InvalidArgumentError(`${path} has a symbol key`);
    if (!isPlainStructure(value)) throw new InvalidArgumentError(`${path} is not a plain object`);
    for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
      if (!descriptor.enumerable) throw new InvalidArgumentError(`${path}.${key} is not enumerable`);
      if (!("value" in descriptor)) throw new InvalidArgumentError(`${path}.${key} is an accessor`);
      assertRoundTrips(descriptor.value, `${path}.${key}`, ancestors);
    }
  }

  ancestors.delete(value);
}

export class Queue<R extends JobRegistry> {
  private readonly keys = new Map<object, string>();
  private readonly now: () => number;
  private readonly name: string;

  constructor(
    private readonly dialect: Dialect,
    options: QueueOptions<R>,
  ) {
    for (const [key, cls] of Object.entries(options.jobs)) {
      const existing = this.keys.get(cls);
      if (existing !== undefined) {
        throw new InvalidArgumentError(
          `Job class ${cls.name} is registered under two keys: "${existing}" and "${key}"`,
        );
      }
      this.keys.set(cls, key);
    }
    this.now = options.now ?? (() => Date.now());
    this.name = options.queue ?? "default";
  }

  async enqueue(job: InstanceType<R[keyof R]>, options: EnqueueOptions = {}): Promise<string> {
    const key = this.keys.get(job.constructor);
    if (key === undefined) throw new InvalidArgumentError(`Job class ${job.constructor.name} is not registered`);

    const delay = options.delay ?? 0;
    if (!Number.isFinite(delay) || delay < 0) {
      throw new InvalidArgumentError(`Invalid delay ${delay}: must be a finite number of ms >= 0`);
    }
    if (delay > this.dialect.capabilities.maxDelayMs) {
      throw new LimitError(`Delay ${delay}ms exceeds maxDelayMs ${this.dialect.capabilities.maxDelayMs}`);
    }

    assertRoundTrips(job.payload, "payload");
    const serialized = JSON.stringify({ job: key, payload: job.payload });
    const bytes = new TextEncoder().encode(serialized).byteLength;
    if (bytes > MAX_PAYLOAD_BYTES) {
      throw new LimitError(`Payload is ${bytes} bytes, over the ${MAX_PAYLOAD_BYTES} byte limit`);
    }

    const now = this.now();
    return this.dialect.enqueue(this.name, serialized, { availableAt: now + delay, now });
  }

  async dequeue(lease = DEFAULT_LEASE_MS): Promise<Message | null> {
    if (!Number.isFinite(lease) || lease <= 0) {
      throw new InvalidArgumentError(`Invalid lease ${lease}: must be a finite number of ms > 0`);
    }
    assertDuration(lease, "Lease", this.dialect.capabilities.maxLeaseMs, "maxLeaseMs");
    const now = this.now();
    const leaseUntil = now + lease;
    if (leaseUntil <= now) {
      throw new InvalidArgumentError(`Invalid lease ${lease}: does not move leaseUntil past now ${now}`);
    }
    return this.dialect.dequeue(this.name, { now, leaseUntil });
  }

  async acknowledge(receipt: Receipt): Promise<boolean> {
    return this.dialect.acknowledge(this.name, receipt, this.now());
  }

  async release(receipt: Receipt, options: ReleaseOptions = {}): Promise<boolean> {
    const delay = options.delay ?? 0;
    assertDuration(delay, "Delay", this.dialect.capabilities.maxDelayMs, "maxDelayMs");
    const now = this.now();
    return this.dialect.release(this.name, receipt, { availableAt: now + delay, now });
  }

  async count(): Promise<QueueCount> {
    return this.dialect.count(this.name, this.now());
  }

  async close(): Promise<void> {
    await this.dialect.close();
  }
}

function assertDuration(value: number, title: string, max: number, maxLabel: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new InvalidArgumentError(`Invalid ${title.toLowerCase()} ${value}: must be a finite number of ms >= 0`);
  }
  if (value > max) {
    throw new LimitError(`${title} ${value}ms exceeds ${maxLabel} ${max}`);
  }
}
