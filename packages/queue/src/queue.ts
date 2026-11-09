import type { Dialect } from "./dialect.ts";
import type { Job } from "./job.ts";

/**
 * A concrete job class that declares its wire key as a type-only brand:
 * `declare readonly __jobKey: "SendMail";`. The brand is the only compile-time link
 * between a class and its registry key. It has no runtime field.
 *
 * Known bypass: a copied brand (`declare readonly __jobKey: "SendMail"` on a class that
 * is not registered, with the same payload shape as the registered `SendMail`) passes
 * compile. The runtime constructor check in `enqueue` is the second layer and rejects
 * any class that is not in `jobs`.
 *
 * The compile-time brand already rejects `{ Alias: SendMail }` under a different key; the
 * constructor's duplicate-class check is the runtime second layer for the same mistake.
 */
type JobClass = abstract new (payload: never) => Job<unknown, unknown> & { readonly __jobKey: string };

/** Registry: wire key to job class. */
export type JobRegistry = { readonly [key: string]: JobClass };

/** Rejects any entry whose class brand does not equal its registry key. */
export type BrandedRegistry<R> = {
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

/**
 * Limit on the serialized envelope `{"job":...,"payload":...}`, the exact string the backend
 * stores. The SQS 256 KiB cap applies to this string, not to `payload` alone.
 */
const MAX_PAYLOAD_BYTES = 262144;

/**
 * True when `value`'s prototype is either null or an object whose own prototype is null,
 * which is how `Object.prototype` looks from every realm. Class instances fail: their
 * prototype's prototype is an ancestor class prototype, never null.
 */
function isPlainStructure(value: object): boolean {
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === null || Object.getPrototypeOf(proto) === null;
}

/**
 * True when `value`'s prototype is some realm's `Array.prototype`: an object whose own
 * `constructor` is named `Array` and whose prototype is an `Object.prototype` (so a
 * `class extends Array` fails, since its prototype's `constructor` is the subclass). Null-prototype
 * arrays fail at the first check. Cross-realm arrays pass: their name is still `Array`.
 */
function isPlainArray(value: unknown[]): boolean {
  const proto: unknown = Object.getPrototypeOf(value);
  if (proto === null) return false;
  return (proto as { constructor: { name?: unknown } }).constructor.name === "Array";
}

/**
 * Throws unless `value` survives `JSON.stringify` and `JSON.parse` unchanged.
 * Plain objects, arrays, strings, finite numbers (not -0), booleans, and null only.
 * `ancestors` holds the current path so cycles fail; shared non-cyclic references pass.
 */
function assertRoundTrips(value: unknown, path: string, ancestors: Set<object> = new Set()): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`${path} is not a finite number`);
    if (Object.is(value, -0)) throw new Error(`${path} is -0, which JSON turns into 0`);
    return;
  }
  if (typeof value !== "object") throw new Error(`${path} has unserializable type ${typeof value}`);

  if (ancestors.has(value)) throw new Error(`${path} is a cycle`);
  ancestors.add(value);

  if (Array.isArray(value)) {
    if (!isPlainArray(value)) throw new Error(`${path} is not a plain array`);
    if (Reflect.ownKeys(value).length !== value.length + 1) {
      throw new Error(`${path} is sparse or has extra properties`);
    }
    for (const [index, item] of value.entries()) assertRoundTrips(item, `${path}.${index}`, ancestors);
  } else {
    // Plain data only: no class instances with methods or internal slots. The prototype is
    // checked by shape (`isPlainStructure`), not identity, so objects from another realm pass.
    if (Object.getOwnPropertySymbols(value).length > 0) throw new Error(`${path} has a symbol key`);
    if (!isPlainStructure(value)) throw new Error(`${path} is not a plain object`);
    for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
      if (!descriptor.enumerable) throw new Error(`${path}.${key} is not enumerable`);
      if (!("value" in descriptor)) throw new Error(`${path}.${key} is an accessor`);
      assertRoundTrips(descriptor.value, `${path}.${key}`, ancestors);
    }
  }

  ancestors.delete(value);
}

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
    // Same class under two keys would silently overwrite the first mapping. The brand catches
    // most of this at compile time; this check is the runtime second layer.
    for (const [key, cls] of Object.entries(options.jobs)) {
      const existing = this.keys.get(cls);
      if (existing !== undefined) {
        throw new Error(`Job class ${cls.name} is registered under two keys: "${existing}" and "${key}"`);
      }
      this.keys.set(cls, key);
    }
    this.now = options.now ?? (() => Date.now());
    this.name = options.queue ?? "default";
  }

  async enqueue(job: InstanceType<R[keyof R]>, options: EnqueueOptions = {}): Promise<string> {
    const key = this.keys.get(job.constructor);
    if (key === undefined) throw new Error(`Job class ${job.constructor.name} is not registered`);

    const delay = options.delay ?? 0;
    if (!Number.isFinite(delay) || delay < 0) {
      throw new Error(`Invalid delay ${delay}: must be a finite number of ms >= 0`);
    }
    if (delay > this.dialect.capabilities.maxDelayMs) {
      throw new Error(`Delay ${delay}ms exceeds maxDelayMs ${this.dialect.capabilities.maxDelayMs}`);
    }

    assertRoundTrips(job.payload, "payload");
    const serialized = JSON.stringify({ job: key, payload: job.payload });
    const bytes = new TextEncoder().encode(serialized).byteLength;
    if (bytes > MAX_PAYLOAD_BYTES) {
      throw new Error(`Payload is ${bytes} bytes, over the ${MAX_PAYLOAD_BYTES} byte limit`);
    }

    const now = this.now();
    return this.dialect.enqueue(this.name, serialized, { availableAt: now + delay, now });
  }
}
