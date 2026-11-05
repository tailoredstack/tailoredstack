import { Data, Effect } from "effect";
import {
  type Cache,
  type FunnelOptions,
  type InvalidateFilter,
  type InvalidateOptions,
  type LockOptions,
  LockTimeoutError,
  type SetOptions,
} from "../cache.ts";
import type { KeyInput } from "../keys.ts";

export class CacheError extends Data.TaggedError("CacheError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {}

/** Effect-returning Cache. Driver and key failures surface as CacheError. */
export class EffectCache {
  readonly #cache: Cache;

  constructor(cache: Cache) {
    this.#cache = cache;
  }

  #try<A>(operation: string, run: () => Promise<A>): Effect.Effect<A, CacheError> {
    return Effect.tryPromise({
      try: run,
      catch: (cause) => new CacheError({ operation, cause }),
    });
  }

  prefix(segments: KeyInput): EffectCache {
    return new EffectCache(this.#cache.prefix(segments));
  }

  ttl(ms: number): EffectCache {
    return new EffectCache(this.#cache.ttl(ms));
  }

  get<T = unknown>(key: KeyInput): Effect.Effect<T | undefined, CacheError> {
    return this.#try("get", () => this.#cache.get<T>(key));
  }

  has(key: KeyInput): Effect.Effect<boolean, CacheError> {
    return this.#try("has", () => this.#cache.has(key));
  }

  set(key: KeyInput, value: unknown, options?: SetOptions): Effect.Effect<void, CacheError> {
    return this.#try("set", () => this.#cache.set(key, value, options));
  }

  delete(key: KeyInput): Effect.Effect<boolean, CacheError> {
    return this.#try("delete", () => this.#cache.delete(key));
  }

  many(keys: KeyInput[]): Effect.Effect<(unknown | undefined)[], CacheError> {
    return this.#try("many", () => this.#cache.many(keys));
  }

  touch(key: KeyInput, ttl: number): Effect.Effect<boolean, CacheError> {
    return this.#try("touch", () => this.#cache.touch(key, ttl));
  }

  add(key: KeyInput, value: unknown, options?: SetOptions): Effect.Effect<boolean, CacheError> {
    return this.#try("add", () => this.#cache.add(key, value, options));
  }

  increment(key: KeyInput, by?: number): Effect.Effect<number, CacheError> {
    return this.#try("increment", () => this.#cache.increment(key, by));
  }

  decrement(key: KeyInput, by?: number): Effect.Effect<number, CacheError> {
    return this.#try("decrement", () => this.#cache.decrement(key, by));
  }

  /** `compute` runs only on a miss or past the stale window. Its failure is a typed failure, not a throw. */
  flexible<A, E, _R>(
    key: KeyInput,
    windows: [number, number],
    compute: Effect.Effect<A, E, never>,
  ): Effect.Effect<A, E | CacheError, never> {
    return Effect.tryPromise({
      try: () => this.#cache.flexible(key, windows, () => Effect.runPromise(compute)),
      catch: (cause) => (cause instanceof CacheError ? cause : new CacheError({ operation: "flexible", cause })),
    }) as Effect.Effect<A, E | CacheError, never>;
  }

  withoutOverlapping<A, E, _R>(
    key: KeyInput,
    callback: Effect.Effect<A, E, never>,
    options?: LockOptions,
  ): Effect.Effect<A, E | CacheError | LockTimeoutError, never> {
    return Effect.tryPromise({
      try: () => this.#cache.withoutOverlapping(key, () => Effect.runPromise(callback), options),
      catch: (cause) =>
        cause instanceof LockTimeoutError ? cause : new CacheError({ operation: "withoutOverlapping", cause }),
    }) as Effect.Effect<A, E | CacheError | LockTimeoutError, never>;
  }

  /** At most `limit` Effects run concurrently for `key`. Exhaustion past `waitFor` is a LockTimeoutError. */
  funnel<A, E, _R>(
    key: KeyInput,
    options: FunnelOptions,
    callback: Effect.Effect<A, E, never>,
  ): Effect.Effect<A, E | CacheError | LockTimeoutError, never> {
    return Effect.tryPromise({
      try: () => this.#cache.funnel(key, options, () => Effect.runPromise(callback)),
      catch: (cause) => (cause instanceof LockTimeoutError ? cause : new CacheError({ operation: "funnel", cause })),
    }) as Effect.Effect<A, E | CacheError | LockTimeoutError, never>;
  }

  invalidate(filter: InvalidateFilter, options?: InvalidateOptions): Effect.Effect<number, CacheError> {
    return this.#try("invalidate", () => this.#cache.invalidate(filter, options));
  }

  getOrSet<A, E, R>(
    key: KeyInput,
    compute: Effect.Effect<A, E, R>,
    options?: SetOptions,
  ): Effect.Effect<A, E | CacheError, R> {
    return Effect.flatMap(this.get<A>(key), (hit) =>
      hit !== undefined
        ? Effect.succeed(hit)
        : Effect.flatMap(compute, (value) => Effect.as(this.set(key, value, options), value)),
    );
  }
}
