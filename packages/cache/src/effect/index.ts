import { type Cause, Data, Effect, Exit } from "effect";
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

/** Private: carries a caller Effect's failure Cause across the Promise boundary so its typed error is re-raised. */
class CallbackFailure {
  constructor(readonly cause: Cause.Cause<unknown>) {}
}

/** Runs a caller Effect as a Promise. Its failure is rethrown as CallbackFailure, never wrapped. */
const runCallback = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromiseExit(effect).then((exit) => {
    if (Exit.isSuccess(exit)) return exit.value;
    throw new CallbackFailure(exit.cause);
  });

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

  /** Callback failures re-raise unchanged; LockTimeoutError passes through; anything else is a CacheError. */
  #guard<A, E, L extends LockTimeoutError = never>(
    operation: string,
    run: () => Promise<A>,
  ): Effect.Effect<A, E | CacheError | L> {
    return Effect.tryPromise({ try: run, catch: (cause) => cause }).pipe(
      Effect.catch((cause): Effect.Effect<never, E | CacheError | L> => {
        if (cause instanceof CallbackFailure) return Effect.failCause(cause.cause as Cause.Cause<E>);
        if (cause instanceof LockTimeoutError) return Effect.fail(cause as L);
        return Effect.fail(new CacheError({ operation, cause }));
      }),
    );
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
    return this.#guard<A, E>("flexible", () => this.#cache.flexible(key, windows, () => runCallback(compute)));
  }

  withoutOverlapping<A, E, _R>(
    key: KeyInput,
    callback: Effect.Effect<A, E, never>,
    options?: LockOptions,
  ): Effect.Effect<A, E | CacheError | LockTimeoutError, never> {
    return this.#guard<A, E, LockTimeoutError>("withoutOverlapping", () =>
      this.#cache.withoutOverlapping(key, () => runCallback(callback), options),
    );
  }

  /** At most `limit` Effects run concurrently for `key`. Exhaustion past `waitFor` is a LockTimeoutError. */
  funnel<A, E, _R>(
    key: KeyInput,
    options: FunnelOptions,
    callback: Effect.Effect<A, E, never>,
  ): Effect.Effect<A, E | CacheError | LockTimeoutError, never> {
    return this.#guard<A, E, LockTimeoutError>("funnel", () =>
      this.#cache.funnel(key, options, () => runCallback(callback)),
    );
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
