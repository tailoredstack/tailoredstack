import { it } from "@effect/vitest";
import { Data, Effect, Exit, Fiber } from "effect";
import { describe, expect } from "vitest";
import { Cache } from "../src/cache.ts";
import type { Store } from "../src/contracts.ts";
import { MemoryStore } from "../src/drivers/memory.ts";
import { EffectCache } from "../src/effect/index.ts";

const make = () => new EffectCache(new Cache(new MemoryStore()));

describe("EffectCache", () => {
  it.effect("get/set/has/delete round-trip inside Effect", () =>
    Effect.gen(function* () {
      const c = make();
      yield* c.set(["k"], { n: 1 });
      const value = yield* c.get<{ n: number }>(["k"]);
      const deleted = yield* c.delete(["k"]);
      expect({ value, deleted }).toEqual({ value: { n: 1 }, deleted: true });
    }),
  );

  it.effect("get on a miss yields undefined, not a failure", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(make().get(["missing"]));
      expect(Exit.isSuccess(exit) && exit.value).toBeUndefined();
    }),
  );

  it.effect("getOrSet is lazy: compute effect runs only on miss", () =>
    Effect.gen(function* () {
      const c = make();
      let calls = 0;
      const compute = Effect.sync(() => ++calls);
      const a = yield* c.getOrSet(["k"], compute);
      const b = yield* c.getOrSet(["k"], compute);
      expect([a, b]).toEqual([1, 1]);
      expect(calls).toBe(1);
    }),
  );

  it.effect("invalidate returns the removed count as an Effect", () =>
    Effect.gen(function* () {
      const c = make();
      yield* c.set(["user", 1], "a");
      yield* c.set(["user", 2], "b");
      expect(yield* c.invalidate(["user"])).toBe(2);
    }),
  );

  it.effect("a failing compute surfaces as a typed failure", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(make().getOrSet(["k"], Effect.fail(new Error("db down"))));
      expect(Exit.isFailure(exit)).toBe(true);
    }),
  );

  it.effect("store failure maps to a tagged CacheError", () =>
    Effect.gen(function* () {
      const broken = {
        get: async () => {
          throw new Error("boom");
        },
      } as unknown as Store;
      const exit = yield* Effect.exit(new EffectCache(new Cache(broken)).get(["k"]));
      expect(Exit.isFailure(exit)).toBe(true);
      expect(String(exit)).toMatch(/CacheError/);
    }),
  );

  it.effect("fluent prefix/ttl carry through the Effect wrapper", () =>
    Effect.gen(function* () {
      const base = new Cache(new MemoryStore());
      const users = new EffectCache(base).prefix(["user"]).ttl(60_000);
      yield* users.set([1], "alice");
      expect(yield* Effect.promise(() => base.get(["user", 1]))).toBe("alice");
    }),
  );
});

describe("EffectCache new operations", () => {
  it.effect("many, touch, add, flexible and withoutOverlapping run as Effects", () =>
    Effect.gen(function* () {
      const c = make();
      yield* c.set(["a"], 1);
      const values = yield* c.many([["a"], ["b"]]);
      const added = yield* c.add(["n"], 2);
      const flex = yield* c.flexible(["f"], [1000, 1000], Effect.succeed("fresh"));
      const locked = yield* c.withoutOverlapping(["l"], Effect.succeed("done"));
      expect({ values, added, flex, locked }).toEqual({
        values: [1, undefined],
        added: true,
        flex: "fresh",
        locked: "done",
      });
    }),
  );
});

describe("EffectCache funnel", () => {
  it.effect("funnel runs the Effect under a concurrency slot and returns its value", () =>
    Effect.gen(function* () {
      expect(yield* make().funnel(["api"], { limit: 2 }, Effect.succeed(7))).toBe(7);
    }),
  );

  // Real time: the funnel's waitFor is enforced by the cache's own timers, not the Effect TestClock.
  it.live("funnel exhaustion surfaces as LockTimeoutError, not a throw", () =>
    Effect.gen(function* () {
      const c = make();
      const holder = yield* Effect.forkChild(
        c.funnel(
          ["api"],
          { limit: 1 },
          Effect.promise(() => new Promise((r) => setTimeout(() => r(1), 200))),
        ),
      );
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 20)));
      const exit = yield* Effect.exit(c.funnel(["api"], { limit: 1, waitFor: 10 }, Effect.succeed(2)));
      expect(Exit.isFailure(exit)).toBe(true);
      expect(String(exit)).toMatch(/LockTimeoutError/);
      yield* Fiber.join(holder);
    }),
  );
});

class MyError extends Data.TaggedError("MyError")<{ readonly reason: string }> {}

const failing = Effect.fail(new MyError({ reason: "nope" }));

describe("EffectCache typed error passthrough", () => {
  it.effect("getOrSet passes the caller's typed error through unchanged", () =>
    Effect.gen(function* () {
      const c = make();
      const result = yield* c
        .getOrSet(["k"], failing)
        .pipe(Effect.catchTag("MyError", (e) => Effect.succeed({ caught: e })));
      expect(result).toHaveProperty("caught");
      expect((result as { caught: unknown }).caught).toBeInstanceOf(MyError);
    }),
  );

  it.effect("flexible passes the caller's typed error through unchanged", () =>
    Effect.gen(function* () {
      const c = make();
      const result = yield* c
        .flexible(["f"], [1000, 1000], failing)
        .pipe(Effect.catchTag("MyError", (e) => Effect.succeed(e)));
      expect(result).toBeInstanceOf(MyError);
    }),
  );

  it.effect("withoutOverlapping passes the caller's typed error through unchanged", () =>
    Effect.gen(function* () {
      const c = make();
      const result = yield* c
        .withoutOverlapping(["l"], failing)
        .pipe(Effect.catchTag("MyError", (e) => Effect.succeed(e)));
      expect(result).toBeInstanceOf(MyError);
    }),
  );

  it.effect("funnel passes the caller's typed error through unchanged", () =>
    Effect.gen(function* () {
      const c = make();
      const result = yield* c
        .funnel(["api"], { limit: 1 }, failing)
        .pipe(Effect.catchTag("MyError", (e) => Effect.succeed(e)));
      expect(result).toBeInstanceOf(MyError);
    }),
  );

  it.effect("a defect in the caller's Effect propagates as a defect, not CacheError", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(make().getOrSet(["k"], Effect.die("boom")));
      expect(Exit.isFailure(exit)).toBe(true);
      expect(String(exit)).not.toMatch(/CacheError/);
      expect(String(exit)).toMatch(/boom/);
    }),
  );

  it.effect("a driver failure still becomes CacheError", () =>
    Effect.gen(function* () {
      const broken = {
        get: async () => {
          throw new Error("boom");
        },
      } as unknown as Store;
      const exit = yield* Effect.exit(new EffectCache(new Cache(broken)).getOrSet(["k"], Effect.succeed(1)));
      expect(String(exit)).toMatch(/CacheError/);
    }),
  );

  // Real time: waitFor is enforced by the cache's own timers, not the Effect TestClock.
  it.live("withoutOverlapping lock timeout stays LockTimeoutError", () =>
    Effect.gen(function* () {
      const c = make();
      const holder = yield* Effect.forkChild(
        c.withoutOverlapping(
          ["held"],
          Effect.promise(() => new Promise((r) => setTimeout(() => r(1), 200))),
        ),
      );
      yield* Effect.promise(() => new Promise((r) => setTimeout(r, 20)));
      const exit = yield* Effect.exit(c.withoutOverlapping(["held"], Effect.succeed(2), { waitFor: 10 }));
      expect(String(exit)).toMatch(/LockTimeoutError/);
      expect(String(exit)).not.toMatch(/CacheError/);
      yield* Fiber.join(holder);
    }),
  );
});
