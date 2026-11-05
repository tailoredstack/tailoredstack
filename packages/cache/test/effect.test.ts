import { Data, Effect, Exit } from "effect";
import { describe, expect, it } from "vitest";
import { Cache } from "../src/cache.ts";
import type { Store } from "../src/contracts.ts";
import { MemoryStore } from "../src/drivers/memory.ts";
import { EffectCache } from "../src/effect/index.ts";

const make = () => new EffectCache(new Cache(new MemoryStore()));

describe("EffectCache", () => {
  it("get/set/has/delete round-trip inside Effect", async () => {
    const c = make();
    const program = Effect.gen(function* () {
      yield* c.set(["k"], { n: 1 });
      const value = yield* c.get<{ n: number }>(["k"]);
      const deleted = yield* c.delete(["k"]);
      return { value, deleted };
    });
    expect(await Effect.runPromise(program)).toEqual({ value: { n: 1 }, deleted: true });
  });

  it("get on a miss yields undefined, not a failure", async () => {
    const exit = await Effect.runPromiseExit(make().get(["missing"]));
    expect(Exit.isSuccess(exit) && exit.value).toBeUndefined();
  });

  it("getOrSet is lazy: compute effect runs only on miss", async () => {
    const c = make();
    let calls = 0;
    const compute = Effect.sync(() => ++calls);
    const program = Effect.gen(function* () {
      const a = yield* c.getOrSet(["k"], compute);
      const b = yield* c.getOrSet(["k"], compute);
      return [a, b];
    });
    expect(await Effect.runPromise(program)).toEqual([1, 1]);
    expect(calls).toBe(1);
  });

  it("invalidate returns the removed count as an Effect", async () => {
    const c = make();
    const program = Effect.gen(function* () {
      yield* c.set(["user", 1], "a");
      yield* c.set(["user", 2], "b");
      return yield* c.invalidate(["user"]);
    });
    expect(await Effect.runPromise(program)).toBe(2);
  });

  it("a failing compute surfaces as a typed failure", async () => {
    const exit = await Effect.runPromiseExit(make().getOrSet(["k"], Effect.fail(new Error("db down"))));
    expect(Exit.isFailure(exit)).toBe(true);
  });

  it("store failure maps to a tagged CacheError", async () => {
    const broken = {
      get: async () => {
        throw new Error("boom");
      },
    } as unknown as Store;
    const exit = await Effect.runPromiseExit(new EffectCache(new Cache(broken)).get(["k"]));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).toMatch(/CacheError/);
  });

  it("fluent prefix/ttl carry through the Effect wrapper", async () => {
    const base = new Cache(new MemoryStore());
    const users = new EffectCache(base).prefix(["user"]).ttl(60_000);
    await Effect.runPromise(users.set([1], "alice"));
    expect(await base.get(["user", 1])).toBe("alice");
  });
});

describe("EffectCache new operations", () => {
  it("many, touch, add, flexible and withoutOverlapping run as Effects", async () => {
    const c = make();
    const program = Effect.gen(function* () {
      yield* c.set(["a"], 1);
      const values = yield* c.many([["a"], ["b"]]);
      const added = yield* c.add(["n"], 2);
      const flex = yield* c.flexible(["f"], [1000, 1000], Effect.succeed("fresh"));
      const locked = yield* c.withoutOverlapping(["l"], Effect.succeed("done"));
      return { values, added, flex, locked };
    });
    expect(await Effect.runPromise(program)).toEqual({
      values: [1, undefined],
      added: true,
      flex: "fresh",
      locked: "done",
    });
  });
});

describe("EffectCache funnel", () => {
  it("funnel runs the Effect under a concurrency slot and returns its value", async () => {
    const c = make();
    expect(await Effect.runPromise(c.funnel(["api"], { limit: 2 }, Effect.succeed(7)))).toBe(7);
  });

  it("funnel exhaustion surfaces as LockTimeoutError, not a throw", async () => {
    const c = make();
    const holder = Effect.runPromise(
      c.funnel(
        ["api"],
        { limit: 1 },
        Effect.promise(() => new Promise((r) => setTimeout(() => r(1), 200))),
      ),
    );
    await new Promise((r) => setTimeout(r, 20));
    const exit = await Effect.runPromiseExit(c.funnel(["api"], { limit: 1, waitFor: 10 }, Effect.succeed(2)));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).toMatch(/LockTimeoutError/);
    await holder;
  });
});

class MyError extends Data.TaggedError("MyError")<{ readonly reason: string }> {}

const failing = Effect.fail(new MyError({ reason: "nope" }));

describe("EffectCache typed error passthrough", () => {
  it("getOrSet passes the caller's typed error through unchanged", async () => {
    const c = make();
    const program = c.getOrSet(["k"], failing).pipe(
      Effect.catchTag("MyError", (e) => Effect.succeed({ caught: e })),
    );
    const result = await Effect.runPromise(program);
    expect(result).toHaveProperty("caught");
    expect((result as { caught: unknown }).caught).toBeInstanceOf(MyError);
  });

  it("flexible passes the caller's typed error through unchanged", async () => {
    const c = make();
    const program = c.flexible(["f"], [1000, 1000], failing).pipe(
      Effect.catchTag("MyError", (e) => Effect.succeed(e)),
    );
    expect(await Effect.runPromise(program)).toBeInstanceOf(MyError);
  });

  it("withoutOverlapping passes the caller's typed error through unchanged", async () => {
    const c = make();
    const program = c.withoutOverlapping(["l"], failing).pipe(
      Effect.catchTag("MyError", (e) => Effect.succeed(e)),
    );
    expect(await Effect.runPromise(program)).toBeInstanceOf(MyError);
  });

  it("funnel passes the caller's typed error through unchanged", async () => {
    const c = make();
    const program = c.funnel(["api"], { limit: 1 }, failing).pipe(
      Effect.catchTag("MyError", (e) => Effect.succeed(e)),
    );
    expect(await Effect.runPromise(program)).toBeInstanceOf(MyError);
  });

  it("a defect in the caller's Effect propagates as a defect, not CacheError", async () => {
    const exit = await Effect.runPromiseExit(make().getOrSet(["k"], Effect.die("boom")));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).not.toMatch(/CacheError/);
    expect(String(exit)).toMatch(/boom/);
  });

  it("a driver failure still becomes CacheError", async () => {
    const broken = {
      get: async () => {
        throw new Error("boom");
      },
    } as unknown as Store;
    const exit = await Effect.runPromiseExit(new EffectCache(new Cache(broken)).getOrSet(["k"], Effect.succeed(1)));
    expect(String(exit)).toMatch(/CacheError/);
  });

  it("withoutOverlapping lock timeout stays LockTimeoutError", async () => {
    const c = make();
    const holder = Effect.runPromise(
      c.withoutOverlapping(
        ["held"],
        Effect.promise(() => new Promise((r) => setTimeout(() => r(1), 200))),
      ),
    );
    await new Promise((r) => setTimeout(r, 20));
    const exit = await Effect.runPromiseExit(c.withoutOverlapping(["held"], Effect.succeed(2), { waitFor: 10 }));
    expect(String(exit)).toMatch(/LockTimeoutError/);
    expect(String(exit)).not.toMatch(/CacheError/);
    await holder;
  });
});
