import { describe, expect, it } from "vitest";
import { Cache, LockTimeoutError, MemoryStore, type Store } from "../src/index.ts";
import { encodeKey } from "../src/keys.ts";
import { type Clock, makeClock } from "../src/testing/index.ts";

// Good actor: uses the API as documented. Bad actor: hostile or malformed input. Idiot actor: a plausible misuse.
// The suite is driver-agnostic: each driver's test file calls actorSuite(name, make) with its own store factory.
export function actorSuite(name: string, make: (clock: Clock) => Store): void {
  const setup = () => {
    const clock = makeClock();
    const store = make(clock);
    return { clock, store, cache: new Cache(store, { now: clock.now }) };
  };

  describe(`${name}: actors`, () => {
    describe(`${name}: idiot actor: bad step`, () => {
      it.each([
        ["fractional", 1.5],
        ["NaN", Number.NaN],
        ["above MAX_SAFE_INTEGER", Number.MAX_SAFE_INTEGER + 2],
      ])("increment rejects a %s step and leaves the stored value unchanged", async (_label, by) => {
        const { cache } = setup();
        await cache.set(["n"], 10);
        await expect(cache.increment(["n"], by)).rejects.toThrow(RangeError);
        expect(await cache.get(["n"])).toBe(10);
      });
    });

    describe("good actor", () => {
      it("getOrSet with a key that is already set never calls compute", async () => {
        const { cache } = setup();
        await cache.set(["k"], "cached");
        let calls = 0;
        expect(
          await cache.getOrSet(["k"], () => {
            calls++;
            return "fresh";
          }),
        ).toBe("cached");
        expect(calls).toBe(0);
      });

      it("invalidating a prefix does not remove a sibling that shares its text", async () => {
        const { cache } = setup();
        await cache.set(["user", 1], "a");
        await cache.set(["users", 1], "b");
        await cache.invalidate(["user"]);
        expect(await cache.get(["users", 1])).toBe("b");
      });
    });

    describe("bad actor", () => {
      it("a key segment that is the __proto__ string is stored as data, not as a prototype", async () => {
        const { cache } = setup();
        await cache.set(["__proto__"], "v");
        expect(await cache.get(["__proto__"])).toBe("v");
        expect(({} as Record<string, unknown>).polluted).toBeUndefined();
      });

      it("a value with __proto__ as an own key round-trips without polluting Object.prototype", async () => {
        const { cache } = setup();
        const hostile = JSON.parse('{"__proto__":{"polluted":1}}');
        await cache.set(["h"], hostile);
        const back = await cache.get<Record<string, unknown>>(["h"]);
        expect(Object.getPrototypeOf(back)).toBe(Object.prototype);
        expect(({} as Record<string, unknown>).polluted).toBeUndefined();
      });

      it("a toJSON that throws on set rejects and stores nothing", async () => {
        const { cache } = setup();
        class Boom {
          toJSON(): never {
            throw new Error("toJSON exploded");
          }
        }
        await expect(cache.set(["b"], new Boom())).rejects.toThrow("toJSON exploded");
        expect(await cache.has(["b"])).toBe(false);
      });

      it("a getOrSet compute that returns a thenable which rejects does not cache", async () => {
        const { cache } = setup();
        // biome-ignore lint/suspicious/noThenProperty: a thenable is the hostile input under test
        const thenable = { then: (_res: unknown, rej: (e: Error) => void) => rej(new Error("late failure")) };
        await expect(cache.getOrSet(["t"], () => thenable as never)).rejects.toThrow("late failure");
        expect(await cache.has(["t"])).toBe(false);
      });

      it("a lock held past lockFor by a crashed holder is taken over, and the old holder cannot free the new one", async () => {
        const store = new MemoryStore();
        const cache = new Cache(store);
        // Holder A's lease, expired (1ms) before B tries to acquire.
        const lockKey = `lock:${encodeKey(["x"])}`;
        await store.add(lockKey, "A-token", 1);
        await new Promise((r) => setTimeout(r, 5));
        await cache.withoutOverlapping(["x"], async () => {
          // B holds the lock now. A's late release must not free it.
          expect(await store.forgetIf(lockKey, "A-token")).toBe(false);
          expect(await store.get(lockKey)).toBeDefined();
        });
      });

      it("a concurrent flood of set calls on one key leaves exactly one final value", async () => {
        const { cache } = setup();
        await Promise.all(Array.from({ length: 200 }, (_, i) => cache.set(["flood"], i)));
        const final = await cache.get<number>(["flood"]);
        expect(final).toBeGreaterThanOrEqual(0);
        expect(final).toBeLessThan(200);
      });
    });

    describe("idiot actor", () => {
      it("withoutOverlapping with waitFor 0 on a held lock throws LockTimeoutError, not a hang", async () => {
        const cache = new Cache(new MemoryStore());
        const held = cache.withoutOverlapping(["h"], () => new Promise((r) => setTimeout(r, 50)));
        await expect(cache.withoutOverlapping(["h"], () => 1, { waitFor: 0 })).rejects.toThrow(LockTimeoutError);
        await held;
      });

      it("invalidate with an empty-string key segment removes only keys that carry that segment", async () => {
        const { cache } = setup();
        await cache.set([""], "empty");
        await cache.set(["x"], "other");
        expect(await cache.invalidate([""], { exact: true })).toBe(1);
        expect(await cache.get(["x"])).toBe("other");
      });

      it("calling increment on a key holding a string rejects with TypeError and leaves the string", async () => {
        const { cache } = setup();
        await cache.set(["s"], "text");
        await expect(cache.increment(["s"])).rejects.toThrow(TypeError);
        expect(await cache.get(["s"])).toBe("text");
      });

      it("flexible with equal fresh and stale windows stops serving stale after the window", async () => {
        const { clock, cache } = setup();
        let n = 0;
        const compute = () => ++n;
        expect(await cache.flexible(["f"], [1000, 1000], compute)).toBe(1);
        clock.advance(1001);
        expect(await cache.flexible(["f"], [1000, 1000], compute)).toBe(2);
      });

      it("a scoped cache used after invalidate([]) on its parent still writes under its own prefix", async () => {
        const { cache } = setup();
        const users = cache.prefix(["user"]);
        await users.set([1], "a");
        await cache.invalidate([]);
        await users.set([2], "b");
        expect(await users.get([2])).toBe("b");
      });
    });

    describe("bad actor: ttl function returns an unusable value", () => {
      it.each([
        ["NaN", Number.NaN],
        ["zero", 0],
        ["negative", -1],
        ["Infinity", Number.POSITIVE_INFINITY],
      ])("set rejects a ttl function returning %s and stores nothing", async (_label, ttl) => {
        const { cache } = setup();
        await expect(cache.set(["t"], "v", { ttl: () => ttl })).rejects.toThrow(RangeError);
        expect(await cache.has(["t"])).toBe(false);
      });

      it("getOrSet rejects a ttl function returning NaN and does not cache the computed value", async () => {
        const { cache } = setup();
        await expect(cache.getOrSet(["g"], () => 1, { ttl: () => Number.NaN })).rejects.toThrow(RangeError);
        expect(await cache.has(["g"])).toBe(false);
      });
    });
  });
}
