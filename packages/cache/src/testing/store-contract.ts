import { describe, expect, it } from "vitest";
import type { Store } from "../contracts.ts";

export interface Clock {
  now(): number;
  advance(ms: number): void;
}

export function makeClock(startMs = 1_700_000_000_000): Clock {
  let t = startMs;
  return {
    now: () => t,
    advance: (ms) => {
      t += ms;
    },
  };
}

export interface ContractOptions {
  /** false when the driver owns its clock (e.g. Redis server time): skips advance()-based tests. */
  timeTravel?: boolean;
}

async function collect(iterable: AsyncIterable<{ key: string; value: unknown }>) {
  const out: Record<string, unknown> = {};
  for await (const { key, value } of iterable) out[key] = value;
  return out;
}

export function storeContract(
  name: string,
  make: (clock: Clock) => Store,
  { timeTravel = true }: ContractOptions = {},
): void {
  const timed = timeTravel ? it : it.skip;

  describe(`${name} Store contract`, () => {
    it("get returns undefined for a missing key", async () => {
      expect(await make(makeClock()).get("nope")).toBeUndefined();
    });

    it("round-trips JSON-serializable values", async () => {
      const store = make(makeClock());
      await store.put("user", { id: 1, tags: ["a"] }, null);
      expect(await store.get("user")).toEqual({ id: 1, tags: ["a"] });
    });

    it("stored null is distinct from a miss", async () => {
      const store = make(makeClock());
      await store.put("k", null, null);
      expect(await store.get("k")).toBeNull();
    });

    it("put overwrites an existing value", async () => {
      const store = make(makeClock());
      await store.put("k", 1, null);
      await store.put("k", 2, null);
      expect(await store.get("k")).toBe(2);
    });

    timed("expires after ttl ms", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.put("k", "v", 60_000);
      clock.advance(59_999);
      expect(await store.get("k")).toBe("v");
      clock.advance(1);
      expect(await store.get("k")).toBeUndefined();
    });

    timed("null ttl never expires", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.put("k", "v", null);
      clock.advance(315_360_000_000);
      expect(await store.get("k")).toBe("v");
    });

    timed("put with a null ttl replaces an existing ttl (fake-clock)", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.put("k", "first", 1_000);
      await store.put("k", "second", null);
      clock.advance(5_000);
      expect(await store.get("k")).toBe("second");
    });

    it("put with a null ttl replaces an existing ttl (real-time)", async () => {
      const store = make(makeClock());
      await store.put("k", "first", 300);
      await store.put("k", "second", null);
      await new Promise((r) => setTimeout(r, 500));
      expect(await store.get("k")).toBe("second");
    });

    it("add stores only when the key is absent", async () => {
      const store = make(makeClock());
      expect(await store.add("k", "first", null)).toBe(true);
      expect(await store.add("k", "second", null)).toBe(false);
      expect(await store.get("k")).toBe("first");
    });

    timed("add succeeds again once the existing value expired", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.put("k", "old", 10_000);
      clock.advance(10_000);
      expect(await store.add("k", "new", null)).toBe(true);
      expect(await store.get("k")).toBe("new");
    });

    it("forget removes a key and reports whether it existed", async () => {
      const store = make(makeClock());
      await store.put("k", 1, null);
      expect(await store.forget("k")).toBe(true);
      expect(await store.get("k")).toBeUndefined();
      expect(await store.forget("k")).toBe(false);
    });

    it("forgetIf removes only when the stored value matches", async () => {
      const store = make(makeClock());
      await store.put("k", { a: 1 }, null);
      expect(await store.forgetIf("k", { a: 2 })).toBe(false);
      expect(await store.get("k")).toEqual({ a: 1 });
      expect(await store.forgetIf("k", { a: 1 })).toBe(true);
      expect(await store.get("k")).toBeUndefined();
      expect(await store.forgetIf("k", { a: 1 })).toBe(false);
    });

    it("entries yields every stored key with its value", async () => {
      const store = make(makeClock());
      await store.put("a", 1, null);
      await store.put("b", { x: 2 }, null);
      expect(await collect(store.entries())).toEqual({ a: 1, b: { x: 2 } });
    });

    it("entries is empty for an empty store", async () => {
      expect(await collect(make(makeClock()).entries())).toEqual({});
    });

    timed("entries skips expired keys", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.put("gone", 1, 1_000);
      await store.put("kept", 2, null);
      clock.advance(1_000);
      expect(await collect(store.entries())).toEqual({ kept: 2 });
    });

    it("many returns values in key order with undefined for misses", async () => {
      const store = make(makeClock());
      await store.put("a", 1, null);
      await store.put("c", 3, null);
      expect(await store.many(["a", "b", "c"])).toEqual([1, undefined, 3]);
    });

    timed("putMany writes every entry with a shared ttl", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.putMany({ a: 1, b: 2 }, 30_000);
      expect(await store.get("a")).toBe(1);
      expect(await store.get("b")).toBe(2);
      clock.advance(30_000);
      expect(await store.get("a")).toBeUndefined();
      expect(await store.get("b")).toBeUndefined();
    });

    it("increment starts a missing key at 0 and returns the new value", async () => {
      const store = make(makeClock());
      expect(await store.increment("n", 1)).toBe(1);
      expect(await store.increment("n", 4)).toBe(5);
      expect(await store.get("n")).toBe(5);
    });

    timed("increment keeps the existing ttl", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.put("n", 1, 10_000);
      await store.increment("n", 1);
      clock.advance(10_000);
      expect(await store.get("n")).toBeUndefined();
    });

    it("decrement subtracts and returns the new value", async () => {
      const store = make(makeClock());
      await store.put("n", 10, null);
      expect(await store.decrement("n", 3)).toBe(7);
      expect(await store.get("n")).toBe(7);
    });

    it("increment rejects a non-integer value", async () => {
      const store = make(makeClock());
      await store.put("s", "text", null);
      await expect(store.increment("s", 1)).rejects.toThrow();
    });

    timed("touch extends the ttl and returns false for missing keys", async () => {
      const clock = makeClock();
      const store = make(clock);
      await store.put("k", "v", 10_000);
      clock.advance(5_000);
      expect(await store.touch("k", 10_000)).toBe(true);
      clock.advance(9_000);
      expect(await store.get("k")).toBe("v");
      expect(await store.touch("missing", 10_000)).toBe(false);
    });
  });
}
