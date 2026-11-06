import { describe, expect, it } from "vitest";
import { Cache, MemoryStore } from "../src/index.ts";

class ThrowingReleaseStore extends MemoryStore {
  override async forgetIf(): Promise<boolean> {
    throw new Error("release failed");
  }
}

describe("a release failure does not replace the callback outcome", () => {
  it("a successful callback still resolves to its result when release throws", async () => {
    const cache = new Cache(new ThrowingReleaseStore());
    await expect(cache.withoutOverlapping(["r"], () => "result")).resolves.toBe("result");
  });

  it("a failing callback still rejects with its own error when release throws", async () => {
    const cache = new Cache(new ThrowingReleaseStore());
    await expect(
      cache.withoutOverlapping(["r"], () => {
        throw new Error("callback failed");
      }),
    ).rejects.toThrow("callback failed");
  });
});

describe("lock misuse and failure paths", () => {
  it("a store add failure during acquire rejects and does not leave a lock behind", async () => {
    const store = new MemoryStore();
    const cache = new Cache(store);
    const original = store.add.bind(store);
    store.add = async () => {
      throw new Error("store unreachable");
    };
    await expect(cache.withoutOverlapping(["l"], () => 1)).rejects.toThrow("store unreachable");
    store.add = original;
    expect(await cache.withoutOverlapping(["l"], () => 2, { waitFor: 0 })).toBe(2);
  });
});
