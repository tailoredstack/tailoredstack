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
