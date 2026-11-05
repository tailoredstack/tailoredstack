import { describe, expect, it } from "vitest";
import { MemoryStore } from "../src/index.ts";

// keysWithPrefix must yield every live match even when the caller deletes keys between yields.
// Before the fix, the generator held an array index across yields, so deleting an earlier key shifted
// the array and the next match was skipped.
describe("MemoryStore.keysWithPrefix under concurrent deletes", () => {
  it("does not skip matches when an already-yielded key is deleted between yields", async () => {
    const store = new MemoryStore();
    const keys = Array.from({ length: 10 }, (_, i) => `["job",${i}]`);
    for (const k of keys) await store.put(k, 1, null);

    const seen: string[] = [];
    for await (const key of store.keysWithPrefix('["job",')) {
      seen.push(key);
      await store.forget(key); // delete the key we just yielded, while the generator is suspended
    }

    expect(seen.sort()).toEqual([...keys].sort());
  });
});
