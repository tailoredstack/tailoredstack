import { describe, expect, it } from "vitest";
import { MemoryStore } from "../src/drivers/memory.ts";
import { encodeKey, type Key } from "../src/keys.ts";

// Seeded PRNG (mulberry32) so random operation sequences are reproducible.
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

async function allLive(store: MemoryStore): Promise<string[]> {
  const keys: string[] = [];
  for await (const { key } of store.entries()) keys.push(key);
  return keys.sort();
}

async function collectKeys(iterable: AsyncIterable<string>): Promise<string[]> {
  const out: string[] = [];
  for await (const key of iterable) out.push(key);
  return out;
}

describe("MemoryStore prefix index", () => {
  it("keysWithPrefix matches a brute-force scan and stays sorted over seeded random operations", async () => {
    const rand = mulberry32(20_260_611);
    const pick = <T>(items: readonly T[]): T => items[Math.floor(rand() * items.length)] as T;
    let t = 1_000;
    const store = new MemoryStore({ now: () => t });

    const pool: Key[] = [];
    for (const name of ["user", "username", "org"]) {
      for (let id = 0; id < 4; id++) {
        pool.push([name, id]);
        pool.push([name, id, "posts"]);
      }
    }
    pool.push([{ tenant: "a" }, 1]);
    const storedKeys = pool.map((key) => encodeKey(key));
    const prefixes = ["[", '["user",', '["user",1,', '["username",', '["org",2]', ...storedKeys];

    for (let step = 0; step < 400; step++) {
      const stored = pick(storedKeys);
      const op = rand();
      const ttl = pick([null, 5, 20]);
      if (op < 0.3) await store.put(stored, step, ttl);
      else if (op < 0.4) await store.add(stored, step, ttl);
      else if (op < 0.5) await store.forget(stored);
      else if (op < 0.55) await store.forgetIf(stored, step);
      else if (op < 0.6) await store.touch(stored, ttl ?? 10);
      else t += Math.floor(rand() * 8); // advance the clock so some entries expire

      const brute = await allLive(store);
      const root = await collectKeys(store.keysWithPrefix("["));
      expect(root).toEqual(brute);
      expect(root.every((key, i) => i === 0 || (root[i - 1] as string) < key)).toBe(true);
      for (const prefix of prefixes) {
        expect(await collectKeys(store.keysWithPrefix(prefix))).toEqual(brute.filter((k) => k.startsWith(prefix)));
      }
    }
  });
});
