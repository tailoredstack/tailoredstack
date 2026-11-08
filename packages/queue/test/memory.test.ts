import { describe, expect, it } from "vitest";
import { createMemoryDriver } from "../src/memory.ts";
import { createQueue } from "../src/queue.ts";
import { driverContract } from "./driver-contract.ts";

const T0 = 1_000_000;
const SECOND = 1_000;
const HOUR = 3_600_000;

driverContract({
  name: "memory",
  capabilities: { shared: false, exactCounts: true, maxPushDelayMs: Number.POSITIVE_INFINITY },
  fixture: async () => ({ driver: createMemoryDriver() }),
});

describe("memory driver: push delay", () => {
  it("accepts a push 13 hours ahead and reserves it once due", async () => {
    const queue = createQueue(createMemoryDriver());
    try {
      const id = await queue.push("jobs", "far", { availableAt: T0 + 13 * HOUR, now: T0 });
      expect(await queue.reserve("jobs", T0 + 13 * HOUR - SECOND, T0 + 13 * HOUR)).toBeNull();
      const reserved = await queue.reserve("jobs", T0 + 13 * HOUR, T0 + 13 * HOUR + SECOND);
      expect(reserved?.id).toBe(id);
      expect(reserved?.payload).toBe("far");
    } finally {
      await queue.close();
    }
  });
});
