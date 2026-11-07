import { createMemoryDriver } from "../src/memory.js";
import { driverContract } from "../src/testing/driver-contract.js";

driverContract({
  name: "memory",
  capabilities: { shared: false, exactCounts: true, maxPushDelayMs: 43_200_000 },
  fixture: async () => ({ driver: createMemoryDriver() }),
});
