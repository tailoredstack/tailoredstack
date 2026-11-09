import { describe, expect, it } from "vitest";
import type { Receipt } from "./dialect.ts";
import { runDialectSuite } from "./dialect-suite.ts";
import { MemoryDialect } from "./memory-dialect.ts";

runDialectSuite("MemoryDialect", () => new MemoryDialect());

describe("MemoryDialect pending methods", () => {
  // TEMPORARY: TS-85 (acknowledge) and TS-84 (release, close) must DELETE this
  // test when they implement those methods. It pins the current "not implemented" stubs.
  it("acknowledge, release, and close throw until TS-85/TS-84 land (delete then)", async () => {
    const d = new MemoryDialect();
    const receipt = "r" as Receipt;
    await expect(d.acknowledge("jobs", receipt, 0)).rejects.toThrow("not implemented");
    await expect(d.release("jobs", receipt, { availableAt: 0, now: 0 })).rejects.toThrow("not implemented");
    await expect(d.close()).rejects.toThrow("not implemented");
  });
});
