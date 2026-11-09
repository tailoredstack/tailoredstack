import { describe, expect, it } from "vitest";
import { runDialectSuite } from "./dialect-suite.ts";
import { MemoryDialect } from "./memory-dialect.ts";

runDialectSuite("MemoryDialect", () => new MemoryDialect());

describe("MemoryDialect pending methods", () => {
  // TEMPORARY: TS-84 must DELETE this test when it implements close.
  // It pins the current "not implemented" stub.
  it("close throws until TS-84 lands (delete then)", async () => {
    const d = new MemoryDialect();
    await expect(d.close()).rejects.toThrow("not implemented");
  });
});
