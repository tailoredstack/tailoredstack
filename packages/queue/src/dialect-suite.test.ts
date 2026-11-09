import { describe, expect, it } from "vitest";
import { exactCountsSkipTitle } from "./dialect-suite.ts";

describe("dialect suite skip reason", () => {
  it("names the dialect and the exactCounts capability in the skipped case title", () => {
    expect(exactCountsSkipTitle("count ignores other names", "FakeDialect")).toBe(
      "count ignores other names [skipped: FakeDialect declares capabilities.exactCounts=false]",
    );
  });
});
