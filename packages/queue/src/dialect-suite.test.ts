import { describe, expect, it } from "vitest";
import { exactCountsSkipTitle, orderedSkipTitle } from "./dialect-suite.ts";

describe("dialect suite skip reasons", () => {
  it("names the dialect and the exactCounts capability in the skipped case title", () => {
    expect(exactCountsSkipTitle("count ignores other names", "FakeDialect")).toBe(
      "count ignores other names [skipped: FakeDialect declares capabilities.exactCounts=false]",
    );
  });

  it("names the dialect and the ordered capability in the skipped case title", () => {
    expect(orderedSkipTitle("dequeue picks the earliest-due ready message", "FakeDialect")).toBe(
      "dequeue picks the earliest-due ready message [skipped: FakeDialect declares capabilities.ordered=false]",
    );
  });
});
