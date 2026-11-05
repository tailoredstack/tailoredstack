import { describe, expect, it } from "vitest";
import { encodeKey, matchKey } from "../src/keys.ts";

describe("encodeKey", () => {
  it("is stable across object key order", () => {
    expect(encodeKey(["user", { a: 1, b: 2 }])).toBe(encodeKey(["user", { b: 2, a: 1 }]));
  });

  it("is stable across nested object key order", () => {
    expect(encodeKey([{ x: { a: 1, b: [{ c: 1, d: 2 }] } }])).toBe(encodeKey([{ x: { b: [{ d: 2, c: 1 }], a: 1 } }]));
  });

  it("does not collide on separators inside segments", () => {
    expect(encodeKey(["a:b"])).not.toBe(encodeKey(["a", "b"]));
  });

  it("distinguishes string 1 from number 1", () => {
    expect(encodeKey(["a", 1])).not.toBe(encodeKey(["a", "1"]));
  });

  it("accepts strings, numbers, booleans, null, arrays and plain objects", () => {
    expect(() => encodeKey(["s", 1, true, null, [1, "x"], { k: "v" }])).not.toThrow();
  });

  it.each([
    ["undefined", undefined],
    ["NaN", Number.NaN],
    ["Infinity", Number.POSITIVE_INFINITY],
    ["a function", () => 1],
    ["a bigint", 1n],
  ])("rejects %s with a TypeError", (_label, bad) => {
    expect(() => encodeKey(["user", bad as never])).toThrow(TypeError);
  });
});

describe("matchKey (TanStack partial match)", () => {
  it("a shorter filter matches longer keys with the same prefix", () => {
    expect(matchKey(["user"], ["user", 1])).toBe(true);
    expect(matchKey(["user"], ["user", 1, "posts"])).toBe(true);
  });

  it("matches the filter itself", () => {
    expect(matchKey(["user"], ["user"])).toBe(true);
  });

  it("never matches a string that merely starts with the filter text", () => {
    expect(matchKey(["user"], ["username", 1])).toBe(false);
  });

  it("a longer filter never matches a shorter key", () => {
    expect(matchKey(["user", 1], ["user"])).toBe(false);
  });

  it("empty filter matches every key", () => {
    expect(matchKey([], ["anything", 1])).toBe(true);
    expect(matchKey([], [])).toBe(true);
  });

  it("partially matches object segments by their fields", () => {
    expect(matchKey(["user", { page: 1 }], ["user", { page: 1, sort: "x" }])).toBe(true);
    expect(matchKey(["user", { page: 1 }], ["user", { page: 2, sort: "x" }])).toBe(false);
  });

  it("partially matches nested objects and arrays", () => {
    expect(matchKey([{ a: { b: 1 } }], [{ a: { b: 1, c: 2 }, z: 3 }])).toBe(true);
    expect(matchKey([[1]], [[1, 2]])).toBe(true);
    expect(matchKey([[1, 2]], [[1]])).toBe(false);
  });

  it("string 1 and number 1 are different segments", () => {
    expect(matchKey(["a", 1], ["a", "1"])).toBe(false);
  });
});

describe("matchKey (exact)", () => {
  it("requires full equality", () => {
    expect(matchKey(["user", 1], ["user", 1], { exact: true })).toBe(true);
    expect(matchKey(["user"], ["user", 1], { exact: true })).toBe(false);
    expect(matchKey(["user", 1], ["user"], { exact: true })).toBe(false);
  });

  it("object segments compare by full value, not partial", () => {
    expect(matchKey(["u", { p: 1 }], ["u", { p: 1, s: "x" }], { exact: true })).toBe(false);
    expect(matchKey(["u", { p: 1, s: "x" }], ["u", { s: "x", p: 1 }], { exact: true })).toBe(true);
  });
});
