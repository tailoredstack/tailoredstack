import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
  exports: Record<string, { types: string; default: string }>;
  peerDependencies?: Record<string, string>;
  peerDependenciesMeta?: Record<string, { optional?: boolean }>;
  dependencies?: Record<string, string>;
  description: string;
};

describe("package.json contract", () => {
  it("exports the root, effect and testing entry points", () => {
    expect(Object.keys(pkg.exports).sort()).toEqual([".", "./effect", "./testing"]);
    expect(pkg.exports["./effect"]?.default).toBe("./dist/effect/index.js");
    expect(pkg.exports["./testing"]?.default).toBe("./dist/testing/index.js");
  });

  it("every export ships types", () => {
    for (const entry of Object.values(pkg.exports)) expect(entry.types).toMatch(/\.d\.ts$/);
  });

  it("effect, ioredis and vitest are optional peers, not hard dependencies", () => {
    for (const name of ["effect", "ioredis", "vitest"]) {
      expect(pkg.peerDependencies?.[name]).toBeDefined();
      expect(pkg.peerDependenciesMeta?.[name]?.optional).toBe(true);
    }
    expect(pkg.dependencies ?? {}).toEqual({});
  });

  it("built declarations do not import ioredis (optional peer must not leak into types)", () => {
    for (const file of ["dist/index.d.ts", "dist/drivers/redis.d.ts"]) {
      const text = readFileSync(new URL(`../${file}`, import.meta.url), "utf8");
      expect(text, file).not.toContain('from "ioredis"');
    }
  });

  it("description is not Laravel-branded", () => {
    expect(pkg.description).not.toMatch(/laravel/i);
  });
});
