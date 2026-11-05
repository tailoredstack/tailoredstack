import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["e2e/**/*.e2e.test.ts"],
    // Packing, installing and container startup are slow by design.
    testTimeout: 300_000,
    hookTimeout: 600_000,
  },
});
