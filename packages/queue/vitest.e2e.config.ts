import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["e2e/**/*.test.ts"],
    hookTimeout: 600_000,
    testTimeout: 600_000,
  },
});
