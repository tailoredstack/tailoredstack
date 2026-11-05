import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    globalSetup: ["./test/global-setup.ts"],
    // Container startup and cross-process tests are slower than unit tests.
    hookTimeout: 120_000,
  },
});
