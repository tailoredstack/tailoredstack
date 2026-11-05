import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SqliteStore } from "../src/drivers/sqlite.ts";

// Cross-process proof: two real processes increment one SQLite file concurrently.
const worker = fileURLToPath(new URL("./fixtures/sqlite-worker.ts", import.meta.url));

function run(dbPath: string, count: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ["--experimental-strip-types", "--no-warnings", worker, dbPath, String(count)],
      { stdio: ["ignore", "ignore", "pipe"] },
    );
    let err = "";
    child.stderr.on("data", (d) => (err += d));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`worker exited ${code}: ${err}`));
    });
  });
}

describe("SqliteStore across processes", () => {
  it("two processes incrementing one file never lose an update", async () => {
    const dir = mkdtempSync(join(tmpdir(), "sqlite-conc-"));
    const dbPath = join(dir, "cache.db");
    try {
      await Promise.all([run(dbPath, 300), run(dbPath, 300)]);
      const store = new SqliteStore({ path: dbPath });
      expect(await store.get("counter")).toBe(600);
      store.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
