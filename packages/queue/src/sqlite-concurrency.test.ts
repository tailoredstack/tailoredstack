import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SqliteDialect } from "./sqlite-dialect.ts";

// Cross-process proof: concurrent drains of one SQLite file must never hand out a message twice. This fails if
// dequeue reads before it takes the write lock (BEGIN DEFERRED) and passes when it locks first (BEGIN IMMEDIATE).
const worker = fileURLToPath(new URL("../test/fixtures/sqlite-worker.ts", import.meta.url));
const TOTAL = 200;
const WORKERS = 4;

interface Drained {
  readonly payloads: string[];
  readonly code: number | null;
  readonly stderr: string;
}

interface Child {
  readonly done: Promise<Drained>;
  go(): void;
}

function start(dbPath: string): Child {
  const child = spawn(
    process.execPath,
    ["--experimental-strip-types", "--no-warnings", worker, dbPath, String(TOTAL)],
    { stdio: ["pipe", "pipe", "pipe"] },
  );
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => (stdout += chunk));
  child.stderr.on("data", (chunk) => (stderr += chunk));
  const done = new Promise<Drained>((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code) => {
      resolve({ payloads: stdout.split("\n").filter((line) => line !== ""), code, stderr });
    });
  });
  return { done, go: () => child.stdin.end("go\n") };
}

describe("SqliteDialect across processes", () => {
  it("four processes draining one file deliver every message exactly once", async () => {
    const dir = mkdtempSync(join(tmpdir(), "queue-sqlite-conc-"));
    const dbPath = join(dir, "queue.db");
    try {
      const setup = new SqliteDialect({ path: dbPath });
      for (let i = 0; i < TOTAL; i += 1) {
        await setup.enqueue("jobs", `msg-${i}`, { availableAt: 1000, now: 1000 });
      }
      await setup.close();

      const children = Array.from({ length: WORKERS }, () => start(dbPath));
      for (const child of children) child.go(); // release them together so all four claim at once
      const runs = await Promise.all(children.map((child) => child.done));
      for (const run of runs) expect(run.code, run.stderr).toBe(0);

      const delivered = runs.flatMap((run) => run.payloads);
      const expected = Array.from({ length: TOTAL }, (_, i) => `msg-${i}`);
      expect(delivered).toHaveLength(TOTAL);
      expect(new Set(delivered).size).toBe(TOTAL);
      expect([...delivered].sort()).toEqual(expected.sort());

      const check = new SqliteDialect({ path: dbPath });
      try {
        await expect(check.count("jobs", 2000)).resolves.toEqual({ ready: 0, delayed: 0, reserved: 0 });
      } finally {
        await check.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60_000);
});
