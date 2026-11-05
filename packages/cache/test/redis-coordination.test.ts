import { describe, it, expect, afterAll } from "vitest";
import { Redis } from "ioredis";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

// Cross-process proof: each operation runs in a real child process sharing only Redis.
const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6390";
const client = new Redis(url, { maxRetriesPerRequest: 1 });
const worker = fileURLToPath(new URL("./fixtures/worker.ts", import.meta.url));

afterAll(() => {
  client.disconnect();
});

function run(prefix: string, op: string, arg = ""): Promise<{ ok: boolean; result?: unknown; error?: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--experimental-strip-types", "--no-warnings", worker, url, prefix, op, arg], { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    child.on("error", reject);
    child.on("close", () => {
      const line = out.trim().split("\n").pop() ?? "";
      try {
        resolve(JSON.parse(line));
      } catch {
        reject(new Error(`worker produced no JSON. stderr: ${err}`));
      }
    });
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("cross-process locks (Redis)", () => {
  it("two processes never hold the same lock at once", async () => {
    const prefix = `coord:${randomUUID()}:`;
    const [a, b] = await Promise.all([run(prefix, "lock-hold", "300"), run(prefix, "lock-hold", "300")]);
    expect(a.ok && b.ok).toBe(true);
    const trace = await client.lrange(`${prefix}trace`, 0, -1);
    // Enter/exit must alternate: enter, exit, enter, exit. Overlap would show two enters in a row.
    const kinds = trace.map((t) => t.split(":")[0]);
    expect(kinds).toEqual(["enter", "exit", "enter", "exit"]);
  });

  it("a lock held by another process makes a no-wait attempt fail", async () => {
    const prefix = `coord:${randomUUID()}:`;
    const holder = run(prefix, "lock-hold", "500");
    await sleep(150);
    const attempt = await run(prefix, "lock-try");
    expect(attempt.ok).toBe(false);
    expect(attempt.error).toBe("LockTimeoutError");
    await holder;
  });

  it("a crashed holder's lock expires and another process can take it", async () => {
    const prefix = `coord:${randomUUID()}:`;
    const child = spawn(process.execPath, ["--experimental-strip-types", "--no-warnings", worker, url, prefix, "lock-crash", "400"], { stdio: "ignore" });
    await sleep(150); // child has acquired the lock and is hanging inside it
    expect((await run(prefix, "lock-try")).ok).toBe(false);
    child.kill("SIGKILL"); // crash: no release runs
    await sleep(450); // past lockFor (400ms)
    expect(await run(prefix, "lock-try")).toEqual({ ok: true, result: "got-it" });
  });
});

describe("cross-process funnel (Redis)", () => {
  it("never more than limit processes run the funnel at once", async () => {
    const prefix = `coord:${randomUUID()}:`;
    await Promise.all(Array.from({ length: 5 }, () => run(prefix, "funnel-hold", "200")));
    const peaks = (await client.lrange(`${prefix}peak`, 0, -1)).map(Number);
    expect(peaks.length).toBe(5);
    expect(Math.max(...peaks)).toBeLessThanOrEqual(2);
  });
});
