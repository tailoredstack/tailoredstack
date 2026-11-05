import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";

// Cross-process proof: each operation runs in a real child process sharing only Redis.
const url = inject("redisUrl");
const client = new Redis(url, { maxRetriesPerRequest: 1 });
const worker = fileURLToPath(new URL("./fixtures/worker.ts", import.meta.url));

afterAll(() => {
  client.disconnect();
});

function run(prefix: string, op: string, arg = ""): Promise<{ ok: boolean; result?: unknown; error?: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      ["--experimental-strip-types", "--no-warnings", worker, url, prefix, op, arg],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
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

// Poll real state instead of sleeping a fixed time, so slow CI runners can't race the test.
async function waitFor(check: () => Promise<boolean>, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timed out waiting for condition");
    await sleep(20);
  }
}

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
    const holder = run(prefix, "lock-hold", "1500");
    // The holder pushes an "enter" entry only from inside the lock, so this means the lock is held.
    await waitFor(async () => (await client.llen(`${prefix}trace`)) >= 1);
    const attempt = await run(prefix, "lock-try");
    expect(attempt.ok).toBe(false);
    expect(attempt.error).toBe("LockTimeoutError");
    await holder;
  });

  it("a holder running past lockFor keeps other processes out", async () => {
    const prefix = `coord:${randomUUID()}:`;
    // lockFor 300 ms, hold 1500 ms: five times the lease, so the lock is only kept by renewal.
    const holder = run(prefix, "lock-hold-renew", "300:1500");
    await waitFor(async () => (await client.llen(`${prefix}trace`)) >= 1);
    const enteredAt = Date.now();
    // Poll until the lease would have lapsed without renewal, while the holder has not exited yet.
    await waitFor(async () => Date.now() - enteredAt >= 900 && (await client.llen(`${prefix}trace`)) === 1);
    expect(await run(prefix, "lock-try")).toEqual({ ok: false, error: "LockTimeoutError" });
    await holder;
    const kinds = (await client.lrange(`${prefix}trace`, 0, -1)).map((t) => t.split(":")[0]);
    expect(kinds).toEqual(["enter", "exit"]);
  });

  it("a crashed holder's lock expires and another process can take it", async () => {
    const prefix = `coord:${randomUUID()}:`;
    const child = spawn(
      process.execPath,
      ["--experimental-strip-types", "--no-warnings", worker, url, prefix, "lock-crash", "2000"],
      { stdio: "ignore" },
    );
    // The worker sets `crashed` right after it issues the lock acquire on the same connection,
    // so once the marker exists the lock is held.
    await waitFor(async () => (await client.exists(`${prefix}crashed`)) === 1);
    expect((await run(prefix, "lock-try")).ok).toBe(false);
    child.kill("SIGKILL"); // crash: no release runs
    // Poll until the lock's lockFor (2s) runs out, rather than guessing a sleep.
    await waitFor(async () => (await run(prefix, "lock-try")).ok === true, 10_000);
    expect(await run(prefix, "lock-try")).toEqual({ ok: true, result: "got-it" });
  });
});

describe("cross-process getOrSet lock (Redis)", () => {
  it("two processes computing one missing key run the compute once", async () => {
    const prefix = `coord:${randomUUID()}:`;
    const results = await Promise.all([run(prefix, "getorset-lock", "200"), run(prefix, "getorset-lock", "200")]);
    expect(results.map((r) => r.result)).toEqual(["computed", "computed"]);
    expect(await client.get(`${prefix}computes`)).toBe("1");
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
