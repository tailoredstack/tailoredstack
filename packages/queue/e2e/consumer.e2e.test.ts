import { spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

// Packs the package, installs the tarball into a clean project, and runs the consumer against the
// installed build. The package ships no public driver factory, so the consumer uses its own Driver.
const pkgDir = fileURLToPath(new URL("..", import.meta.url));
const fixtures = fileURLToPath(new URL("./fixtures", import.meta.url));

let work: string;
let packDir: string;

function run(file: string, args: string[], cwd: string, env: NodeJS.ProcessEnv = process.env): string {
  const r = spawnSync(file, args, { cwd, env, encoding: "utf8" });
  if (r.status !== 0) {
    throw new Error(`${file} ${args.join(" ")} exited ${r.status}\n${(r.stderr || r.stdout).slice(-2000)}`);
  }
  return r.stdout;
}

beforeAll(() => {
  run("pnpm", ["build"], pkgDir);

  packDir = mkdtempSync(join(tmpdir(), "queue-pack-"));
  const [packed] = JSON.parse(run("npm", ["pack", "--json", "--pack-destination", packDir], pkgDir)) as {
    filename: string;
  }[];
  if (!packed) throw new Error("npm pack produced no tarball");

  work = mkdtempSync(join(tmpdir(), "queue-consumer-"));
  writeFileSync(join(work, "package.json"), JSON.stringify({ name: "consumer", private: true, type: "module" }));
  run(
    "npm",
    ["install", "--no-audit", "--no-fund", join(packDir, packed.filename), "typescript@^7", "@types/node@^26"],
    work,
  );
  cpSync(join(fixtures, "consumer.ts"), join(work, "consumer.ts"));
  cpSync(join(fixtures, "tsconfig.json"), join(work, "tsconfig.json"));
}, 600_000);

afterAll(() => {
  if (work) rmSync(work, { recursive: true, force: true });
  if (packDir) rmSync(packDir, { recursive: true, force: true });
});

describe("installed @tailoredstack/queue", () => {
  it("typechecks against its published exports", () => {
    run(join(work, "node_modules", ".bin", "tsc"), ["-p", "tsconfig.json"], work);
  });

  it("runs the public API end to end with exact outcomes", () => {
    const out = run("node", ["consumer.ts"], work);
    const last = out.trim().split("\n").pop() ?? "";
    const result = JSON.parse(last) as { ok: boolean; observed: Record<string, unknown> };

    expect(result.ok).toBe(true);
    expect(result.observed).toEqual({
      resolvesToDist: true,
      invalidName: "InvalidArgumentError",
      invalidNow: "InvalidArgumentError",
      emptyReserve: true,
      pushedReady: "job-1",
      pushedDelayed: "job-2",
      sizeBeforeReserve: { ready: 1, delayed: 1, reserved: 0 },
      reserved: { id: "job-1", payload: "a", attempts: 1 },
      sizeWhileReserved: { ready: 0, delayed: 1, reserved: 1 },
      reserveWhileLeased: true,
      releaseFirst: true,
      releaseStaleToken: false,
      sizeAfterRelease: { ready: 0, delayed: 2, reserved: 0 },
      reservedAgain: { id: "job-1", payload: "a", attempts: 2 },
      deleteSecond: true,
      deleteStaleToken: false,
      sizeAfterDelete: { ready: 0, delayed: 1, reserved: 0 },
      leaseNotLater: "InvalidArgumentError",
      leaseTooLong: "LimitError",
      payloadTooLarge: "LimitError",
      payloadAtLimit: "resolved",
      pushAfterClose: "QueueClosedError",
      reserveAfterClose: "QueueClosedError",
    });
  });
});
