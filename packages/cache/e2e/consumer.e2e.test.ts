import { spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import { type StartedValkeyContainer, ValkeyContainer } from "@testcontainers/valkey";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

// Packs the package, installs the tarball into a clean project, and runs the consumer
// once per server backend (Redis, Valkey), each against its own real container.
const pkgDir = fileURLToPath(new URL("..", import.meta.url));
const fixtures = fileURLToPath(new URL("./fixtures", import.meta.url));

let work: string;
let packDir: string;
let redis: StartedRedisContainer;
let valkey: StartedValkeyContainer;

function run(file: string, args: string[], cwd: string, env: NodeJS.ProcessEnv = process.env): string {
  const r = spawnSync(file, args, { cwd, env, encoding: "utf8" });
  if (r.status !== 0) {
    throw new Error(`${file} ${args.join(" ")} exited ${r.status}\n${(r.stderr || r.stdout).slice(-2000)}`);
  }
  return r.stdout;
}

beforeAll(async () => {
  run("pnpm", ["build"], pkgDir);

  packDir = mkdtempSync(join(tmpdir(), "cache-pack-"));
  const [packed] = JSON.parse(run("npm", ["pack", "--json", "--pack-destination", packDir], pkgDir)) as {
    filename: string;
  }[];
  if (!packed) throw new Error("npm pack produced no tarball");

  work = mkdtempSync(join(tmpdir(), "cache-consumer-"));
  writeFileSync(join(work, "package.json"), JSON.stringify({ name: "consumer", private: true, type: "module" }));
  run(
    "npm",
    [
      "install",
      "--no-audit",
      "--no-fund",
      join(packDir, packed.filename),
      "ioredis@^6",
      "effect@^4",
      "typescript@^7",
      "@types/node@^26",
      "vitest@^5",
    ],
    work,
  );
  cpSync(join(fixtures, "consumer.ts"), join(work, "consumer.ts"));
  cpSync(join(fixtures, "tsconfig.json"), join(work, "tsconfig.json"));

  [redis, valkey] = await Promise.all([
    new RedisContainer("redis:8-alpine").start(),
    new ValkeyContainer("valkey/valkey:8-alpine").start(),
  ]);
}, 600_000);

afterAll(async () => {
  await Promise.all([redis?.stop(), valkey?.stop()]);
  if (work) rmSync(work, { recursive: true, force: true });
  if (packDir) rmSync(packDir, { recursive: true, force: true });
});

function runConsumer(backend: string, url: string): { ok: boolean; failed: string[] } {
  const out = run("node", ["consumer.ts"], work, { ...process.env, BACKEND: backend, REDIS_URL: url });
  const last = out.trim().split("\n").pop() ?? "";
  return JSON.parse(last) as { ok: boolean; failed: string[] };
}

describe("installed @tailoredstack/cache", () => {
  it("typechecks against its published exports", () => {
    run(join(work, "node_modules", ".bin", "tsc"), ["-p", "tsconfig.json"], work);
  });

  it("passes every check against a real Redis", () => {
    const result = runConsumer("redis", redis.getConnectionUrl());
    expect(result.failed).toEqual([]);
    expect(result.ok).toBe(true);
  });

  it("passes every check against a real Valkey", () => {
    const result = runConsumer("valkey", valkey.getConnectionUrl());
    expect(result.failed).toEqual([]);
    expect(result.ok).toBe(true);
  });
});
