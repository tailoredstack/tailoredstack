// Runs as a consumer would: imports the installed package, never the source tree.
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cache, MemoryStore, RedisStore, SqliteStore, type Store } from "@tailoredstack/cache";
import { EffectCache } from "@tailoredstack/cache/effect";
import { Effect } from "effect";
import { Redis } from "ioredis";

// REDIS_URL points at whichever server container the harness started. BACKEND names it for the report.
const url = process.env.REDIS_URL;
const backend = process.env.BACKEND ?? "redis";
if (!url) throw new Error("REDIS_URL is required");

const results: Record<string, boolean> = {};
const check = (name: string, ok: boolean) => {
  results[name] = ok;
};

// Entry points resolve through the exports map to the built dist, not src.
check(
  "root resolves to dist",
  import.meta.resolve("@tailoredstack/cache").includes("/node_modules/@tailoredstack/cache/dist/"),
);
check("effect subpath resolves to dist", import.meta.resolve("@tailoredstack/cache/effect").includes("/dist/effect/"));

// Same behavior checks, run against every store.
async function exercise<S extends Store>(store: string, cache: Cache<S>): Promise<void> {
  const p = (name: string) => `${store}: ${name}`;

  await cache.set(["user", 1], { name: "ada" }, { ttl: 1500.5 });
  check(p("round-trip with fractional ttl"), (await cache.get<{ name: string }>(["user", 1]))?.name === "ada");
  check(p("partial invalidate"), (await cache.invalidate(["user"])) === 1);

  check(p("flexible computes on miss"), (await cache.flexible(["r"], [60_000, 120_000], () => 42)) === 42);
  check(p("flexible serves cached"), (await cache.flexible(["r"], [60_000, 120_000], () => 0)) === 42);

  const locked = await cache.withoutOverlapping(["job"], () => "ran", { lockFor: 1500.5, waitFor: 0 });
  check(p("lock with fractional lockFor"), locked === "ran");

  const fromEffect = await Effect.runPromise(
    new EffectCache(cache).getOrSet(["e"], Effect.succeed("from-effect"), { ttl: 1000 }),
  );
  check(p("effect getOrSet"), fromEffect === "from-effect");
}

// Data written through one store instance must be readable through a fresh one.
async function persists(store: string, open: () => Store): Promise<void> {
  await new Cache(open()).set(["persist"], "kept", { ttl: 60_000 });
  check(`${store}: survives a new store instance`, (await new Cache(open()).get(["persist"])) === "kept");
}

const dir = mkdtempSync(join(tmpdir(), "cache-e2e-"));
const sqlitePath = join(dir, "cache.db");
const client = new Redis(url, { maxRetriesPerRequest: 1 });
const prefix = `e2e:${backend}:${randomUUID()}:`;

// Memory: in process by definition, still a real implementation under the public API.
await exercise("memory", new Cache(new MemoryStore()));

// SQLite: a real file on disk.
await exercise("sqlite", new Cache(new SqliteStore({ path: sqlitePath })));
await persists("sqlite", () => new SqliteStore({ path: sqlitePath }));

// Server backend: a real server reached through the container URL.
await exercise(backend, new Cache(new RedisStore({ client, prefix })));
await persists(backend, () => new RedisStore({ client, prefix }));

await client.quit();
rmSync(dir, { recursive: true, force: true });

const failed = Object.entries(results)
  .filter(([, ok]) => !ok)
  .map(([name]) => name);
console.log(JSON.stringify({ backend, ok: failed.length === 0, failed, results }));
process.exitCode = failed.length === 0 ? 0 : 1;
