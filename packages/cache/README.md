# @tailoredstack/cache

One typed cache API over pluggable stores: memory, SQLite, and Redis/Valkey. TanStack-style array keys, fluent scoping, prefix invalidation, stale-while-revalidate, and locks.

The backend is an injected object (the same idea as Kysely's dialects). The core imports no driver, so you pay only for what you use.

## Install

```sh
pnpm add @tailoredstack/cache
pnpm add ioredis        # only if you use RedisStore
pnpm add effect         # only if you use @tailoredstack/cache/effect
```

## Quick start

```ts
import { Cache, MemoryStore } from "@tailoredstack/cache";

const cache = new Cache(new MemoryStore());

await cache.set(["user", 1], { name: "Ada" }, { ttl: 60_000 }); // ms; omit ttl = no expiry
await cache.get<User>(["user", 1]); // User | undefined
await cache.getOrSet(["user", 2], () => loadUser(2), { ttl: 60_000 });
await cache.delete(["user", 1]);
```

A string key is the same as a one-segment array: `cache.get("config")` equals `cache.get(["config"])`.

## Keys

Keys are arrays of segments: strings, finite numbers, booleans, `null`, arrays, and plain objects. Object key order does not matter.

Invalid segments throw `TypeError`: `undefined`, `NaN`, `Infinity`, functions, bigints.

## Fluent scopes

`prefix()` and `ttl()` return a new `Cache` and never mutate. Chains can be shared safely.

```ts
const users = cache.prefix(["user"]).ttl(5 * 60_000);
await users.set([1], user); // stored at ["user", 1], expires in 5 minutes
await users.get<User>([1]);
await cache.ttl(30_000).set("tmp", value);
```

## Invalidation (all stores)

`invalidate()` follows TanStack Query's matching rules:

```ts
await cache.invalidate(["user"]); // partial: ["user", 1], ["user", 1, "posts"], ...
await cache.invalidate(["user", { page: 1 }]); // partial object match
await cache.invalidate(["user", 1], { exact: true }); // only ["user", 1]
await cache.invalidate({ predicate: (key) => key[0] === "post" });
await cache.clear(); // invalidate([]): everything in this scope
```

Array-key invalidation (`invalidate([...])`, `clear()`, `exact`) is O(matches) on every store. Each driver keeps a prefix index: a sorted key array in memory, a primary-key range scan in SQLite, and index sets in Redis. Nothing scans the whole store.

Predicate invalidation (`{ predicate }`) is a full scan of the store, O(N). Use an array key when you can.

`invalidate([])` is `clear()`: it removes every key in this scope.

## Typed accessors

```ts
await cache.string(["name"], "anon");
await cache.integer(["visits"], 0);
await cache.float(["ratio"]);
await cache.boolean(["flag"], false);
await cache.array(["ids"], []);
```

Each returns the fallback when the stored value has the wrong type.

## Bulk, counters, and presence

```ts
await cache.many([["a"], ["b"]]); // [value | undefined, ...]
await cache.putMany(
  [
    [["a"], 1],
    [["b"], 2],
  ],
  { ttl: 60_000 },
);
await cache.deleteMultiple([["a"], ["b"]]); // number removed
await cache.add(["once"], value, { ttl: 1000 }); // true only if absent
await cache.touch(["once"], 5000); // extend ttl; false if missing
await cache.increment(["hits"], 1);
await cache.decrement(["hits"], 1);
await cache.has(["x"]);
await cache.missing(["x"]);
```

`set` rejects `undefined`: use `delete()` or store `null`.

## `getOrSet` with a TTL function

```ts
await cache.getOrSet(["report"], buildReport, { ttl: (r) => (r.isFinal ? 86_400_000 : 60_000) });
```

## Stale-while-revalidate: `flexible`

`flexible(key, [freshMs, staleMs], compute)`:

- Within `freshMs`: returns the cached value.
- Within `staleMs`: returns the stale value and refreshes in the background. A failed refresh keeps serving the stale value.
- Past `staleMs`: computes synchronously.

```ts
const report = await cache.flexible(["report"], [60_000, 3_600_000], () => buildReport());
```

## Locks and concurrency

```ts
// Only one caller runs at a time for this key. Waits up to waitFor ms, else LockTimeoutError.
await cache.withoutOverlapping(["import", "org:1"], () => runImport(), {
  lockFor: 30_000, // lease length: a crashed holder's lock expires after this
  maxHoldFor: 300_000, // optional, default 300_000: longest renewal, see below
  waitFor: 5_000,
});

// At most 3 callers at once for this key.
await cache.funnel(["external-api"], { limit: 3, waitFor: 2_000 }, () => callApi());

// Compute a missing key once across callers and processes. Off by default.
await cache.getOrSet(["report", 1], () => buildReport(), { lock: { waitFor: 10_000 } });
```

`getOrSet` with `lock: true` (or a `LockOptions` object) reads the key, and on a miss takes `withoutOverlapping` on it. Inside the lock it re-reads the key, so a value another holder just wrote is returned without computing. Otherwise it computes, stores with the given `ttl`, and returns. With `lock: true` the default `waitFor` is 0, so a concurrent caller that cannot take the lock immediately gets `LockTimeoutError`. Pass `{ waitFor }` to wait for the winner instead.

While the callback runs, the lease is renewed every `lockFor / 3`, so a live holder keeps exclusivity past `lockFor`. Renewal stops after `maxHoldFor` (default 300 s). A callback that runs longer than `maxHoldFor` loses its lock: the lease then lapses within `lockFor`, and another caller can enter while it still runs. Set `maxHoldFor` above your longest expected run. `funnel` slots renew the same way.

Release is token-checked: a holder whose lock expired cannot release its successor's lock.

Coordination is per-store. For cross-process protection, use a shared store (Redis). `MemoryStore` and `SqliteStore` coordinate only within one process, or one file.

`getOrSet` and `flexible` also share in-flight computes within one process. Across processes, use `withoutOverlapping`.

## Events

```ts
const off = cache.on("miss", (e) => metrics.inc("cache.miss", { key: e.key }));
// event types: "hit" | "miss" | "write" | "forget"
off();
```

Listeners are shared across chained scopes, so one listener on the base cache sees writes from `prefix()` chains.

## Drivers

```ts
import { Redis } from "ioredis";
import { Cache, MemoryStore, SqliteStore, RedisStore } from "@tailoredstack/cache";

new Cache(new MemoryStore()); // in process, lost on restart
new Cache(new SqliteStore({ path: "./cache.db" })); // file, single host, uses node:sqlite, no server
new Cache(new RedisStore({ client: new Redis("redis://localhost:6379"), prefix: "app:" })); // shared; works with Valkey
```

`prefix` namespaces every key the store writes, so two caches on one server don't collide and `entries()` only sees its own keys. Redis index sets live under the same prefix and are never returned by `entries()`.

Every driver supports array keys with the same semantics. No driver has a capability gate, and none throws `UnsupportedError` for invalidation.

Expiry for Redis uses the server clock, so timing is accurate across processes.

### Writing a driver

Implement `Store` from `@tailoredstack/cache` (see `src/contracts.ts`). Then prove it passes the shared contract suite:

```ts
// your-driver.test.ts
import { storeContract } from "@tailoredstack/cache/testing";
import { MyStore } from "./my-store.ts";

storeContract("MyStore", (clock) => new MyStore({ now: clock.now }));
// For drivers that own their clock (like Redis server time), run TTL cases in real time:
// storeContract("MyStore", () => new MyStore(), { timing: "real-time" });
```

`@tailoredstack/cache/testing` needs `vitest` as the test runner.

Implement `keysWithPrefix(prefix)` too. It must cost O(matches), using an index or a range scan. `Cache.invalidate` calls it with an exact key (ending in `]`) or a children prefix (ending in `,`), then applies `matchKey` to each candidate. `src/drivers/memory.ts`, `sqlite.ts`, and `redis.ts` are the references.

## Effect

```ts
import { EffectCache } from "@tailoredstack/cache/effect";
import { Effect } from "effect";

const cache = new EffectCache(new Cache(new RedisStore({ client: redis })));

const program = Effect.gen(function* () {
  yield* cache.set(["user", 1], user, { ttl: 60_000 });
  return yield* cache.getOrSet(
    ["report"],
    Effect.promise(() => buildReport()),
  );
});
```

Every method returns an `Effect`. Driver and key failures become `CacheError`. Lock exhaustion becomes `LockTimeoutError`. `compute` arguments must have no requirements (`R = never`); provide them before passing in.

## Errors

- `TypeError`: invalid key segment, invalid value (`undefined`), non-integer `increment`.
- `RangeError`: non-positive or non-finite `ttl`, invalid funnel `limit`.
- `LockTimeoutError`: `withoutOverlapping` or `funnel` could not acquire within `waitFor`.

## Testing your code

Inject the same clock into both `Cache` and `MemoryStore`:

```ts
const clock = { t: 0 };
const now = () => clock.t;
const cache = new Cache(new MemoryStore({ now }), { now });
```

Cache's `now` controls `flexible()` freshness windows. MemoryStore's `now` controls entry expiry. Passing only the store clock leaves `flexible()` on the real clock.

## Running tests

The Redis suites start their own Redis 8 container through Testcontainers, so you need a running Docker daemon and no fixed port.

```sh
pnpm --filter @tailoredstack/cache test
```

On Colima (macOS), Testcontainers' cleanup container can't mount the Colima socket. Disable it for local runs:

```sh
TESTCONTAINERS_RYUK_DISABLED=true pnpm --filter @tailoredstack/cache test
```

Type tests run separately:

```sh
pnpm --filter @tailoredstack/cache typecheck
```

## Limits (v0)

- Predicate `invalidate` is O(N) over the store. Array-key invalidation is O(matches).
- Redis index sets grow for keys without TTL. A member is removed lazily, the next time its index is read by `invalidate`. Keys that expire on their own with a TTL are cleaned the same way.
- Memory prefix index: each key insert or delete splices a sorted array, O(n) per change.
- `flexible` and `getOrSet` single-flight per process only.
- Locks are advisory: a callback longer than `maxHoldFor` loses its lock, and its work can overlap with the next holder's.
- Values are JSON. Dates come back as strings; store `toISOString()` or a number.
