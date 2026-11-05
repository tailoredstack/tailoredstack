# @tailoredstack/cache

One typed cache API over pluggable stores: memory, SQLite, and Redis/Valkey. TanStack-style array keys, fluent scoping, stale-while-revalidate, locks, and optional Redis tags.

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

`invalidate` scans every entry in the store, so it costs O(N). Use one `Cache` per store, and use tags (below) when you invalidate often on Redis.

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
  lockFor: 30_000, // lock expires after this, so a crashed holder can't block forever
  waitFor: 5_000,
});

// At most 3 callers at once for this key.
await cache.funnel(["external-api"], { limit: 3, waitFor: 2_000 }, () => callApi());
```

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

## Tags (Redis / Valkey only)

Tags let you flush a group of items in O(1), by rotating a tag id instead of scanning:

```ts
const cache = new Cache(new RedisStore({ client: redis }));

await cache.tags(["user", "org:1"]).set(["profile", 1], profile, { ttl: 300_000 });
await cache.tags(["user", "org:1"]).get(["profile", 1]);
await cache.tags(["user"]).flush(); // every item tagged "user" becomes unreachable
```

Tags are a compile-time capability. `tags()` is only callable on a `Cache<TaggableStore>`, so `new Cache(new MemoryStore()).tags(...)` is a type error. Untyped JS callers get an `UnsupportedError` at runtime.

Costs (Laravel-style, by design):

- Each tag name leaves one small Redis key with no TTL. Tag ids accumulate.
- Flushed items stay in Redis until their own TTL. **Always set a TTL on tagged items.**
- A write that races a `flush()` can land under the old tag id and become unreachable. The next read misses; it does not return stale data.
- `invalidate()` and `clear()` are rejected on tagged scopes. Use `flush()`.

## Drivers

```ts
import { Redis } from "ioredis";
import Database from "better-sqlite3"; // or node:sqlite
import { Cache, MemoryStore, SqliteStore, RedisStore } from "@tailoredstack/cache";

new Cache(new MemoryStore()); // in process, lost on restart
new Cache(new SqliteStore({ path: "./cache.db" })); // file, single host, no server
new Cache(new RedisStore({ client: new Redis(url), prefix: "app:" })); // shared; works with Valkey
```

`prefix` namespaces every key the store writes, so two caches on one server don't collide and `entries()` only sees its own keys.

Expiry for Redis uses the server clock, so timing is accurate across processes.

### Writing a driver

Implement `Store` from `@tailoredstack/cache` (see `src/contracts.ts`). Then prove it passes the shared contract suite:

```ts
// your-driver.test.ts
import { storeContract } from "@tailoredstack/cache/testing";
import { MyStore } from "./my-store.ts";

storeContract("MyStore", (clock) => new MyStore({ now: clock.now }));
// For drivers that own their clock (like Redis server time), skip time-travel tests:
// storeContract("MyStore", () => new MyStore(), { timeTravel: false });
```

`@tailoredstack/cache/testing` needs `vitest` as the test runner.

To support tags, also implement `TaggableStore` (`tagId`, `rotateTag`). The Redis driver is the reference.

## Effect

```ts
import { EffectCache } from "@tailoredstack/cache/effect";
import { Effect } from "effect";

const cache = new EffectCache(new Cache(new RedisStore({ client: redis })));

const program = Effect.gen(function* () {
  yield* cache.set(["user", 1], user, { ttl: 60_000 });
  return yield* cache.getOrSet(["report"], buildReportEffect);
});
```

Every method returns an `Effect`. Driver and key failures become `CacheError`. Lock exhaustion becomes `LockTimeoutError`. `compute` arguments must have no requirements (`R = never`); provide them before passing in.

## Errors

- `TypeError`: invalid key segment, invalid value (`undefined`), non-integer `increment`.
- `RangeError`: non-positive or non-finite `ttl`, empty `tags([])`, invalid funnel `limit`.
- `UnsupportedError`: `tags()` on a non-taggable store; `flush()` on an untagged cache; `invalidate()`/`clear()` on a tagged scope.
- `LockTimeoutError`: `withoutOverlapping` or `funnel` could not acquire within `waitFor`.

## Testing your code

Use `MemoryStore` with an injected clock to control time:

```ts
const clock = { t: 0 };
const cache = new Cache(new MemoryStore({ now: () => clock.t }));
```

## Limits (v0)

- `invalidate()` is O(N) over the store.
- `flexible` and `getOrSet` single-flight per process only.
- Tags require Redis or Valkey. No tags on memory or SQLite.
- Locks are advisory: a holder that outlives `lockFor` loses the lock, and its work can overlap with the next holder's.
- Values are JSON. Dates come back as strings; store `toISOString()` or a number.
