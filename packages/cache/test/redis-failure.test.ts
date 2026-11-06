import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, describe, expect, inject, it } from "vitest";
import { Cache } from "../src/cache.ts";
import { RedisStore } from "../src/drivers/redis.ts";

const client = new Redis(inject("redisUrl"), { maxRetriesPerRequest: 1 });

afterAll(() => {
  client.disconnect();
});

// Wraps the real client. `fail(cmd, args)` returns an Error to reject that call, or undefined to pass it through.
// Only the commands a test names are affected, so setup calls and the assertions' own reads stay real.
function faulty(fail: (name: string, args: unknown[]) => Error | undefined) {
  // Pipelines from multi() are wrapped too: a failing exec() must throw before anything is sent.
  const wrap = <T extends object>(target: T, onCall: (name: string, obj: T, args: unknown[]) => unknown): T =>
    new Proxy(target, {
      get(obj, prop, receiver) {
        const value = Reflect.get(obj, prop, receiver);
        if (typeof value !== "function") return value;
        const name = String(prop);
        return (...args: unknown[]) => onCall(name, obj, args);
      },
    });
  type Method = (...a: unknown[]) => unknown;
  const call = (obj: object, name: string, args: unknown[]): unknown =>
    (obj as Record<string, Method>)[name]?.(...args);
  const pipelineWrap = (pipeline: object): object =>
    wrap(pipeline, (name, obj, args) => {
      const err = fail(name, args);
      if (err) throw err;
      const result = call(obj, name, args);
      return result === obj ? pipelineWrap(obj) : result;
    });
  return wrap(client, (name, obj, args) => {
    const err = fail(name, args);
    if (err) throw err;
    const result = call(obj, name, args);
    return name === "multi" ? pipelineWrap(result as object) : result;
  });
}

const freshPrefix = () => `rf:${randomUUID()}:`;

describe("RedisStore failure paths: a failed write leaves no partial state", () => {
  it("a failed MULTI exec on put leaves neither the value nor its prefix-index membership", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({
      client: faulty((name) => (name === "exec" ? new Error("connection reset") : undefined)),
      prefix,
    });
    await expect(store.put('["u",1]', "v", null)).rejects.toThrow("connection reset");
    // The MULTI never reached the server: no value key and no index set may exist.
    expect(await client.keys(`${prefix}*`)).toEqual([]);
  });

  it("a failed add (script) stores neither the value nor an index member", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({
      client: faulty((name) => (name === "eval" ? new Error("script aborted") : undefined)),
      prefix,
    });
    await expect(store.add('["u",1]', "v", null)).rejects.toThrow("script aborted");
    expect(await client.keys(`${prefix}*`)).toEqual([]);
  });

  it("a failed increment leaves the stored value unchanged and writes no index member", async () => {
    const prefix = freshPrefix();
    const healthy = new RedisStore({ client, prefix });
    await healthy.put('["n"]', 5, null);
    const before = await client.keys(`${prefix}*`);

    const store = new RedisStore({
      client: faulty((name) => (name === "eval" ? new Error("script aborted") : undefined)),
      prefix,
    });
    await expect(store.increment('["n"]', 1)).rejects.toThrow("script aborted");
    expect(await healthy.get('["n"]')).toBe(5);
    expect((await client.keys(`${prefix}*`)).sort()).toEqual(before.sort());
  });

  it("a failed set through the Cache emits no write event and stores nothing", async () => {
    const prefix = freshPrefix();
    const cache = new Cache(
      new RedisStore({
        client: faulty((name) => (name === "exec" ? new Error("connection reset") : undefined)),
        prefix,
      }),
    );
    const writes: unknown[] = [];
    cache.on("write", (e) => writes.push(e.key));
    await expect(cache.set(["k"], 1)).rejects.toThrow("connection reset");
    expect(writes).toEqual([]);
    expect(await client.keys(`${prefix}*`)).toEqual([]);
  });
});

describe("RedisStore failure paths: a failed delete mid-invalidate is partial and retry finishes", () => {
  it("a DEL failure stops the invalidate: earlier keys removed, failing and later keys kept", async () => {
    const prefix = freshPrefix();
    const healthy = new RedisStore({ client, prefix });
    const cache = new Cache(healthy);
    await cache.set(["u", "a"], 1);
    await cache.set(["u", "b"], 2);
    await cache.set(["u", "c"], 3);

    const failOnB = (name: string, args: unknown[]) =>
      name === "del" && String(args[0]).endsWith(JSON.stringify(["u", "b"])) ? new Error("io error") : undefined;
    const flaky = new Cache(new RedisStore({ client: faulty(failOnB), prefix }));

    await expect(flaky.invalidate(["u"])).rejects.toThrow("io error");
    expect(await cache.get(["u", "b"])).toBe(2);
    expect(await cache.get(["u", "c"])).toBe(3);

    expect(await cache.invalidate(["u"])).toBe(2);
    expect(await cache.get(["u", "c"])).toBeUndefined();
  });
});

describe("RedisStore failure paths: a failed lock release keeps the callback outcome", () => {
  it("a forgetIf script failure does not replace the callback result", async () => {
    const prefix = freshPrefix();
    const cache = new Cache(
      new RedisStore({
        client: faulty((name, args) =>
          name === "eval" && String(args[0]).includes("DEL") ? new Error("release failed") : undefined,
        ),
        prefix,
      }),
    );
    await expect(cache.withoutOverlapping(["r"], () => "result")).resolves.toBe("result");
  });
});

describe("RedisStore failure paths: per-command errors and stale lock tokens", () => {
  it("a per-command error inside MULTI exec is thrown, not swallowed as success", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({ client, prefix });
    // A non-integer value sitting under the key makes INCRBY inside a MULTI fail per-command.
    await client.set(`${prefix}["n"]`, "text");
    const tx = client.multi();
    tx.incrby(`${prefix}["n"]`, 1);
    const results = (await tx.exec()) as [Error | null, unknown][];
    expect(results[0]?.[0]).toBeInstanceOf(Error);
    // The store's own write path must surface such per-command errors too.
    const cache = new Cache(store);
    await cache.set(["ok"], 1);
    expect(await cache.get(["ok"])).toBe(1);
  });

  it("a stale holder's release cannot delete a successor's lock on redis", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({ client, prefix });
    const lockKey = `lock:["r"]`;
    expect(await store.add(lockKey, "old-token", 60_000)).toBe(true);
    // Old holder's lease lapses; a successor takes the lock with a new token.
    await store.forget(lockKey);
    expect(await store.add(lockKey, "new-token", 60_000)).toBe(true);
    expect(await store.forgetIf(lockKey, "old-token")).toBe(false);
    expect(await store.get(lockKey)).toBe("new-token");
  });

  it("add on an existing key returns false and keeps the original value on redis", async () => {
    const prefix = freshPrefix();
    const store = new RedisStore({ client, prefix });
    expect(await store.add('["a"]', "first", null)).toBe(true);
    expect(await store.add('["a"]', "second", null)).toBe(false);
    expect(await store.get('["a"]')).toBe("first");
  });
});

describe("RedisStore failure paths: MULTI per-command errors reach the caller", () => {
  it("a put whose queued command fails per-command rejects instead of reporting success", async () => {
    const prefix = freshPrefix();
    // exec() resolves with a per-command error entry, the way ioredis reports a command rejected inside MULTI.
    const wrapped = new Proxy(client, {
      get(obj, prop) {
        if (prop !== "multi") return Reflect.get(obj, prop);
        return () => {
          const tx = obj.multi();
          return new Proxy(tx, {
            get(t, p) {
              if (p === "exec") return async () => [[new Error("WRONGTYPE"), null]];
              const v = Reflect.get(t, p);
              return typeof v === "function" ? (...a: unknown[]) => (v as (...x: unknown[]) => unknown).apply(t, a) : v;
            },
          });
        };
      },
    });
    const store = new RedisStore({ client: wrapped as typeof client, prefix });
    await expect(store.put('["x"]', 1, null)).rejects.toThrow("WRONGTYPE");
  });
});
