import { randomUUID } from "node:crypto";
import { RedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import type { Receipt } from "./dialect.ts";
import { runDialectSuite } from "./dialect-suite.ts";
import { type RedisClient, RedisDialect } from "./redis-dialect.ts";

const externalUrl = process.env.REDIS_URL || undefined;
const container = externalUrl ? null : await new RedisContainer("redis:8-alpine").start();
const url = externalUrl ?? container?.getConnectionUrl() ?? "";
if (url === "") throw new Error("Redis URL is unavailable");

const root = `queue:test:${randomUUID()}:`;
const clients: Redis[] = [];

function connect(): Redis {
  const client = new Redis(url, { maxRetriesPerRequest: 1 });
  clients.push(client);
  return client;
}

const client = connect();

const open = (prefix = `${root}${randomUUID()}:`): RedisDialect => new RedisDialect({ client, prefix });

afterAll(async () => {
  const keys: string[] = [];
  for await (const batch of client.scanStream({ match: `${root}*`, count: 100 })) keys.push(...(batch as string[]));
  if (keys.length > 0) await client.del(...keys);
  for (const each of clients) each.disconnect();
  await container?.stop();
});

describe("RedisDialect", () => {
  runDialectSuite("RedisDialect", open);

  it("declares ordered, exact counts, a caller clock, and no delay or lease caps", () => {
    expect(new RedisDialect({ client }).capabilities).toEqual({
      ordered: true,
      exactCounts: true,
      clockSource: "caller",
      maxDelayMs: Number.POSITIVE_INFINITY,
      maxLeaseMs: Number.POSITIVE_INFINITY,
    });
  });

  it("uses queue: as the default key namespace", async () => {
    const calls: { numKeys: number; args: string[] }[] = [];
    const fake: RedisClient = {
      eval: async (_script, numKeys, ...args) => {
        calls.push({ numKeys, args });
        return "id";
      },
    };

    await expect(
      new RedisDialect({ client: fake }).enqueue("jobs", "a", { availableAt: 1000, now: 1000 }),
    ).resolves.toBe("id");
    expect(calls[0]?.numKeys).toBe(2);
    expect(calls[0]?.args.slice(0, 3)).toEqual(["queue:seq", "queue:ready:jobs", "queue:"]);
  });

  it("keeps two dialects on one Redis apart by prefix", async () => {
    const billing = open();
    const reports = open();

    await billing.enqueue("jobs", "invoice", { availableAt: 1000, now: 1000 });
    expect(await reports.count("jobs", 1000)).toEqual({ ready: 0, delayed: 0, reserved: 0 });
    expect(await billing.count("jobs", 1000)).toEqual({ ready: 1, delayed: 0, reserved: 0 });
    expect((await billing.dequeue("jobs", { now: 1000, leaseUntil: 2000 }))?.payload).toBe("invoice");
    expect(await reports.dequeue("jobs", { now: 1000, leaseUntil: 2000 })).toBeNull();
  });

  it("requeues an expired reservation with its original due time", async () => {
    const dialect = open();
    const first = await dialect.enqueue("jobs", "first", { availableAt: 1000, now: 1000 });
    await dialect.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
    const second = await dialect.enqueue("jobs", "second", { availableAt: 1000, now: 1000 });

    const redelivered = await dialect.dequeue("jobs", { now: 2000, leaseUntil: 3000 });
    expect(redelivered?.id).toBe(first);
    expect(redelivered?.attempts).toBe(2);
    expect((await dialect.dequeue("jobs", { now: 2000, leaseUntil: 3000 }))?.id).toBe(second);
  });

  it("counts ready, delayed, live, and expired reservations together", async () => {
    const dialect = open();
    await dialect.enqueue("jobs", "ready", { availableAt: 1000, now: 1000 });
    await dialect.enqueue("jobs", "live", { availableAt: 1000, now: 1000 });
    await dialect.enqueue("jobs", "expired", { availableAt: 1000, now: 1000 });
    await dialect.enqueue("jobs", "delayed", { availableAt: 5000, now: 1000 });
    await dialect.enqueue("jobs", "later", { availableAt: 2000, now: 1000 });
    await dialect.dequeue("jobs", { now: 1000, leaseUntil: 4000 });
    await dialect.dequeue("jobs", { now: 1000, leaseUntil: 4000 });
    await dialect.dequeue("jobs", { now: 1000, leaseUntil: 2000 });

    expect(await dialect.count("jobs", 1500)).toEqual({ ready: 0, delayed: 2, reserved: 3 });
    expect(await dialect.count("jobs", 3000)).toEqual({ ready: 2, delayed: 1, reserved: 2 });
  });

  it("two connections never claim the same message", async () => {
    const prefix = `${root}race:${randomUUID()}:`;
    const first = new RedisDialect({ client: connect(), prefix });
    const second = new RedisDialect({ client: connect(), prefix });

    const total = 12;
    for (let i = 0; i < total; i += 1) {
      await first.enqueue("jobs", `message-${i}`, { availableAt: 1000, now: 1000 });
    }

    const claimed = await Promise.all(
      Array.from({ length: total }, (_, i) =>
        (i % 2 === 0 ? first : second).dequeue("jobs", { now: 1000, leaseUntil: 2000 }),
      ),
    );
    const ids = claimed.map((message) => message?.id);
    expect(ids.every((id) => typeof id === "string")).toBe(true);
    expect(new Set(ids).size).toBe(total);
    expect(new Set(claimed.map((message) => message?.payload)).size).toBe(total);
    expect(await first.dequeue("jobs", { now: 1500, leaseUntil: 2500 })).toBeNull();
  });

  it("drops an expired reservation when its message hash is gone", async () => {
    const prefix = `${root}missing:${randomUUID()}:`;
    const dialect = open(prefix);
    const id = await dialect.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
    await dialect.dequeue("jobs", { now: 1000, leaseUntil: 2000 });

    await client.del(`${prefix}job:${id}`);

    expect(await dialect.count("jobs", 2000)).toEqual({ ready: 0, delayed: 0, reserved: 0 });
    await expect(dialect.dequeue("jobs", { now: 2000, leaseUntil: 3000 })).resolves.toBeNull();
  });

  it("rejects non-finite times before calling Redis", async () => {
    const dialect = open();

    await expect(dialect.enqueue("jobs", "a", { availableAt: Number.NaN, now: 1000 })).rejects.toThrow(TypeError);
    await expect(dialect.dequeue("jobs", { now: 1000, leaseUntil: Number.POSITIVE_INFINITY })).rejects.toThrow(
      TypeError,
    );
    await expect(dialect.acknowledge("jobs", "unknown" as Receipt, Number.NaN)).rejects.toThrow(TypeError);
    await expect(dialect.release("jobs", "unknown" as Receipt, { availableAt: Number.NaN, now: 1000 })).rejects.toThrow(
      TypeError,
    );
    await expect(dialect.count("jobs", Number.POSITIVE_INFINITY)).rejects.toThrow(TypeError);
  });

  it("close leaves the caller's client open", async () => {
    await open().close();
    await expect(client.ping()).resolves.toBe("PONG");
    await expect(open().enqueue("jobs", "a", { availableAt: 1000, now: 1000 })).resolves.toEqual(expect.any(String));
  });

  it("returns false for a receipt it did not issue", async () => {
    const dialect = open();
    await dialect.enqueue("jobs", "a", { availableAt: 1000, now: 1000 });
    const message = await dialect.dequeue("jobs", { now: 1000, leaseUntil: 2000 });
    if (message === null) throw new Error("expected a message");

    expect(await dialect.acknowledge("jobs", "unknown" as Receipt, 1500)).toBe(false);
    expect(await dialect.release("jobs", "unknown" as Receipt, { availableAt: 1600, now: 1500 })).toBe(false);
    expect(await dialect.acknowledge("jobs", `${message.id}:bogus` as Receipt, 1500)).toBe(false);
    expect(await dialect.acknowledge("jobs", message.receipt, 1500)).toBe(true);
  });
});
