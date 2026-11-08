import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createRedisDriver, type RedisClient } from "../src/redis.ts";
import { driverContract } from "./driver-contract.ts";

let container: StartedRedisContainer;
let url: string;
const clients: Redis[] = [];

function connect(): Redis {
  const client = new Redis(url);
  clients.push(client);
  return client;
}

beforeAll(async () => {
  container = await new RedisContainer("redis:7").start();
  url = container.getConnectionUrl();
}, 120_000);

afterAll(async () => {
  for (const client of clients.splice(0)) {
    client.disconnect();
  }
  await container.stop();
});

// Two driver instances, each with its own connection to one server: the shared-backend contract (B7) needs a peer.
driverContract({
  name: "redis",
  capabilities: { shared: true, exactCounts: true, maxPushDelayMs: 43_200_000 },
  fixture: async () => {
    await connect().flushdb();
    return { driver: createRedisDriver({ client: connect() }), peer: createRedisDriver({ client: connect() }) };
  },
});

describe("redis driver backend faults", () => {
  it("propagates a backend failure unchanged", async () => {
    const failure = new Error("connection reset");
    const client: RedisClient = {
      eval: () => Promise.reject(failure),
      quit: () => Promise.resolve("OK"),
    };
    const driver = createRedisDriver({ client });
    await expect(driver.push("jobs", "body", { availableAt: 0, now: 0 })).rejects.toBe(failure);
  });

  it("rejects when the connection is lost", async () => {
    const client = connect();
    const driver = createRedisDriver({ client });
    client.disconnect();
    await expect(driver.size("jobs", 0)).rejects.toThrow();
  });
});
