import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import { type StartedValkeyContainer, ValkeyContainer } from "@testcontainers/valkey";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createRedisDriver, type RedisClient } from "../src/drivers/redis.ts";
import { driverContract } from "./driver-contract.ts";

interface Backend {
  readonly name: string;
  readonly image: string;
  start(): Promise<{ url: string; stop(): Promise<unknown> }>;
}

const backends: readonly Backend[] = [
  {
    name: "redis",
    image: "redis:7",
    start: async () => {
      const container: StartedRedisContainer = await new RedisContainer("redis:7").start();
      return { url: container.getConnectionUrl(), stop: () => container.stop() };
    },
  },
  {
    name: "valkey",
    image: "valkey/valkey:8",
    start: async () => {
      const container: StartedValkeyContainer = await new ValkeyContainer("valkey/valkey:8").start();
      return { url: container.getConnectionUrl(), stop: () => container.stop() };
    },
  },
];

for (const backend of backends) {
  describe(`redis driver on ${backend.image}`, () => {
    let stop: () => Promise<unknown>;
    let url: string;
    const clients: Redis[] = [];

    function connect(): Redis {
      const client = new Redis(url);
      clients.push(client);
      return client;
    }

    beforeAll(async () => {
      const started = await backend.start();
      url = started.url;
      stop = started.stop;
    }, 120_000);

    afterAll(async () => {
      for (const client of clients.splice(0)) {
        client.disconnect();
      }
      await stop();
    });

    // Two driver instances, each with its own connection to one server: the shared-backend contract (B7) needs a peer.
    driverContract({
      name: backend.name,
      capabilities: { shared: true, exactCounts: true, maxPushDelayMs: 43_200_000 },
      fixture: async () => {
        await connect().flushdb();
        return { driver: createRedisDriver({ client: connect() }), peer: createRedisDriver({ client: connect() }) };
      },
    });

    describe("backend faults", () => {
      it("propagates a backend failure unchanged", async () => {
        const failure = new Error("connection reset");
        const client: RedisClient = {
          eval: () => Promise.reject(failure),
          quit: () => Promise.resolve("OK"),
        };
        const driver = createRedisDriver({ client });
        await expect(driver.push("jobs", "body", { availableAt: 0, now: 0 })).rejects.toBe(failure);
      });

      it("rejects operations after the client is disconnected", async () => {
        const client = connect();
        const driver = createRedisDriver({ client });
        client.disconnect();
        await expect(driver.size("jobs", 0)).rejects.toThrow();
      });
    });
  });
}
