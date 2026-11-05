import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import type { TestProject } from "vitest/node";

// One real Redis per test run. Fails the run if Docker is unavailable: no silent skips.
let container: StartedRedisContainer | undefined;

export default async function setup(project: TestProject): Promise<() => Promise<void>> {
  container = await new RedisContainer("redis:8-alpine").start();
  project.provide("redisUrl", container.getConnectionUrl());
  return async () => {
    await container?.stop();
  };
}

declare module "vitest" {
  export interface ProvidedContext {
    redisUrl: string;
  }
}
