import { SQSClient } from "@aws-sdk/client-sqs";
import { FlociContainer, type StartedFlociContainer } from "@floci/testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { runDialectSuite } from "./dialect-suite.ts";
import { SqsDialect } from "./sqs-dialect.ts";

let floci: StartedFlociContainer;
let client: SQSClient;
let instances = 0;

const open = (prefix = `q${++instances}`): SqsDialect => new SqsDialect({ client, queueNamePrefix: prefix });

beforeAll(async () => {
  floci = await new FlociContainer("floci/floci:2.4.0").withoutDockerSocket().start();
  client = new SQSClient({
    endpoint: floci.getEndpoint(),
    region: floci.getRegion(),
    credentials: {
      accessKeyId: floci.getAccessKey(),
      secretAccessKey: floci.getSecretKey(),
    },
  });
}, 120_000);

afterAll(async () => {
  client.destroy();
  await floci.stop();
});

describe("SqsDialect", () => {
  runDialectSuite("SqsDialect", open);

  it("declares no order, approximate counts, a backend clock, and SQS delay and lease caps", () => {
    expect(open().capabilities).toEqual({
      ordered: false,
      exactCounts: false,
      clockSource: "backend",
      maxDelayMs: 900_000,
      maxLeaseMs: 43_200_000,
    });
  });

  it("reports ready, delayed, and reserved buckets from the queue attributes", async () => {
    const d = open();
    const now = Date.now();
    await d.enqueue("counts", "ready", { availableAt: now, now });
    await d.enqueue("counts", "delayed", { availableAt: now + 30_000, now });
    await d.enqueue("counts", "reserved", { availableAt: now, now });
    const held = await d.dequeue("counts", { now, leaseUntil: now + 30_000 });
    expect(held).not.toBeNull();

    await expect
      .poll(() => d.count("counts", Date.now()), { timeout: 10_000, interval: 100 })
      .toEqual({ ready: 1, delayed: 1, reserved: 1 });
  });

  it("lets a second instance on the same prefix consume and settle a message", async () => {
    const producer = open("shared");
    const consumer = open("shared");
    const now = Date.now();
    await producer.enqueue("jobs", "hello", { availableAt: now, now });

    const message = await consumer.dequeue("jobs", { now, leaseUntil: now + 30_000 });
    if (!message) throw new Error("expected a message");
    expect(message.payload).toBe("hello");
    expect(await producer.acknowledge("jobs", message.receipt, Date.now())).toBe(true);
    expect(await consumer.dequeue("jobs", { now: Date.now(), leaseUntil: Date.now() + 30_000 })).toBeNull();
  });
});
