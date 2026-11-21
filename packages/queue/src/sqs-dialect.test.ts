import { SQSClient } from "@aws-sdk/client-sqs";
import { FlociContainer, type StartedFlociContainer } from "@floci/testcontainers";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { runDialectSuite } from "./dialect-suite.ts";
import { SQS_CAPABILITIES, SqsDialect } from "./sqs-dialect.ts";

// A backend-clock dialect spends real seconds on lease expiry and delayed release. Keep the longer timeout local to
// this file so the rest of the package keeps the default fast-failure budget.
vi.setConfig({ testTimeout: 30_000 });

// Digest of floci/floci:2.4.0, so a moved tag cannot change what CI runs.
const FLOCI_IMAGE = "floci/floci@sha256:5ee85db604c4aa74ac76e07ea5cc83086f37bb472bf3dbb97e34019e48efae9c";

let floci: StartedFlociContainer | undefined;
let client: SQSClient | undefined;
let instances = 0;

const open = (prefix = `q${++instances}`): SqsDialect => {
  if (!client) throw new Error("SQS client is not ready");
  return new SqsDialect({ client, queueNamePrefix: prefix });
};

beforeAll(async () => {
  floci = await new FlociContainer(FLOCI_IMAGE).withoutDockerSocket().start();
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
  client?.destroy();
  await floci?.stop();
});

describe("SqsDialect", () => {
  runDialectSuite("SqsDialect", open, SQS_CAPABILITIES);

  it("declares no order, approximate counts, a backend clock, and SQS delay and lease caps", () => {
    expect(SQS_CAPABILITIES).toEqual({
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

  it("keeps different queue names on different SQS queues", async () => {
    const d = open();
    const now = Date.now();
    await d.enqueue("alpha", "a", { availableAt: now, now });

    await expect(d.dequeue("beta", { now, leaseUntil: now + 30_000 })).resolves.toBeNull();
    expect(await d.count("beta", now)).toEqual({ ready: 0, delayed: 0, reserved: 0 });
    expect((await d.dequeue("alpha", { now, leaseUntil: now + 30_000 }))?.payload).toBe("a");
  });

  it("hides an enqueued message until its delay", async () => {
    const d = open();
    const now = Date.now();
    await d.enqueue("delayed", "later", { availableAt: now + 1_000, now });
    await expect(d.dequeue("delayed", { now, leaseUntil: now + 30_000 })).resolves.toBeNull();

    await new Promise((resolve) => setTimeout(resolve, 2_200));
    expect((await d.dequeue("delayed", { now: Date.now(), leaseUntil: Date.now() + 30_000 }))?.payload).toBe("later");
  });

  it("reports a delayed release in the SQS visibility bucket", async () => {
    const d = open();
    const now = Date.now();
    await d.enqueue("releases", "held", { availableAt: now, now });
    const message = await d.dequeue("releases", { now, leaseUntil: now + 30_000 });
    if (!message) throw new Error("expected a message");

    await d.release("releases", message.receipt, { availableAt: now + 60_000, now });
    await expect
      .poll(() => d.count("releases", Date.now()), { timeout: 10_000, interval: 100 })
      .toEqual({ ready: 0, delayed: 0, reserved: 1 });
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
