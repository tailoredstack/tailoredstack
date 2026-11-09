import { describe, expectTypeOf, it } from "vitest";
import type { Dialect, DialectCapabilities, Message, QueueCount, Receipt } from "./dialect.js";

const capabilities: DialectCapabilities = {
  ordered: true,
  exactCounts: true,
  clockSource: "caller",
  maxDelayMs: 0,
  maxLeaseMs: 0,
};

const receipt = "r" as Receipt;

const enqueue = async (_name: string, _payload: string, _at: { availableAt: number; now: number }) => "id";
const dequeue = async (_name: string, _at: { now: number; leaseUntil: number }): Promise<Message | null> => null;
const acknowledge = async (_name: string, _receipt: Receipt, _now: number) => true;
const release = async (_name: string, _receipt: Receipt, _at: { availableAt: number; now: number }) => true;
const count = async (_name: string, _now: number): Promise<QueueCount> => ({ ready: 0, delayed: 0, reserved: 0 });
const close = async () => {};

describe("Dialect contract", () => {
  it("accepts an object with exactly the six members", () => {
    const full: Dialect = { capabilities, enqueue, dequeue, acknowledge, release, count, close };
    expectTypeOf(full).toExtend<Dialect>();
    expectTypeOf<Dialect["dequeue"]>().parameter(1).toEqualTypeOf<{ now: number; leaseUntil: number }>();
  });

  it("rejects an object missing capabilities", () => {
    // @ts-expect-error missing capabilities
    const d: Dialect = { enqueue, dequeue, acknowledge, release, count, close };
    expectTypeOf(d).toExtend<Dialect>();
  });

  it("rejects an object missing enqueue", () => {
    // @ts-expect-error missing enqueue
    const d: Dialect = { capabilities, dequeue, acknowledge, release, count, close };
    expectTypeOf(d).toExtend<Dialect>();
  });

  it("rejects an object missing dequeue", () => {
    // @ts-expect-error missing dequeue
    const d: Dialect = { capabilities, enqueue, acknowledge, release, count, close };
    expectTypeOf(d).toExtend<Dialect>();
  });

  it("rejects an object missing acknowledge", () => {
    // @ts-expect-error missing acknowledge
    const d: Dialect = { capabilities, enqueue, dequeue, release, count, close };
    expectTypeOf(d).toExtend<Dialect>();
  });

  it("rejects an object missing release", () => {
    // @ts-expect-error missing release
    const d: Dialect = { capabilities, enqueue, dequeue, acknowledge, count, close };
    expectTypeOf(d).toExtend<Dialect>();
  });

  it("rejects an object missing count", () => {
    // @ts-expect-error missing count
    const d: Dialect = { capabilities, enqueue, dequeue, acknowledge, release, close };
    expectTypeOf(d).toExtend<Dialect>();
  });

  it("rejects an object missing close", () => {
    // @ts-expect-error missing close
    const d: Dialect = { capabilities, enqueue, dequeue, acknowledge, release, count };
    expectTypeOf(d).toExtend<Dialect>();
  });

  it("requires lease arguments on dequeue", () => {
    const d = { capabilities, enqueue, dequeue, acknowledge, release, count, close } satisfies Dialect;
    // @ts-expect-error dequeue requires { now, leaseUntil }
    void d.dequeue("q");
    void receipt;
  });
});
