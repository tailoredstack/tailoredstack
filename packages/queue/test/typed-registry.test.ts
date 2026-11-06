import { describe, expect, it } from "vitest";
import { Queue } from "../src/index.ts";
import { FakeDriver } from "./fake-driver.fake.ts";

// The handler map is the single source of truth for job names. Names are derived from it, so a job
// name typo is a compile error rather than a runtime failure.
const handlers = {
  SendInvoice: async () => {},
  Ping: async () => {},
} satisfies Record<string, () => Promise<void>>;

type Names = keyof typeof handlers;

describe("typed job registry", () => {
  it("accepts registered names in push, pushOn, and later", async () => {
    const queue = new Queue<Names>({ driver: new FakeDriver() });

    await queue.push({ name: "SendInvoice" });
    await queue.pushOn("mail", { name: "Ping" });
    await queue.later(0, { name: "SendInvoice" });

    // push and later go to the default queue; pushOn went to "mail".
    expect(await queue.size()).toBe(2);
    expect(await queue.size("mail")).toBe(1);
  });

  it("rejects an unregistered name in push at compile time", () => {
    const queue = new Queue<Names>({ driver: new FakeDriver() });
    // Never invoked: this test only checks that the call does not compile.
    const attempt = () => queue.push({ name: "CancelInvoice" });
    // @ts-expect-error CancelInvoice is not a registered job name
    void attempt;
  });

  it("rejects an unregistered name in pushOn at compile time", () => {
    const queue = new Queue<Names>({ driver: new FakeDriver() });
    const attempt = () => queue.pushOn("mail", { name: "CancelInvoice" });
    // @ts-expect-error CancelInvoice is not a registered job name
    void attempt;
  });

  it("rejects an unregistered name in later at compile time", () => {
    const queue = new Queue<Names>({ driver: new FakeDriver() });
    const attempt = () => queue.later(1_000, { name: "CancelInvoice" });
    // @ts-expect-error CancelInvoice is not a registered job name
    void attempt;
  });

  it("keeps an untyped queue accepting any name, so existing callers do not change", async () => {
    const queue = new Queue({ driver: new FakeDriver() });

    await queue.push({ name: "AnythingAtAll" });

    expect(await queue.size()).toBe(1);
  });
});
