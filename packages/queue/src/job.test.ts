import { describe, expect, expectTypeOf, it } from "vitest";
import { Job } from "./job.ts";

interface SendEmailPayload {
  readonly to: string;
  readonly subject: string;
}

class SendEmail extends Job<SendEmailPayload> {
  readonly seen: AbortSignal[] = [];

  async handle(signal: AbortSignal): Promise<void> {
    this.seen.push(signal);
  }
}

describe("Job", () => {
  it("exposes a typed, readable payload", () => {
    const job = new SendEmail({ to: "a@example.com", subject: "hi" });
    expect(job.payload).toEqual({ to: "a@example.com", subject: "hi" });
    expectTypeOf(job.payload).toEqualTypeOf<SendEmailPayload>();
  });

  it("calls handle with an AbortSignal and resolves", async () => {
    const job = new SendEmail({ to: "a@example.com", subject: "hi" });
    const controller = new AbortController();
    await expect(job.handle(controller.signal)).resolves.toBeUndefined();
    expect(job.seen).toHaveLength(1);
    expect(job.seen[0]).toBeInstanceOf(AbortSignal);
  });

  it("rejects a subclass that does not implement handle at compile time", () => {
    // @ts-expect-error handle is abstract and must be implemented
    class Incomplete extends Job<SendEmailPayload> {}
    expect(Incomplete).toBeDefined();
  });
});
