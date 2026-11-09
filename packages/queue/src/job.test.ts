import { describe, expect, expectTypeOf, it } from "vitest";
import { Job, type JobContext } from "./job.ts";

interface SendEmailPayload {
  readonly to: string;
  readonly subject: string;
}

interface MailContext {
  readonly tenant: string;
}

class SendEmail extends Job<SendEmailPayload, MailContext> {
  readonly seen: Array<JobContext<MailContext>> = [];

  async handle(ctx: JobContext<MailContext>): Promise<void> {
    this.seen.push(ctx);
  }
}

describe("Job", () => {
  it("exposes a typed, readable payload", () => {
    const job = new SendEmail({ to: "a@example.com", subject: "hi" });
    expect(job.payload).toEqual({ to: "a@example.com", subject: "hi" });
    expectTypeOf(job.payload).toEqualTypeOf<SendEmailPayload>();
  });

  it("calls handle with one context argument carrying signal and context fields", async () => {
    const job = new SendEmail({ to: "a@example.com", subject: "hi" });
    const controller = new AbortController();
    await expect(job.handle({ tenant: "acme", signal: controller.signal })).resolves.toBeUndefined();
    expect(job.seen).toHaveLength(1);
    expect(job.seen[0]?.signal).toBeInstanceOf(AbortSignal);
    expect(job.seen[0]?.tenant).toBe("acme");
  });

  it("defaults Ctx to unknown so Job<Payload> still works", () => {
    class Plain extends Job<SendEmailPayload> {
      async handle(ctx: JobContext<unknown>): Promise<void> {
        expect(ctx.signal).toBeInstanceOf(AbortSignal);
      }
    }
    expectTypeOf<JobContext<unknown>["signal"]>().toEqualTypeOf<AbortSignal>();
    expect(new Plain({ to: "a", subject: "b" })).toBeDefined();
  });

  it("rejects a subclass that does not implement handle at compile time", () => {
    // @ts-expect-error handle is abstract and must be implemented
    class Incomplete extends Job<SendEmailPayload> {}
    expect(Incomplete).toBeDefined();
  });

  it("rejects the old two-argument handle shape at compile time", () => {
    const compileOnly = () => {
      class OldShape extends Job<SendEmailPayload> {
        // @ts-expect-error handle takes one JobContext argument, not (signal, extra)
        async handle(_signal: AbortSignal, _extra: unknown): Promise<void> {}
      }
      return OldShape;
    };
    expect(compileOnly).toBeTypeOf("function");
  });
});
