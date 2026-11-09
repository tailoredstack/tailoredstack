import { runInNewContext } from "node:vm";
import { describe, expect, it, vi } from "vitest";
import type { Dialect, DialectCapabilities } from "./dialect.ts";
import { Job, type JobContext } from "./job.ts";
import { Queue } from "./queue.ts";

interface SendMailPayload {
  readonly to: string;
}

interface ResizeImagePayload {
  readonly id: number;
}

class SendMail extends Job<SendMailPayload> {
  declare readonly __jobKey: "SendMail";

  async handle(_ctx: JobContext<unknown>): Promise<void> {}
}

class ResizeImage extends Job<ResizeImagePayload> {
  declare readonly __jobKey: "ResizeImage";

  async handle(_ctx: JobContext<unknown>): Promise<void> {}
}

// Same shape as SendMail, never registered, no brand.
class Rogue extends Job<SendMailPayload> {
  async handle(_ctx: JobContext<unknown>): Promise<void> {}
}

const MAX_PAYLOAD_BYTES = 262144;
const MAX_DELAY_MS = 60_000;

const capabilities: DialectCapabilities = {
  ordered: true,
  exactCounts: true,
  clockSource: "caller",
  maxDelayMs: MAX_DELAY_MS,
  maxLeaseMs: 0,
};

function fakeDialect() {
  const enqueue = vi.fn(async (_name: string, _payload: string, _at: { availableAt: number; now: number }) => "id-1");
  const dialect = {
    capabilities,
    enqueue,
    dequeue: vi.fn(async () => null),
    acknowledge: vi.fn(async () => true),
    release: vi.fn(async () => true),
    count: vi.fn(async () => ({ ready: 0, delayed: 0, reserved: 0 })),
    close: vi.fn(async () => {}),
  } satisfies Dialect;
  return { dialect, enqueue };
}

function mailQueue(dialect: Dialect, now?: () => number, queue?: string) {
  return new Queue(dialect, { jobs: { SendMail, ResizeImage }, now, queue });
}

/** Builds a `to` string so the serialized SendMail payload is exactly `totalBytes` UTF-8 bytes. */
function mailOfBytes(totalBytes: number, multiByte: boolean): SendMail {
  const overhead = Buffer.byteLength(JSON.stringify({ job: "SendMail", payload: { to: "" } }), "utf8");
  const room = totalBytes - overhead;
  if (!multiByte) return new SendMail({ to: "a".repeat(room) });
  const twoByte = Math.floor(room / 2);
  return new SendMail({ to: "é".repeat(twoByte) + "a".repeat(room - twoByte * 2) });
}

describe("Queue", () => {
  describe("delay conversion", () => {
    it("sets availableAt to now() plus the relative delay", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await queue.enqueue(new SendMail({ to: "a@example.com" }), { delay: 500 });
      expect(enqueue).toHaveBeenCalledWith(
        "default",
        JSON.stringify({ job: "SendMail", payload: { to: "a@example.com" } }),
        { availableAt: 1_500, now: 1_000 },
      );
    });

    it("sets availableAt to now() when no delay is given", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await queue.enqueue(new SendMail({ to: "a@example.com" }));
      expect(enqueue.mock.calls[0]?.[2]).toEqual({ availableAt: 1_000, now: 1_000 });
    });

    it("treats a zero delay as no delay", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 1_000);
      await queue.enqueue(new SendMail({ to: "a@example.com" }), { delay: 0 });
      expect(enqueue.mock.calls[0]?.[2]).toEqual({ availableAt: 1_000, now: 1_000 });
    });
  });

  describe("clock injection", () => {
    it("uses the injected now for availableAt and the dialect now", async () => {
      const { dialect, enqueue } = fakeDialect();
      const clock = vi.fn(() => 7_777);
      const queue = mailQueue(dialect, clock);
      await queue.enqueue(new SendMail({ to: "a" }), { delay: 3 });
      expect(clock).toHaveBeenCalled();
      expect(enqueue.mock.calls[0]?.[2]).toEqual({ availableAt: 7_780, now: 7_777 });
    });

    it("defaults to Date.now when no clock is injected", async () => {
      const spy = vi.spyOn(Date, "now").mockReturnValue(42);
      try {
        const { dialect, enqueue } = fakeDialect();
        const queue = mailQueue(dialect);
        await queue.enqueue(new SendMail({ to: "a" }), { delay: 8 });
        expect(enqueue.mock.calls[0]?.[2]).toEqual({ availableAt: 50, now: 42 });
      } finally {
        spy.mockRestore();
      }
    });

    it("uses the queue name option, defaulting to default", async () => {
      const { dialect, enqueue } = fakeDialect();
      await mailQueue(dialect, () => 0, "mail").enqueue(new SendMail({ to: "a" }));
      expect(enqueue.mock.calls[0]?.[0]).toBe("mail");
      await mailQueue(dialect, () => 0).enqueue(new SendMail({ to: "a" }));
      expect(enqueue.mock.calls[1]?.[0]).toBe("default");
    });
  });

  describe("maxDelayMs", () => {
    it("rejects a delay over maxDelayMs without calling the dialect", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      await expect(queue.enqueue(new SendMail({ to: "a" }), { delay: MAX_DELAY_MS + 1 })).rejects.toThrow();
      expect(enqueue).not.toHaveBeenCalled();
    });

    it("accepts a delay equal to maxDelayMs", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      await queue.enqueue(new SendMail({ to: "a" }), { delay: MAX_DELAY_MS });
      expect(enqueue).toHaveBeenCalledTimes(1);
    });
  });

  describe("invalid delay", () => {
    it.each([-1, Number.NaN, Number.POSITIVE_INFINITY])("rejects delay %s before the dialect call", async (delay) => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      await expect(queue.enqueue(new SendMail({ to: "a" }), { delay })).rejects.toThrow(/finite number of ms >= 0/);
      expect(enqueue).not.toHaveBeenCalled();
    });
  });

  describe("envelope size limit", () => {
    it("accepts an envelope of exactly 262144 bytes", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      const job = mailOfBytes(MAX_PAYLOAD_BYTES, false);
      await queue.enqueue(job);
      const payload = enqueue.mock.calls[0]?.[1] ?? "";
      expect(Buffer.byteLength(payload, "utf8")).toBe(MAX_PAYLOAD_BYTES);
    });

    it("rejects an envelope one byte over the limit (262145 bytes) without calling the dialect", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      await expect(queue.enqueue(mailOfBytes(MAX_PAYLOAD_BYTES + 1, false))).rejects.toThrow();
      expect(enqueue).not.toHaveBeenCalled();
    });

    it("counts a multi-byte envelope by UTF-8 bytes, not characters", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      // Fewer than 262144 characters, but over 262144 bytes.
      const overhead = Buffer.byteLength(JSON.stringify({ job: "SendMail", payload: { to: "" } }), "utf8");
      const job = new SendMail({ to: "é".repeat(Math.ceil((MAX_PAYLOAD_BYTES - overhead) / 2) + 1) });
      await expect(queue.enqueue(job)).rejects.toThrow();
      expect(enqueue).not.toHaveBeenCalled();
    });

    it("passes a multi-byte envelope that is exactly 262144 bytes", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      await queue.enqueue(mailOfBytes(MAX_PAYLOAD_BYTES, true));
      expect(Buffer.byteLength(enqueue.mock.calls[0]?.[1] ?? "", "utf8")).toBe(MAX_PAYLOAD_BYTES);
    });
  });

  describe("registry", () => {
    it("throws at runtime for an instance whose class is not registered", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      const rogue = new Rogue({ to: "a" }) as unknown as SendMail;
      await expect(queue.enqueue(rogue)).rejects.toThrow();
      expect(enqueue).not.toHaveBeenCalled();
    });

    it("throws at runtime for a hand-copied brand on an unregistered class", async () => {
      class Copied extends Job<SendMailPayload> {
        declare readonly __jobKey: "SendMail";

        async handle(_ctx: JobContext<unknown>): Promise<void> {}
      }
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      await expect(queue.enqueue(new Copied({ to: "a" }))).rejects.toThrow(/Copied is not registered/);
      expect(enqueue).not.toHaveBeenCalled();
    });

    it("throws at construction when one class is registered under two keys", () => {
      const { dialect } = fakeDialect();
      expect(() => new Queue(dialect, { jobs: { A: SendMail, B: SendMail } as never })).toThrow(/SendMail.*"A".*"B"/);
    });

    it("rejects an Array subclass with its own prototype methods", async () => {
      const { dialect, enqueue } = fakeDialect();
      class Tagged extends Array<number> {
        tag(): string {
          return "x";
        }
      }
      await expect(mailQueue(dialect, () => 0).enqueue(new SendMail(Tagged.from([1, 2]) as never))).rejects.toThrow(
        /payload is not a plain array/,
      );
      expect(enqueue).not.toHaveBeenCalled();
    });

    it("rejects an array with a null prototype with a clean message", async () => {
      const { dialect, enqueue } = fakeDialect();
      const nullProtoArray = Object.setPrototypeOf([1, 2], null) as never;
      await expect(mailQueue(dialect, () => 0).enqueue(new SendMail(nullProtoArray))).rejects.toThrow(
        /payload is not a plain array/,
      );
      expect(enqueue).not.toHaveBeenCalled();
    });

    it("accepts an array created in another realm", async () => {
      const { dialect, enqueue } = fakeDialect();
      const foreign = runInNewContext("[1, 2]") as never;
      await mailQueue(dialect, () => 0).enqueue(new SendMail(foreign));
      expect(enqueue).toHaveBeenCalledTimes(1);
    });

    it("accepts a plain object created in another realm", async () => {
      const { dialect, enqueue } = fakeDialect();
      const foreign = runInNewContext("({ to: 'a', nested: [1, 2] })") as SendMailPayload;
      await mailQueue(dialect, () => 0).enqueue(new SendMail(foreign));
      expect(enqueue).toHaveBeenCalledTimes(1);
    });

    it("serializes to the wire format { job: key, payload }", async () => {
      const { dialect, enqueue } = fakeDialect();
      const queue = mailQueue(dialect, () => 0);
      await queue.enqueue(new ResizeImage({ id: 9 }));
      expect(JSON.parse(enqueue.mock.calls[0]?.[1] ?? "")).toEqual({ job: "ResizeImage", payload: { id: 9 } });
    });

    it("returns the id from the dialect", async () => {
      const { dialect } = fakeDialect();
      await expect(mailQueue(dialect, () => 0).enqueue(new SendMail({ to: "a" }))).resolves.toBe("id-1");
    });
  });

  describe("strict payload walk", () => {
    class Anything extends Job<unknown> {
      declare readonly __jobKey: "Anything";

      async handle(_ctx: JobContext<unknown>): Promise<void> {}
    }

    const anyQueue = (dialect: Dialect) => new Queue(dialect, { jobs: { Anything } });

    async function expectRejected(payload: unknown, pattern: RegExp): Promise<void> {
      const { dialect, enqueue } = fakeDialect();
      await expect(anyQueue(dialect).enqueue(new Anything(payload))).rejects.toThrow(pattern);
      expect(enqueue).not.toHaveBeenCalled();
    }

    async function expectAccepted(payload: unknown): Promise<void> {
      const { dialect, enqueue } = fakeDialect();
      await anyQueue(dialect).enqueue(new Anything(payload));
      expect(enqueue).toHaveBeenCalledTimes(1);
    }

    it("rejects values JSON cannot represent: undefined, functions, symbols, bigint", async () => {
      await expectRejected(undefined, /unserializable type undefined/);
      await expectRejected(() => 1, /unserializable type function/);
      await expectRejected(Symbol("s"), /unserializable type symbol/);
      await expectRejected(1n, /unserializable type bigint/);
    });

    it("reports the path of a nested undefined, which JSON would silently drop", async () => {
      await expectRejected({ a: { b: undefined } }, /payload\.a\.b is not|payload\.a\.b has/);
      await expectRejected([undefined], /payload\.0 has unserializable type undefined/);
    });

    it("rejects non-finite numbers and -0", async () => {
      await expectRejected(Number.NaN, /not a finite number/);
      await expectRejected(Number.POSITIVE_INFINITY, /not a finite number/);
      await expectRejected(Number.NEGATIVE_INFINITY, /not a finite number/);
      await expectRejected(-0, /-0/);
    });

    it("rejects cycles", async () => {
      const a: Record<string, unknown> = {};
      a.self = a;
      await expectRejected(a, /payload\.self is a cycle/);
    });

    it("accepts a shared reference that is not a cycle", async () => {
      const shared = { x: 1 };
      await expectAccepted({ a: shared, b: shared });
    });

    it("rejects non-plain objects: Date, Map, class instances", async () => {
      await expectRejected(new Date(0), /not a plain object/);
      await expectRejected(new Map(), /not a plain object/);
      await expectRejected(new Anything(1), /not a plain object/);
    });

    it("rejects symbol keys, non-enumerable properties, and getters", async () => {
      await expectRejected({ [Symbol("k")]: 1 }, /has a symbol key/);
      await expectRejected(
        Object.defineProperty({}, "hidden", { value: 1, enumerable: false }),
        /hidden is not enumerable/,
      );
      await expectRejected(
        {
          get x() {
            return 1;
          },
        },
        /payload\.x is an accessor/,
      );
    });

    it("rejects sparse arrays and arrays with extra properties", async () => {
      const sparse: unknown[] = [];
      sparse[2] = 1;
      await expectRejected(sparse, /is sparse or has extra properties/);
      await expectRejected(Object.assign([1], { extra: 2 }), /is sparse or has extra properties/);
    });

    it("accepts every JSON-safe shape, including null-prototype objects", async () => {
      const nullProto = Object.assign(Object.create(null) as Record<string, unknown>, { k: "v" });
      await expectAccepted(null);
      await expectAccepted("s");
      await expectAccepted(true);
      await expectAccepted(0);
      await expectAccepted(1.5);
      await expectAccepted({});
      await expectAccepted([]);
      await expectAccepted({ nested: [1, "two", { three: null }], empty: {} });
      await expectAccepted(nullProto);
    });
  });

  describe("compile-time checks", () => {
    it("declares the compile-time checks (type-checked only, never invoked)", () => {
      // Nothing here runs: the function exists so tsc checks each @ts-expect-error line.
      const compileOnly = (dialect: Dialect) => {
        // @ts-expect-error Rogue has no __jobKey brand and is not registered
        new Queue(dialect, { jobs: { SendMail, Rogue } });
        const queue = new Queue(dialect, { jobs: { SendMail } });
        // @ts-expect-error Rogue is not in this queue's registry, and has no brand
        void queue.enqueue(new Rogue({ to: "a" }));
        // @ts-expect-error ResizeImage is not in this queue's registry
        void queue.enqueue(new ResizeImage({ id: 1 }));
        // @ts-expect-error payload.to must be a string
        void queue.enqueue(new SendMail({ to: 1 }));
      };
      expect(compileOnly).toBeTypeOf("function");
    });
  });
});
