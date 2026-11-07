import { describe, expect, it } from "vitest";
import { createQueue, InvalidArgumentError, LimitError, QueueClosedError } from "../src/index.js";
import { deferred, fakeDriver } from "./fake-driver.fake.js";

const NOW = 1_000_000;
const HOUR = 3_600_000;
const MAX_PAYLOAD_BYTES = 262_144;

function setup(maxPushDelayMs?: number) {
  const driver = fakeDriver(maxPushDelayMs);
  return { driver, queue: createQueue(driver) };
}

describe("push validation (P1-P9)", () => {
  it("P1: availableAt equal to now is passed through unchanged", async () => {
    const { driver, queue } = setup();
    await queue.push("jobs", "x", { availableAt: NOW, now: NOW });
    expect(driver.push).toHaveBeenCalledWith("jobs", "x", { availableAt: NOW, now: NOW });
  });

  it("P2: a future availableAt is passed through unchanged", async () => {
    const { driver, queue } = setup();
    await queue.push("jobs", "x", { availableAt: NOW + 5_000, now: NOW });
    expect(driver.push).toHaveBeenCalledWith("jobs", "x", { availableAt: NOW + 5_000, now: NOW });
  });

  it("P3: an availableAt before now is clamped to now before the driver sees it", async () => {
    const { driver, queue } = setup();
    await queue.push("jobs", "x", { availableAt: NOW - 10, now: NOW });
    expect(driver.push).toHaveBeenCalledWith("jobs", "x", { availableAt: NOW, now: NOW });
  });

  it("P4: an empty payload is accepted", async () => {
    const { driver, queue } = setup();
    await expect(queue.push("jobs", "", { availableAt: NOW, now: NOW })).resolves.toBe("id-1");
    expect(driver.push).toHaveBeenCalledOnce();
  });

  it("P5: a non-string payload is an InvalidArgumentError and nothing is pushed", async () => {
    const { driver, queue } = setup();
    const at = { availableAt: NOW, now: NOW };
    await expect(queue.push("jobs", 42 as unknown as string, at)).rejects.toThrow(InvalidArgumentError);
    expect(driver.push).not.toHaveBeenCalled();
  });

  it("P5: a payload with a lone surrogate is an InvalidArgumentError", async () => {
    const { driver, queue } = setup();
    const at = { availableAt: NOW, now: NOW };
    await expect(queue.push("jobs", "ab\uD800", at)).rejects.toThrow(InvalidArgumentError);
    await expect(queue.push("jobs", "\uDC00ab", at)).rejects.toThrow(InvalidArgumentError);
    expect(driver.push).not.toHaveBeenCalled();
  });

  it("P5: a well-formed surrogate pair is accepted", async () => {
    const { driver, queue } = setup();
    await queue.push("jobs", "\u{1F600}", { availableAt: NOW, now: NOW });
    expect(driver.push).toHaveBeenCalledOnce();
  });

  it("P6 and B4: the payload limit counts UTF-8 bytes; 262144 is accepted and 262145 is a LimitError", async () => {
    const { driver, queue } = setup();
    const at = { availableAt: NOW, now: NOW };
    const exact = "a".repeat(MAX_PAYLOAD_BYTES);
    await queue.push("jobs", exact, at);
    expect(driver.push).toHaveBeenCalledOnce();
    await expect(queue.push("jobs", `${exact}a`, at)).rejects.toThrow(LimitError);
    expect(driver.push).toHaveBeenCalledOnce();
  });

  it("B4: multi-byte characters at the boundary count three bytes each", async () => {
    const { driver, queue } = setup();
    const at = { availableAt: NOW, now: NOW };
    const euros = "\u20AC".repeat(87_381); // 262143 bytes
    await queue.push("jobs", `${euros}a`, at); // 262144 bytes
    expect(driver.push).toHaveBeenCalledOnce();
    await expect(queue.push("jobs", `${euros}aa`, at)).rejects.toThrow(LimitError);
  });

  it("P7 and B1: names outside ^[A-Za-z0-9_-]{1,80}$ are InvalidArgumentErrors", async () => {
    const { driver, queue } = setup();
    const at = { availableAt: NOW, now: NOW };
    for (const name of ["", " ", "a/b", "a.b", "a".repeat(81), "naïve"]) {
      await expect(queue.push(name, "x", at)).rejects.toThrow(InvalidArgumentError);
    }
    expect(driver.push).not.toHaveBeenCalled();
  });

  it("P7: names at the edges of the rule are accepted", async () => {
    const { driver, queue } = setup();
    const at = { availableAt: NOW, now: NOW };
    await queue.push("a".repeat(80), "x", at);
    await queue.push("a-b_C9", "x", at);
    expect(driver.push).toHaveBeenCalledTimes(2);
  });

  it("P8 and B2: NaN, Infinity, negative, and fractional times are InvalidArgumentErrors", async () => {
    const { driver, queue } = setup();
    const bad = [Number.NaN, Number.POSITIVE_INFINITY, -1, 1.5];
    for (const value of bad) {
      await expect(queue.push("jobs", "x", { availableAt: value, now: NOW })).rejects.toThrow(InvalidArgumentError);
      await expect(queue.push("jobs", "x", { availableAt: NOW, now: value })).rejects.toThrow(InvalidArgumentError);
    }
    expect(driver.push).not.toHaveBeenCalled();
  });

  it("P9: a push delay over maxPushDelayMs is a LimitError; equal to it is accepted", async () => {
    const { driver, queue } = setup(12 * HOUR);
    await expect(queue.push("jobs", "x", { availableAt: NOW + 12 * HOUR + 1, now: NOW })).rejects.toThrow(LimitError);
    expect(driver.push).not.toHaveBeenCalled();
    await queue.push("jobs", "x", { availableAt: NOW + 12 * HOUR, now: NOW });
    expect(driver.push).toHaveBeenCalledOnce();
  });

  it("P9: the push delay is measured after clamping, so a past availableAt is never a LimitError", async () => {
    const { driver, queue } = setup(900_000);
    await queue.push("jobs", "x", { availableAt: NOW - 1_000, now: NOW });
    expect(driver.push).toHaveBeenCalledOnce();
  });

  it("returns the id the driver assigned", async () => {
    const { driver, queue } = setup();
    driver.push.mockResolvedValueOnce("abc");
    await expect(queue.push("jobs", "x", { availableAt: NOW, now: NOW })).resolves.toBe("abc");
  });
});

describe("reserve validation (S7, S8)", () => {
  it("S7: leaseUntil equal to or before now is an InvalidArgumentError", async () => {
    const { driver, queue } = setup();
    await expect(queue.reserve("jobs", NOW, NOW)).rejects.toThrow(InvalidArgumentError);
    await expect(queue.reserve("jobs", NOW, NOW - 1)).rejects.toThrow(InvalidArgumentError);
    expect(driver.reserve).not.toHaveBeenCalled();
  });

  it("S8 and B10: a lease over 12h is a LimitError; exactly 12h is accepted", async () => {
    const { driver, queue } = setup();
    await expect(queue.reserve("jobs", NOW, NOW + 12 * HOUR + 1)).rejects.toThrow(LimitError);
    expect(driver.reserve).not.toHaveBeenCalled();
    await queue.reserve("jobs", NOW, NOW + 12 * HOUR);
    expect(driver.reserve).toHaveBeenCalledWith("jobs", NOW, NOW + 12 * HOUR);
  });

  it("returns null when the driver has nothing ready", async () => {
    const { queue } = setup();
    await expect(queue.reserve("jobs", NOW, NOW + 1_000)).resolves.toBeNull();
  });

  it("rejects an invalid name before the driver is called", async () => {
    const { driver, queue } = setup();
    await expect(queue.reserve("a/b", NOW, NOW + 1_000)).rejects.toThrow(InvalidArgumentError);
    expect(driver.reserve).not.toHaveBeenCalled();
  });
});

describe("Reserved (R7, R8)", () => {
  it("exposes the driver id and attempts, and does not expose the token", async () => {
    const { driver, queue } = setup();
    driver.reserve.mockResolvedValueOnce({ id: "m1", attempts: 2, token: "t-secret", payload: "body" });
    const reserved = await queue.reserve("jobs", NOW, NOW + 1_000);
    expect(reserved?.id).toBe("m1");
    expect(reserved?.attempts).toBe(2);
    expect(reserved?.payload).toBe("body");
    expect(JSON.stringify(reserved)).not.toContain("t-secret");
    expect(Object.keys(reserved ?? {})).not.toContain("token");
  });

  it("delete passes the token and now to the driver and returns its result", async () => {
    const { driver, queue } = setup();
    driver.reserve.mockResolvedValueOnce({ id: "m1", attempts: 1, token: "t1", payload: "x" });
    const reserved = await queue.reserve("jobs", NOW, NOW + 1_000);
    await expect(reserved?.delete(NOW)).resolves.toBe(true);
    expect(driver.delete).toHaveBeenCalledWith("jobs", "t1", NOW);
  });

  it("release clamps a past availableAt and passes it with the token", async () => {
    const { driver, queue } = setup();
    driver.reserve.mockResolvedValueOnce({ id: "m1", attempts: 1, token: "t1", payload: "x" });
    const reserved = await queue.reserve("jobs", NOW, NOW + 1_000);
    await expect(reserved?.release(NOW - 5, NOW)).resolves.toBe(true);
    expect(driver.release).toHaveBeenCalledWith("jobs", "t1", NOW, NOW);
  });

  it("stale delete and release return the driver's false without throwing", async () => {
    const { driver, queue } = setup();
    driver.reserve.mockResolvedValueOnce({ id: "m1", attempts: 1, token: "t1", payload: "x" });
    driver.delete.mockResolvedValueOnce(false);
    driver.release.mockResolvedValueOnce(false);
    const reserved = await queue.reserve("jobs", NOW, NOW + 1_000);
    await expect(reserved?.delete(NOW)).resolves.toBe(false);
    await expect(reserved?.release(NOW, NOW)).resolves.toBe(false);
  });

  it("L6: release with an invalid time is an InvalidArgumentError", async () => {
    const { driver, queue } = setup();
    driver.reserve.mockResolvedValueOnce({ id: "m1", attempts: 1, token: "t1", payload: "x" });
    const reserved = await queue.reserve("jobs", NOW, NOW + 1_000);
    await expect(reserved?.release(Number.NaN, NOW)).rejects.toThrow(InvalidArgumentError);
    await expect(reserved?.release(NOW, 1.5)).rejects.toThrow(InvalidArgumentError);
    expect(driver.release).not.toHaveBeenCalled();
  });

  it("L7: a release delay over 12h is a LimitError; exactly 12h is accepted", async () => {
    const { driver, queue } = setup();
    driver.reserve.mockResolvedValueOnce({ id: "m1", attempts: 1, token: "t1", payload: "x" });
    const reserved = await queue.reserve("jobs", NOW, NOW + 1_000);
    await expect(reserved?.release(NOW + 12 * HOUR + 1, NOW)).rejects.toThrow(LimitError);
    expect(driver.release).not.toHaveBeenCalled();
    await reserved?.release(NOW + 12 * HOUR, NOW);
    expect(driver.release).toHaveBeenCalledWith("jobs", "t1", NOW + 12 * HOUR, NOW);
  });
});

describe("size validation (Z6 input)", () => {
  it("rejects an invalid name and an invalid now", async () => {
    const { driver, queue } = setup();
    await expect(queue.size("", NOW)).rejects.toThrow(InvalidArgumentError);
    await expect(queue.size("jobs", Number.NaN)).rejects.toThrow(InvalidArgumentError);
    expect(driver.size).not.toHaveBeenCalled();
  });

  it("passes the caller's now to the driver and returns its counts", async () => {
    const { driver, queue } = setup();
    driver.size.mockResolvedValueOnce({ ready: 1, delayed: 2, reserved: 3 });
    await expect(queue.size("jobs", NOW)).resolves.toEqual({ ready: 1, delayed: 2, reserved: 3 });
    expect(driver.size).toHaveBeenCalledWith("jobs", NOW);
  });
});

describe("driver errors", () => {
  it("propagate unchanged", async () => {
    const { driver, queue } = setup();
    const boom = new Error("backend down");
    driver.push.mockRejectedValueOnce(boom);
    await expect(queue.push("jobs", "x", { availableAt: NOW, now: NOW })).rejects.toBe(boom);
  });
});

describe("close (C1-C4, Z6, D7, L8)", () => {
  it("C2: every operation after close throws QueueClosedError without calling the driver", async () => {
    const { driver, queue } = setup();
    await queue.close();
    const at = { availableAt: NOW, now: NOW };
    await expect(queue.push("jobs", "x", at)).rejects.toThrow(QueueClosedError);
    await expect(queue.reserve("jobs", NOW, NOW + 1_000)).rejects.toThrow(QueueClosedError);
    await expect(queue.size("jobs", NOW)).rejects.toThrow(QueueClosedError);
    expect(driver.push).not.toHaveBeenCalled();
    expect(driver.reserve).not.toHaveBeenCalled();
    expect(driver.size).not.toHaveBeenCalled();
  });

  it("C2: the closed check runs before validation", async () => {
    const { queue } = setup();
    await queue.close();
    await expect(queue.push("a/b", "x", { availableAt: NOW, now: NOW })).rejects.toThrow(QueueClosedError);
  });

  it("Z6: size after close throws QueueClosedError", async () => {
    const { queue } = setup();
    await queue.close();
    await expect(queue.size("jobs", NOW)).rejects.toThrow(QueueClosedError);
  });

  it("C1: a second close is a no-op and the driver is closed once", async () => {
    const { driver, queue } = setup();
    await queue.close();
    await queue.close();
    expect(driver.close).toHaveBeenCalledOnce();
  });

  it("C4, B6, D7, L8: a Reserved from a closed queue throws QueueClosedError on delete and release", async () => {
    const { driver, queue } = setup();
    driver.reserve.mockResolvedValueOnce({ id: "m1", attempts: 1, token: "t1", payload: "x" });
    const reserved = await queue.reserve("jobs", NOW, NOW + 1_000);
    await queue.close();
    await expect(reserved?.delete(NOW)).rejects.toThrow(QueueClosedError);
    await expect(reserved?.release(NOW, NOW)).rejects.toThrow(QueueClosedError);
    expect(driver.delete).not.toHaveBeenCalled();
    expect(driver.release).not.toHaveBeenCalled();
  });

  it("C3: close waits for an in-flight call, and the in-flight call completes normally", async () => {
    const { driver, queue } = setup();
    const gate = deferred<null>();
    driver.reserve.mockImplementationOnce(() => gate.promise);
    const inFlight = queue.reserve("jobs", NOW, NOW + 1_000);
    let closed = false;
    const closing = queue.close().then(() => {
      closed = true;
    });
    await Promise.resolve();
    expect(closed).toBe(false);
    expect(driver.close).not.toHaveBeenCalled();
    gate.resolve(null);
    await expect(inFlight).resolves.toBeNull();
    await closing;
    expect(closed).toBe(true);
    expect(driver.close).toHaveBeenCalledOnce();
  });

  it("I11: a call started after close is requested throws QueueClosedError", async () => {
    const { driver, queue } = setup();
    const gate = deferred<null>();
    driver.reserve.mockImplementationOnce(() => gate.promise);
    const inFlight = queue.reserve("jobs", NOW, NOW + 1_000);
    const closing = queue.close();
    await expect(queue.size("jobs", NOW)).rejects.toThrow(QueueClosedError);
    gate.resolve(null);
    await inFlight;
    await closing;
    expect(driver.size).not.toHaveBeenCalled();
  });
});
