import { expect } from "vitest";
import { InvalidArgumentError, LimitError, QueueClosedError } from "../errors.js";
import type { Queue, QueueSize, Reserved } from "../types.js";

// The suite injects every `now`. Nothing here reads a wall clock.
export const NAME = "jobs";
export const T0 = 1_000_000;
export const SECOND = 1_000;
export const TWELVE_HOURS = 43_200_000;
export const MAX_PAYLOAD_BYTES = 262_144;
export const BAD_TIMES = [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY, -1, 1.5];

export interface Harness {
  readonly queue: Queue;
  /** A second queue on the same backend. Present for shared drivers only. */
  readonly peer?: Queue;
  readonly exactCounts: boolean;
}

export function at(availableAt: number, now = T0): { availableAt: number; now: number } {
  return { availableAt, now };
}

export function empty(): QueueSize {
  return { ready: 0, delayed: 0, reserved: 0 };
}

export async function mustReserve(queue: Queue, now: number, leaseUntil: number, name = NAME): Promise<Reserved> {
  const reserved = await queue.reserve(name, now, leaseUntil);
  if (reserved === null) {
    throw new Error(`expected a reservation at now=${now}`);
  }
  return reserved;
}

// Exact drivers must match. Approximate drivers only stay within [0, messages that exist].
export async function expectSize(
  queue: Queue,
  exactCounts: boolean,
  expected: QueueSize,
  now: number,
  name = NAME,
): Promise<void> {
  const actual = await queue.size(name, now);
  if (exactCounts) {
    expect(actual).toEqual(expected);
    return;
  }
  const total = expected.ready + expected.delayed + expected.reserved;
  for (const bucket of [actual.ready, actual.delayed, actual.reserved]) {
    expect(bucket).toBeGreaterThanOrEqual(0);
    expect(bucket).toBeLessThanOrEqual(total);
  }
}

// Good actors (spec §6 G1-G3)

export async function goodG1({ queue, exactCounts }: Harness): Promise<void> {
  await queue.push(NAME, "g1-a", at(T0));
  await queue.push(NAME, "g1-b", at(T0));
  await queue.push(NAME, "g1-later", at(T0 + 60 * SECOND));
  const first = await mustReserve(queue, T0, T0 + 30 * SECOND);
  expect(first.attempts).toBe(1);
  expect(await first.delete(T0)).toBe(true);
  const second = await mustReserve(queue, T0, T0 + 30 * SECOND);
  expect(second.payload).toBe("g1-b");
  expect(await second.release(T0 + 10 * SECOND, T0)).toBe(true);
  await expectSize(queue, exactCounts, { ready: 0, delayed: 2, reserved: 0 }, T0);
  await expectSize(queue, exactCounts, { ready: 1, delayed: 1, reserved: 0 }, T0 + 10 * SECOND);
}

export async function goodG2({ queue }: Harness): Promise<void> {
  const pushed = ["g2-0", "g2-1", "g2-2", "g2-3", "g2-4"];
  for (const payload of pushed) {
    await queue.push(NAME, payload, at(T0));
  }
  const handled: string[] = [];
  const ids = new Set<string>();
  let reserved = await queue.reserve(NAME, T0, T0 + 30 * SECOND);
  while (reserved !== null) {
    expect(reserved.attempts).toBe(1);
    expect(await reserved.delete(T0)).toBe(true);
    handled.push(reserved.payload);
    ids.add(reserved.id);
    reserved = await queue.reserve(NAME, T0, T0 + 30 * SECOND);
  }
  expect(handled.sort()).toEqual([...pushed].sort());
  expect(ids.size).toBe(pushed.length);
}

export async function goodG3({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "g3", at(T0));
  const first = await mustReserve(queue, T0, T0 + SECOND);
  expect(first.attempts).toBe(1);
  const second = await mustReserve(queue, T0 + SECOND, T0 + 2 * SECOND);
  expect(second.id).toBe(first.id);
  expect(second.attempts).toBe(2);
  expect(await second.delete(T0 + SECOND)).toBe(true);
}

// Bad actors (spec §6 B1-B10, plus B7 for shared drivers)

export async function badB1InvalidNames({ queue, exactCounts }: Harness): Promise<void> {
  for (const name of ["", " ", "a".repeat(81), "a/b"]) {
    await expect(queue.push(name, "x", at(T0))).rejects.toThrow(InvalidArgumentError);
    await expect(queue.reserve(name, T0, T0 + SECOND)).rejects.toThrow(InvalidArgumentError);
    await expect(queue.size(name, T0)).rejects.toThrow(InvalidArgumentError);
  }
  await expectSize(queue, exactCounts, empty(), T0);
}

export async function badB2InvalidTimes({ queue }: Harness): Promise<void> {
  for (const bad of BAD_TIMES) {
    await expect(queue.push(NAME, "x", at(bad, T0))).rejects.toThrow(InvalidArgumentError);
    await expect(queue.push(NAME, "x", at(T0, bad))).rejects.toThrow(InvalidArgumentError);
    await expect(queue.reserve(NAME, bad, T0 + SECOND)).rejects.toThrow(InvalidArgumentError);
    await expect(queue.reserve(NAME, T0, bad)).rejects.toThrow(InvalidArgumentError);
    await expect(queue.size(NAME, bad)).rejects.toThrow(InvalidArgumentError);
  }
}

export async function badB3LeaseNotAfterNow({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "b3", at(T0));
  await expect(queue.reserve(NAME, T0, T0)).rejects.toThrow(InvalidArgumentError);
  await expect(queue.reserve(NAME, T0, T0 - 1)).rejects.toThrow(InvalidArgumentError);
}

export async function badB4PayloadLimit({ queue, exactCounts }: Harness): Promise<void> {
  const fullAscii = "a".repeat(MAX_PAYLOAD_BYTES);
  // Each "é" is two UTF-8 bytes, so this is exactly MAX_PAYLOAD_BYTES bytes.
  const fullMultiByte = "é".repeat(MAX_PAYLOAD_BYTES / 2);
  await queue.push(NAME, fullAscii, at(T0));
  await queue.push(NAME, fullMultiByte, at(T0));
  await expect(queue.push(NAME, `${fullMultiByte}a`, at(T0))).rejects.toThrow(LimitError);
  await expectSize(queue, exactCounts, { ready: 2, delayed: 0, reserved: 0 }, T0);
  const first = await mustReserve(queue, T0, T0 + SECOND);
  expect(first.payload).toBe(fullAscii);
  const second = await mustReserve(queue, T0, T0 + SECOND);
  expect(second.payload).toBe(fullMultiByte);
}

export async function badB5ByteExact({ queue }: Harness): Promise<void> {
  const tricky = "nul\u0000 quote\" apostrophe' newline\n cr\r tab\t emoji\u{1F600} accent é separator\u2028 end";
  await queue.push(NAME, tricky, at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  expect(reserved.payload).toBe(tricky);
}

export async function badB6ReservedAfterClose({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "b6", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  await queue.close();
  await expect(reserved.delete(T0)).rejects.toThrow(QueueClosedError);
  await expect(reserved.release(T0, T0)).rejects.toThrow(QueueClosedError);
}

// Shared drivers only. The Reserved comes from one instance; the other instance observes it.
export async function badB7SharedHandle({ queue, peer, exactCounts }: Harness): Promise<void> {
  if (peer === undefined) {
    throw new Error("B7 needs a peer queue on the same backend");
  }
  await peer.push(NAME, "b7", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  await expectSize(peer, exactCounts, { ready: 0, delayed: 0, reserved: 1 }, T0);
  expect(await reserved.delete(T0)).toBe(true);
  await expectSize(peer, exactCounts, empty(), T0);
  expect(await peer.reserve(NAME, T0, T0 + SECOND)).toBeNull();
}

export async function badB8ParallelReserve({ queue }: Harness): Promise<void> {
  for (let i = 0; i < 10; i++) {
    await queue.push(NAME, `b8-${i}`, at(T0));
  }
  const attempts = await Promise.all(Array.from({ length: 50 }, () => queue.reserve(NAME, T0, T0 + SECOND)));
  const won = attempts.filter((reserved): reserved is Reserved => reserved !== null);
  expect(won).toHaveLength(10);
  expect(new Set(won.map((reserved) => reserved.id)).size).toBe(10);
  expect(attempts.filter((reserved) => reserved === null)).toHaveLength(40);
}

export async function badB9ParallelDelete({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "b9", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  const outcomes = await Promise.all([reserved.delete(T0), reserved.delete(T0)]);
  expect(outcomes.filter((ok) => ok)).toHaveLength(1);
}

export async function badB10LeaseOverTwelveHours({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "b10", at(T0));
  await expect(queue.reserve(NAME, T0, T0 + TWELVE_HOURS + 1)).rejects.toThrow(LimitError);
  const reserved = await mustReserve(queue, T0, T0 + TWELVE_HOURS);
  expect(reserved.payload).toBe("b10");
}

// Idiot actors (spec §6 I1-I11)

export async function idiotI1NeverDeletes({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "i1", at(T0));
  const first = await mustReserve(queue, T0, T0 + SECOND);
  const again = await mustReserve(queue, T0 + SECOND, T0 + 2 * SECOND);
  expect(again.id).toBe(first.id);
  expect(again.attempts).toBe(2);
}

export async function idiotI2DeletesAfterExpiry({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "i2", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  expect(await reserved.delete(T0 + SECOND)).toBe(false);
  const again = await mustReserve(queue, T0 + SECOND, T0 + 2 * SECOND);
  expect(again.attempts).toBe(2);
}

export async function idiotI3SecondsAsMilliseconds({ queue }: Harness): Promise<void> {
  // The caller meant 60 seconds but passed 60 milliseconds. The driver accepts it and it is a 60 ms delay.
  await queue.push(NAME, "i3", at(T0 + 60));
  expect(await queue.reserve(NAME, T0 + 59, T0 + 1_000)).toBeNull();
  const reserved = await mustReserve(queue, T0 + 60, T0 + 1_000);
  expect(reserved.payload).toBe("i3");
}

export async function idiotI4EarlierNowToDelete({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "i4", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  // The caller's clock is behind. The lease is still live by that clock, so the delete is accepted.
  expect(await reserved.delete(T0 - SECOND / 2)).toBe(true);
}

export async function idiotI5PastAvailableAt({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "i5", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  expect(await reserved.release(T0 - 5 * SECOND, T0)).toBe(true);
  const again = await mustReserve(queue, T0, T0 + SECOND);
  expect(again.payload).toBe("i5");
}

export async function idiotI6ReleasesTwice({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "i6", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  expect(await reserved.release(T0 + SECOND, T0)).toBe(true);
  expect(await reserved.release(T0 + SECOND, T0)).toBe(false);
}

export async function idiotI7ReuseAfterDelete({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "i7", at(T0));
  const reserved = await mustReserve(queue, T0, T0 + SECOND);
  expect(await reserved.delete(T0)).toBe(true);
  expect(await reserved.delete(T0)).toBe(false);
  expect(await reserved.release(T0, T0)).toBe(false);
}

export async function idiotI8IgnoresReturnValues({ queue }: Harness): Promise<void> {
  // Fire-and-forget calls must not leave unhandled rejections. close() waits for them.
  void queue.push(NAME, "i8-a", at(T0));
  void queue.push(NAME, "i8-b", at(T0));
  await queue.close();
}

export async function idiotI9TwoConsumers({ queue, peer }: Harness): Promise<void> {
  const other = peer ?? queue;
  await queue.push(NAME, "i9-a", at(T0));
  await queue.push(NAME, "i9-b", at(T0));
  const held = await mustReserve(queue, T0, T0 + 30 * SECOND);
  const heldByOther = await mustReserve(other, T0, T0 + 30 * SECOND);
  expect(heldByOther.id).not.toBe(held.id);
  expect(await queue.reserve(NAME, T0, T0 + 30 * SECOND)).toBeNull();
}

export async function idiotI10SamePayloadTwice({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "same", at(T0));
  await queue.push(NAME, "same", at(T0));
  const first = await mustReserve(queue, T0, T0 + SECOND);
  const second = await mustReserve(queue, T0, T0 + SECOND);
  expect(second.id).not.toBe(first.id);
  expect([first.payload, second.payload]).toEqual(["same", "same"]);
}

export async function idiotI11CloseDuringReserve({ queue }: Harness): Promise<void> {
  await queue.push(NAME, "i11", at(T0));
  const pending = queue.reserve(NAME, T0, T0 + SECOND);
  const closing = queue.close();
  expect(await pending).not.toBeNull();
  await closing;
  await expect(queue.push(NAME, "late", at(T0))).rejects.toThrow(QueueClosedError);
}
