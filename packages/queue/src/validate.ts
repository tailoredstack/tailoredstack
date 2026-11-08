import { InvalidArgumentError, LimitError } from "./errors.ts";

export const MAX_NAME_PATTERN = /^[A-Za-z0-9_-]{1,80}$/;
export const MAX_PAYLOAD_BYTES = 262_144;
export const MAX_LEASE_MS = 43_200_000;

const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;
const encoder = new TextEncoder();

export function validateName(name: unknown): void {
  if (typeof name !== "string" || !MAX_NAME_PATTERN.test(name)) {
    throw new InvalidArgumentError("name must match ^[A-Za-z0-9_-]{1,80}$");
  }
}

export function validateTime(label: string, value: unknown): void {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new InvalidArgumentError(`${label} must be a non-negative integer number of milliseconds`);
  }
}

export function validatePayload(payload: unknown): void {
  if (typeof payload !== "string") {
    throw new InvalidArgumentError("payload must be a string");
  }
  if (LONE_SURROGATE.test(payload)) {
    throw new InvalidArgumentError("payload must be well-formed UTF-16");
  }
  if (encoder.encode(payload).byteLength > MAX_PAYLOAD_BYTES) {
    throw new LimitError(`payload must be at most ${MAX_PAYLOAD_BYTES} UTF-8 bytes`);
  }
}

export function clampAvailableAt(availableAt: number, now: number): number {
  return Math.max(availableAt, now);
}

export function validateLeaseWindow(now: number, leaseUntil: number): void {
  if (leaseUntil <= now) {
    throw new InvalidArgumentError("leaseUntil must be later than now");
  }
  if (leaseUntil - now > MAX_LEASE_MS) {
    throw new LimitError(`lease must be at most ${MAX_LEASE_MS} ms`);
  }
}

export function validateReleaseDelay(availableAt: number, now: number): void {
  if (availableAt - now > MAX_LEASE_MS) {
    throw new LimitError(`release delay must be at most ${MAX_LEASE_MS} ms`);
  }
}

export function validatePushDelay(availableAt: number, now: number, maxPushDelayMs: number): void {
  if (availableAt - now > maxPushDelayMs) {
    throw new LimitError(`push delay must be at most ${maxPushDelayMs} ms`);
  }
}
