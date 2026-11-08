import { QueueClosedError } from "./errors.ts";
import type { Driver, DriverLease, Queue, Reserved } from "./types.ts";
import {
  clampAvailableAt,
  validateLeaseWindow,
  validateName,
  validatePayload,
  validatePushDelay,
  validateReleaseDelay,
  validateTime,
} from "./validate.ts";

export function createQueue(driver: Driver): Queue {
  let closing = false;
  let inFlight = 0;
  let drained: PromiseWithResolvers<void> | undefined;
  let closed: Promise<void> | undefined;

  function exit(): void {
    inFlight--;
    if (closing && inFlight === 0) {
      drained?.resolve();
    }
  }

  async function run<T>(operation: () => Promise<T>): Promise<T> {
    if (closing) {
      throw new QueueClosedError();
    }
    inFlight++;
    try {
      return await operation();
    } finally {
      exit();
    }
  }

  function wrap(name: string, lease: DriverLease): Reserved {
    const { token } = lease;
    return {
      id: lease.id,
      payload: lease.payload,
      attempts: lease.attempts,
      delete: (now) =>
        run(async () => {
          validateTime("now", now);
          return driver.delete(name, token, now);
        }),
      release: (availableAt, now) =>
        run(async () => {
          validateTime("availableAt", availableAt);
          validateTime("now", now);
          const clamped = clampAvailableAt(availableAt, now);
          validateReleaseDelay(clamped, now);
          return driver.release(name, token, clamped, now);
        }),
    };
  }

  return {
    push: (name, payload, at) =>
      run(async () => {
        validateName(name);
        validatePayload(payload);
        validateTime("now", at.now);
        validateTime("availableAt", at.availableAt);
        const availableAt = clampAvailableAt(at.availableAt, at.now);
        validatePushDelay(availableAt, at.now, driver.capabilities.maxPushDelayMs);
        return driver.push(name, payload, { availableAt, now: at.now });
      }),

    reserve: (name, now, leaseUntil) =>
      run(async () => {
        validateName(name);
        validateTime("now", now);
        validateTime("leaseUntil", leaseUntil);
        validateLeaseWindow(now, leaseUntil);
        const lease = await driver.reserve(name, now, leaseUntil);
        return lease === null ? null : wrap(name, lease);
      }),

    size: (name, now) =>
      run(async () => {
        validateName(name);
        validateTime("now", now);
        return driver.size(name, now);
      }),

    close: () => {
      closed ??= (async () => {
        closing = true;
        if (inFlight > 0) {
          drained = Promise.withResolvers<void>();
          await drained.promise;
        }
        await driver.close();
      })();
      return closed;
    },
  };
}
