import { vi } from "vitest";
import type { Driver } from "../src/index.js";

export function fakeDriver(maxPushDelayMs = Number.POSITIVE_INFINITY) {
  const driver = {
    capabilities: { shared: false, exactCounts: true, maxPushDelayMs },
    push: vi.fn<Driver["push"]>(async () => "id-1"),
    reserve: vi.fn<Driver["reserve"]>(async () => null),
    delete: vi.fn<Driver["delete"]>(async () => true),
    release: vi.fn<Driver["release"]>(async () => true),
    size: vi.fn<Driver["size"]>(async () => ({ ready: 0, delayed: 0, reserved: 0 })),
    close: vi.fn<Driver["close"]>(async () => {}),
  };
  return driver satisfies Driver;
}

export function deferred<T>() {
  return Promise.withResolvers<T>();
}
