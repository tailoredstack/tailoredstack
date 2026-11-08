import type { Driver, DriverCapabilities, QueueSize } from "./types.ts";

interface Entry {
  readonly id: string;
  readonly name: string;
  readonly payload: string;
  availableAt: number;
  attempts: number;
  leaseUntil: number | null;
  token: string | null;
}

export const MEMORY_CAPABILITIES: DriverCapabilities = {
  shared: false,
  exactCounts: true,
  maxPushDelayMs: Number.POSITIVE_INFINITY,
};

// Entries stay in push order, so the first entry with the smallest availableAt is also the earliest push.
export function createMemoryDriver(): Driver {
  let entries: Entry[] = [];
  let idSequence = 0;
  let tokenSequence = 0;

  const isLeased = (entry: Entry, now: number): boolean => entry.leaseUntil !== null && entry.leaseUntil > now;

  // A token is current only while its lease is live. Expired or replaced tokens are stale.
  function current(name: string, token: string, now: number): Entry | undefined {
    const entry = entries.find((candidate) => candidate.name === name && candidate.token === token);
    return entry !== undefined && isLeased(entry, now) ? entry : undefined;
  }

  return {
    capabilities: MEMORY_CAPABILITIES,

    async push(name, payload, at) {
      idSequence += 1;
      const id = `m${idSequence}`;
      entries.push({
        id,
        name,
        payload,
        availableAt: at.availableAt,
        attempts: 0,
        leaseUntil: null,
        token: null,
      });
      return id;
    },

    async reserve(name, now, leaseUntil) {
      let best: Entry | undefined;
      for (const entry of entries) {
        if (entry.name !== name || isLeased(entry, now) || entry.availableAt > now) {
          continue;
        }
        if (best === undefined || entry.availableAt < best.availableAt) {
          best = entry;
        }
      }
      if (best === undefined) {
        return null;
      }
      tokenSequence += 1;
      best.attempts += 1;
      best.leaseUntil = leaseUntil;
      best.token = `t${tokenSequence}`;
      return { id: best.id, payload: best.payload, attempts: best.attempts, token: best.token };
    },

    async delete(name, token, now) {
      const entry = current(name, token, now);
      if (entry === undefined) {
        return false;
      }
      entries = entries.filter((candidate) => candidate !== entry);
      return true;
    },

    async release(name, token, availableAt, now) {
      const entry = current(name, token, now);
      if (entry === undefined) {
        return false;
      }
      entry.availableAt = availableAt;
      entry.leaseUntil = null;
      entry.token = null;
      return true;
    },

    async size(name, now): Promise<QueueSize> {
      const size: QueueSize = { ready: 0, delayed: 0, reserved: 0 };
      for (const entry of entries) {
        if (entry.name !== name) {
          continue;
        }
        if (isLeased(entry, now)) {
          size.reserved += 1;
        } else if (entry.availableAt > now) {
          size.delayed += 1;
        } else {
          size.ready += 1;
        }
      }
      return size;
    },

    async close() {
      entries = [];
    },
  };
}
