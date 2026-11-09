// Runs as a consumer would: imports the installed package, never the source tree.
import {
  createQueue,
  type Driver,
  type DriverLease,
  InvalidArgumentError,
  LimitError,
  QueueClosedError,
  type QueueSize,
} from "@tailoredstack/queue";

// The package exposes no public driver factory (src/index.ts exports createQueue, the errors and types only),
// so the consumer supplies its own in-process Driver. It is the only backend reachable through the public API.
interface Row {
  id: string;
  name: string;
  payload: string;
  availableAt: number;
  attempts: number;
  leaseUntil: number | null;
  token: string | null;
}

function createConsumerDriver(): Driver {
  const rows: Row[] = [];
  let sequence = 0;
  const leased = (row: Row, now: number): boolean => row.leaseUntil !== null && row.leaseUntil > now;
  const live = (name: string, token: string, now: number): Row | undefined =>
    rows.find((row) => row.name === name && row.token === token && leased(row, now));

  return {
    capabilities: { shared: false, exactCounts: true, maxPushDelayMs: Number.POSITIVE_INFINITY },
    async push(name, payload, at) {
      sequence++;
      const id = `job-${sequence}`;
      rows.push({ id, name, payload, availableAt: at.availableAt, attempts: 0, leaseUntil: null, token: null });
      return id;
    },
    async reserve(name, now, leaseUntil) {
      const row = rows.find(
        (candidate) => candidate.name === name && !leased(candidate, now) && candidate.availableAt <= now,
      );
      if (row === undefined) return null;
      row.attempts++;
      row.leaseUntil = leaseUntil;
      row.token = `token-${row.id}-${row.attempts}`;
      const lease: DriverLease = { id: row.id, payload: row.payload, attempts: row.attempts, token: row.token };
      return lease;
    },
    async delete(name, token, now) {
      const row = live(name, token, now);
      if (row === undefined) return false;
      rows.splice(rows.indexOf(row), 1);
      return true;
    },
    async release(name, token, availableAt, now) {
      const row = live(name, token, now);
      if (row === undefined) return false;
      row.availableAt = availableAt;
      row.leaseUntil = null;
      row.token = null;
      return true;
    },
    async size(name, now): Promise<QueueSize> {
      const size: QueueSize = { ready: 0, delayed: 0, reserved: 0 };
      for (const row of rows) {
        if (row.name !== name) continue;
        if (leased(row, now)) size.reserved++;
        else if (row.availableAt <= now) size.ready++;
        else size.delayed++;
      }
      return size;
    },
    async close() {},
  };
}

// Maps each public error class to a stable label so the test can assert the exact type thrown.
function outcome(error: unknown): string {
  if (error instanceof InvalidArgumentError) return "InvalidArgumentError";
  if (error instanceof LimitError) return "LimitError";
  if (error instanceof QueueClosedError) return "QueueClosedError";
  return `unexpected: ${String(error)}`;
}

async function thrown(operation: Promise<unknown>): Promise<string> {
  try {
    await operation;
    return "resolved";
  } catch (error) {
    return outcome(error);
  }
}

const NOW = 1_000_000;
const observed: Record<string, unknown> = {};

observed.resolvesToDist = import.meta
  .resolve("@tailoredstack/queue")
  .includes("/node_modules/@tailoredstack/queue/dist/");

const queue = createQueue(createConsumerDriver());

observed.invalidName = await thrown(queue.push("bad name!", "x", { availableAt: NOW, now: NOW }));
observed.invalidNow = await thrown(queue.size("emails", -1));
observed.emptyReserve = (await queue.reserve("emails", NOW, NOW + 1000)) === null;

observed.pushedReady = await queue.push("emails", "a", { availableAt: NOW, now: NOW });
observed.pushedDelayed = await queue.push("emails", "b", { availableAt: NOW + 5000, now: NOW });
observed.sizeBeforeReserve = await queue.size("emails", NOW);

const first = await queue.reserve("emails", NOW, NOW + 1000);
observed.reserved = first && { id: first.id, payload: first.payload, attempts: first.attempts };
observed.sizeWhileReserved = await queue.size("emails", NOW);
observed.reserveWhileLeased = (await queue.reserve("emails", NOW, NOW + 1000)) === null;

observed.releaseFirst = first ? await first.release(NOW + 2000, NOW) : null;
observed.releaseStaleToken = first ? await first.release(NOW + 2000, NOW) : null;
observed.sizeAfterRelease = await queue.size("emails", NOW);

const second = await queue.reserve("emails", NOW + 2000, NOW + 3000);
observed.reservedAgain = second && { id: second.id, payload: second.payload, attempts: second.attempts };
observed.deleteSecond = second ? await second.delete(NOW + 2000) : null;
observed.deleteStaleToken = second ? await second.delete(NOW + 2000) : null;
observed.sizeAfterDelete = await queue.size("emails", NOW + 2000);

observed.leaseNotLater = await thrown(queue.reserve("emails", NOW, NOW));
observed.leaseTooLong = await thrown(queue.reserve("emails", NOW, NOW + 43_200_001));
observed.payloadTooLarge = await thrown(queue.push("emails", "x".repeat(262_145), { availableAt: NOW, now: NOW }));
observed.payloadAtLimit = await thrown(queue.push("emails", "x".repeat(262_144), { availableAt: NOW, now: NOW }));

await queue.close();
observed.pushAfterClose = await thrown(queue.push("emails", "c", { availableAt: NOW, now: NOW }));
observed.reserveAfterClose = await thrown(queue.reserve("emails", NOW, NOW + 1000));

console.log(JSON.stringify({ ok: true, observed }));
