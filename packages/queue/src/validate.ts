export function checkQueueName(queue: string): string {
  if (typeof queue !== "string" || queue.length === 0) {
    throw new TypeError("queue name must be a non-empty string");
  }
  return queue;
}

export function checkDuration(ms: number, label: string): number {
  if (!Number.isFinite(ms) || ms < 0) {
    throw new RangeError(`${label} must be a finite number of milliseconds >= 0`);
  }
  return ms;
}
