/**
 * Who owns time for a dialect. `caller`: the dialect never reads a clock and
 * uses only the absolute times passed in. `backend`: the backend reads its own clock.
 */
export type ClockSource = "caller" | "backend";

export interface DialectCapabilities {
  readonly ordered: boolean;
  readonly exactCounts: boolean;
  readonly clockSource: ClockSource;
  readonly maxDelayMs: number;
  readonly maxLeaseMs: number;
}

/** Opaque token issued by `dequeue`. Only the backend that issued it can verify it. */
export type Receipt = string & { readonly __brand: "Receipt" };

export interface Message {
  readonly id: string;
  readonly payload: string;
  readonly attempts: number;
  readonly receipt: Receipt;
}

export interface QueueCount {
  readonly ready: number;
  readonly delayed: number;
  readonly reserved: number;
}

/**
 * Pluggable storage backend. Every time-dependent method takes absolute epoch-ms
 * values computed by the caller from its own clock.
 */
export interface Dialect {
  readonly capabilities: DialectCapabilities;

  /**
   * Stores a payload on queue `name`, visible to `dequeue` at or after `at.availableAt`.
   * Resolves the new message id. A dialect with `clockSource: 'caller'` never reads a clock.
   */
  enqueue(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string>;

  /**
   * Reserves the next ready message until `at.leaseUntil` and returns it with a receipt,
   * or `null` if none is ready. A dialect with `clockSource: 'caller'` never reads a clock.
   */
  dequeue(name: string, at: { now: number; leaseUntil: number }): Promise<Message | null>;

  /**
   * Permanently removes the reserved message identified by `receipt`.
   * Resolves `true` on success. A stale or unknown receipt resolves `false`; it never throws.
   * A dialect with `clockSource: 'caller'` never reads a clock.
   */
  acknowledge(name: string, receipt: Receipt, now: number): Promise<boolean>;

  /**
   * Returns the reserved message identified by `receipt` to the queue, visible again at
   * `at.availableAt`. Resolves `true` on success. A stale or unknown receipt resolves `false`;
   * it never throws. A dialect with `clockSource: 'caller'` never reads a clock.
   */
  release(name: string, receipt: Receipt, at: { availableAt: number; now: number }): Promise<boolean>;

  /**
   * Returns ready, delayed, and reserved counts for queue `name` as of `now`.
   * A dialect with `clockSource: 'caller'` never reads a clock.
   */
  count(name: string, now: number): Promise<QueueCount>;

  /** Releases backend resources. Pending operations may reject after close. */
  close(): Promise<void>;
}
