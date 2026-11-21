export type ClockSource = "caller" | "backend";

export interface DialectCapabilities {
  readonly ordered: boolean;
  readonly exactCounts: boolean;
  readonly clockSource: ClockSource;
  readonly maxDelayMs: number;
  readonly maxLeaseMs: number;
}

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

export interface Dialect {
  readonly capabilities: DialectCapabilities;

  enqueue(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string>;

  dequeue(name: string, at: { now: number; leaseUntil: number }): Promise<Message | null>;

  acknowledge(name: string, receipt: Receipt, now: number): Promise<boolean>;

  release(name: string, receipt: Receipt, at: { availableAt: number; now: number }): Promise<boolean>;

  count(name: string, now: number): Promise<QueueCount>;

  close(): Promise<void>;
}
