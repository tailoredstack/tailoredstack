export interface QueueSize {
  ready: number;
  delayed: number;
  reserved: number;
}

export interface Reserved {
  readonly id: string;
  readonly payload: string;
  readonly attempts: number;
  delete(now: number): Promise<boolean>;
  release(availableAt: number, now: number): Promise<boolean>;
}

export interface Queue {
  push(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string>;
  reserve(name: string, now: number, leaseUntil: number): Promise<Reserved | null>;
  size(name: string, now: number): Promise<QueueSize>;
  close(): Promise<void>;
}

export interface DriverCapabilities {
  readonly shared: boolean;
  readonly exactCounts: boolean;
  readonly maxPushDelayMs: number;
}

export interface DriverLease {
  id: string;
  payload: string;
  attempts: number;
  token: string;
}

export interface Driver {
  readonly capabilities: DriverCapabilities;
  push(name: string, payload: string, at: { availableAt: number; now: number }): Promise<string>;
  reserve(name: string, now: number, leaseUntil: number): Promise<DriverLease | null>;
  delete(name: string, token: string, now: number): Promise<boolean>;
  release(name: string, token: string, availableAt: number, now: number): Promise<boolean>;
  size(name: string, now: number): Promise<QueueSize>;
  close(): Promise<void>;
}
