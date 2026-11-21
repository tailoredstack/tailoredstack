/** An argument the caller can fix. Thrown before the dialect sees the call. */
export class InvalidArgumentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidArgumentError";
  }
}

/** A value is valid but exceeds a configured or dialect limit. */
export class LimitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LimitError";
  }
}

/** The dialect was closed and no longer accepts calls. */
export class QueueClosedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueueClosedError";
  }
}
