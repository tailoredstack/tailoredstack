export class InvalidArgumentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidArgumentError";
  }
}

export class LimitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LimitError";
  }
}

export class QueueClosedError extends Error {
  constructor() {
    super("queue is closed");
    this.name = "QueueClosedError";
  }
}
