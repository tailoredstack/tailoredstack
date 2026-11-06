import type {
  ChangeVisibilityInput,
  DeleteInput,
  GetAttributesInput,
  ReceiveInput,
  SendInput,
  SqsClient,
} from "../src/drivers/sqs.ts";

interface Message {
  readonly id: string;
  readonly body: string;
  receiveCount: number;
  received: boolean;
  /** Epoch ms when the message is next visible. Before its first receive this is the delay end. */
  visibleAt: number;
  receipt: string | null;
}

export interface SqsCall {
  readonly op: string;
  readonly input: Record<string, unknown>;
}

/**
 * In-memory SQS for tests. Time comes from `clock`, which the test sets, so visibility timeouts and
 * delays are exact. Records every call in `calls`. Not exported from the package.
 */
export class FakeSqsClient implements SqsClient {
  clock = 0;
  readonly calls: SqsCall[] = [];
  readonly #queues = new Map<string, Message[]>();
  #seq = 0;
  #receipts = 0;

  async sendMessage(input: SendInput): Promise<{ MessageId?: string }> {
    this.calls.push({ op: "SendMessage", input: { ...input } });
    const id = String(++this.#seq);
    this.#queue(input.QueueUrl).push({
      id,
      body: input.MessageBody,
      receiveCount: 0,
      received: false,
      visibleAt: this.clock + (input.DelaySeconds ?? 0) * 1000,
      receipt: null,
    });
    return { MessageId: id };
  }

  async receiveMessage(input: ReceiveInput): Promise<{ Messages?: Record<string, unknown>[] }> {
    this.calls.push({ op: "ReceiveMessage", input: { ...input } });
    const max = input.MaxNumberOfMessages ?? 1;
    const timeout = input.VisibilityTimeout ?? 30;
    const visible = this.#queue(input.QueueUrl)
      .filter((m) => m.visibleAt <= this.clock)
      .slice(0, max);
    if (visible.length === 0) return {};
    return {
      Messages: visible.map((m) => {
        m.received = true;
        m.receiveCount += 1;
        m.visibleAt = this.clock + timeout * 1000;
        m.receipt = `receipt-${++this.#receipts}`;
        return {
          MessageId: m.id,
          ReceiptHandle: m.receipt,
          Body: m.body,
          Attributes: { ApproximateReceiveCount: String(m.receiveCount) },
        };
      }),
    };
  }

  async deleteMessage(input: DeleteInput): Promise<unknown> {
    this.calls.push({ op: "DeleteMessage", input: { ...input } });
    const queue = this.#queue(input.QueueUrl);
    const index = queue.findIndex((m) => m.receipt === input.ReceiptHandle || m.id === input.ReceiptHandle);
    if (index < 0) throw new Error("ReceiptHandleIsInvalid");
    queue.splice(index, 1);
    return {};
  }

  async changeMessageVisibility(input: ChangeVisibilityInput): Promise<unknown> {
    this.calls.push({ op: "ChangeMessageVisibility", input: { ...input } });
    const message = this.#queue(input.QueueUrl).find((m) => m.receipt === input.ReceiptHandle);
    if (!message) throw new Error("ReceiptHandleIsInvalid");
    message.visibleAt = this.clock + input.VisibilityTimeout * 1000;
    return {};
  }

  async getQueueAttributes(input: GetAttributesInput): Promise<{ Attributes?: Record<string, string> }> {
    this.calls.push({ op: "GetQueueAttributes", input: { ...input } });
    let visible = 0;
    let delayed = 0;
    let notVisible = 0;
    for (const m of this.#queue(input.QueueUrl)) {
      if (m.visibleAt <= this.clock) visible += 1;
      else if (m.received) notVisible += 1;
      else delayed += 1;
    }
    return {
      Attributes: {
        ApproximateNumberOfMessages: String(visible),
        ApproximateNumberOfMessagesNotVisible: String(notVisible),
        ApproximateNumberOfMessagesDelayed: String(delayed),
      },
    };
  }

  #queue(url: string): Message[] {
    let queue = this.#queues.get(url);
    if (!queue) {
      queue = [];
      this.#queues.set(url, queue);
    }
    return queue;
  }
}
