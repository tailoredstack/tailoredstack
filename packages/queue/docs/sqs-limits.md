# SQS limits and approximate-count tolerance (TS-64)

Research for the SQS driver (TS-34). Sources were read on 2026-11-07. AWS documentation URLs are in the research record for this issue.

## Count tolerance T

- **T = 0**, asserted only after a quiet period of at least 60 seconds with no producer sending to the queue.
- AWS says `ApproximateNumberOfMessages`, `ApproximateNumberOfMessagesNotVisible`, and `ApproximateNumberOfMessagesDelayed` "may not achieve consistency until at least 1 minute after the producers stop sending messages". AWS documents no numeric tolerance during traffic.
- During traffic the contract checks only `0 <= count <= sent - acked` for each bucket.
- The 60-second figure is AWS's propagation note, not a per-count error bound. The tolerance is a test-design choice. It does not promise exact counts.

## Limits that the SQS driver depends on

| Limit | Value | Driver use |
|---|---|---|
| Per-message delay (`DelaySeconds`) | 0 to 900 s | `maxPushDelayMs` = 900000 |
| Queue delay | 0 to 900 s | not used |
| Message size | 1 byte to 1,048,576 bytes (1 MiB) | the spec cap of 262144 bytes is lower, so the spec cap applies |
| Batch entries (`SendMessageBatch`, `DeleteMessageBatch`) | 10 | batch size 10 |
| `ReceiveMessage` `MaxNumberOfMessages` | 1 to 10 | receive size 1 to 10 |
| Visibility timeout (`ChangeMessageVisibility`) | 0 to 43,200 s | lease window must stay within 43,200 s; the spec cap is 43,200 s |
| Visibility cap from first receive | 12 h; extending does not reset it | a lease cannot outlast the first-receive cap |
| Message content | `#x9`, `#xA`, `#xD`, `#x20`-`#xD7FF`, `#xE000`-`#xFFFD`, `#x10000`-`#x10FFFF` | payloads containing other code points are rejected by SQS |

## Notes

- The Linear issue text gives 256 KiB for message size. The AWS quota is 1 MiB. The spec applies 262144 bytes to all drivers, so the spec value holds.
- Per-message timers are not supported on FIFO queues. The driver uses standard queues.
- `@aws-sdk/client-sqs` typings have no runtime range checks. SQS enforces the limits.
- The AWS SDK docs landing page and the Linear TS-64 page could not be read. The definition of T in the issue text was not read, so T = 0 follows AWS's consistency note and the spec's count rules.
