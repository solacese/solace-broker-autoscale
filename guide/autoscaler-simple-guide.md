# Autoscaling in simple English

The autoscaler watches how much work each managed queue partition sends through its broker. When one broker stays too busy, it chooses a partition that will fit on another broker and moves its ownership automatically.

A business key such as an account or order ID always maps to the same partition. We do not send successive payments round robin across unrelated brokers. The recorded owner tells publishers and consumers where that partition lives.

Imagine A is at 90% load and B is a spare. Moving a partition worth 30% can leave A at 60% and B at 30%. These are illustrative numbers. The real decision uses your matching measured performance profile and current queue counters.

The handover has six steps:

1. Create the destination queue with incoming messages disabled.
2. Wait for a consumer to bind there.
3. Block new writes on the original queue. The broker rejects even publishers using old cached addresses.
4. Retain new payments in the publisher's durable buffer while the original consumer finishes the old queue.
5. Wait for the grace period and a continuously empty queue, including all outstanding acknowledgments.
6. Record the new owner, enable it, and retry buffered messages there.

Other partitions keep processing. A restart resumes the saved handover phase. A timeout never permits throwing away payments. Delivery retries can duplicate messages, so the business application must record each event ID and its business changes in the same database transaction.

`solace-autoscale run` is the unattended controller. `serve` provides routing and worker discovery. Both use the same configuration and persistent local state. Keep consumer workers running so they can prepare new destinations. Optional Cloud provisioning replaces used warm spares within the configured broker ceiling.

Use [the complete automatic scaling guide](automatic-scaling.md) for setup, every operational YAML option, the payment application example and failure behavior. [The full YAML example](../examples/measured/payments-automatic.yaml) includes grace, empty-settle, timeout, load thresholds, warm capacity and Cloud creation settings.

The implemented path is managed guaranteed SMF queues on a dedicated homogeneous fleet. It has passed a real two-broker handover test. Cloud creation is mock-tested; the real Cloud rollout remains to be validated. It does not automatically delete brokers, copy pending backlogs, split an indivisible hot key, or accelerate a slow business consumer.
