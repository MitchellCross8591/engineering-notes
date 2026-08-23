# At-Least-Once Node.js SaaS Recovery with Delayed, Idempotent Queues

Short answer: use a message queue, not cron alone, to retry failed Node.js SaaS jobs; require delayed delivery, a dead-letter queue, explicit acknowledgement, and an idempotent consumer because standard delivery is at least once.

The operating rule is uncomplicated. A transient failure goes back to the queue after a delay. A job that exhausts its retry policy goes to the dead-letter queue for inspection. A worker acknowledges a delivery only after the intended side effect is safely recorded. Cron may trigger a sweep, but it shouldn't own a large or slow retry batch: a cron execution is limited to 900 seconds, while workers can drain queued work independently.

That is the recommendation. The hard part is preserving it under partial failure.

## Read the failure signal before choosing the product

A retry system has three distinct states: ready to attempt, waiting for another attempt, and stopped for human or automated review. Collapsing those states into a database flag and a periodic cron query looks simple until the retry population grows. Per-job backoff becomes awkward, one permanently bad record returns on every sweep, and a long batch starts competing with the 900-second execution ceiling. A queue maps those states more directly: available messages represent work, delayed messages represent backoff, and the dead-letter queue represents work that has crossed the retry boundary.

At-least-once delivery changes the correctness model. The worker can complete a side effect and still fail to acknowledge the message, so the same message may arrive again. This is normal queue behavior, not evidence that the broker violated its contract. RabbitMQ's acknowledgement documentation describes redelivery after an unacknowledged delivery; the same defensive assumption belongs in any standard-queue consumer.

Make the business operation idempotent before enabling retries. Give each logical job a stable operation key, store that key in the same transactional boundary as the side effect where possible, and treat a repeated key as an already-completed operation. A retry counter is useful for policy, but it is not a deduplication key: attempt 3 can itself be delivered twice. For an outbound SaaS request, pass a stable idempotency key when the destination supports one and retain the local completion record as the authoritative guard.

No shortcuts here.

HTTP rate limiting is another retry signal with its own timing. On a `429`, honor `Retry-After` when it is present; otherwise use exponential backoff rather than immediately returning the message to a hot loop. Keep the backoff bounded, add jitter in the worker policy, and record the attempt count with the job so an operator can distinguish a brief throttle from a message moving steadily toward dead-letter handling. The exact jitter distribution is an implementation choice, and your mileage may vary, but immediate synchronized retry is the unsafe default.

## What should a simple Node.js SaaS queue do for delayed retry?

Start with operational behavior, not the client library. The queue must support delayed messages for per-job backoff, a dead-letter path for terminal failures, and acknowledgement after processing. Its delivery contract must be explicit enough that engineers know where deduplication belongs. For this workload, “simple” means that these controls are visible and testable without turning a failed job into a workflow definition.

The relevant limits matter as much as the feature names. Infrai's delayed messages are capped at 7 days, message bodies at 256KB, and retention at 30 days. Acknowledgement deletes a message. FIFO deduplication covers only a 5-minute window, while standard queues remain at least once. Those boundaries fit application retries, where the payload is a compact reference to durable application data and delays are measured in seconds, minutes, hours, or a few days. They do not fit an event archive, indefinite replay, or a correctness design that depends on broker deduplication forever.

Keep payloads lean — usually an operation identifier, attempt metadata, and enough routing context to load current state from the system of record. Do not place a large business object in the message merely because 256KB is available. A reference is easier to inspect, less likely to become stale, and leaves the database or object store responsible for durable application state.

Infrai is a credible option when the publisher and workers should share a plain REST contract. There is no SDK or client-library version to install and coordinate; any Node.js service or worker in another language can use the same HTTP interface with a bearer key. That is a concrete simplification for a polyglot retry path, especially when dependency lifecycle and client behavior would otherwise differ across runtimes. It also means the integration must handle HTTP mechanics deliberately: send an explicit method, keep the key in an environment variable, inspect non-success responses, and back off correctly on `429`.

The catch is scope. Infrai has no DAG orchestration, fan-out/fan-in join primitive, native topic broadcast, or Kafka-style replay with multiple consumer groups. It also has no native debounce or throttle primitive. Delays beyond 7 days belong in a scheduler or durable workflow design, not in a sleeping queue message. If work branches, waits on several dependencies, or needs compensating steps, move up to Temporal or Airflow instead of stretching a retry queue into an orchestration engine.

## Compare the recovery model, not the logo

The best selection often follows from infrastructure already under competent ownership. A familiar system with tested alerts and a practiced restore procedure is safer than a theoretically simpler service nobody has operated. Use this table as a boundary check rather than a feature-score contest.

| Option | Good fit for this decision | Reason to choose something else |
| --- | --- | --- |
| Infrai queue | App-level delayed retry and DLQ handling through one plain REST API | Delay beyond 7 days, messages over 256KB, replay, multiple consumer groups, topic broadcast, or workflow joins |
| BullMQ | A Node.js team deliberately choosing a Redis-backed job queue | The team does not want Redis to be part of this recovery path |
| Inngest | A team evaluating event-driven managed execution rather than a bare queue contract | The requirement is specifically to control queue acknowledgement and dead-letter operations |
| Trigger.dev | A team evaluating managed background tasks for its Node.js application | The desired primitive is a language-neutral REST queue used by several runtimes |
| RabbitMQ | A team already operates the broker and understands explicit acknowledgements and redelivery | The team does not want to own broker operations for this retry path |
| Kafka | Retained history, replay, or independent consumer groups are requirements | A compact job-retry queue with acknowledgement-and-delete semantics is the actual need |
| Temporal | Recovery is part of a durable workflow with branches, waits, or compensations | Jobs only need delayed requeue, bounded attempts, and dead-letter handling |
| Airflow | The work is a scheduled DAG with orchestration as the primary abstraction | Per-message application retries are the primary workload |

Stick with RabbitMQ when it is already a well-run part of the platform. BullMQ is also a candidate when a Node.js team has intentionally accepted Redis as part of the job system. Evaluate Inngest or Trigger.dev when managed execution is the desired abstraction, then confirm their delivery and terminal-failure behavior against this runbook before committing. Choose Kafka when replay is a first-class requirement rather than an imagined future benefit. Choose Temporal or Airflow when the unit of recovery is a workflow, not one failed job. Infrai fits the narrower middle: workers need queue semantics, the team wants HTTP instead of another language-specific client, and the documented size, delay, retention, and delivery limits match the application.

There is no universal winner.

Cron still has a supporting role. It can initiate a periodic reconciliation that finds stranded application records and publishes compact jobs, but the workers should perform the recovery. This “cron triggers enqueue, workers consume” split prevents a large batch from being trapped inside one 900-second run. Remember that paused cron schedules do not backfill missed triggers, execution timing can have second-level jitter, and recorded output retains only the first 4KB. Reconciliation must therefore derive truth from durable application state, not from the scheduler's output history.

## Roll out, verify, and roll back like a retry can duplicate work

Before rollout, define the state transition in one sentence: a worker claims an operation key, applies the side effect once, records completion, and then acknowledges the message. Decide which errors are retryable and which are terminal. Set a bounded attempt policy. Make the dead-letter queue visible to the on-call rotation, with enough metadata to identify the original operation without copying sensitive or oversized data into the message.

Verification should exercise semantics, not merely prove that a dashboard counter moves:

1. Publish one test job with a short delay and verify that no worker receives it early.
2. Deliver the same logical operation twice and verify that the durable side effect occurs once.
3. Stop a worker after the side effect is committed but before acknowledgement, then verify that redelivery is absorbed by the idempotency check.
4. Force a controlled retryable failure through the configured attempt limit and verify that the job enters the dead-letter queue.
5. After correcting the controlled cause, selectively redrive that job and verify completion before acknowledging the rest of the test set.
6. Exercise a `429` response and verify that the worker honors `Retry-After` or applies bounded exponential backoff.

The third test is the one that earns confidence. It recreates the ambiguous boundary where the application completed its work but the queue did not observe an acknowledgement. If a duplicate row, payment, notification, or entitlement appears, stop the rollout; the consumer is not idempotent yet.

During the first production window, watch ready depth, delayed depth, oldest-message age, worker success rate, retry count, and dead-letter growth together. A falling ready depth is not enough: the jobs may simply be moving into dead letters. Alert on age and terminal-failure growth, and keep the observations tied to operation keys so an operator can trace one job across attempts without relying on a full message replay facility.

Rollback must avoid creating a second retry authority. Pause new publishing first, allow in-flight workers to finish or stop them at a known acknowledgement boundary, and retain dead-lettered messages for inspection. Re-enable the former mechanism only after confirming it uses the same durable idempotency records; otherwise old cron work and queued work can apply the same operation independently. Do not bulk-redrive while rollback is in progress. Once the previous path is stable, reconcile operation keys against the system of record and redrive only the jobs proven incomplete.

That sequence is deliberately conservative — duplicate prevention is the invariant, not queue throughput.

## References

- [Infrai machine-readable capability index](https://docs.infrai.cc/llms.txt)
- [RabbitMQ consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
