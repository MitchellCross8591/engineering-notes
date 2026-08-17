# Small Business Queue Choice: FIFO, Standard Delivery, and Failed-Job Retry

Short answer: use a standard queue for failed-job retries unless a single business entity truly requires ordered processing; make every effect idempotent in the worker, because neither FIFO delivery nor broker deduplication can establish correctness after a retry.

The least complex safe design for a small business app is usually a durable jobs table, a worker that claims due rows, and an idempotency key whose lifetime matches the business effect. A queue can improve isolation and throughput, but it should not be the only place that remembers what already happened. I've been paged for both missed work and duplicate deliveries; those pages have the same root cause more often than teams expect: delivery was treated as proof that an effect occurred.

## How should a small business app handle failed-job retries, FIFO or standard queues, ordering, deduplication, and idempotency?

Start by separating four promises that are easy to merge into one vague idea of "reliable queueing."

| Concern | Question to answer | Owner of the promise |
| --- | --- | --- |
| Retry | When does unfinished work become eligible again? | Scheduler and worker state |
| Ordering | Must two operations for one entity be applied in sequence? | Partitioning and application rules |
| Deduplication | Should an already-seen delivery be discarded? | Consumer or broker, depending on its scope |
| Idempotency | Can a repeated attempt leave the same business result? | The code that records the effect |

FIFO is appropriate when the order of mutations for one entity changes the result: applying a refund after its capture, for example, is different from applying those operations in the opposite order. Give that entity a narrow partition or message group. A failed item at the head can then delay later work for that same entity, so the retry policy needs a bounded attempt count, a place to inspect stuck work, and an explicit operator decision for items that cannot progress.

Most retry workloads do not need that constraint. Receipt emails, search indexing, thumbnail generation, and many notifications can arrive out of order without changing the final business state. A standard queue lets workers make progress independently, but it permits duplicate delivery and reordering. That is not a defect to paper over. It is the contract the worker must tolerate.

Make that boundary explicit.

Use FIFO because the domain needs order, not because it sounds like a substitute for idempotency. The queue only sees messages. It cannot tell whether a payment, an email, or a database mutation already took effect.

## The invariant belongs beside the business effect

For every job, define a stable idempotency key from the user-visible intent, such as `invoice:123:send-receipt` or `account:88:close-period:2026-08`. Do not derive it from an individual queue delivery. A redelivery must carry the same key, while a legitimate new intent must get a different one.

The worker should persist a record keyed by that value before declaring success. When the effect is a local database mutation, the strongest simple pattern is to write the effect and the idempotency record in one transaction. A second delivery then finds the existing key and becomes a no-op. For an external side effect, pass the same key to the external system when its documented contract supports one, and retain a local receipt or reconciliation state. If the external system cannot accept a stable key, the remaining uncertainty is real: after a timeout, the worker may not know whether the remote action happened. A reconciliation process and a business-safe compensating action are better answers than an optimistic retry loop.

This is where retry design becomes operational rather than theoretical. Record attempt count, next eligible time, last error class, and the idempotency key. Back off transient failures. Stop retrying invalid input. Alert on a growing population of overdue jobs rather than on every individual failure, otherwise an upstream outage turns the alert channel into the outage.

Celery describes itself as a distributed task queue; its introduction is useful context because it keeps task submission and task execution separate. That separation is why application-level idempotency remains necessary even when a queue is working as designed.

## A small database-backed claim path

For a modest workload, a database table can make the state machine visible and auditable. PostgreSQL documents `SKIP LOCKED` as a locking option that skips rows it cannot immediately lock; paired with a state change in the same transaction, workers can claim different due jobs without waiting behind one locked row.

```go
package jobs

import (
	"context"
	"database/sql"
	"time"
)

type Job struct {
	ID             int64
	IdempotencyKey string
	Payload        []byte
}

func ClaimDue(ctx context.Context, db *sql.DB, limit int) ([]Job, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		WITH due AS (
			SELECT id
			FROM jobs
			WHERE state = 'ready' AND run_after <= now()
			ORDER BY run_after, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET state = 'running', claimed_at = now()
		WHERE id IN (SELECT id FROM due)
		RETURNING id, idempotency_key, payload`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var claimed []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.IdempotencyKey, &job.Payload); err != nil {
			return nil, err
		}
		claimed = append(claimed, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func RetryAt(attempt int, now time.Time) time.Time {
	// The actual delay policy belongs to the job's operational contract.
	return now.Add(time.Duration(attempt+1) * time.Minute)
}
```

This snippet only claims work; it intentionally does not hold a database transaction open while calling another service. After processing, write either `done` with the idempotency receipt or `ready` with a later `run_after`, using a compare-and-set rule tied to the claim. A lease expiry or a recovery sweep can return abandoned `running` jobs to `ready`. Test that path by stopping a worker after it claims a job and before it records completion. Then run the same job twice on purpose. Those are routine failure cases, not edge cases.

The catch is that this approach needs a database that can carry the job state and an operator who will inspect a dead-letter or terminal-failure state. A separate queue is a better fit when traffic spikes would compete with transactional application work, when independent services need a shared delivery boundary, or when a managed retention policy is part of the requirement. Keep the same idempotency design in those cases.

## What to test before choosing the delivery model

Run the decision through actual business effects, not a feature checklist. For each job type, write down its ordering key, its idempotency key, the maximum useful retry horizon, and what a human should see when the job ends in terminal failure. Then test these cases in a non-production environment:

- Deliver the same job twice concurrently and verify that only one effect is committed.
- Complete a later job before an earlier one and verify that the result is acceptable for job types assigned to standard delivery.
- Keep one ordered entity failing while unrelated entities continue to make progress.
- Interrupt a worker between claiming work and recording its result, then verify recovery does not lose the job.

The right answer can differ inside one application. Use ordered delivery for the narrow set of entity histories where sequence is part of correctness. Use standard delivery for independent work. In both paths, measure queue age, retry population, terminal failures, and duplicate suppression. A queue choice is a throughput and coordination choice; idempotency is the correctness boundary.

## References

- Celery introduction documentation: https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- PostgreSQL `SELECT` documentation, including `FOR UPDATE SKIP LOCKED`: https://www.postgresql.org/docs/current/sql-select.html
