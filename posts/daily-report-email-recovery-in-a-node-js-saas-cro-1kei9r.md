# Daily Report Email Recovery in a Node.js SaaS: Cron, Queue, or Both?

Short answer: choose the design that leaves the clearest recovery evidence. For a bounded daily report email, start with a cron-triggered work record; add a message queue when a tenant report or marketplace webhook needs an independent retry, pause, and replay boundary. The deciding question is not how many components the diagram has. It is what an on-call engineer can prove after a process stops halfway through an external send.

I've been paged for missed jobs and duplicate deliveries. Those are different incidents, even when both begin with a red “job failed” alert. A scheduler proves that a time was reached. It does not prove that work was created. A queue can redeliver a message, but it cannot prove whether the destination accepted an HTTP request just before the worker crashed.

That is the incident lesson: design the evidence trail before choosing the handoff mechanism.

## The failure window behind a retry decision

The first useful artifact is a durable work identity. For one report, use tenant ID, report date in UTC, and report kind. For one outbound webhook, use the source event ID and destination. Put a uniqueness rule in the database. A check followed by an insert is not enough when two scheduler invocations overlap.

The record should distinguish at least these transitions: scheduled, created, claimed, accepted by the external provider, acknowledged, and held for review. The exact names can differ, but the boundary must be visible. A report that was never created needs a different action from a webhook that may have been accepted before a crash.

The external call is the uncomfortable part. The local system cannot know with certainty that the provider rejected a request if the process dies after the provider accepts it and before the local state write. Pass the work identity as an idempotency key when the provider supports one. When it does not, retain an uncertain state and reconcile before replaying. Calling this exactly-once delivery would conceal the real failure window. In a daily report run, that means the recovery screen should let an operator move from the schedule timestamp to the tenant work key, then to the provider request record, without guessing which batch iteration produced it. In a webhook run, the same screen should expose the source event and destination so a replay can be compared with destination-side evidence. This is deliberately more detail than a green scheduler metric, because the useful question during an incident is not whether a process was alive; it is which external side effect might already exist.

This is where operational recovery beats implementation preference. The smallest design that can answer “what happened to tenant A for 2026-08-11?” is usually better than a larger design that only reports “attempt 3 failed.”

## Rollout plan: move a daily report from cron to durable work

Cron is a clock. It is a poor place to hide an unbounded delivery loop.

The Linux `crontab(5)` model is useful for starting work at a time, but the application still owns the durable record and the consequences of overlap. A daily process can calculate the report date, insert one work item per tenant, and exit. A worker, inline or separate, then renders and sends each item under a bounded retry policy.

Keeping the send inside the scheduler can be reasonable when the recipient set is small, runtime is predictable, and a whole-run retry is safe. The limitation is batch coupling: one slow mail call or malformed tenant can delay later recipients, while repeating the run may repeat sends that already succeeded. The design is not suitable when operators must replay one marketplace webhook without replaying every report.

That boundary gives a practical migration path. First make the cron run idempotently create work records. Then move the send step behind a queue if the records need independent ownership. The scheduler contract does not need to change, and an operator can compare the old inline result with the new worker result during a controlled rollout.

## How should a Node.js SaaS implement daily report email and webhook recovery?

They should share identity, state transitions, and observability, while keeping their retry policies separate. A report email is usually generated for a tenant and date. A webhook is tied to an event and destination. Treating both as anonymous “jobs” loses the information needed to decide if replay is safe.

The worker order below makes the uncertainty explicit. It claims the identity, returns successfully for work already marked sent, calls the external sender, persists the accepted result, and acknowledges the queue only after that result is durable. The interfaces are generic so the same contract can sit behind a Node.js service or another runtime; the example is intentionally in Go because this repository's article format requires Go code.

```go
package main

import (
	"context"
	"errors"
)

type Work struct {
	Key        string
	TenantID   string
	ReportDate string
	Kind       string
}

type Delivery struct {
	Work  Work
	Ack   func(context.Context) error
	Retry func(context.Context) error
}

type WorkStore interface {
	Claim(context.Context, Work) (alreadySent bool, err error)
	MarkSent(context.Context, Work) error
}

type Sender interface {
	Send(context.Context, Work) error
}

func process(ctx context.Context, store WorkStore, sender Sender, d Delivery) error {
	sent, err := store.Claim(ctx, d.Work)
	if err != nil {
		return d.Retry(ctx)
	}
	if sent {
		return d.Ack(ctx)
	}

	if err := sender.Send(ctx, d.Work); err != nil {
		if retryErr := d.Retry(ctx); retryErr != nil {
			return errors.Join(err, retryErr)
		}
		return err
	}
	if err := store.MarkSent(ctx, d.Work); err != nil {
		return err
	}
	return d.Ack(ctx)
}
```

The store still needs a transaction appropriate to its database and a uniqueness guarantee. The queue's acknowledgement semantics matter too: RabbitMQ documents acknowledgements and redelivery as part of consumer delivery behavior, which is why the handler must tolerate an item arriving again. Acknowledging before `MarkSent` can lose work. Marking sent before `Send` can create a false success.

The same model works without a queue. In that case, the scheduler or a bounded worker loop owns `Retry`, and the durable record remains the source of truth. A queue changes the ownership and timing of retries; it does not change the external provider boundary.

## Code and API boundaries for queue ownership

Use cron with durable work records when the daily report is short, bounded, and safe to retry as a unit. Add a queue when one tenant or one marketplace webhook must be retried without delaying unrelated work, or when operators need per-item pause and resume.

The catch is ownership. A queue requires someone to watch worker health, retry age, held messages, and the gap between published work and completed work. It is not suitable for a team that has no runbook for stuck consumers or no reconciliation view. A simple cron process with clear state can be easier to operate than a queue that nobody checks.

| Design | Best recovery question | Main trade-off |
| --- | --- | --- |
| Cron sends inline | Did this bounded run finish? | One slow or invalid tenant couples the batch |
| Cron creates records | Which tenant work exists? | A worker or recovery loop is still required |
| Cron plus queue | Which item can be replayed safely? | Worker health and retry age become owned signals |
| Workflow engine | Which long-lived step is waiting? | More machinery than one bounded daily report may need |

This is a choice about recovery scope, not a ranking. Stick with cron when the report's retry unit is the whole bounded run. Choose a queue when the retry unit must be one record and the team is prepared to operate that boundary. Choose a workflow engine only when the process has real waits, joins, or human decisions.

## Test harness: production replay before it matters

Run the same scheduler twice for one tenant and report date. Deliver one work key twice. Stop the process after `Send` returns and before `MarkSent`. Exercise a temporary provider rejection and a permanently invalid tenant. The assertions should cover durable state and the next operator action, not just the handler's returned error.

For a morning report alert, I want three answers: did the schedule run, did it create the work identity, and did the provider accept a request? Those answers must remain separate. If an alert compresses them into “failed,” the next person may replay an already accepted webhook.

Your mileage may vary at the provider boundary. I'm not sure any queue choice can remove that uncertainty; a good design exposes it, bounds automatic attempts, and makes reconciliation explicit.

The decision rule is short: cron owns time, application state owns identity, and a queue owns independent retries. Introduce the queue when that last responsibility is worth operating. Do not introduce it to compensate for missing idempotency.

## References

- https://man7.org/linux/man-pages/man5/crontab.5.html
- https://www.rabbitmq.com/docs/confirms

## Sources

- https://man7.org/linux/man-pages/man5/crontab.5.html
- https://www.rabbitmq.com/docs/confirms
