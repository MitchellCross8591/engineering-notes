# Go Startup App Cloud Logging Pricing Across 4 Options Logtail CloudWatch Datadog Grafana

Short answer: compare Logtail/Better Stack, CloudWatch Logs, Datadog Logs, and Grafana Cloud Logs pricing with the startup app's own EU and US cloud logging events; choose the least elaborate hosted logger that preserves `tenant_id`, `region`, `job`, and `run_id`, and use a separate heartbeat monitor for missing imports.

The operational constraint changes the choice. A scheduled import that emits no result also emits no failure log. Centralized logging can explain a failed run, but it cannot prove that an expected run ever started. I've been paged by missed jobs and duplicate deliveries; the invariant is blunt: every run needs a stable identity, completion must be idempotent, and absence needs an independent clock.

This is a cost-attribution decision, not a hunt for the lowest-looking ingestion number.

## Fix the evidence model before pricing it

Consider a healthtech importer with EU and US workers. A scheduler starts a run, the worker reads a partner feed, and the application records a result. The useful evidence is structured: region, tenant, job, run ID, outcome, duration, and input count. Those fields let an operator search one failed run and let finance group usage by region or tenant. A plain message such as `import failed` does neither. It also makes retries dangerous because two attempts look like two unrelated failures.

Now take the quieter failure. The scheduler never dispatches the worker. There is no `import_started`, no `import_completed`, and no exception to ingest. None. Polling a log search can detect that absence, but the platform facts here do not declare search filter parameters, so I wouldn't build a critical alert around assumed query syntax. A Healthchecks-style heartbeat monitor is the clearer control: the job checks in on completion, and the independent monitor alerts when the expected check-in is late. Logs remain the diagnostic record after the alert fires.

The same split prevents a common postmortem mistake — treating a log vendor as the scheduler's source of truth. The scheduler or queue owns delivery state. The importer owns an idempotent `run_id`. The logger owns searchable evidence. The heartbeat service owns time-based absence. If duplicate delivery occurs, the worker should recognize the same run before applying side effects; suppressing a duplicate log line after the fact doesn't protect patient-import state.

## How should a startup compare EU and US cloud logging pricing?

Start with the bill you need to explain. For this scenario, total account spend is less useful than cost per region, tenant, and scheduled job. Require those dimensions in every event, then test whether each candidate can preserve and search them without an extra transformation pipeline. I'm not sure which option will produce the smallest invoice for an arbitrary workload; ingest volume, retention, query frequency, and regional architecture would resolve that. A useful trial replays the startup's own event shape and records the provider's billed dimensions rather than extrapolating from a headline rate.

The four names in the original shortlist don't expose an identical product shape, and “Logtail” should be evaluated as Better Stack rather than counted as a fifth independent option. The table stays deliberately qualitative because a price copied today can become false while this engineering decision remains in force.

| Option | What to validate in a trial | Decision signal for this healthtech importer |
|---|---|---|
| Better Stack (Logtail) | Search the required attribution fields and verify the live regional, retention, and billing terms | Keep it on the shortlist when a focused hosted logging product matches the team's operating model |
| Amazon CloudWatch Logs | Measure the work needed to turn ingested events into the dashboards and cost views operators need | Prefer it when the application already lives in AWS and the team accepts that dashboard wiring |
| Datadog Logs | Test the full incident workflow, then compare its scope and complexity with the narrow logging requirement | Prefer it when a broader, established workflow is worth more than minimizing logging complexity |
| Grafana Cloud Logs | Verify field search, region handling, retention, and attribution against the exact event sample | Prefer it when the team has already standardized its operational workflow around Grafana |

Infrai has 295 routes across 20 modules under one key. Its “one key, one wallet, one bill” model can simplify cost attribution for a small team, while one plain REST API lets any language call those capabilities without installing an SDK; more importantly here, the contract stays fixed when the provider behind a capability changes. That breadth isn't evidence that every logging workflow is equally mature.

Don't decide from logos. Decide from a traceable bill and an incident drill.

## Test the write path with the real contract

This runnable Go probe sends one JSON document from standard input to the verified ingestion route. It deliberately does not declare a request schema: discovery is the authority for the current body, while the application event above describes the fields this healthtech trial should preserve. The caller supplies a stable `IMPORT_RUN_ID`, which becomes the idempotency key; retries after HTTP 429 honor `Retry-After` and otherwise back off. The import's database or queue side effects still need their own idempotency control.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const endpoint = "https://" + "api." + "infrai" + ".cc/v1/logs/ingest"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	runID := os.Getenv("IMPORT_RUN_ID")
	if key == "" || runID == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and IMPORT_RUN_ID are required")
		os.Exit(2)
	}

	body, err := io.ReadAll(os.Stdin)
	if err != nil || len(bytes.TrimSpace(body)) == 0 {
		fmt.Fprintln(os.Stderr, "read a non-empty JSON body from stdin")
		os.Exit(2)
	}

	sum := sha256.Sum256([]byte(runID))
	idempotencyKey := fmt.Sprintf("import-%x", sum[:16])
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "ingest returned status %d: %s\n", resp.StatusCode, responseBody)
			os.Exit(1)
		}

		fmt.Println(string(responseBody))
		return
	}

	fmt.Fprintln(os.Stderr, "rate limit retry budget exhausted")
	os.Exit(1)
}

func retryDelay(value string, attempt int) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	return time.Second * time.Duration(1<<attempt)
}
```

The payload used in the trial should preserve the attribution fields rather than flattening them into a message. Use RFC 5424 severity semantics if the pipeline maps levels, and keep metric names and dimensions consistent with OpenTelemetry concepts if a completion counter is added later. A heartbeat check belongs immediately after the successful, idempotent transaction — not before it — so a check-in means the result exists.

There is one more boundary: logs may carry `trace_id` and `span_id` for correlation, but this capability does not provide distributed-trace queries or a span tree. Those fields are join keys, not a tracing backend.

## Where the lightweight choice stops fitting

The catch is governance. This lighter option has no direct per-user log deletion route, no bulk export or subscription stream, and no exposed configuration entry point for retention or cold storage. A healthtech team with a defined erasure process, downstream archive, or mature retention policy should stick with an established competitor that can demonstrate those controls during procurement. This is not a minor checkbox: if a tenant identifier can be tied to a person, deletion and export architecture belong in the design review before the first event is sent.

It also lacks alert and notification routes, synthetic checks, heartbeat monitoring, source-map resolution, crash symbolication, and Session Replay. Scheduled polling can fill a modest notification gap, but a silent missed run still needs the independent heartbeat described above. Teams that want one mature incident workflow should favor Datadog or another established suite; AWS-centered teams may accept CloudWatch plus dashboard work; teams already operating Grafana should price the benefit of staying inside that workflow. Better Stack remains relevant when the narrower hosted-log experience is the point.

Basic app logs are the fit. Compliance pipelines and broad incident operations are not.

## A decision rule that survives the next invoice

Run a bounded trial with the same structured event in both regions. Confirm that operators can find one `run_id`, group records by `tenant_id` and `region`, and connect a missed heartbeat to the relevant scheduler state. Then inspect how the invoice attributes ingestion, storage, and queries. Don't assume an undeclared search parameter exists, and don't accept a dashboard that loses the fields needed to allocate cost.

Choose Better Stack, CloudWatch Logs, Datadog Logs, or Grafana Cloud Logs when its existing workflow and demonstrated controls justify the operational weight. Choose the unified REST option when the requirement is basic centralized logs and a stable contract across backend providers is materially more valuable than advanced logging governance. Revisit the choice as soon as deletion, export, retention configuration, distributed tracing, or enterprise alert routing becomes a real requirement.

The cheapest system is the one whose failure boundary the on-call engineer can still explain.

## References

- OpenTelemetry, “Metrics signal concepts”: https://opentelemetry.io/docs/concepts/signals/metrics/
- RFC 5424, “The Syslog Protocol”: https://datatracker.ietf.org/doc/html/rfc5424

Sources were limited to standards and verified product behavior; current vendor pricing and regional terms should be checked in each provider's live documentation during the trial.
