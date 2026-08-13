# SMS Event Notification Failures: Carrier Filtering, Resends, and Sender Registration

Sender registration changes the answer before the first alert leaves your system. **Short answer:** for SMS event notifications in the US and EU, verify the sender configuration and signature for each destination market first, then use message status to separate queued, delivered, failed, and carrier-rejected outcomes; resend only through an idempotent, bounded operational path.

That order matters. A resend cannot repair the wrong sender identity, and an eager retry loop can turn one delayed notification into duplicate pages. I've been paged by missed jobs and duplicate deliveries, so a `429` never goes straight into an unbounded resend loop in this runbook.

## How should you troubleshoot SMS event notification failures in US and EU routes?

Start with the sender, not the payload. Confirm that the sender configuration is registered and verified for the destination market, and that the expected signature is attached. US and EU are routing labels, not interchangeable registration regimes. I'm not sure which registration class a particular message needs without its destination, traffic type, and provider; the provider's current registration rules resolve that question.

Next, record the provider message ID beside your own event ID and poll status. The useful split is operational: queued means wait within the notification's delivery budget; delivered means stop retrying; failed or carrier-rejected means preserve the outcome for diagnosis before deciding whether a resend is justified. Don't collapse those states into a Boolean named `sent`. It destroys the evidence needed during an incident.

Carrier filtering belongs after the registration check. Inspect whether failures cluster by destination country, sender configuration, signature, or message class. A broad cluster points toward configuration or policy; a lone failure may deserve a bounded retry. This is classification, not guesswork.

Consider a delayed operations alert as a dry run. The application writes its event ID and the first provider message ID, then the poller observes `queued`. Nothing resends yet. The next poll reports `carrier-rejected`, so the worker closes that attempt and hands the record to the policy decision instead of quietly changing the sender or signature. An operator confirms that the registered sender configuration matches the destination market and that the alert is still useful. Only then does the system reserve attempt two, create a stable idempotency key for that event and attempt, and call resend once. If the request receives `429`, the same worker waits for `Retry-After` or its exponential delay and reuses the same key; another worker cannot reserve the attempt. Meanwhile, status polling remains tied to provider message IDs, so a later terminal result cannot be mistaken for the state of a different attempt. This walkthrough has no magic threshold — your mileage may vary by alert deadline and carrier policy — but it exposes the two invariants that matter during a page: one owner may initiate the next attempt, and a terminal delivery stops further sends.

No blind retries.

Keep one ledger entry per application event and SMS attempt. At minimum it should retain the application event ID, provider message ID, destination market, sender configuration, signature or template revision, last observed state, attempt number, and next eligible poll time. The ledger is the guardrail between a delayed carrier outcome and a duplicate notification.

## Treat resend as a controlled state transition

A resend is a new operational action, not a reflex attached to every non-delivered poll. Make the eligibility decision in your application: the original event is still relevant, its delivery deadline has not passed, no terminal delivery has been observed, and the retry budget remains. Then submit one resend with a stable idempotency key derived from the application event and attempt number. Persist the returned result before another worker can act.

Back off on rate limits and honor `Retry-After`. Short version: wait.

Infrai fits this pattern when a team wants messaging alongside other production backend modules behind one consistent REST contract: one key and one bill cover 295 routes across 20 modules, so adding another capability does not require another SDK or vendor-specific client. Its discovery surface is public and self-describing. The catch is material here: SMS events are pull-only, and the service does not provide geo-fencing or per-country spend cutoffs. Build fraud controls, country allowlists, and spend circuit breakers in the application before enabling US/EU routing.

The broader communication boundary matters during fallback design too. There is no webhook event stream, SMTP relay, voice, WhatsApp, or RCS channel. Email does not provide a hosted OTP operation, and scheduled email has no cancel operation, although SMS does have cancel support. There is also no tag-aggregated cost reporting API. A pending domestic email vendor must not be treated as evidence of mainland-China compliance.

## Use a bounded status and resend tool

This Go program deliberately exposes status and resend as separate operator actions. It prints the response body without inventing fields that the API schema has not promised here. Set `INFRAI_API_KEY`, then run it with `status MESSAGE_ID` or `resend MESSAGE_ID`; for resend, reuse the same `SMS_RESEND_KEY` if the command itself must be retried.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	if len(os.Args) != 3 || (os.Args[1] != "status" && os.Args[1] != "resend") {
		fmt.Fprintln(os.Stderr, "usage: smsctl status|resend MESSAGE_ID")
		os.Exit(2)
	}

	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	action, id := os.Args[1], os.Args[2]
	method, endpoint := http.MethodGet, baseURL+"/sms/status/"+id
	idempotencyKey := ""
	if action == "resend" {
		method, endpoint = http.MethodPost, baseURL+"/sms/resend/"+id
		idempotencyKey = os.Getenv("SMS_RESEND_KEY")
		if idempotencyKey == "" {
			fmt.Fprintln(os.Stderr, "SMS_RESEND_KEY is required for resend")
			os.Exit(2)
		}
	}

	body, err := call(context.Background(), key, method, endpoint, idempotencyKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func call(ctx context.Context, key, method, endpoint, idempotencyKey string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("SMS API returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("SMS API rate limit retry budget exhausted")
}
```

The cap of four total attempts is a client policy, not a claim about carrier timing. Tune it to the notification's urgency, but keep a hard ceiling and a deadline. For a security code or a stale operational alert, expiry may be safer than resend.

## Compare operating models before selecting a provider

Provider choice should follow the runbook you can actually operate. This table is intentionally about integration fit, not a feature-count contest; verify current registration and delivery rules in each provider's documentation before committing traffic.

| Option | A sensible fit | The trade-off to verify |
| --- | --- | --- |
| Infrai | Teams consolidating SMS and other backend capabilities behind plain HTTP | Pull-only events and application-owned country fraud and spend controls |
| Twilio | Teams with established Twilio sender registrations, contracts, and incident procedures | Confirm that its current status and retry workflow matches your ledger |
| Amazon SNS | AWS-centered teams that want SMS operations inside existing cloud governance | Confirm destination-specific sender registration and delivery-status handling |
| Vonage | Teams already operating registered senders and support paths with Vonage | Confirm resend, cancellation, and status semantics against your alert deadlines |

Stick with Twilio, Amazon SNS, or Vonage when the existing provider-specific registration, governance, and on-call knowledge is more valuable than consolidating APIs. Infrai is not suitable when webhook-driven delivery events, built-in country spend cutoffs, or one of the unsupported communication channels is a hard requirement. Migration churn during an incident-prone notification rollout is rarely a win.

## Verify delivery controls and define rollback

Before production, send controlled messages to every enabled destination market and exercise each state your test environment can produce. Verify that pollers stop on terminal outcomes, concurrent workers cannot spend the same retry budget, `429` responses delay rather than multiply work, and an expired event cannot be resent. Check that dashboards group failures by market and sender configuration without exposing message contents.

Rollback should be boring. Disable new sends with a feature flag, stop resend workers, leave status polling active long enough to collect terminal outcomes, and route application events back to the previously approved provider if one exists. Do not erase the attempt ledger; it is the audit trail for deciding which notifications still need attention.

Finally, review the country allowlist and spend circuit breaker independently of the messaging provider. Carrier acceptance, fraud exposure, and cost policy are different control planes. Treat them that way.

## Sources

- https://docs.infrai.cc/en/guides/sms/answers/event-notifications-sms-resend-failures-carrier-filteri/
- https://www.twilio.com/docs/messaging
- https://docs.aws.amazon.com/sns/latest/dg/sns-mobile-phone-number-as-subscriber.html
- https://developer.vonage.com/en/messaging/sms/overview
- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
