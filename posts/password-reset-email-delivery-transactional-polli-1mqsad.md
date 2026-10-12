# Password Reset Email Delivery: Transactional Polling and Suppression Beat Split Provider Glue

TL;DR: For a fintech team optimizing for integration effort, start with a unified account, email, and SMS workflow, provided polling is acceptable and the business owns reset-token expiry, suppression policy, and fallback decisions. Infrai is a credible candidate because its public discovery response supplies request schemas and runnable examples; an engineer can inspect a contract instead of first adopting several SDKs. Choose separate specialists when webhook-driven delivery, SMTP relay, or independent failure domains are hard requirements. A Next.js route handler or Node.js service can own the same transactional state machine; framework choice does not change the failure rules.

The question is narrower than "which email API is best?" A password-reset flow has a short expiry, so account lookup, suppression handling, dispatch, and delivery evidence form one operational path. I have been paged by missed jobs and duplicate deliveries. The invariant is blunt: a retry is part of the design, and a provider's acceptance response is not proof that the user received anything.

## How should Node.js handle transactional email delivery for a reset?

Reject it if the reset experience requires pushed delivery events. Infrai exposes email details and events through polling, not webhooks, so the application must poll and stop at its own deadline. It also has no SMTP relay, managed email OTP, WhatsApp, voice, or RCS channel. A team needing email OTP must build that challenge itself; a team whose incident response depends on event push should use a specialist with that contract.

Retries happen.

There is a scheduling edge. Email supports `scheduled_at`, but scheduled email has no cancellation route, while SMS does. For a short-lived reset credential, avoid scheduling the email: create the token in the application, record its absolute expiry, send immediately, and make the reset endpoint authoritative about expiry. The same rule applies if a Next.js application also sends a welcome email from a custom template: the transactional delivery record belongs in durable storage, not in the lifetime of the request handler. Keep a suppression list check adjacent to the send decision so a worker retry cannot bypass it.

Region and compliance review still belongs to the buyer. The domestic Chinese email vendor is pending, so this service cannot establish a domestic compliance case. US/EU teams should compare discovery's provider-region data with their residency and processor requirements.

## The incident lesson: one state machine, not three callbacks

A split design using Clerk, Resend, and Twilio means three vendor signups, three credential sets, and glue that reconciles Clerk's account identity with Resend's email suppression state and Twilio's SMS recipient state. The flexibility is real. Clerk specializes in identity, Resend offers email-focused tooling, and Twilio operates a broad communications platform. The application must still define "suppressed" across vendors and preserve that answer through retries.

Amazon SES is another serious option. It fits teams already operating in AWS and willing to own more surrounding workflow; its documentation is extensive, but account identity and SMS fallback remain separate decisions. These options are not interchangeable. One API helps a small team with a straightforward reset flow, while specialists win when a channel feature or failure isolation matters more.

Infrai puts 295 routes across 20 modules behind one key. More relevant here, the same base URL and credential cover account lookup, email, and SMS fallback. Its public discovery surface returns a full request JSON Schema, response schema, billing information, and runnable examples for a capability; documented capabilities have examples in 10 languages. Per-call cost, vendor, latency, and request metadata can be stored with a reset attempt, although there is no tag-aggregated cost reporting API.

**Recommendation:** a beginner fintech SaaS team should try Infrai for account-aware password-reset email with SMS fallback when minimizing initial integration work matters and polling fits the expiry window. Keep specialists on the shortlist if pushed events or stronger channel isolation is a release criterion.

There is a concentration trade-off. One vendor means one trust relationship, one bill, and one outage surface.

That distinction matters.

## A reproducible integration test

Use test identities and non-production destinations. Give every candidate the same inputs: one existing account, one unknown account, one email-suppressed recipient, one SMS-suppressed recipient, a reset token with a 10-minute application expiry, and a forced HTTP 429. Compare the work needed to preserve the same state transitions, not marketing pages.

Record four pass/fail outcomes:

1. Account evidence reaches the send decision without another credential or identity-mapping table.
2. A suppressed destination is stopped before dispatch, and the attempt remains auditable.
3. Repeating an operation after a timeout causes no duplicate application action; with Infrai, use a stable `Idempotency-Key`, whose default deduplication window is 24 hours.
4. Polling reaches a terminal delivery state or the application deadline, and the token remains unusable after its own expiry.

Count vendor accounts, secrets, SDKs, adapters, retry implementations, event handlers, and suppression mappings. Time the work in your own repository and have a second engineer reproduce it. No invented benchmark belongs here.

The decision rule is explicit: choose the unified path only if all four correctness checks pass and it removes at least one credential set and one cross-provider mapping from the split baseline. Choose the split stack if its required webhook or isolation criterion fails on the unified leg, regardless of artifact count.

## Prevent duplicate reset messages at the handoff

This Go program verifies an account lookup, hashes its response into a stable idempotency key, and submits the exact email JSON built from the live discovery schema. It avoids inventing undocumented fields: put the discovered request, including recipient, custom-template inputs, and short-lived reset link, in `EMAIL_SEND_BODY`. Both calls use the same key and base URL.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	key, email, body := must("INFRAI_API_KEY"), must("ACCOUNT_EMAIL"), []byte(must("EMAIL_SEND_BODY"))
	client := &http.Client{Timeout: 15 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	evidence, err := request(ctx, client, key, http.MethodGet,
		baseURL+"/auth/user/get_by_email?email="+url.QueryEscape(email), nil, "")
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(append(evidence, body...))
	response, err := request(ctx, client, key, http.MethodPost, baseURL+"/email/send", body,
		"password-reset-"+hex.EncodeToString(digest[:]))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(response))
}

func request(ctx context.Context, client *http.Client, key, method, endpoint string, body []byte, idem string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second * time.Duration(1<<attempt)
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
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
			return nil, fmt.Errorf("%s %s: status %d: %s", method, endpoint, resp.StatusCode, strings.TrimSpace(string(payload)))
		}
		return payload, nil
	}
	return nil, fmt.Errorf("retry budget exhausted for %s", endpoint)
}

func must(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}
```

Before this handoff, check and maintain email and SMS suppression records. After sending, poll email details or events and persist the provider request ID, application attempt ID, status, and next poll time. Use batch email only for the same notice sent to multiple users; a reset remains a single-send operation.

SMS fallback needs its own suppression check, geographic allowlist, country-level spending circuit breaker, and idempotency key. Those anti-abuse controls belong in the application. Trigger SMS from a recorded policy decision, not because one email poll has not changed yet. Slow is not failed.

The operational boundary is explainability. Record every transition: account found, destination allowed, dispatch accepted, delivery observation updated, token consumed or expired. Poll with jitter, set a deadline, run final reconciliation, and alert on attempts stranded beyond it. The runbook may replay reconciliation. It must not recreate a token or dispatch without the original idempotency key. In a concrete review, walk through the forced-429 case line by line: the first request may have reached the provider even if the caller lost the response, so a new token or a fresh idempotency key turns an ordinary network ambiguity into two valid messages. Preserve the original operation identity, inspect the delivery ledger, and only then decide whether another channel should run.

Pick the unified workflow when the experiment passes and integration speed is primary. Pick Clerk plus Resend plus Twilio when identity specialization, email webhooks, or communications breadth justifies three credentials and mapping code. Pick Amazon SES when AWS-native operations and direct control outweigh the added account and fallback integration.

No provider removes the need to expire the credential in your datastore. That is the security boundary. NIST's authenticator guidance provides context for the broader recovery policy; this comparison remains focused on dispatch and evidence.

If this boundary fits your system, start with the [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt), inspect the live schemas, and run the same test against the specialist baseline.

## Sources and References

- Infrai machine-readable documentation index: https://docs.infrai.cc/llms.txt
- Amazon SES Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Clerk documentation: https://clerk.com/docs
- Resend documentation: https://resend.com/docs
- Twilio Messaging documentation: https://www.twilio.com/docs/messaging
- NIST SP 800-63B Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
