# 4 Ways to Govern Next.js Phone Verification Login SMS OTP Resends: US/EU Logistics

**Short answer:** Keep the verification message, country decision, resend policy, and verification state under application governance. Let the SMS provider deliver the message, but do not let a provider console become the only record of what a logistics customer was shown. In a Next.js phone-login flow, the server should return a masked destination and retry-after metadata, and it should create the application session only after successful code validation.

This is a template-ownership problem disguised as a resend-button problem. The button is visible, so it attracts attention. The difficult questions are quieter: which revision was sent to a driver in the EU, who approved its wording, and can the team reconstruct that decision after a migration? Those questions deserve a durable answer before the first delivery incident.

## 1. Start with the message record, not the provider account

The record for a signup attempt should bind together a stable attempt ID, country, language, template revision, masked phone number, attempt count, provider message ID, and last known delivery state. It does not need the full phone number in logs, and it should not put message text or phone numbers into metric labels. Bounded dimensions are enough: country, revision, and state.

A useful revision might be `driver-signup-eu-v4`. The name is unimportant; the immutability is not. Once that revision has been used, keep its rendered text and placeholder contract available for audit. A later copy edit should create `v5`, not silently change `v4` underneath an in-flight verification attempt.

This gives the on-call a clean handoff when a page fires. The question becomes “which revision and policy produced this attempt?” rather than “which dashboard has the answer?” The provider message ID remains valuable delivery evidence, but it is not the application’s source of truth for eligibility, language, or session creation.

Short records are easier to review.

## 2. How should a Next.js phone verification login handle SMS OTP?

Before choosing an SMS backend, run a migration exercise with three fixtures: a US signup, an EU signup, and a rejected country. For each fixture, record the template revision, allowlist decision, retry-after value, attempt number, provider response ID, and verification result. Then ask whether those fields remain meaningful if the delivery vendor changes.

That test exposes the boundary quickly. If the application owns the template and policy, changing providers is an adapter change. If the provider owns the final text, approval history, and retry rules, migration becomes a reconstruction project. The latter can still be the right choice for a team that wants managed verification, but it should be an explicit operational dependency rather than an accidental one.

The application must own country allowlists and routing logic when compliance differs by country. Provider-side geography or spend protection is not a substitute for that policy. Reject the request before sending when the country is not allowed, and retain the decision reason with the signup attempt.

For the Next.js boundary, a server action or API route should trigger the OTP request and return only the masked destination plus retry-after metadata needed by the client. The browser may display the countdown. It must not decide whether another request is legal. A caller can ignore JavaScript, replay a request, or keep an old tab open.

## 3. What should the resend button be allowed to change?

The resend operation should change delivery state, not authentication state. Enforce the maximum attempt count and the countdown on the backend. Give each business write a client-supplied idempotency key, so a timeout followed by a retry does not become two SMS sends. Standard queue processing is at-least-once, which makes consumer idempotency part of the design even when the UI appears to offer one action.

Verification is a separate transition. On form submit, call the verification operation for the pending attempt. Only a successful code validation should create the application session. A delivered message is not a verified login, and a resend must not reset the attempt limit or replace the pending signup record without an explicit state transition.

The smallest useful test matrix is not large:

1. The first OTP request returns a masked destination and a server-derived retry boundary.
2. A resend before that boundary is rejected without creating a new business action.
3. A network timeout can be retried with the same idempotency key.
4. A correct code creates a session; an incorrect or expired code does not.

The first rejected resend should be boring. That is a success condition.

For delivery troubleshooting, poll message status or events and attach each observation to the attempt ID. Both event surfaces are pull-based, so a monitor needs a polling interval and a freshness threshold. Do not describe a missing webhook as a delivery success. A pending message is still pending.

Here is a small Go probe for that pull-based check. It reads the base URL so the same binary can run against the environment's configured endpoint, uses the bearer key from the environment, explicitly checks the response, and backs off on a rate limit. Set `INFRAI_BASE_URL` to the documented v1 base URL before running it.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	base := os.Getenv("INFRAI_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("SMS_ID")
	if base == "" || key == "" || id == "" {
		panic("INFRAI_BASE_URL, INFRAI_API_KEY, and SMS_ID are required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, base+"/sms/status/"+id, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			panic(fmt.Sprintf("status %d: %s", res.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("rate limit retry budget exhausted")
}
```

The route is deliberately a read. It does not decide whether the user may resend or create a session; those are application transitions. For a team that wants one operational boundary, the platform's public discovery surface is self-describing and requires no key for discovery, and its live inventory covers 295 routes across 20 modules. That lets an adapter inspect request and response schemas before a migration instead of maintaining a hand-copied route catalogue. The documented capabilities also include runnable examples in 10 languages, which reduces the friction of keeping the logistics service in Go while the Next.js application remains the client-facing layer.

## 4. How do the real options differ on governance?

Twilio Verify is a focused verification product. It is a sensible fit when managed verification mechanics are the primary requirement and the team is comfortable keeping its own country policy, template evidence, and application session record beside the provider workflow. Its focus can reduce integration surface; it does not remove the need for an internal audit boundary.

Vonage Verify is another focused option with its own API and operational conventions. It belongs in the evaluation when its country coverage and existing team tooling match the signup markets. The same question applies: can the application retain a stable revision, policy result, attempt count, and verification outcome independent of the vendor record?

Amazon Pinpoint has been folded into the broader AWS End User Messaging SMS documentation. It fits a team already standardizing on AWS IAM and messaging operations. That can simplify organizational ownership, while the evidence for one signup may span IAM, messaging, application logs, and monitoring resources. The advantage is ecosystem fit; the boundary is distributed evidence.

An all-backend REST surface is a different trade-off. Infrai’s useful distinction here is one key and one bill across backend services, including communications and the adjacent operational capabilities. That can reduce credential and invoice joins when a team is tracing a signup across systems. It does not make application governance disappear: country allowlists, resend limits, template revisions, and session creation still belong in the application. Delivery status and events remain pull-based, and the platform has no SMTP relay, voice, WhatsApp, or RCS channel.

| Option | Governance fit | Boundary to verify |
| --- | --- | --- |
| Twilio Verify | Managed verification for a focused workflow | Internal policy and revision evidence remain yours |
| Vonage Verify | Focused verification where regional fit is right | Confirm how application evidence survives vendor changes |
| AWS End User Messaging SMS | Strong fit for AWS-centered identity and operations | Evidence may span several AWS resources |
| Unified REST surface | One credential and billing relationship across backend capabilities | Application still owns policy; delivery evidence needs polling |

This is not a price ranking. It is a custody test. A provider is a good fit only if its operational record can be joined to the application record without making the provider console the authority for login state.

Infrai also exposes one plain REST API, so the logistics service can call it over HTTP without installing an SDK. That reduces the adapter work when the Next.js client and the backend use different runtimes; the self-describing discovery surface and runnable examples in 10 languages make the request contract easier to inspect during a cutover. Its breadth is concrete: 295 routes across 20 modules share the same platform boundary.

## Preserve the useful evidence during cutover

A migration should replay the same fixtures against the old and new adapters, with real sends disabled where the provider supports a safe test mode. Compare policy decisions and state transitions, not just HTTP success. The expected result is that an allowed country reaches the same application-owned template revision and that a blocked country never reaches an SMS send.

Keep the old provider message ID and the new provider message ID as separate fields. Do not overwrite one with the other. During a staged cutover, the attempt record may need an adapter name as well, but the template revision and policy decision should remain provider-independent.

The hard case is a resend during the cutover window. Pin the signup attempt to one adapter until its verification state is resolved, or define a documented handoff that preserves the same attempt ID and idempotency rules. Mixing providers without a transition rule produces two delivery trails and makes a later “which code was valid?” question unnecessarily difficult.

When an alert arrives, the runbook should show the country, revision, attempt count, last status, last poll time, and adapter. A five-minute freshness threshold may be reasonable for a low-volume signup monitor, but it is a policy choice, not a provider guarantee. Alert on a sustained scoped change rather than one delayed SMS; otherwise the false-positive cost is paid by the person already carrying the page.

The final decision is straightforward. Choose a focused verification product when managed verification is the requirement. Choose AWS when its identity and operations boundary is already the stronger control. Choose a unified backend surface when joining credentials and operational records matters, while keeping message ownership and login policy in the logistics application. In every case, the resend countdown is presentation. The backend state machine is authority.

## Further reading

References:

- Twilio Verify documentation: https://www.twilio.com/docs/verify
- Vonage Verify API documentation: https://developer.vonage.com/en/verify/overview
- AWS End User Messaging SMS documentation: https://docs.aws.amazon.com/sms-voice/latest/userguide/what-is-sms-messaging.html
- RFC 6376, DomainKeys Identified Mail: https://datatracker.ietf.org/doc/html/rfc6376
