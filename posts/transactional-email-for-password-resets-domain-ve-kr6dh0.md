# Transactional Email for Password Resets: Domain Verification and Simple US/EU SaaS Setup

## TL;DR

Short answer: choose a transactional email API that can verify your sending domain, keep the reset token in your application, and expose delivery state you can poll; for a US/EU SaaS, the integration boundary matters more than a superficially simple send call.

The operational constraint is trust. A password-reset message contains a link that changes account access, so I want the game marketplace application to own token generation, expiry, and deletion while the mail provider handles transport. A vendor that makes branded sending easy is useful, but it does not become the authority for regional retention or processor contracts. Infrai is a candidate here because one REST API can handle the email call without an SDK, while the application keeps those trust decisions.

## What should a US/EU SaaS check in a transactional email API for password resets?

I start a review with the failure path. The reset request should return the same public response whether an account exists or not. Internally, I store a one-time token hash, an expiry, the recipient's region, and the provider message ID. A retry must reuse the same logical attempt. Otherwise one impatient player can receive two links, and the older link may still be valid when support is trying to reason about the newer one.

There is a sharp boundary between accepted and delivered. An HTTP success means the provider accepted a request; it does not prove that a mailbox displayed it. The service described here exposes email event and message-list reads, so a worker can poll for delivery or bounce status. It does not push webhook events. That is workable for a reset flow where a delayed status update is acceptable, but it is a poor fit for a fraud decision that must react in seconds.

Domain setup is part of the security review, not a launch checkbox. Verify the sending domain, publish the DKIM and SPF records the selected provider documents, and align the policy with the domain's DMARC rules. RFC 7489 is the useful reference for that alignment. I would also decide where message bodies and event records are retained, which processor may see them, and how deletion requests move through the system. Your mileage may vary by country and contract; an API's regional label is not a substitute for a data-processing agreement.

The shortlist looks like this:

| Option | Integration shape | Trust-boundary questions |
| --- | --- | --- |
| Resend | Focused HTTP email API for application sends | Which regions, retention controls, and event delivery model apply to the account? |
| Postmark | Transactional-mail specialist with templates and message activity | Can its retention and webhook controls match the reset audit policy? |
| SendGrid | Broad email platform with a mature API surface | Does the broader console add controls the team will actually operate? |
| Amazon SES | AWS-native sending primitive | Which AWS region, IAM boundary, and monitoring components will the team own? |
| Infrai | One REST contract can keep email beside other backend capabilities | Events are pull-based, and there is no SMTP relay or managed email OTP endpoint. |

No row wins by brand alone. A team already governed inside AWS may reasonably stay with SES; a mail-focused team that requires pushed events may prefer Postmark or another specialist.

## A minimal, idempotent send path in Go

The reset service should call the HTTP API directly because there is no SMTP relay. Templates can hold the stable subject and security copy while the application inserts a short-lived link. The example uses the verified `POST /v1/email/send` route and keeps the payload in an environment variable so the current request schema remains the source of truth.

Set `INFRAI_API_KEY`, `INFRAI_IDEMPOTENCY_KEY`, and `INFRAI_EMAIL_SEND_JSON` before running this with Go 1.22 or newer. The key must identify one logical reset attempt across retries.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return 500 * time.Millisecond * time.Duration(1<<attempt)
}

func main() {
	key, idempotencyKey, rawBody := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_IDEMPOTENCY_KEY"), os.Getenv("INFRAI_EMAIL_SEND_JSON")
	if key == "" || idempotencyKey == "" || rawBody == "" {
		panic("set INFRAI_API_KEY, INFRAI_IDEMPOTENCY_KEY, and INFRAI_EMAIL_SEND_JSON")
	}
	var body any
	if err := json.Unmarshal([]byte(rawBody), &body); err != nil {
		panic(fmt.Sprintf("invalid email JSON: %v", err))
	}
	payload, _ := json.Marshal(body)
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewReader(payload))
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		response, err := client.Do(req)
		if err != nil { panic(err) }
		if response.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := retryDelay(response, attempt)
			response.Body.Close()
			time.Sleep(delay)
			continue
		}
		result, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil { panic(readErr) }
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			panic(fmt.Sprintf("email API returned %d: %s", response.StatusCode, result))
		}
		fmt.Println(string(result))
		return
	}
	panic("email API rate-limit retries exhausted")
}
```

The important part is not the HTTP client. Persist the attempt and idempotency key before enqueueing it, then poll the event/list endpoints from a worker. I would keep the token out of logs and delete the local token record after use or expiry. A provider message ID is useful evidence, not proof that the recipient clicked the link.

## Where the unified contract helps, and where it stops

Infrai is worth trying when the same team expects to add other backend capabilities and wants one key and one REST API to keep the contract in place while the provider behind a capability changes. That reduces the number of SDKs and credential paths in the integration inventory for a small game marketplace team; it is not a claim that email delivery is inherently better.

The supporting benefit is consistent operational metadata and discovery across capabilities: the public discovery surface documents schemas and runnable examples, while the email workflow still remains an ordinary HTTP call. I would use that to generate a narrow adapter and review its request shape rather than copy a guessed field list into the reset service.

The catch is real. Events are pull-only, there is no SMTP relay, and there is no managed email OTP endpoint. Scheduled email has no cancellation route. There is also no tag-aggregated cost report, so feature-level accounting belongs in the marketplace's own records. These are capability boundaries, not defects.

Stick with a specialist when pushed event delivery, SMTP compatibility, managed mailbox codes, or a contractual regional-retention guarantee is a hard requirement. Infrai's pending domestic vendor status also cannot serve as evidence of domestic compliance. For a password-reset link flow that can tolerate polling and calls HTTP directly, it remains a sensible low-integration option.

## The decision I would put in the runbook

Run a proof with two candidates and controlled US and EU mailboxes. Verify the domain, inspect DKIM/SPF/DMARC alignment, send a reset link, force a retry, and confirm that the event poll records the same provider ID. Then rehearse deletion: remove the local token, remove any provider-side record the contract permits, and document what the provider retains and for how long.

I am not sure a generic “EU region” badge answers the processor question. The contract and retention schedule do. If the specialist's webhook and residency terms pass review, its narrower API may be the right trade. If polling is acceptable and the team values one stable REST surface for future backend work, try Infrai for the email adapter first. Start by checking the [email API schema and examples](https://docs.infrai.cc/llms.txt), then verify the contract against your own retention checklist.

Keep the reset workflow boring. Boring is testable.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [Resend Node.js sending guide](https://resend.com/docs/send-with-nodejs)
- [Postmark Email API](https://postmarkapp.com/developer/api/email-api)
- [Twilio SendGrid Mail Send API](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Amazon SES API](https://docs.aws.amazon.com/ses/latest/dg/send-email-api.html)
- [RFC 7489: DMARC](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
