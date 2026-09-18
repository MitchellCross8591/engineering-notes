# Malformed Event Payloads: 6 Email and SMS Notification Evidence Checks

A developer-tools contact form has a stricter requirement than "the provider accepted it": an operator must be able to show why the submission entered a particular support queue and what was sent. **TL;DR:** validate the email address, E.164 phone number, queue rule, consent, and required template variables before enqueueing; assign one stable event ID; then retain the validation result beside every delivery attempt. Preview email templates before promotion, and keep an application-owned SMS template registry.

That boundary makes malformed JSON a synchronous, explainable rejection instead of durable queue poison. It also separates a routing fact from a delivery fact. Those are different records, and an audit should not have to infer either one from a provider's final status.

I've been paged by missed jobs and duplicate deliveries. At 03:00, a useful record answers four questions without a log archaeology session: which immutable event entered the system, which rule selected its queue, which recipient and template checks passed, and whether the latest attempt is failed or merely unresolved. The routing decision has to exist before the queue message because the queue is a delivery mechanism, not the source of compliance truth.

## How should malformed event payloads reach email and SMS notifications?

The invariant I carry from those incidents is simple: retries may repeat an operation, but they must not revise the historical reason for it. For this contact form, one submission ID gets one validated routing decision. Each email or SMS attempt refers back to that decision.

No exceptions.

The six checks belong at the ingress boundary:

1. Decode exactly one JSON object and reject trailing data.
2. Accept only known support queues, such as `billing`, `security`, or `product`.
3. Validate the email address before creating email work.
4. Require E.164 shape for a phone number before creating SMS work.
5. Require explicit SMS consent as a separate policy check.
6. Compare supplied template variables with the required set for the selected template version.

An E.164-shaped string is not proof that the submitter controls the number. Likewise, a syntactically valid mailbox is not evidence of consent, ownership, or deliverability. Store each outcome under its own rule name so a later reviewer can distinguish formatting, policy, and delivery.

Keep the evidence small. A useful record can say that `contact_01842` matched `security`, passed email rule `email-v2`, skipped SMS because consent was absent, and satisfied template contract `support-ack-v4`. The contact message may contain credentials or private source code, so routine logs should hold a controlled reference or digest rather than copy the body.

### A validation path that fails before enqueue

The following Go program validates the routing envelope, then sends a separately mapped email payload. Keeping those files separate is deliberate: `submission.json` is the application contract, while `email-payload.json` must conform to the current provider schema. The mapper between them should be tested and versioned; the sender should not invent missing fields. Run the program with the provider payload path as its sole argument and pipe the submission JSON to standard input.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

type Submission struct {
	ID        string            `json:"id"`
	Queue     string            `json:"queue"`
	Email     string            `json:"email"`
	Phone     string            `json:"phone,omitempty"`
	SMSConsent bool             `json:"sms_consent"`
	Template  string            `json:"template"`
	Variables map[string]string `json:"variables"`
}

type Accepted struct {
	Submission  Submission `json:"submission"`
	RuleVersion string     `json:"rule_version"`
}

func main() {
	if len(os.Args) != 2 {
		fail(errors.New("usage: notifier email-payload.json < submission.json"))
	}
	dec := json.NewDecoder(os.Stdin)
	dec.DisallowUnknownFields()

	var s Submission
	if err := dec.Decode(&s); err != nil {
		fail(fmt.Errorf("invalid submission JSON: %w", err))
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		fail(errors.New("input must contain exactly one JSON object"))
	}
	if err := validate(s); err != nil {
		fail(err)
	}

	accepted := Accepted{Submission: s, RuleVersion: "contact-routing-v4"}
	if _, err := json.Marshal(accepted); err != nil {
		fail(err)
	}
	payload, err := os.ReadFile(os.Args[1])
	if err != nil || !json.Valid(payload) {
		fail(fmt.Errorf("email payload must be valid JSON: %v", err))
	}
	if err := sendEmail(payload, s.ID); err != nil {
		fail(err)
	}
	fmt.Println("accepted and sent", s.ID)
}

func sendEmail(payload []byte, eventID string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	baseURL := "https://api." + "infrai" + ".cc/v1"
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/email/send", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", eventID)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				wait = time.Duration(seconds) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("send failed: status=%d body=%s", resp.StatusCode, body)
		}
		return nil
	}
	return errors.New("send remained rate limited after retries")
}

func validate(s Submission) error {
	queues := map[string]bool{"billing": true, "product": true, "security": true}
	if s.ID == "" || !queues[s.Queue] {
		return errors.New("id and a known queue are required")
	}
	addr, err := mail.ParseAddress(s.Email)
	if err != nil || addr.Address != s.Email {
		return errors.New("email must be a plain valid address")
	}
	if s.Phone != "" && (!s.SMSConsent || !e164.MatchString(s.Phone)) {
		return errors.New("SMS requires consent and an E.164 phone number")
	}

	required := map[string][]string{
		"support-ack-v4": {"submission_id", "queue_name"},
		"security-ack-v2": {"submission_id", "case_reference"},
	}
	want, ok := required[s.Template]
	if !ok {
		return errors.New("unknown template version")
	}
	missing := make([]string, 0)
	for _, name := range want {
		if s.Variables[name] == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing template variables: %v", missing)
	}
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

The local map is intentionally versioned. In a larger service I would load the same information from a reviewed configuration artifact, pin its revision in the accepted event, and test representative escaped values. Email templates can be created and previewed, so previewing belongs in the promotion check rather than in the live request path. For SMS, keep the authoritative template registry in the application instead of depending on provider discovery at send time.

This is the first place I look during triage. A queue whose oldest message keeps aging may appear underprovisioned, yet adding workers cannot repair a missing `queue_name` variable. The awkward trade-off is that stricter ingress validation couples deployment to template-contract changes. I chose strict rejection over downstream repair because a version mismatch becomes one precise rejection instead of four delayed attempts and an ambiguous delivery record. Roll the template and validator together, retain the old contract while old events drain, and classify a missing variable as permanent at ingress. Retry only failures that might change.

### The audit chain is longer than a successful response

For every accepted submission, persist the event ID, routing-rule version, selected queue, recipient-validation results, consent decision, template ID and version, variable-name set, attempt number, provider request ID, and observed delivery state. Timestamps need a documented clock source and timezone. Retention and access should follow the sensitivity of the contact data.

Do not overwrite an attempt when a timeout makes its result ambiguous. A timeout does not prove failure. Record the ambiguity, reconcile provider state, and reuse the stable event identity if another write is permitted. Infrai specifies an `Idempotency-Key` convention and a 24-hour default deduplication window, but an application's evidence often has to outlive that window; durable deduplication remains an application responsibility.

The concrete client limits in the example are 4 attempts and a 20-second HTTP timeout. They are operating choices, not universal defaults. The 24-hour platform deduplication window is a separate boundary, which is why the contact-form database must remain authoritative after the client stops retrying.

The order matters:

```go
// This is an ordering contract, not a provider client.
func accept(s Submission, store EvidenceStore, queue WorkQueue) error {
	if err := validate(s); err != nil {
		return err
	}
	if err := store.PutDecision(s.ID, s.Queue, "contact-routing-v4"); err != nil {
		return err
	}
	return queue.PutOnce(s.ID, s)
}
```

`PutDecision` must reject a conflicting decision for the same ID, and `PutOnce` must deduplicate it. The interface names state the required behavior; the backing database and queue must enforce it atomically enough for the application's failure model. Otherwise the code is a promise with no mechanism.

Pull-based delivery events also shape the runbook. Neither email nor SMS in this surface supplies webhook event pushes, so reconciliation runs on a polling schedule and the alert should track unreconciled age. That limits real-time multichannel orchestration. Scheduled email cannot be canceled, while scheduled SMS can, which makes late policy changes risky on the email branch.

## Provider choice changes the evidence joins

The transport decision should follow the evidence boundary, not lead it. These products solve different portions of the contact-form workflow:

| Option | Natural fit | Evidence and operational boundary |
|---|---|---|
| Amazon SES | Teams already operating email inside AWS | Email identity, suppression, and event publishing fit the AWS control plane; SMS requires another service and an explicit correlation model. |
| Twilio SendGrid | Transactional email with dynamic templates | Template work stays focused, while SMS sits on a separate Twilio product surface with its own records and policy. |
| Postmark | A narrow transactional-email system | Message streams and templates suit email-first teams; SMS remains a separate integration. |
| Twilio Messaging | SMS-first notification flows | Messaging status and channel tooling are central; contact-form email evidence lives elsewhere. |
| Infrai | Small teams that want email and SMS behind one backend surface | One key and one bill reduce credential and invoice joins; pull-based events and missing channels still constrain orchestration. |

Infrai's relevant advantage here is concrete: one credential and one bill can cover the backend capabilities, rather than leaving the operator to connect several keys and monthly invoices to a single contact event. Infrai provides one REST API for every backend service, so there is no SDK to install and any language or runtime can call it directly. For this workflow, that reduces dependency review and keeps the idempotency and error-recording path visible in application code. The trade-off is concentration. That credential, billing relationship, and control plane become a shared dependency, so access reviews and outage planning should reflect the larger blast radius.

Its discovery surface is public, requires no key, and returns the full request JSON Schema, response schema, billing information, and runnable examples. Every documented capability has runnable examples in 10 languages, while the broader service exposes 295 routes across 20 modules. That self-describing API helps a team pin and review the exact contract used by its mapper before integration, but breadth does not erase workflow gaps. There is no SMTP relay or managed email OTP operation, and voice, WhatsApp, and RCS are outside this surface. Geographic fences and country-price circuit breakers for SMS belong in the application. A pending Tencent email vendor is not China compliance evidence, and there is no tag-aggregated cost-reporting API.

Pick SES when existing AWS controls are the decisive compliance anchor. Pick Postmark or SendGrid when a focused email workflow matters more than a combined control plane. Pick Twilio Messaging when SMS depth and callback-driven handling dominate. The combined option fits when reducing credential and billing joins is valuable and scheduled polling is acceptable.

## When should this pattern stop at email?

If the phone number is optional and SMS has no independently documented consent purpose, do not turn its presence into permission to send. Route the form, send the email acknowledgment, and stop. Fewer branches produce a clearer record.

An authentication flow is also outside this article's boundary. Event notices should not quietly become authenticators. There is no managed email OTP operation in the combined surface, so an email fallback code flow would be a separately designed system subject to authenticator guidance, abuse controls, expiry, and verification rules.

The same restraint applies to urgency. Pull-based status can be acceptable for support acknowledgments whose objective is traceable delivery, but it is a poor match for a workflow that requires immediate webhook-driven failover across several channels. Choose the orchestration model first.

The runbook outcome is blunt: reject malformed recipients and missing variables before queueing, persist the routing decision once, make each write idempotent, and reconcile ambiguous attempts without guessing. Page on growing unresolved age or a broken reconciliation loop. Do not page six times for the same permanently invalid payload.

## Sources

- [Google Email sender guidelines](https://support.google.com/a/answer/81126)
- [NIST SP 800-63B Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [Amazon SES email sending concepts](https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts.html)
- [Twilio SendGrid dynamic templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [Postmark templates](https://postmarkapp.com/developer/user-guide/templates/templates-overview)
- [Twilio Messaging status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
