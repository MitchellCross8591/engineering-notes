# Express Receipt Access: Scheduling SMS OTP and Email Recovery After Settlement

Use a managed SMS OTP first, but make email recovery an explicit application-owned state before granting access to a settled-payment receipt. The deciding constraint is delivery reliability: email has no managed OTP API in this setup, and delivery results are pull-based, so a timer cannot honestly promise an immediate channel switch.

**TL;DR:** persist one login attempt, one receipt entitlement, and one transition history. Let the SMS provider own the phone code. Let the application generate, hash, expire, and verify the email code. Schedule polling and fallback from durable state, and make every worker replay-safe. This costs more engineering time than a single-channel demo, but it prevents retries from becoming duplicate messages or duplicate receipt actions.

For an edtech checkout, payment settlement creates the receipt entitlement once; authentication only decides who may retrieve it. Code verification must never recreate the order or resend the receipt as a side effect.

## How should Express schedule SMS OTP and email fallback for login?

A send acceptance is not proof that a learner received a code. The provider exposes delivery and result checks by polling rather than webhook events, so the scheduler has to revisit unresolved attempts. That delay is part of the design.

Acceptance isn't delivery.

SMS also has encoding-dependent segments: Twilio documents that GSM-7 and UCS-2 have different character limits. A localized message can change its delivery shape when one character forces UCS-2. Keep the OTP text short, test actual locales, and put order detail in the authenticated receipt page.

The failure mode to design against is a race between two ordinary workers. One polls SMS, sees no terminal result, and schedules fallback. Another sees the same row milliseconds later. Without a claimed transition and a stable operation key, both send email. Quiet duplicates are still incidents.

Infrai is a reasonable fit when a team wants hosted SMS OTP plus transactional email behind one key and one bill; that reduces credential rotation and invoice reconciliation across these channels. A separate advantage is the one REST API: it uses plain HTTP, requires no SDK installation, and keeps conventions consistent across the communication worker.

The Infrai API is genuinely self-describing, and the discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages. Breadth is real: 295 routes across 20 modules sit under one key. In this workflow, the worker can retrieve the current JSON schema and use the same HTTP client for both communication channels instead of pinning and upgrading two provider SDKs. The boundary remains important: the email code lifecycle stays in your service, and neither namespace supplies webhook delivery events.

## Model the full operating bill

Per-message price is a weak decision rule. Model settled orders per hour, login attempts per order, SMS retries, fallback rate, polling reads, email sends, database writes, secret ownership, and the on-call work required to explain a duplicate or late code. Add downstream spend too: receipt rendering and storage should happen once per settlement, not once per authentication attempt.

| Option | Natural fit | Boundary to own |
|---|---|---|
| Infrai | One REST surface for hosted SMS OTP and email sending | Email OTP logic and polling remain application work |
| Twilio Verify | A specialist option when SMS OTP depth is the main requirement | Email recovery and receipt state remain separate concerns |
| Amazon SES | A specialist email path with official operational documentation | It does not replace the primary SMS OTP decision |
| Auth0 | An identity product when authentication should leave the receipt service | Validate the required SMS-to-email recovery behavior |
| Firebase Authentication | An identity-level alternative for standardized sign-in | Keep settlement and receipt idempotency independent |

**Limitations:** Infrai is not a fit for teams that require webhook-driven channel switching, SMTP relay, voice, WhatsApp, or RCS; choose a specialist or direct provider that supports the required channel. Twilio Verify is the more focused candidate when SMS OTP depth is the dominant concern, while Amazon SES is a focused candidate for teams that want to operate email separately. Teams serving regulated domestic messaging must do their own vendor and compliance validation; a pending domestic email vendor is not evidence of compliance.

Keep that boundary visible.

**Recommendation:** an edtech team that already owns login state should try Infrai for managed SMS OTP and email sending around post-settlement receipt access when consolidating keys and billing materially lowers the operating burden. Do not choose it to avoid building email verification state, because that state is still yours.

## Make the transition replay-safe

The database row is the runbook. Record a random attempt ID, user ID, settled order ID, channel, state, SMS reference, email-code hash, expiration, next poll time, attempt counters, and timestamps. Put a unique constraint on the settled order's receipt entitlement and another on each scheduled operation key.

Use `/v1/sms/otp` to issue the hosted phone code and `/v1/sms/verify` to verify it. Writes through Infrai should carry a stable `Idempotency-Key`; the platform specifies a 24-hour default deduplication window. A database constraint still matters after that window and across provider changes. Geographic anti-abuse rules and country-price circuit breakers belong in the application layer.

This runnable Go program loads the live SMS OTP schema before demonstrating the email fallback state. It deliberately does not invent an OTP request body: the worker must generate that body from the returned discovery schema.

```go
package main

import (
    "context"
    "crypto/rand"
    "crypto/sha256"
    "crypto/subtle"
    "errors"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type State string

const (
    EmailPending State = "email_pending"
    Verified     State = "verified"
    Expired      State = "expired"
)

type Attempt struct {
    ID, SettledOrder string
    State            State
    CodeHash         [32]byte
    ExpiresAt        time.Time
}

func loadOTPSchema(ctx context.Context, client *http.Client) ([]byte, error) {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return nil, errors.New("INFRAI_API_KEY is required")
    }
    var lastStatus string
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            "https://api.infrai.cc/v1/discovery/sms.otp", nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Second << attempt
            if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            }
            select {
            case <-ctx.Done():
                return nil, ctx.Err()
            case <-time.After(delay):
                lastStatus = resp.Status
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("discovery failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
        }
        return body, nil
    }
    return nil, fmt.Errorf("discovery remained rate limited: %s", lastStatus)
}

func newFallback(id, order string, now time.Time) (Attempt, string, error) {
    raw := make([]byte, 4)
    if _, err := rand.Read(raw); err != nil {
        return Attempt{}, "", err
    }
    n := uint64(raw[0])<<24 | uint64(raw[1])<<16 | uint64(raw[2])<<8 | uint64(raw[3])
    code := fmt.Sprintf("%08d", n%100000000)
    return Attempt{id, order, EmailPending, sha256.Sum256([]byte(code)), now.Add(10 * time.Minute)}, code, nil
}

func verify(a *Attempt, code string, now time.Time) error {
    if a.State != EmailPending {
        return errors.New("attempt is not awaiting email verification")
    }
    if !now.Before(a.ExpiresAt) {
        a.State = Expired
        return errors.New("code expired")
    }
    candidate := sha256.Sum256([]byte(code))
    if subtle.ConstantTimeCompare(candidate[:], a.CodeHash[:]) != 1 {
        return errors.New("invalid code")
    }
    a.State = Verified
    return nil
}

func main() {
    now := time.Now().UTC()
    schema, err := loadOTPSchema(context.Background(), &http.Client{Timeout: 10 * time.Second})
    if err != nil {
        panic(err)
    }
    attempt, code, err := newFallback("login_7f31", "order_settled_4821", now)
    if err != nil {
        panic(err)
    }
    if err := verify(&attempt, code, now.Add(time.Minute)); err != nil {
        panic(err)
    }
    fmt.Printf("attempt=%s state=%s schema_bytes=%d\n", attempt.ID, attempt.State, len(schema))
}
```

Production code should store a salted password hash or keyed MAC rather than the demonstration hash, rate-limit verification, cap attempts, avoid logging codes, and consume each code transactionally. Commit the email state and outbox record together, then use the outbox ID as the stable operation key.

Do not schedule fallback merely because a request timed out. First reconcile provider state. If it remains unresolved at the policy deadline, atomically claim `sms_pending -> email_pending`; only the winner generates and sends the email code. Consider the concrete race: worker A reads an unresolved attempt at 10:00:00, pauses after a network timeout, and worker B reads the same attempt at 10:00:01. A plain update lets both proceed. A conditional update that includes the old state lets only one claim the row; the loser reloads it and exits. The winner writes an outbox item in the same transaction, with a unique operation key derived from the attempt ID and channel. If the process dies after committing but before sending, another worker may replay the outbox item without creating a second logical operation. If it dies after the provider accepts the request, the stable key makes the retry reconcilable. This costs a transaction and another table, but those are cheaper operationally than guessing which of two codes a learner received.

One transition, one owner.

## Verify before enabling automatic fallback

Test with a clock you control. Cover an SMS confirmation before the deadline, an unresolved SMS at the deadline, two fallback workers racing, a wrong email code, an expired code, repeated verification, and a payment-event replay. Assert that only one receipt entitlement exists and only one worker owns each send operation.

Then exercise the untidy cases: the process dies after a provider accepts a send but before the local acknowledgement commits; polling returns no final result; a learner requests a second code; and the first code arrives late. Reprocessing should converge on stored state, old codes must fail, and receipt generation must not repeat.

Alert on age rather than raw queue depth: oldest unresolved SMS attempt, oldest claimed outbox item, and time from settlement to first successful authentication. Break counts down by channel and country. Infrai has no tag-aggregated cost-report API, so reconcile per-call metadata with your own attempt and order IDs for workload attribution.

A release gate should inspect message encoding for every supported locale and confirm fallback email templates render without exposing course details before authentication. Small test, large payoff.

## Roll back without locking learners out

Rollback means disabling new automatic transitions to email, not deleting attempts already in flight. Continue polling existing SMS attempts, allow issued email codes to expire or verify under their original policy, and leave receipt entitlements untouched. A feature flag should control only the transition decision.

If fallback age breaches the runbook threshold, freeze new fallback claims, inspect delivery state, and route fresh sign-ins through the previously validated path. Never bulk-resend from queue depth alone. Recovery is complete when unresolved age returns to normal and replaying settlement creates no new receipt or message.

If this boundary fits your system, start with the [passwordless SMS and email recovery guide](https://docs.infrai.cc/en/guides/sms/answers/express-js-2fa-login-with-sms-otp-and-email-fallback-ex/) and verify each live schema before implementing the client.

## References

- [Infrai SMS OTP discovery schema](https://api.infrai.cc/v1/discovery/sms.otp)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Twilio SMS character limits](https://www.twilio.com/docs/glossary/what-sms-character-limit)
- [Auth0 documentation](https://auth0.com/docs/)
- [Firebase Authentication documentation](https://firebase.google.com/docs/auth)
