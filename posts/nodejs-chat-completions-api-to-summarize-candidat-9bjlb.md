# Nodejs Chat Completions API to Summarize Candidate Text — Structured Rubrics

The least complex design is a two-stage boundary: ask the summarization API for a small, typed record, then validate that record before it can affect a candidate score. Keep the original text beside the result. A summary is evidence for a reviewer, not a replacement for the rubric.

Keep it typed.

**Short answer:** for a B2B SaaS hiring workflow, send the job rubric and candidate text as separate labeled inputs, require one JSON object with fixed fields, reject malformed or incomplete output, and make retries idempotent. The queue should page on a growing set of rejected results or missing completions, not on a single transient request failure.

## The page fires after the score is already wrong

The on-call view usually arrives too late: a candidate has a score but the explanation is empty, or a downstream record contains a prose paragraph where the scoring service expects a number. The visible failure is a bad decision record. The earlier signal was quieter: a response parser accepted an optional field, a retry wrote a second result, or a batch item finished without the required rubric dimensions.

Work backward from that page. Store a request fingerprint, the rubric version, the input text hash, the completion status, and validation errors as separate fields. Emit counters for `submitted`, `completed`, `rejected_schema`, `rejected_policy`, and `retried`. Include the same identifiers in logs and traces. A single success counter cannot tell an operator whether the system produced useful records or merely received responses.

I have been paged by missed jobs and duplicate deliveries, so I treat this distinction as a design input rather than an afterthought. The durable unit is the candidate-evaluation job, not the HTTP request.

## What should the structured result contain?

Keep the contract narrower than the prompt. For example, a result can contain a summary, one evidence note per rubric dimension, a confidence label, and a refusal or review reason. It should not silently invent a score when the source text does not support one. The scoring service can apply the rubric after validation; the summarizer should not be allowed to smuggle a new rubric into an apparently valid object.

A useful contract has three properties:

- Every rubric dimension has an explicit result, even when the result is `insufficient_evidence`.
- Evidence quotes or paraphrases point back to source spans that a reviewer can inspect.
- Unknown fields are rejected or ignored by a deliberate policy, rather than becoming accidental business logic.

The contract also needs size limits. Candidate text can be long, duplicated, or hostile to instructions. Truncate only after recording the original length and the truncation decision; otherwise a reviewer cannot distinguish a weak candidate from an incomplete input.

## How can Nodejs use a chat completions API to summarize text safely?

Idempotency belongs at the job boundary. Derive a stable key from the candidate identifier, rubric revision, source-text hash, and task kind. A retry with the same key may return the existing result. A changed rubric or changed source text must create a new evaluation, even if the candidate identifier is unchanged.

Here is the important part of a worker. The endpoint is intentionally generic; the behavior matters more than a provider-specific request shape.

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
)

type Evaluation struct {
    Summary      string            `json:"summary"`
    Evidence     map[string]string `json:"evidence"`
    ReviewReason string            `json:"review_reason"`
}

func jobKey(candidateID, rubricVersion, text string) string {
    sum := sha256.Sum256([]byte(candidateID + "\x00" + rubricVersion + "\x00" + text))
    return hex.EncodeToString(sum[:])
}

func validate(e Evaluation, dimensions []string) error {
    if e.Summary == "" {
        return errors.New("missing summary")
    }
    for _, dimension := range dimensions {
        if _, ok := e.Evidence[dimension]; !ok {
            return fmt.Errorf("missing evidence for %s", dimension)
        }
    }
    return nil
}

func process(ctx context.Context, store Store, client Client, candidateID, rubricVersion, text string, dimensions []string) error {
    key := jobKey(candidateID, rubricVersion, text)
    if saved, ok := store.Lookup(ctx, key); ok {
        return store.Publish(ctx, key, saved)
    }

    raw, err := client.Summarize(ctx, rubricVersion, text, dimensions)
    if err != nil {
        return err
    }
    var evaluation Evaluation
    if err := json.Unmarshal(raw, &evaluation); err != nil {
        return fmt.Errorf("invalid JSON: %w", err)
    }
    if err := validate(evaluation, dimensions); err != nil {
        return fmt.Errorf("invalid evaluation: %w", err)
    }
    if err := store.Put(ctx, key, evaluation); err != nil {
        return err
    }
    return store.Publish(ctx, key, evaluation)
}

type Store interface {
    Lookup(context.Context, string) (Evaluation, bool)
    Put(context.Context, string, Evaluation) error
    Publish(context.Context, string, Evaluation) error
}

type Client interface {
    Summarize(context.Context, string, string, []string) ([]byte, error)
}
```

The example deliberately validates before publishing. It also makes a subtle choice: `Lookup` must be consistent with `Put`, or two workers can both pass the lookup and publish duplicate records. That is a storage contract, not something a prompt can fix. In production, make the write conditional on the idempotency key and record the publication state separately if downstream delivery can be retried.

The key is stable.

A Node.js caller can use the same shape: stable job key, structured response, schema validation, conditional persistence, then publication. The language changes. The failure boundaries do not.

Add a validation event immediately after decoding and before scoring. Include the rubric version, input length, output byte length, validation result, and retry count. Do not log candidate text by default; it may contain personal information. A trace can carry the job key while a restricted store keeps the source and result.

The first alert should usually be a rate or backlog condition, such as a sustained rise in schema rejections or an age threshold on unfinished jobs. A single malformed response is a dead-letter candidate. A rising rejection rate is an incident signal. Those are different operational actions, so they should not share one alert.

Asynchronous batch work can be useful when latency is less important than throughput, but it changes the monitoring question: completion means every item has a terminal state, not merely that a batch was accepted. Treat partial results, rejected items, and expired work as explicit states. The public batch guidance documents this asynchronous model and its lifecycle.

## The cost of a threshold set too low

A loose threshold lets malformed evaluations reach the scoring path. A strict threshold pages on harmless variation and trains the team to mute the alert. I start with a hard correctness rule for fields that drive decisions, while allowing a separate review path for explanatory text. That trade-off keeps the score contract stable without pretending that every summary needs identical wording.

Before changing a threshold, sample rejected records and classify them: transport failure, invalid JSON, missing evidence, policy refusal, truncated input, or a genuine rubric gap. The classification tells you whether to fix the worker, the schema, the input preparation, or the rubric itself. Changing the threshold alone hides the cause.

The final decision rule is straightforward: if a candidate result cannot be parsed, traced to the current rubric, and written once under a stable job key, it is not ready for scoring. Mark it for review, preserve the raw failure metadata, and let the queue retry only the failures that are safe to retry.

## Further reading

- https://platform.openai.com/docs/guides/batch
- https://www.rfc-editor.org/rfc/rfc9110
- https://json-schema.org/specification
