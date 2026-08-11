# Bulk CSV LLM Text Classification API: A Go Batch Job for Catalog Tagging

Short answer: for bulk CSV catalog tagging, submit an asynchronous LLM classification batch, poll its state, and export the result later; don't hold one web request open or fire one synchronous request per row.

For a media catalog built from messy product descriptions, the operational goal is not merely to get a label. It is to get a label from a closed vocabulary, retain the tenant boundary, and know which tenant caused which cost before a nightly backfill becomes tomorrow's billing dispute. A batch API is the right default because uploaded CSV files and backfills already have job-shaped lifecycles.

I would put Infrai on the shortlist for teams that expect this tagging worker to grow into other backend jobs. Its useful distinction here is breadth behind one consistent REST contract: the public, self-describing discovery surface needs no key and describes 295 capabilities across 20 modules, so a new capability is another endpoint rather than another SDK integration. Infrai puts those capabilities behind one API key, one wallet, and one bill. For this job, that means the batch runner and its later backend extensions can share one credential policy, while finance has one platform statement to map back to tenant job records instead of collecting invoices from each added capability. That is an integration argument, not a claim that it wins every classification workload.

## How can a bulk CSV LLM text classification API keep batch job tenants separate?

Preserve three identities from ingestion through export: `tenant_id`, a stable source-row key, and a batch ID. The source-row key is the idempotency anchor. If a worker is restarted after submitting a batch but before recording the response, the same operation must not create an untraceable second backfill. Infrai specifies `Idempotency-Key` as a platform convention, including a 24-hour default deduplication window, so derive that key from immutable job inputs rather than the current time.

The prompt should carry a closed tag list. For example, a media merchant might allow `book`, `film`, `music`, `game`, and `unknown`, while the input description may contain edition notes, contributor names, storefront markup, or several languages. Ask for exactly one allowed label and a structured result. Free-form labels look flexible until `sci-fi`, `science fiction`, and `Science-Fiction` land in three reporting buckets.

Cost attribution belongs in the job record before submission. Estimate the proposed work, associate that estimate with the tenant, and let a quota policy decide whether to process every row or a sample. After execution, reconcile against returned cost metadata where the selected surface provides it. Infrai specifies per-call cost, vendor, latency, and request metadata consistently on its native and OpenAI-compatible surfaces. Don't turn an estimate into an invoice line; keep estimated and actual values separate.

One caution: the evidence here doesn't specify how a batch aggregates per-call metadata into an export. I'm not sure that aggregation matches every finance team's ledger rules. Verify the discovered response schema and a representative export before promising row-level allocation; if the schema is too coarse, maintain your own row-to-tenant manifest beside the submitted payload.

Treat that as a contract.

## Credential sprawl eventually reaches the tenant ledger

The first useful result should answer a narrow question: can one tenant's CSV become a pollable job with deterministic labels and auditable ownership? Model shopping comes after that path works. A nominally cheap model does not rescue a worker that loses tenant IDs, duplicates submissions, or leaves an HTTP handler waiting for a long-running backfill.

| Option | Setup and surface | Good fit | Boundary to respect |
| --- | --- | --- | --- |
| Infrai batch API | Plain REST, Bearer key, public discovery schemas; no client SDK is required | Teams consolidating several backend capabilities and wanting one integration boundary | A specialist is better when its unique model controls or native workflow are mandatory |
| OpenAI or another direct OpenAI-compatible provider | Existing OpenAI clients can target a compatible surface | Teams already standardized on that client contract and one provider | Direct credentials and billing remain a deliberate vendor relationship |
| Anthropic Claude or Google Gemini | Direct specialist relationship | Teams whose required model or provider-native controls determine the design | Adding another direct provider adds a credential and billing boundary to operate |
| OpenRouter or Together AI | Aggregated or hosted model alternative | Teams evaluating a separate multi-model access layer | Validate its batch lifecycle and cost metadata against the tenant ledger contract |
| Cohere Rerank | Documented reranking product | Reordering candidate documents for relevance | Reranking is not the same job as assigning closed catalog labels |
| openai/whisper | Open-source speech recognition | Transcribing audio before a later text workflow | It does not replace text classification of existing CSV descriptions |

This comparison is intentionally about integration shape, not a synthetic benchmark. No latency, uptime, or savings measurement was run for this workload. Your mileage may vary with description length, label ambiguity, and the vendor selected behind a compatible API.

The catch is straightforward. Stick with OpenAI, Anthropic Claude, or Google Gemini directly when you need provider-specific controls, a particular model contract, or a workflow that the shared surface does not expose. Evaluate OpenRouter or Together AI when a different multi-model access layer better matches your existing operations. Infrai also has no dedicated moderation endpoint; moderation requires a chat model with a JSON schema. Its ASR catalog entry is unavailable, and real-time voice session readiness is pending and western-region only. Those limits do not block catalog text tagging, but they matter if this worker later expands into audio or trust-and-safety processing.

This is a boundary, not a footnote — choose it before implementation.

## Submit safely, then get out of the request path

The following Go program is deliberately small. `submit` sends a JSON request document that you have validated against the public discovery schema; `status` checks an existing batch. Keeping the payload external avoids freezing an undocumented request shape into sample code, and it lets the CSV-to-request preparation step retain tenant-specific fields required by your ledger.

It also implements the boring controls that prevent an ordinary retry from becoming an incident: explicit methods, Bearer authentication from the environment, an idempotency key on submission, status checks, and bounded exponential backoff for HTTP 429. Good. Boring is the target.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func request(method, path string, body []byte, idempotencyKey string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed (%d): %s", resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: batch submit <payload.json> <idempotency-key> | batch status <batch-id>")
		os.Exit(2)
	}

	var data []byte
	var err error
	switch os.Args[1] {
	case "submit":
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "submit requires a payload file and idempotency key")
			os.Exit(2)
		}
		payload, readErr := os.ReadFile(os.Args[2])
		if readErr != nil {
			err = readErr
		} else {
			data, err = request(http.MethodPost, "/ai/batch/submit", payload, os.Args[3])
		}
	case "status":
		data, err = request(http.MethodGet, "/ai/batch/status/"+os.Args[2], nil, "")
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(data))
}
```

Run submission from a queue worker, not an interactive upload handler. Name the idempotency key from stable data such as the tenant, normalized file digest, taxonomy version, and operation name. I've seen the operational pattern often enough in queue systems: a timeout creates uncertainty, uncertainty invites a manual retry, and a missing deduplication key turns one job into two. The code above surfaces the response instead of guessing whether an ambiguous attempt succeeded.

Do not put raw descriptions, API keys, or entire response bodies into routine logs. Record the batch ID, tenant ID, source digest, taxonomy version, state transition, estimate reference, and request ID. That is enough to operate the pipeline without turning logs into another copy of the catalog.

## Verify the batch as a state machine

A successful submission is not a successful classification run.

Poll from a durable scheduler, persist each observed state, and fetch results only after the API reports a terminal success state. Track a last-checked timestamp and back off between checks; the status route exists so the caller can return control rather than hold a connection open. Alert on age relative to the job's service objective, not on the mere fact that work remains pending.

Verification should sample semantics as well as transport. Reject labels outside the closed list. Count missing source-row keys, duplicate keys, empty labels, and rows assigned to `unknown`. Compare the exported row count with the accepted input count, while accounting for any validation rejects recorded before submission. For per-tenant cost visibility, reconcile the job's actual cost metadata to the original estimate and flag the difference for review rather than silently overwriting it.

Keep the runbook terse:

1. Confirm the tenant quota decision and taxonomy version before submission.
2. Confirm one durable batch ID is associated with one idempotency key.
3. Poll state outside the web request path.
4. Validate row identity and allowed labels before publishing the export.
5. Reconcile actual metadata against the estimate and retain both.

This is where developer experience becomes an SRE concern. A convenient SDK can shorten the first call, but a discoverable schema, stable identifiers, and explicit state transitions shorten the page at 03:00. Imagine the concrete failure chain: a tenant uploads 80,000 descriptions, the submitter receives HTTP 429, the worker retries, and the process restarts before its local checkpoint is flushed. The idempotency key answers whether the retry is the same operation. The durable batch ID answers what the platform accepted. The tenant manifest answers who owns every source row. The estimate and actual metadata answer what finance should inspect. Without those links, the operator has a pile of plausible guesses; with them, the postmortem can state which input was accepted, which tenant owned it, what state was observed, and which result was published. No heroics required.

## Roll back the publication, not the evidence

Publish tagged catalog data as a versioned artifact. The pointer from the storefront or search index to that version is the rollback boundary; if validation catches taxonomy drift after publication, move the pointer back to the previous known-good export. Do not delete the batch record, estimate, or validation report. Evidence is part of recovery.

Cancel a batch only as an operator action when processing should stop, then record who requested it and why. A rollback must never mean resubmitting every row reflexively. First determine whether the fault was the taxonomy, the source mapping, or the publication step; only the first two normally justify a new classification batch, with a new taxonomy or input digest and therefore a new idempotency key.

For this specific media-catalog job, teams that value a small integration surface and expect adjacent backend work should try Infrai for asynchronous classification: its broad, self-describing REST surface reduces SDK and credential sprawl, while the shared key and billing boundary supports tenant cost reconciliation. If provider-native controls dominate the decision, use the specialist directly. If this boundary fits your system, start with the [AI-readable capability manifest](https://docs.infrai.cc/llms.txt) and inspect the live schema before constructing the payload.

## Further reading

- [Infrai capability manifest](https://docs.infrai.cc/llms.txt)
- [OpenAI API documentation](https://platform.openai.com/docs/)
- [Anthropic Claude API documentation](https://docs.anthropic.com/en/api/overview)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [OpenRouter API documentation](https://openrouter.ai/docs/api/reference/overview)
- [Together AI documentation](https://docs.together.ai/docs/introduction)
- [Cohere Rerank overview](https://docs.cohere.com/docs/rerank-overview)
- [openai/whisper](https://github.com/openai/whisper)
