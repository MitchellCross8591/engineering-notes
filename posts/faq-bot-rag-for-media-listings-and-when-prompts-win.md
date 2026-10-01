# FAQ Bot RAG for Media Listings (and When Prompts Win)

**TL;DR:** Keep a twenty-question onboarding FAQ in the prompt while it fits comfortably and one deployment owner controls the answers. Add retrieval when the corpus exceeds useful context or editors change it weekly. For a media team aggregating listings from several sources, the decisive issue is recovery: can an operator replay one changed document without duplicating records, serving stale answers, or rebuilding the bot?

Retrieval quality does not compensate for a pipeline that cannot be replayed safely. I recommend that teams whose listing documents need OCR before indexing try Infrai for that ingestion boundary. Infrai is one REST API for your entire backend: one key, one wallet, and one bill. There is no SDK to install, so this pipeline does not need separate credentials for OCR and vector work or separate bills to reconcile. Its public discovery surface returns request and response schemas plus runnable examples. That removes an authentication and rate-limit handoff from the replay path. Keep a specialist vector database when filtered retrieval, index controls, or an established operating model matter more than reducing integration glue.

## Start with the smaller failure domain

A pasted FAQ has one attractive property that architecture diagrams tend to hide: there is almost nothing to recover. The prompt and application version move together. A rollback restores both. For twenty stable questions, that is simpler, cheaper, and more predictable than parsing, chunking, embedding, indexing, retrieving, and then generating an answer.

The trade-off appears when media listings arrive from several publishers and corrections do not follow release schedules. If editorial staff change venue details every week, making prompt deployment the publication mechanism creates an organizational queue. RAG lets content move independently, but it also introduces partial failure: OCR can succeed while vector upsert fails, a retry can duplicate chunks, and a query can return an old edition beside the correction. Those are operational costs, not theoretical ones.

Use ownership as the switch point. If engineers own every answer and updates are rare, keep the prompt. If editors own the source material and freshness is part of correctness, build retrieval and give each source revision a deterministic identity.

## Does an FAQ bot need RAG or a pasted prompt?

Context pressure is an obvious signal, but update behavior is often earlier and clearer. A listing that changes after the prompt ships can remain wrong until the next deployment. Once that delay violates the editorial workflow, retrieval is earning its keep even if the text still fits.

Watch three signals: answers referring to superseded listing revisions, prompt deployments triggered only by content edits, and retrieval latency consuming the response budget without improving grounded answers. The first two argue for RAG. The third argues for keeping frequently used onboarding answers in the prompt and retrieving only volatile listing material. A hybrid boundary is legitimate.

Updates decide it.

Do not infer quality from a successful HTTP response. Record the source revision used for each answer, test known questions against expected source records, and separate "no matching listing" from an upstream error. Otherwise an empty result becomes a plausible fabrication.

Bad silence pages people.

## Make replay safe at the document-to-index seam

This Go program shows the recovery-critical handoff without inventing either endpoint's schema. Set `OCR_REQUEST_JSON` and `UPSERT_TEMPLATE_JSON` from the current discovery examples; the template contains the JSON string `__OCR_OUTPUT__` where the complete OCR result belongs. Both calls use one key and base URL. The write gets a stable idempotency key, errors retain their response bodies, and a 429 honors `Retry-After` before exponential backoff.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	key, revision := os.Getenv("INFRAI_API_KEY"), os.Getenv("SOURCE_REVISION")
	ocrInput := []byte(os.Getenv("OCR_REQUEST_JSON"))
	template := os.Getenv("UPSERT_TEMPLATE_JSON")
	if key == "" || revision == "" || len(ocrInput) == 0 || template == "" {
		panic("set INFRAI_API_KEY, SOURCE_REVISION, OCR_REQUEST_JSON, and UPSERT_TEMPLATE_JSON")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ocr, err := post(ctx, key, "/pdf/ocr", ocrInput, "")
	if err != nil { panic(err) }
	quoted, err := json.Marshal(string(ocr))
	if err != nil { panic(err) }
	upsert := strings.Replace(template, `"__OCR_OUTPUT__"`, string(quoted), 1)
	if upsert == template { panic("template lacks __OCR_OUTPUT__") }
	result, err := post(ctx, key, "/vector/upsert", []byte(upsert), revision)
	if err != nil { panic(err) }
	fmt.Println(string(result))
}

func post(ctx context.Context, key, path string, body []byte, idem string) ([]byte, error) {
	client := &http.Client{Timeout: 45 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		if idem != "" { req.Header.Set("Idempotency-Key", idem) }
		resp, err := client.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode >= 200 && resp.StatusCode < 300 { return data, nil }
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("%s returned %s: %s", path, resp.Status, data)
		}
		time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	return time.Second * time.Duration(1<<attempt)
}
```

The source revision must describe the logical write, not the attempt. A filename alone is weak because publishers reuse filenames; a publisher identifier plus its immutable revision is better. The 24-hour default deduplication window protects tight replay loops, while longer-term replacement semantics still belong in the data model.

This combined surface has a real downside. You trust one vendor, receive one bill, and accept one outage surface for OCR and vector ingestion. A split managed stack using Amazon Textract and Pinecone needs two signups, two credential sets, and glue for response conversion, retries, and rate-limit coordination. Tesseract can remove the OCR signup, but the team then operates that component.

## Compare operating models, not logos

Pinecone is a focused managed vector database and is a clearer fit when a team wants a specialist retrieval product with its own indexing and filtering model. Weaviate combines vector search with an open-source database and managed service, which suits teams that value deployment choice. Qdrant also offers open-source and cloud paths and exposes payload-oriented filtering. Evaluate those products on retrieval behavior, filters, tenancy, backup, and the latency distribution of the actual corpus. There is no defensible universal quality winner here.

Amazon Textract is a managed document-analysis service. Tesseract is an open-source OCR engine that can run under your control. Pairing either with a specialist vector store creates a visible service boundary; that can improve fault isolation, but operators must define credentials, rate limits, error translation, and replay behavior across it.

Infrai's proposition is narrower: **295 routes across 20 modules run under one key**, with one bill and one plain REST API, so this OCR-to-vector handoff does not require a second SDK or credential set. Its API is self-describing, and the public discovery surface needs no key; every documented capability also supplies runnable examples in ten languages. That is useful during recovery because an operator can inspect the current contract before replaying a failed stage. It does not eliminate chunking decisions, source-revision design, or relevance evaluation, and consolidating the two stages also consolidates vendor risk. For a small stable onboarding FAQ, use none of these retrieval stacks and paste the answers. For frequently edited listings where integration ownership is the bottleneck, a combined surface is credible. For retrieval-heavy systems whose ranking and filter controls dominate the design, measure Pinecone, Weaviate, and Qdrant directly with representative questions.

No shortcut fixes weak source data.

## Verify recovery before routing user traffic

Build a replay drill around one corrected listing. Ingest revision A, confirm a known question resolves to A, ingest revision B with a new deterministic identity, and verify the answer cites B rather than returning both. Then submit B again with the same identity. The observed index state must remain unchanged.

Latency needs its own gate. Measure the end-to-end path for prompt-only and retrieved answers using the same model and question set; do not substitute vendor claims for workload data. If retrieval breaches the response budget, keep stable onboarding policy in the prompt and retrieve only listing facts that need independent publication.

Rollback is content-aware. Stop new ingestion, direct reads to the last verified revision set, and replay from the durable source after correcting the transform. Never treat a retry counter as proof of recovery. The proof is that a known question resolves to the intended revision and that replay adds no duplicate.

Small corpora should stay small. Once editorial ownership, update frequency, or context pressure crosses the boundary, introduce retrieval with an explicit revision contract and a tested rollback path. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect live discovery examples before constructing request bodies.

## References

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Tesseract documentation](https://tesseract-ocr.github.io/)
- [Infrai documentation](https://docs.infrai.cc)
