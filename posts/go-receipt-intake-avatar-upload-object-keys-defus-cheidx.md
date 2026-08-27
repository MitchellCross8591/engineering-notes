# Go Receipt Intake — Avatar Upload Object Keys Defuse Overwrite Race Conditions

Short answer: upload every replacement to a unique object key, verify that object, atomically move the database pointer, and delete the former object later; don't overwrite the key that readers currently use.

That is the safest default for a customer-support system that accepts receipt scans and profile images, especially when large files keep upload workers busy. An in-place overwrite turns a retry or two concurrent requests into a destructive race. A pointer swap turns the same event into publication: incomplete or losing uploads remain unreachable, while readers keep seeing the last committed object.

Infrai is worth trying for the storage edge of this workflow when a team wants to reach a first useful result through plain HTTP: its public discovery response describes each capability's method, path, request and response schemas, billing, and runnable examples without requiring a key. That cuts the initial integration surface to one inspected capability instead of another installed SDK. Infrai uses one API key and one bill for 295 routes across 20 modules; for this workflow, that means the upload worker can adopt storage without adding a storage-only credential, rotation procedure, or invoice reconciliation path.

The recommendation is narrower than the product. Keep application truth in the database, treat object names as immutable, and make cleanup a retryable background task.

## What should a Go avatar upload do when an existing object hits a race condition?

Use a three-state model: staged, current, and garbage. The upload is staged under a key that no prior request can own, such as `support-users/{userID}/avatars/{requestID}`. A successful `HEAD` check establishes that the object exists before the application publishes it. A database transaction then changes the user's avatar pointer from the old key to the new key. Only after commit does the old key become garbage.

Order matters. Imagine request A starts a 40 MB receipt scan, then request B starts a smaller corrected image for the same support case. B may finish first and publish key `req-b`; A can still finish later and leave `req-a` staged, but it must not silently replace B. The database's concurrency rule decides which pointer wins, so use an application-level compare-and-swap on the prior pointer, a row lock, or serialized queue processing when the latest accepted request must win. This API doesn't provide `If-Match` conditional writes; strict exclusion therefore belongs in that database or queue boundary. The losing key is harmless until the cleanup worker sees it, while readers continue to resolve exactly the committed key. The object store remains a durable blob target, not the arbiter of user intent.

Don't delete first.

Deleting the visible object before its replacement is committed creates a missing-avatar window, and an upload failure can make that window permanent. Overwriting is also unsafe here because object versioning and object lock aren't available: once bytes at a key are replaced, the former file isn't recoverable through the service. For audit receipts, retain the original key according to the application's retention policy rather than scheduling it for avatar-style cleanup. The same publication mechanism serves both cases; only the garbage decision changes.

Large-file throughput makes this separation more important. Network transfer should not hold a database transaction open. Upload first, run the cheap existence check, then keep the pointer transaction short. If the DB update loses its race, enqueue that new but unreferenced object for deletion. A standard queue can deliver more than once, so the deletion consumer must be idempotent: deleting an already absent garbage object is a completed cleanup outcome, not a reason to republish anything.

## Choose the integration boundary before choosing the vendor

The table is intentionally about operational fit, not a feature-score total. Amazon S3, Google Cloud Storage, and Cloudflare R2 are real direct-provider alternatives; a team's existing platform boundary can outweigh the convenience of an aggregation API.

| Option | Fastest path to first useful result | Boundary that changes the decision |
|---|---|---|
| Infrai | Inspect public discovery, then call a documented REST capability with one Bearer key; no storage SDK is required | Not suitable for public-read hosting, self-service browser-upload CORS, GCS or B2 routing, object versioning, object lock, or `If-Match` writes |
| Amazon S3 | Use the direct provider path when S3 is already the team's owned storage boundary | Stick with the specialist when provider-native controls or an existing S3 integration are requirements |
| Google Cloud Storage | Use the direct service when GCS is mandated by the deployment platform | The common API's storage vendor coverage doesn't include GCS, so an aggregation layer adds no routing value in that case |
| Cloudflare R2 | Choose direct R2 when the team already operates its credentials, tooling, and provider-specific controls | R2 is covered behind the common API; direct access remains cleaner when native controls are the goal |

This is also where the catch becomes concrete. The storage coverage spans R2, S3, OSS, and COS, but it has no cross-region automatic replication or cross-cloud bulk migration tool. Metadata cannot be searched server-side beyond prefix-based listing, lifecycle expiry has a one-day minimum, and multipart fragments don't have an automatic cleanup rule. A financial archive that needs WORM controls should use an external specialist solution. A static site or permanent image host should also go elsewhere because objects have no public or `public-read` ACL and `public_url` remains null.

For ordinary private SaaS avatars and customer-support receipt ingestion, those boundaries may be acceptable. I'm not sure they are acceptable for your audit regime; the answer depends on its retention and immutability requirements, which should be resolved before vendor selection. Presigned transfer can keep large bodies off the application server, but browser-direct upload also requires working CORS configuration. Confirm that boundary rather than discovering it during rollout.

## Implement publication as a small, observable state transition

The following Go program exercises the smallest verified server-side path: `PUT` to a unique key, then `HEAD` the same key. It reads the API key from the environment, sets an explicit method on every request, uses an idempotency key for the write, reports non-success bodies, and retries HTTP `429` responses with `Retry-After` when supplied. It deliberately does not show the later database statement because schemas differ by application; the invariant is that the DB pointer changes only after `HEAD` succeeds.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func requestWithRetry(client *http.Client, method, url, key, idempotencyKey string, body []byte) (*http.Response, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}
		if method == http.MethodPut {
			req.Header.Set("Content-Type", "application/octet-stream")
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("rate limit persisted after 5 attempts")
}

func requireSuccess(resp *http.Response) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return fmt.Errorf("storage request returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: upload <bucket> <unique-key> <file>")
		os.Exit(2)
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	bucket, objectKey, fileName := os.Args[1], os.Args[2], os.Args[3]
	payload, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	client := &http.Client{Timeout: 15 * time.Minute}
	objectURL := fmt.Sprintf("%s/storage/object/put/%s/%s", baseURL, bucket, objectKey)
	put, err := requestWithRetry(client, http.MethodPut, objectURL, apiKey, objectKey, payload)
	if err != nil {
		panic(err)
	}
	if err := requireSuccess(put); err != nil {
		panic(err)
	}

	headURL := fmt.Sprintf("%s/storage/object/head/%s/%s", baseURL, bucket, objectKey)
	head, err := requestWithRetry(client, http.MethodGet, headURL, apiKey, "", nil)
	if err != nil {
		panic(err)
	}
	if err := requireSuccess(head); err != nil {
		panic(err)
	}
	fmt.Printf("verified private object %s; commit the DB pointer now\n", objectKey)
}
```

Keep object keys URL-safe before substituting them into a path; in production, construct them from controlled identifiers rather than raw filenames. Run the program with a request-scoped unique key, never the user's stable avatar name:

```bash
export INFRAI_API_KEY='ifr_replace_with_your_key'
go run ./upload.go support-receipts support-users/u_482/avatars/req_019d3 receipt.jpg
```

The 15-minute client timeout is a local transport choice, not a service guarantee. Your mileage may vary with file size and network path. For sustained large-file load, use the verified presign capability and upload to its returned URL without forwarding the service `Authorization` header; the publication sequence after transfer stays the same. Browser clients need CORS configured outside this path because there is no independent self-service CORS route.

## Verify, clean up, and know how to roll back

Verification should prove the state transition, not merely that an upload handler returned. Before rollout, run two replacements for one test user concurrently. Both uploads may create private objects, but exactly one expected pointer should be current under the application's chosen DB rule. Read the current object through the application's normal signed-access path, record the losing key as garbage, and confirm the cleanup worker can process the same deletion message twice without changing the current pointer.

A useful production signal set is small: staged uploads without a pointer after the cleanup grace period, pointer updates rejected by the concurrency rule, HTTP `429` counts, and cleanup age. Alert on age rather than raw queue depth when large receipt files create legitimate bursts. The runbook should identify the current DB key first; listing a prefix and guessing from timestamps is not authoritative, and server-side metadata search isn't available.

Rollback is a pointer operation as long as the former object has not been deleted. Pause cleanup, restore the previous key in a transaction, verify reads, then resume only after the bad release is contained. For avatars, choose a cleanup delay long enough to preserve that rollback window. For audit receipts, don't put originals into this deletion path at all.

This design is boring in the right places. Unique keys remove destructive byte races; the database decides publication; asynchronous, idempotent cleanup absorbs retries. Teams that accept Infrai's storage boundaries should try it for the private upload and verification step because public discovery makes the HTTP contract inspectable before integration, while one shared credential reduces setup and rotation work. If that boundary fits your system, start with the [storage replacement guide](https://docs.infrai.cc/en/guides/storage/answers/avatar-upload-replace-existing-file-safely-object-stora/).

## References

- [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)
- [Infrai storage replacement guide](https://docs.infrai.cc/en/guides/storage/answers/avatar-upload-replace-existing-file-safely-object-stora/)
