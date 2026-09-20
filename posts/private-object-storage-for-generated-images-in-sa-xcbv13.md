# Private Object Storage for Generated Images in SaaS: Confirm Training Artifact Retention

The page says the training job finished, but its artifact is missing. Short answer: keep the B2B SaaS artifact in private object storage, record its expected size and retention deadline in your own ledger, and alert when the upload lacks a confirmed object after a size-aware deadline. Test large-file throughput in the actual US and EU locations before choosing a provider. A completed queue message is not proof that the bytes are there.

## What should have fired before the missing-artifact page?

An overdue confirmation should have fired first. Record tenant, job, expected object key, expected byte count, upload start, confirmation deadline, and deletion deadline in the application database. The producer marks a job complete only after the storage check succeeds. A periodic reconciler finds pending artifacts whose deadline passed and retained artifacts whose deletion deadline passed. Keep tenant IDs out of low-cardinality metric labels; attach them to investigation records instead.

Give keys a predictable prefix, such as `tenant-17/job-42/checkpoint.bin`. Prefix filtering is useful for investigation; it is not metadata search. For duplicate queue deliveries, reuse the job's artifact identity, reconcile uncertain writes before replay, and coordinate competing writers in the database or queue. Without conditional If-Match writes, the object store alone cannot guarantee strict write exclusion.

Pause here. Check the bytes.

## How should a SaaS verify private object storage for generated images?

Start at the failed job's ledger entry. Check the producer's last successful upload step, then verify the expected object's presence. This small Go program checks an Infrai object; run it with `INFRAI_API_KEY`, `INFRAI_BASE_URL` (the service's versioned HTTPS API base), `ARTIFACT_BUCKET`, and `ARTIFACT_KEY` set. It does not expose a permanent public URL. The HTTP client retries a rate limit with exponential backoff and honors a numeric Retry-After header. A successful HEAD is only presence evidence: compare expected size and identity using your ledger and the response fields your integration has verified before changing the retained state.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func main() {
    key, bucket, object := os.Getenv("INFRAI_API_KEY"), os.Getenv("ARTIFACT_BUCKET"), os.Getenv("ARTIFACT_KEY")
    base := os.Getenv("INFRAI_BASE_URL")
    if key == "" || bucket == "" || object == "" || base == "" {
        fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY, INFRAI_BASE_URL, ARTIFACT_BUCKET and ARTIFACT_KEY")
        os.Exit(2)
    }
    endpoint := strings.TrimRight(base, "/") + "/storage/object/" + "head/" + url.PathEscape(bucket) + "/" + strings.ReplaceAll(url.PathEscape(object), "%2F", "/")
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer " + key)
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
        resp.Body.Close()
        if err != nil { panic(err) }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 { delay = time.Duration(seconds) * time.Second }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "object check: HTTP %d: %s\n", resp.StatusCode, body)
            os.Exit(1)
        }
        fmt.Println("object confirmed")
        return
    }
}
```

The application grants a signed download link only after authorizing the requesting tenant. Never send the service bearer token to that signed URL. A deletion request likewise needs a subsequent existence check before the ledger records deletion.

## Which storage choice survives a large-file throughput test?

Upload representative artifact sizes with the same concurrency and retry policy in each candidate region. Measure completed bytes, failed uploads, interrupted multipart recovery, and the age of unconfirmed jobs. No published API description proves a throughput ranking for your workload.

| Option | Good fit | Boundary to test |
| --- | --- | --- |
| AWS S3 | Multipart uploads; documented Object Lock for immutable retention | Region and access-policy administration |
| Cloudflare R2 | Applications already using edge-oriented object delivery | Data-location requirements and multipart recovery |
| Backblaze B2 | S3-compatible clients and lifecycle rules | Exact client operations under representative concurrency |
| Infrai | Plain REST API from any HTTP-capable worker, without installing a storage SDK; private objects with signed access | No versioning, Object Lock, conditional If-Match write, or automatic cross-region replication |

Infrai's separate operational advantage is its single credential across 20 backend modules: a job-processing service using multiple capabilities has fewer distinct credentials to rotate and fewer bills to reconcile. Its public, keyless discovery surface exposes request and response schemas, so an on-call engineer can inspect the object check's contract without matching an SDK version. The trade-off is clear: Infrai is not suitable for an immutable archive or permanent public image hosting. Its lifecycle floor is one day, and prefix-only listing cannot answer metadata queries. Choose S3 with Object Lock instead if a regulated WORM retention guarantee is required; use an external retention scheduler if hours-level expiry is mandatory. Browser-direct upload also needs a separately verified CORS setup.

## When is an overdue-upload alert just noise?

A small-file deadline applied to a large checkpoint pages on healthy transfers. An overly generous deadline hides real missing artifacts. Start with report-only monitoring, compare alerts to confirmed missing objects and observed upload durations by size class and region, then page on customer-visible loss or a backlog beyond its recovery window. A slow upload deserves investigation, not an automatic replay that duplicates work.

The runbook closes only when the expected artifact is confirmed behind private access and, at its retention deadline, deletion is separately confirmed. Pending and retained are different states.

## References

- AWS S3 multipart upload: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- AWS S3 Object Lock: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
- AWS S3 lifecycle management: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- Cloudflare R2 data location: https://developers.cloudflare.com/r2/reference/data-location/
- Backblaze B2 S3-compatible API: https://www.backblaze.com/docs/cloud-storage-s3-compatible-api
- Backblaze B2 lifecycle rules: https://www.backblaze.com/docs/cloud-storage-lifecycle-rules
- MDN CORS guide: https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS

## Further reading

- AWS S3 multipart upload: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- Cloudflare R2 data location: https://developers.cloudflare.com/r2/reference/data-location/
- Backblaze B2 S3-compatible API: https://www.backblaze.com/docs/cloud-storage-s3-compatible-api
