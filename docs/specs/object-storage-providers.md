# LoonFS object-storage provider reference

This reference summarizes the provider limits and performance characteristics behind LoonFS's design, along with the bucket configuration and adapter requirements every deployment MUST meet. It does not replace running the provider conformance tests.

The key word MUST in this document is to be interpreted as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119), [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, it appears in capitals.

## 1. Frequently used data points

- **Same-key write ceiling:** Do not assume any CAS object can be updated faster than once per second. GCS limits writes to 1/s per object name, and R2 limits concurrent writes to 1/s per key.[^1][^2] LoonFS raises a namespace's manifest boundary only during a namespace collection (right before deleting manifests), while manifest publications read the boundary without writing it ([One complete collection](format.md#111-one-complete-collection)). Other CAS writes (such as extending or deleting a user snapshot) happen on demand and return provider throttling errors to that caller.
- **ETags aren't hashes:** Never assume an `ETag` is a portable MD5 or content hash, especially across multipart uploads.
- **Typical latency:** Assume roughly 30 ms for a HEAD request and 200 ms for a small full GET. Prefer HEAD or conditional GET (`If-None-Match`) when checking control objects.

## 2. Provider matrix

| Feature | AWS S3 | Google Cloud Storage | Cloudflare R2 | Azure Blob Storage | S3-Compatible |
| --- | --- | --- | --- | --- | --- |
| CAS / conditional write | Yes | Yes | Yes | Yes | Unknown until tested |
| Same-key CAS/write ceiling | No documented 1/s cap | 1 write/s and 1 metadata update/s per object name[^12] | 1 concurrent write/s per key (excess can 429)[^23] | No documented 1/s cap[^35][^36] | Vendor-specific |
| Multipart / large upload | Up to 10,000 parts[^4] | Resumable / XML multipart[^13][^14] | Single <= 5 GiB, multipart <= 5 TiB (10,000 parts of 5 MiB to 5 GiB)[^24][^25] | Put Block / Put Block List: up to 50,000 blocks x 4,000 MiB (~190.7 TiB)[^37][^38][^39] | S3-like (limits vary) |
| Parallel / ranged download | `Range` and `partNumber`[^5] | Sliced parallel ranged GETs[^15] | `Range` and `PartNumber`[^26][^27] | Ranged `Get Blob`[^40] | Unknown until tested |
| ETag and full-object checksums | Opaque ETag, stored CRC64NVME, CRC32/C, SHA-1/256, MD5 for single and multipart[^6][^7] | Every object has CRC32C, no MD5 on composite/XML multipart, XML vs JSON ETags differ[^16][^17] | Full-object CRC64NVME, composite CRC32/C, SHA-1/256, S3 multipart ETag[^28][^29][^30] | Conditional-only ETag, optional full-blob MD5, range MD5/CRC64 <= 4 MiB[^41][^42] | Check vendor docs |
| Availability SLA / target | 99.9% Standard (99.0% some IA)[^8] | 99.95% multi/dual-region, 99.9% regional Standard[^18] | 99.9% SLA, 11 nines annual durability[^31] | >= 99.9% hot tiers (>= 99.99% RA-GRS/GZRS reads), lower on cool/cold/archive[^43] | Vendor-specific |
| Request scale and throughput | >= 3,500 write/s and 5,500 GET/HEAD/s per prefix (gradual ramp, 503 Slow Down), up to 100 Gb/s per EC2[^9] | Starts ~1,000 write/s and 5,000 read/s, auto-scales (ramp <= 2x / 20 min), ~200 Gbps regional egress[^19][^20] | Avoid `r2.dev` and REST management API for production. Use custom domains or S3/Workers APIs[^32] | 40,000 req/s (20,000 some regions), 60/25 Gbps ingress, 200/50 Gbps egress, backoff on 503/500[^44][^45] | Load-test first |
| Published GET/HEAD latency | ~100-200 ms first-byte rough figure[^10] | Not published (benchmark) | Not published (benchmark) | Not published (benchmark) | Benchmark |
| Content checksum | CRC-64/NVME | CRC-32C | CRC-64/NVME | CRC-64/NVME | CRC-64/NVME |
| Stored checksum readback | Verified & stored CRC-64/NVME, HEAD with checksum mode | Verified & stored CRC-32C, HEAD reads `x-goog-hash` | Same as AWS S3 | Verified per block via `x-ms-content-crc64`, full CRC-64/NVME stored in blob metadata (`crc64nvme`), read via HEAD | CRC-64/NVME required |
| Conditional create completion | `If-None-Match: *` on PUT (< 8 MiB) and multipart completion (>= 8 MiB)[^48] | JSON media (< 8 MiB) or resumable (>= 8 MiB) with `ifGenerationMatch=0`[^14][^17][^49] | Same as AWS S3 (*live* verified) | Put Blob (< 8 MiB) or Put Block List (>= 8 MiB) with `If-None-Match: *`[^53] | Required (unknown until tested) |
| Conditional control-object read | GET with `If-None-Match` ETag (304 if unchanged) | Full GET (~150 B boundary), compares generation locally | Same as AWS S3 | Same as AWS S3 | Unknown until tested |
| Assembly (>= 8 MiB) | `UploadPartCopy` (>= 5 MiB parts, <= 5 GiB), combines part CRCs before completion[^48][^50] | `objects.compose` (<= 32 sources/step via temp objects), `ifGenerationMatch=0` (*live* verified)[^51] | 64 MiB chunks, copies single-source chunks, re-uploads spanning chunks, verifies full CRC via HEAD (*live* verified) | `Put Block From URL` (>= 5 MiB ranges via SAS, <= 4,000 MiB blocks), combines block CRCs before `Put Block List`[^52][^53] | Unknown until tested |

Items marked *live* are verified by the ignored conformance tests in `crates/loonfs-objectstore/tests/it/objectstore_conformance.rs`.

### Conditional create, assembly, and retries

- **Conditional create at every size.** Every provider MUST enforce create-only completion at every object size, including multipart completions. Otherwise two concurrent writes to one service-proxied upload session could overwrite already-verified bytes ([Service-proxied staging](format.md#53-service-proxied-staging)). On S3 and R2, a direct multipart completion refused because the key already exists (such as a retry after a lost response) is verified against the stored checksum. On R2, `cloudflare_r2_direct_completion_is_create_only` tests this live. The store contract probe (`POST /v0/maintenance/store/probe`) checks this via `multipart_round_trip`.
- **Assembly below 8 MiB.** All adapters read source ranges in chunks of at most 8 MiB, verify their checksums, and publish with one create-only PUT (or JSON media upload on GCS, or Put Blob on Azure).
- **Assembly at or above 8 MiB:**
  - *AWS S3:* Starts a multipart upload with full-object CRC-64/NVME, copies source ranges of at least 5 MiB (up to 5 GiB per part), reads small runs and tail bytes in chunks up to 8 MiB (padding short non-final parts from the next source), combines part CRCs, and rejects mismatches before `CompleteMultipartUpload` with `If-None-Match: *`.[^48][^50]
  - *GCS:* Composes whole sources directly, stages ranged sources and tail bytes as temporary objects, chains `objects.compose` in batches of at most 32 sources onto the destination with `ifGenerationMatch=0`, checks source CRCs beforehand and returned CRC-32C afterward, and deletes every temporary object (*live* verified up to 40 whole sources).[^51]
  - *Cloudflare R2:* Cuts the output into 64 MiB chunks (except the last), copies chunks inside a single source without `x-amz-copy-source-if-match`, and reads/uploads chunks spanning sources. Because R2 omits checksums on copied parts, LoonFS completes with `If-None-Match: *` and verifies the stored full-object CRC-64/NVME with HEAD. On mismatch or missing checksum, assembly fails and leaves the object for GC (writers never delete immutable keys).
  - *Azure Blob Storage:* Copies ranges of at least 5 MiB (up to 4,000 MiB per block) via `Put Block From URL` using short-lived SAS URLs, uploads small runs and tail bytes with `x-ms-content-crc64` (padding short non-final blocks), combines the service-reported block CRCs (since `Put Block From URL` has no source precondition), and rejects mismatches before `Put Block List` with `If-None-Match: *`.[^52][^53]
- **Conditional control-object reads.** S3, R2, and Azure use GET with `If-None-Match` (receiving 304 on match). GCS uses object generation as its version token because GCS conditional writes compare generations and the client library cannot send generation-conditional GETs, so LoonFS reads the small object (around 150 bytes for a boundary) and compares generations locally.
- **Listing timestamps.** For unchanged objects, LIST and HEAD timestamps differ by less than 1 second (R2 and GCS listings keep fractional seconds while HEAD reports whole seconds). GC uses LIST timestamps without per-object HEAD requests, covered by `GC_SAFETY_MARGIN_MS` (300,000 ms).
- **Retry budget.** Each request gets one retry budget: up to 10 attempts within the 120-second `PROVIDER_OPERATION_DEADLINE_MS` (covering provider-client retries, LoonFS-signed requests, verified immutable writes, and timed-out deletes). Exhausting the budget returns a non-retryable error. Conditional publications always make a single attempt.

### Ambient credentials

- **AWS S3 (`kind = "ambient"`).** Uses the standard AWS SDK credential chain (environment variables, shared config/credentials files, credential processes, SSO, web identity, ECS task credentials, and EC2 instance metadata, honoring `AWS_PROFILE`). Both the client and presigned URL signer share one in-memory cache per ambient source (no background task or persistence):
  - Credentials are reused until 60 seconds before expiry, with concurrent callers sharing one on-demand refresh. Cancelling a refresh releases the lock and keeps the previous credentials.
  - After a failed refresh or near-expiry response, still-valid credentials can be reused for at most 1 second before retrying (never past expiry). Cold failures and expired credentials are never cached.
  - If an ambient source returns credentials with no expiration, later lookups bypass the cache and lock for the rest of the source's lifetime (still checking expiration on each result). Explicit static credentials are used verbatim.
  - Presigned URLs include the active session token and can trigger an early refresh (subject to the 1-second backoff). If credentials still expire before the requested URL lifetime, LoonFS issues the URL and sets `expires_at_ms` to the earlier of the two expirations (AWS enforces credential expiry on its side). Already-expired credentials are rejected. Static session tokens have no configured expiry, so callers must keep them valid.
- **Google Cloud Storage (`kind = "ambient"`).** Resolves credentials from `GOOGLE_APPLICATION_CREDENTIALS` (service-account key or authorized-user file), gcloud application default credentials, or the VM/pod metadata server (workload identity). Workload identity federation files (`external_account`) aren't supported and fail startup. Because LoonFS reads no private key from ambient sources, it signs direct-transfer URLs via the IAM Credentials API's `signBlob` (1 `signBlob` call per signed URL, requiring `roles/iam.serviceAccountTokenCreator` on the service account itself). Authorized-user credentials can't call `signBlob` this way, so stores using them disable direct transfers and proxy all traffic. On GKE, Kubernetes ServiceAccounts linked with `iam.gke.io/gcp-service-account` also need `iam.gke.io/return-principal-id-as-email: "true"` (without it, the metadata server reports a `.svc.id.goog` identity that `signBlob` rejects). To sign locally with a key file instead, configure `kind = "service_account_file"`.
- **Cloudflare R2.** Doesn't use the AWS chain. Reads `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` at startup.

## 3. Proven providers and direct-put

Direct PUT is enabled only when a provider can both enforce create-only uploads (preventing overwrites of existing content objects) and return a durable stored checksum for completion verification. Because many S3-compatible gateways silently ignore conditional write headers, direct transfers are restricted to endpoints covered by live provider tests, with no configuration override (other endpoints use service-proxied transfers):

- AWS S3: default endpoint or HTTPS under `amazonaws.com` or `amazonaws.com.cn` (CRC-64/NVME, `upload.direct_put.max_content_bytes` = 5 GiB).
- Cloudflare R2: HTTPS under `r2.cloudflarestorage.com` (CRC-64/NVME, limit = 5 GiB minus 5 MiB).
- Google Cloud Storage: native API at `storage.googleapis.com` (CRC-32C, limit = 5 TiB).[^1]

Every provider offering direct writes also offers direct reads so objects exceeding the proxied size limit can always be downloaded without passing through the server.

| Provider | `direct_get` | `direct_put` | `direct_multipart` | Direct PUT contract | Proven endpoints |
| --- | --- | --- | --- | --- | --- |
| AWS S3 | Yes | Yes | Yes | SigV4, `if-none-match: *`, stored CRC-64/NVME | Default endpoint, `amazonaws.com`, `amazonaws.com.cn` |
| Cloudflare R2 | Yes | Yes | Yes | SigV4, `if-none-match: *`, stored CRC-64/NVME | `r2.cloudflarestorage.com` |
| Other S3-compatible endpoints | No | No | No | n/a | None (unproven) |
| Google Cloud Storage | Yes[^47] | Yes[^47] | No | GOOG4-RSA-SHA256, generation 0, stored CRC-32C | `storage.googleapis.com` (no override) |
| Azure Blob Storage | No | No | No | n/a | n/a |
| Local filesystem | No | No | No | n/a | n/a |

The direct-PUT begin response returns the required checksum algorithm (formatted as lowercase hex), and the client hashes bytes while uploading and sends the checksum on completion.

The GCS adapter signs native V4 URLs locally (with a service-account key) or via IAM `signBlob` (under ambient credentials), using the JSON API for requests and XML API for object metadata. It does not use GCS S3-interoperability or XML multipart uploads.[^46]

### Browser CORS requirements

Because browser clients transfer directly to object storage, the bucket's CORS configuration MUST:

- Allow the application origin and the `PUT` and `GET` methods.
- Allow the `range` request header (signed on every presigned download).
- For S3 and R2, allow `if-none-match` and `x-amz-checksum-crc64nvme`.
- For GCS, allow `x-goog-if-generation-match`.
- Expose the `ETag` response header (required by browser multipart uploads to read per-part ETags).

## 4. Incremental writes by provider

The server hashes proxied upload streams on the fly without buffering the whole payload. Every built-in provider streams incrementally, keeping only one part or chunk in server memory regardless of object size (an internal memory property not advertised as a capability):

| Provider | Incremental write path | Server memory per large proxied upload |
| --- | --- | --- |
| AWS S3 | Provider multipart | 1 part (8 MiB default) |
| Cloudflare R2 | Provider multipart | 1 part |
| Other S3-compatible endpoints | Provider multipart | 1 part |
| Google Cloud Storage | Resumable upload | 1 part |
| Azure Blob Storage | Put Block and Put Block List | 1 block (8 MiB default) |
| Local filesystem | Staging file for uploads and assemblies | 1 chunk (assembly reads <= 8 MiB) |

On Azure Blob Storage, the adapter verifies each block's CRC-64/NVME (base64 little-endian) via `x-ms-content-crc64`, copies source ranges via `Put Block From URL` under the same check, combines block CRCs, stores the whole-blob checksum in blob metadata on `Put Block List`, and reads it back with HEAD. Creates enforce `If-None-Match: *` at every size. Azure has no direct uploads because SAS URLs cannot enforce a checksum header. Abandoned uploads need no explicit abort: Azure discards uncommitted blocks after a week, and each upload uses unique block IDs so concurrent commits never mix blocks.

## 5. Local filesystem provider

The local filesystem provider supports development and testing on Unix-family platforms (construction fails on other platforms rather than weakening atomicity). Replacements write to a staging file in the target directory, sync it, and atomically rename it over the destination so concurrent readers never see partial or missing objects.

HEAD requests compute CRC-64/NVME in bounded chunks and return ETag `local-fs-v1:crc64nvme:<hex>`. Assemblies stream source ranges (chunks <= 8 MiB) and ordered tail pieces into a staging file, compute CRC-64/NVME on the fly, verify the expected checksum, and publish with a create-only link.

## 6. Sources

[^1]: Google Cloud Storage, "Quotas & limits": [https://cloud.google.com/storage/quotas](https://cloud.google.com/storage/quotas)

[^2]: Cloudflare R2, "Limits": [https://developers.cloudflare.com/r2/platform/limits/](https://developers.cloudflare.com/r2/platform/limits/)

[^4]: AWS S3 User Guide, "Uploading and copying objects using multipart upload": [https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)

[^5]: AWS S3 API Reference, `GetObject`: [https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

[^6]: AWS S3 User Guide, "Checking object integrity": [https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)

[^7]: AWS S3 API Reference, `HeadObject`: [https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

[^8]: AWS S3 Service Level Agreement: [https://aws.amazon.com/s3/sla/](https://aws.amazon.com/s3/sla/)

[^9]: AWS S3 User Guide, "Best practices design patterns: optimizing Amazon S3 performance": [https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)

[^10]: AWS S3 User Guide, "Best practices design patterns: optimizing Amazon S3 performance": [https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)

[^12]: Google Cloud Storage, "Quotas & limits": [https://cloud.google.com/storage/quotas](https://cloud.google.com/storage/quotas)

[^13]: Google Cloud Storage, "XML API multipart uploads": [https://cloud.google.com/storage/docs/multipart-uploads](https://cloud.google.com/storage/docs/multipart-uploads)

[^14]: Google Cloud Storage, "Request preconditions": [https://cloud.google.com/storage/docs/request-preconditions](https://cloud.google.com/storage/docs/request-preconditions)

[^15]: Google Cloud Storage, "Sliced object downloads": [https://cloud.google.com/storage/docs/sliced-object-downloads](https://cloud.google.com/storage/docs/sliced-object-downloads)

[^16]: Google Cloud Storage, "Object metadata": [https://cloud.google.com/storage/docs/metadata](https://cloud.google.com/storage/docs/metadata)

[^17]: Google Cloud Storage, "Data validation and checksums": [https://cloud.google.com/storage/docs/data-validation](https://cloud.google.com/storage/docs/data-validation)

[^18]: Google Cloud Storage SLA: [https://cloud.google.com/storage/sla](https://cloud.google.com/storage/sla)

[^19]: Google Cloud Storage, "Request rate and access distribution guidelines": [https://cloud.google.com/storage/docs/request-rate](https://cloud.google.com/storage/docs/request-rate)

[^20]: Google Cloud Storage, "Quotas & limits - Bandwidth": [https://cloud.google.com/storage/quotas#bandwidth](https://cloud.google.com/storage/quotas#bandwidth)

[^23]: Cloudflare R2, "Limits": [https://developers.cloudflare.com/r2/platform/limits/](https://developers.cloudflare.com/r2/platform/limits/)

[^24]: Cloudflare R2, "Upload objects": [https://developers.cloudflare.com/r2/objects/upload-objects/](https://developers.cloudflare.com/r2/objects/upload-objects/)

[^25]: Cloudflare R2, "Limits": [https://developers.cloudflare.com/r2/platform/limits/](https://developers.cloudflare.com/r2/platform/limits/)

[^26]: Cloudflare R2, "S3 API compatibility": [https://developers.cloudflare.com/r2/api/s3/api/](https://developers.cloudflare.com/r2/api/s3/api/)

[^27]: Cloudflare R2, "Workers API reference": [https://developers.cloudflare.com/r2/api/workers/workers-api-reference/](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/)

[^28]: Cloudflare R2, "S3 API compatibility": [https://developers.cloudflare.com/r2/api/s3/api/](https://developers.cloudflare.com/r2/api/s3/api/)

[^29]: Cloudflare R2, "Upload objects": [https://developers.cloudflare.com/r2/objects/upload-objects/](https://developers.cloudflare.com/r2/objects/upload-objects/)

[^30]: Cloudflare R2 Workers API reference, checksum metadata: [https://developers.cloudflare.com/r2/api/workers/workers-api-reference/](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/)

[^31]: Cloudflare R2, "Durability": [https://developers.cloudflare.com/r2/reference/durability/](https://developers.cloudflare.com/r2/reference/durability/)

[^32]: Cloudflare R2, "Limits": [https://developers.cloudflare.com/r2/platform/limits/](https://developers.cloudflare.com/r2/platform/limits/)

[^35]: Microsoft Learn, "Scalability and performance targets for Blob storage": [https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets](https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets)

[^36]: Microsoft Learn, "Manage concurrency in Blob Storage": [https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage](https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage)

[^37]: Microsoft Learn, "Scalability and performance targets for Blob storage": [https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets](https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets)

[^38]: Microsoft Learn, "Put Block": [https://learn.microsoft.com/en-us/rest/api/storageservices/put-block](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block)

[^39]: Microsoft Learn, "Put Block List": [https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list)

[^40]: Microsoft Learn, "Get Blob": [https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob)

[^41]: Microsoft Learn, "Get Blob": [https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob)

[^42]: Microsoft Learn, "Put Block List": [https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list)

[^43]: Microsoft Learn, "Azure Storage redundancy": [https://learn.microsoft.com/en-us/azure/storage/common/storage-redundancy](https://learn.microsoft.com/en-us/azure/storage/common/storage-redundancy)

[^44]: Microsoft Learn, "Scalability and performance targets for standard storage accounts": [https://learn.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account](https://learn.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account)

[^45]: Microsoft Learn, "Performance checklist for Blob Storage": [https://learn.microsoft.com/en-us/azure/storage/blobs/storage-performance-checklist](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-performance-checklist)

[^46]: Google Cloud Storage, "XML API multipart uploads": [https://cloud.google.com/storage/docs/multipart-uploads](https://cloud.google.com/storage/docs/multipart-uploads)

[^47]: Verified by credentialed live runs of `crates/loonfs-objectstore/tests/it/objectstore_conformance.rs` and `crates/loonfs-server/tests/it/direct_put_real_provider.rs` (round trip and scoped, bounded, single-use capability checks).

[^48]: AWS S3 User Guide, "Checking object integrity for data uploads in Amazon S3": [https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)

[^49]: Google Cloud Storage, "Resumable uploads": [https://cloud.google.com/storage/docs/resumable-uploads](https://cloud.google.com/storage/docs/resumable-uploads)

[^50]: AWS S3 API Reference, `UploadPartCopy`: [https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)

[^51]: Google Cloud Storage JSON API, `objects.compose`: [https://cloud.google.com/storage/docs/json_api/v1/objects/compose](https://cloud.google.com/storage/docs/json_api/v1/objects/compose)

[^52]: Microsoft Learn, "Put Block From URL": [https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-from-url](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-from-url)

[^53]: Microsoft Learn, "Put Block List": [https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list)
