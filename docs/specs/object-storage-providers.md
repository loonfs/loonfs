# LoonFS Object Storage Provider Reference

This document is a non-normative reference: provider limits and performance data points that inform LoonFS design work. It is not a substitute for provider conformance tests.

## 1. Frequently Used Data Points

- Do not assume any CAS object can be updated faster than **once per second**. GCS documents one write per second to the same object name; R2 documents one concurrent write per second to the same object key.[^1][^2]
- `ETag` cannot be assumed to be a portable content hash, and is not consistent across providers, especially for multipart uploads.
- GET HEAD is usually around 30ms, and a full GET of a small object (with content) should be assumed to be ~200ms. GET HEAD (or GET if-none-match) is a useful acceleration for evaluating control objects. 

## 2. Provider Matrix

| Feature | AWS S3 | Google Cloud Storage | Cloudflare R2 | Azure Blob Storage | S3-Compatible |
| ----- | ----- | ----- | ----- | ----- | ----- |
| **CAS / conditional write** | Yes. | Yes. | Yes. | Yes. | Unknown until tested. |
| **Same-key CAS/write ceiling to remember** | No documented 1/s same-key cap found. | **1 write/s to the same object name and 1 metadata update/s to the same object; exceeding this can throttle.[^12]** | **1 concurrent write/s to the same object key; excess concurrent writes can return 429.[^23]** | No 1/s same-key cap found.[^35][^36] | Unknown. Some providers may impose same-key, partition, account, or hidden fairness limits. |
| **Multipart / large upload** | Yes. Up to 10,000 parts. [^4] | Yes. [^13][^14] | Yes. Single upload up to 5 GiB; multipart up to 5 TiB, 10,000 parts, 5 MiB-5 GiB per part; SDKs can upload parts in parallel.[^24][^25] | Native block-blob upload: Put Block plus Put Block List. Up to 50,000 blocks, 4,000 MiB/block, and about 190.7 TiB max block blob for current service versions.[^37][^38][^39] | Usually S3-like, but limits and checksum semantics vary. |
| **Parallel/ranged download** | Yes: Range and partNumber. [^5] | Yes: sliced object downloads use parallel ranged GETs.[^15] | Yes. S3 API GetObject supports Range and PartNumber.[^26][^27] | Yes.[^40] | Unknown until tested. |
| **ETag and full-object checksums** | ETag is opaque in many cases. S3 supports stored object checksums including CRC64NVME, CRC32/CRC32C, SHA-1, SHA-256, MD5, and others; full-object checksums can be stored for single or multipart uploads.[^6][^7] | All objects have CRC32C. MD5 is absent for composite and XML API multipart objects. ETags exist, but values can differ across XML/JSON APIs.[^16][^17] | S3 API supports full-object CRC64NVME and composite CRC32/CRC32C/SHA-1/SHA-256. Multipart ETags follow S3-style multipart MD5-of-part-MD5s format.[^28][^29][^30] | ETag is for conditional operations, not a portable hash. Full-blob Content-MD5 can be set/returned for full reads; range MD5/CRC64 are limited to <=4 MiB ranges.[^41][^42] | Never assume ETag is MD5. Check documentation. |
| **Availability SLA / target** | S3 Standard family service credits begin when monthly uptime is below 99.9%; some IA classes use 99.0% thresholds.[^8] | Standard: 99.95% for multi-region / dual-region and 99.9% for regional; other storage classes and locations vary.[^18] | R2 docs state an availability SLA of 99.9% and 99.999999999% designed annual durability.[^31] | Availability depends on redundancy/access tier. Redundancy table: read/write at least 99.9% for common hot tiers; RA-GRS/RA-GZRS read availability at least 99.99%; cool/cold/archive can be lower.[^43] | Vendor/contract-specific. |
| **Request scale, partitions, throughput** | At least 3,500 write-class requests/s and 5,500 GET/HEAD requests/s per partitioned prefix; unlimited prefixes; scaling is gradual and may return 503 Slow Down. AWS also cites up to 100 Gb/s from a single EC2 instance and aggregate multi-Tb/s workloads.[^9] | Initial bucket scale is about 1,000 writes/s and 5,000 reads/s, then auto-scales. Ramp no faster than roughly doubling every 20 minutes. Sequential names can hotspot; random prefixes improve initial fanout. Internet egress default quota is commonly 200 Gbps per region, subject to account history and quotas.[^19][^20] | Public r2.dev buckets are test-only and may throttle at hundreds of req/s; production should use custom domains or direct APIs. Cloudflare REST management API is not for high-throughput object I/O; use S3-compatible or Workers APIs.[^32] | Standard GPv2/blob accounts: default max request rate 40,000 req/s in many listed regions, 20,000 elsewhere; ingress commonly 60/25 Gbps and egress 200/50 Gbps by region, increaseable by request. Partition hot spots can cause 503/500; use distribution and backoff.[^44][^45] | Vendor-specific. Run compatibility and load tests before relying on any numbers. |
| **Published GET/HEAD latency** | AWS publishes rough general S3 small-object / first-byte latencies of about 100-200 ms.[^10] | No provider-published average GET/HEAD latency found. Need to benchmark. | No provider-published average GET/HEAD latency found. Need to benchmark. | No provider-published average GET/HEAD latency found. Need to benchmark. | Need to benchmark to verify. |

AWS S3 credentials with `kind = "ambient"` use the standard AWS SDK credential chain. It checks environment variables, shared config and credentials files, credential processes, SSO, web identity, ECS task credentials, and EC2 instance metadata. `AWS_PROFILE` selects a named profile.

The provider client and presigned URLs use the same credential source. Temporary credentials refresh automatically, and presigned URLs include the current session token.

Cloudflare R2 does not use this chain. R2 ambient credentials read `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` when the store starts.

## 3. Proven providers and direct-put

Direct PUT is enabled only when the provider can do both of these things:

1. Enforce a create-only upload, so a client cannot replace an existing
   content object.
2. Return a durable checksum for the stored object.

At completion, LoonFS compares the client's byte count and checksum with the
provider metadata. The object cannot be published unless they match. S3 API
compatibility is not enough by itself because some compatible gateways ignore
write preconditions.

Direct transfers are limited to endpoints covered by live provider tests:

- AWS S3 with its default endpoint or an HTTPS endpoint under
  `amazonaws.com` or `amazonaws.com.cn`.
- Cloudflare R2 with an HTTPS endpoint under `r2.cloudflarestorage.com`.
- Google Cloud Storage through the native API at `storage.googleapis.com`.

Other S3-compatible endpoints do not receive direct-transfer capabilities.
There is no configuration override. The service-proxied upload path remains
available.

LoonFS represents checksums as an algorithm and a lowercase hexadecimal value.
The direct-PUT begin response returns the required algorithm. The client
calculates that checksum while uploading and sends it at completion. AWS S3
and R2 use CRC-64/NVME; GCS uses CRC-32C.

The capability document also returns the provider's single-request limit as
`upload.direct_put_max_content_bytes`. The current limits are 5 GiB for AWS
S3, 5 MiB less for R2, and 5 TiB for GCS.[^1]

Every provider that supports direct writes also supports direct reads. This
ensures that an object too large for a proxied upload can also be downloaded
without passing through the server.

| Provider | `direct_get` | `direct_put` | `direct_multipart` | Direct PUT contract | Proven endpoints |
| --- | --- | --- | --- | --- | --- |
| AWS S3 | Yes | Yes | Yes | SigV4, `if-none-match: *`, stored CRC-64/NVME | Default endpoint, `amazonaws.com`, `amazonaws.com.cn` |
| Cloudflare R2 | Yes | Yes | Yes | SigV4, `if-none-match: *`, stored CRC-64/NVME | `r2.cloudflarestorage.com` |
| Other S3-compatible endpoints | No | No | No | n/a | None — unproven |
| Google Cloud Storage | Yes[^47] | Yes[^47] | No | GOOG4-RSA-SHA256, generation 0, stored CRC-32C | `storage.googleapis.com` (no override exists) |
| Azure Blob Storage | No | No | No | n/a | n/a |
| Local filesystem | No | No | No | n/a | n/a |

The GCS adapter uses native V4 signed URLs. It does not use the GCS
S3-interoperability API or implement multipart uploads, even though GCS
documents an XML multipart API.[^46]

Browser clients send direct transfers to the provider. Bucket CORS must allow
the application origin. Allowed methods must include `PUT` and `GET`. Allowed
request headers must include `if-none-match` and `x-amz-checksum-crc64nvme`
for the S3 family, and `x-goog-if-generation-match` for GCS. The response must
expose `ETag`. Browser multipart reads each part's `ETag` and fails when the
header is not exposed.

## 4. Incremental writes and where they are real

A proxied upload never holds its body: the server hashes the payload as it
forwards it into the store. Whether the *store* then holds it depends on the
provider, and the difference is observable only as memory, so it is stated
here rather than advertised as a capability.

| Provider | Incremental write | What a large proxied upload costs the server |
| --- | --- | --- |
| AWS S3 | Yes, provider multipart | One part (8 MiB by default), whatever the object's size |
| Cloudflare R2 | Yes, provider multipart | One part, whatever the object's size |
| Other S3-compatible endpoints | Yes, provider multipart | One part, whatever the object's size |
| Google Cloud Storage | Yes, provider multipart | One part, whatever the object's size |
| Azure Blob Storage | Yes, provider multipart | One part, whatever the object's size |
| Local filesystem | Yes, staging file | One chunk as it arrives |

Every built-in provider has a real incremental write path. Cloud providers
regroup the incoming stream into provider multipart parts, while the local
filesystem stages chunks into a file. The server therefore retains only one
part or chunk for a proxied upload, independent of the object's total size.

## 5. Local Filesystem Provider

The local provider is a development and test provider supported on Unix-family platforms. It stages each replacement in the destination directory, makes the staged bytes durable, and atomically renames the staged file over the destination. A concurrent reader therefore observes either the complete prior object or the complete replacement, never a missing or partial object. Construction fails on other platforms rather than claiming a weaker replacement contract.

## 6. LoonFS Design Implications

1. **WAL/head flush cadence:** one update per second is the same-key CAS ceiling for GCS and R2. For more throughput, write immutable segment objects and update multiple sharded heads or a batched manifest.
2. **Immutable content path:** content-addressed or monotonic keys can scale through multipart upload and distributed prefixes. 
3. **Checksums:** LoonFS should own end-to-end integrity. Provider checksums help validate transport/storage, but ETag/checksum semantics diverge sharply across providers and multipart modes.

## 7. Sources

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

[^47]: Proven by a credentialed conformance run against a live bucket: the provider conformance assertions and both signed direct-transfer tests (the round trip, and the scoped, bounded, single-use capability checks) passed. `GCS_DIRECT_TRANSFERS_PROVEN` in `crates/loonfs-objectstore/src/configured.rs` records the run.
