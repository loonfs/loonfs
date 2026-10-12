# LoonFS Object Storage Provider Reference

This document lists provider limits and performance data points that inform LoonFS design work, and the requirements that a bucket's configuration and a LoonFS provider adapter MUST meet. It isn't a substitute for provider conformance tests.

The key word MUST in this document is to be interpreted as described in BCP 14 ([RFC 2119](https://www.rfc-editor.org/rfc/rfc2119), [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174)) when, and only when, it appears in capitals.

## 1. Frequently Used Data Points

This section lists the provider data points that come up most often in LoonFS design work.

- Don't assume that any CAS object can be updated more often than once per second. GCS documents a limit of one write per second to the same object name, and R2 documents a limit of one concurrent write per second to the same object key.[^1][^2]
- Don't assume that an `ETag` is a portable content hash. ETags aren't consistent across providers, especially for multipart uploads.
- A HEAD request usually takes around 30 ms, and a full GET of a small object (with content) should be assumed to take about 200 ms. A HEAD request (or a GET with if-none-match) is a useful way to check control objects faster.

## 2. Provider Matrix

This section compares the providers feature by feature. The paragraphs after the table describe listing timestamps and ambient AWS credentials.

| Feature | AWS S3 | Google Cloud Storage | Cloudflare R2 | Azure Blob Storage (not yet supported) | S3-Compatible |
| ----- | ----- | ----- | ----- | ----- | ----- |
| CAS / conditional write | Yes. | Yes. | Yes. | Yes. | Unknown until tested. |
| Same-key CAS/write ceiling to remember | No documented 1/s same-key cap found. | 1 write/s to the same object name, and 1 metadata update/s to the same object. Exceeding this can throttle.[^12] | 1 concurrent write/s to the same object key. Excess concurrent writes can return 429.[^23] | No 1/s same-key cap found.[^35][^36] | Unknown. Some providers might impose same-key, partition, account, or hidden fairness limits. |
| Multipart / large upload | Yes, with up to 10,000 parts.[^4] | Yes.[^13][^14] | Yes. A single upload can be up to 5 GiB. A multipart upload can be up to 5 TiB, with up to 10,000 parts of 5 MiB to 5 GiB each. SDKs can upload parts in parallel.[^24][^25] | Native block-blob upload, with Put Block and Put Block List. For current service versions, a block blob can have up to 50,000 blocks of up to 4,000 MiB each, and a maximum size of about 190.7 TiB.[^37][^38][^39] | Usually S3-like, but limits and checksum semantics vary. |
| Parallel/ranged download | Yes, with Range and partNumber.[^5] | Yes. Sliced object downloads use parallel ranged GETs.[^15] | Yes. The S3 API's GetObject supports Range and PartNumber.[^26][^27] | Yes.[^40] | Unknown until tested. |
| ETag and full-object checksums | The ETag is opaque in many cases. S3 supports stored object checksums, including CRC64NVME, CRC32, CRC32C, SHA-1, SHA-256, MD5, and others. Full-object checksums can be stored for single-part or multipart uploads.[^6][^7] | Every object has a CRC32C. Composite objects and XML API multipart objects have no MD5. ETags exist, but their values can differ between the XML and JSON APIs.[^16][^17] | The S3 API supports full-object CRC64NVME, and composite CRC32, CRC32C, SHA-1, and SHA-256. Multipart ETags use the S3 multipart format (an MD5 of the parts' MD5s).[^28][^29][^30] | The ETag is for conditional operations, and it isn't a portable hash. A full-blob Content-MD5 can be set, and it's returned for full reads. Range MD5 and CRC64 are limited to ranges of at most 4 MiB.[^41][^42] | Never assume that the ETag is an MD5. Check the provider's documentation. |
| Availability SLA / target | For the S3 Standard family, service credits begin when monthly uptime is below 99.9%. Some IA classes use a 99.0% threshold.[^8] | For Standard storage, 99.95% in multi-region and dual-region locations, and 99.9% in regional ones. Other storage classes and locations vary.[^18] | The R2 docs state an availability SLA of 99.9%, and a designed annual durability of 99.999999999%.[^31] | Availability depends on the redundancy option and the access tier. In the redundancy table, reads and writes are at least 99.9% available for common hot tiers, and RA-GRS and RA-GZRS reads are at least 99.99% available. The cool, cold, and archive tiers can be lower.[^43] | Depends on the vendor and the contract. |
| Request scale, partitions, throughput | At least 3,500 write-class requests/s and 5,500 GET or HEAD requests/s per partitioned prefix, with no limit on the number of prefixes. Scaling is gradual, and requests can fail with 503 Slow Down while it happens. AWS also cites up to 100 Gb/s from a single EC2 instance, and aggregate workloads of several Tb/s.[^9] | A bucket starts at about 1,000 writes/s and 5,000 reads/s, and then scales automatically. Increase the request rate no faster than roughly doubling it every 20 minutes. Sequential object names can create hotspots, and random prefixes improve the initial fan-out. The default internet egress quota is commonly 200 Gbps per region, subject to account history and quotas.[^19][^20] | Public r2.dev buckets are for testing only, and they might throttle at hundreds of requests/s. Production deployments should use custom domains or direct API access. The Cloudflare REST management API isn't meant for high-throughput object I/O, so use the S3-compatible or Workers APIs instead.[^32] | For standard GPv2 and blob storage accounts, the default maximum request rate is 40,000 requests/s in many of the listed regions, and 20,000 elsewhere. Ingress is commonly 60 or 25 Gbps, and egress 200 or 50 Gbps, depending on the region, and both can be increased on request. Partition hot spots can cause 503 or 500 responses, so distribute requests and use backoff.[^44][^45] | Vendor-specific. Run compatibility and load tests before relying on any numbers. |
| Published GET/HEAD latency | AWS publishes rough, general S3 latencies of about 100-200 ms (for small objects, or to the first byte).[^10] | No provider-published average GET or HEAD latency was found, so it needs to be benchmarked. | No provider-published average GET or HEAD latency was found, so it needs to be benchmarked. | No provider-published average GET or HEAD latency was found, so it needs to be benchmarked. | Needs to be benchmarked. |
| Content checksums | CRC-64/NVME for every content reference. | CRC-32C for every content reference. | CRC-64/NVME for every content reference. | Not yet supported. | CRC-64/NVME for every content reference. |
| Stored checksums | Uploads verify and store CRC-64/NVME. A HEAD request asks for checksum mode. | Uploads verify and store CRC-32C. A HEAD request reads `x-goog-hash`. | Same as AWS S3. | Not yet supported. | CRC-64/NVME verification and storage are required. |
| Conditional create completion | A publication (a WAL object, manifest, or control object) is one PUT with `If-None-Match: *`, at every size. An immutable write of less than 8 MiB is the same kind of PUT. From 8 MiB, it's LoonFS's own signed multipart upload, completed with `If-None-Match: *` and the full-object CRC-64/NVME.[^48] | A publication or an immutable write below 8 MiB uses one JSON API media upload, with `ifGenerationMatch=0` and the CRC-32C. A larger immutable write uses a JSON API resumable upload with the same precondition, and it sends the full-object checksum on the final request.[^14][^17][^49] | Same as AWS S3. *Live:* R2 documents conditional completion, but it's unconfirmed which header R2 honors on CompleteMultipartUpload. | Not yet supported. | Unknown until tested. |
| Assembly | Below 8 MiB, read the sources in chunks of at most 8 MiB, verify their checksums, and send one create-only PUT. At or above 8 MiB, start a CreateMultipartUpload with the full-object CRC-64/NVME. Copy source ranges of at least 5 MiB, in parts of at most 5 GiB. Read small runs and tail bytes in ranges of at most 8 MiB, and pad a short part that isn't the last one with bytes from the next source. Combine the reported part CRCs, and reject a wrong expected CRC before CompleteMultipartUpload with `If-None-Match: *`.[^48][^50] | Below 8 MiB, read the sources in chunks of at most 8 MiB, verify their checksums, and send one create-only media request. At or above 8 MiB, compose whole sources directly, and first write ranged sources and tail bytes as temporary objects. Compose at most 32 sources per request, through intermediate temporary objects, and then onto the destination with `ifGenerationMatch=0`. Check the source CRCs before composing, and the returned CRC-32C afterwards. Delete every temporary object. *Live:* the source and destination generation preconditions, and a chained compose of 40 whole sources.[^51] | Below 8 MiB, read the sources in chunks of at most 8 MiB, verify their checksums, and send one create-only PUT. At or above 8 MiB, cut the result into 64 MiB chunks (except the last). Copy a chunk that lies within one source, and read and upload every chunk that spans sources. Don't send `x-amz-copy-source-if-match`. R2 reports no checksum for copied parts. Complete with `If-None-Match: *`, and then send a HEAD with checksum mode and compare the stored full-object CRC-64/NVME with the expected CRC. A missing or different checksum fails the assembly and leaves the object for garbage collection. (A writer never deletes an immutable key, because another writer might already have accepted it.) Live runs cover copied parts and a 64 MiB chunk that spans two sources. | Not yet supported. | Unknown until tested. |

Items marked *live* are documented behavior that only a run against the provider confirms. The ignored tests in `crates/loonfs-objectstore/tests/it/objectstore_conformance.rs` exercise them.

For an object that hasn't changed, the modification time in a listing and the modification time a HEAD request returns differ by less than one second. R2 and GCS listings keep fractional seconds, while HEAD reports whole seconds. The garbage collector uses listing times, without a HEAD request for each object. Its `GC_SAFETY_MARGIN_MS` is 180,000 milliseconds, and that allowance includes timestamp precision.

AWS S3 credentials with `kind = "ambient"` use the standard AWS SDK credential chain. The chain checks environment variables, shared config and credentials files, credential processes, SSO, web identity, ECS task credentials, and EC2 instance metadata. `AWS_PROFILE` selects a profile by its name.

The provider client and presigned URLs use the same credential source. Each ambient AWS source caches one resolved credential value, with its native expiration. Requests reuse that value until 60 seconds before it expires, and concurrent callers share one refresh. A refresh runs on demand, with the provider's existing timeout and retry policy. Cancellation releases the refresh lock and keeps the previous value. If a refresh fails, or returns the same nearly expired credentials, a value that's still valid can be reused for at most one second before another refresh attempt, and never past its expiration. Cold failures and expired values aren't cached.

Once an ambient source returns credentials without an expiration, later lookups skip the cache and its refresh lock for the rest of that source's lifetime. This keeps lookups concurrent, and it keeps the source's rotation behavior. Later results are still checked for expiration, but they aren't cached. Explicit static credentials are used exactly as configured. The cache is in memory and scoped to the shared source, and it has no background task or persistence.

Presigned URLs include the current session token. A requested signing lifetime can trigger an early refresh of the shared cache, subject to the same one-second refresh backoff. If the resolved credentials still expire before that lifetime ends, LoonFS still issues the URL, and it reports the effective `expires_at_ms`, which is the earlier of the requested expiry and the credential expiry. The signed query keeps the requested lifetime, and AWS also enforces the credential expiration. Credentials that have already expired are rejected. This applies to reads, whole-object uploads, multipart operations, and checksum readback. Explicit static session tokens have no declared expiration in the configuration schema, so the caller is responsible for keeping them valid.

Cloudflare R2 doesn't use this chain. R2 ambient credentials are read from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` when the store starts.

## 3. Proven providers and direct-put

This section describes which providers support direct transfers, in which a client sends bytes to the provider instead of through the server.

Direct PUT is enabled only when the provider can do both of these things:

1. Enforce a create-only upload, so a client can't replace an existing content object.
2. Return a durable checksum for the stored object.

When an upload completes, LoonFS compares the client's byte count and checksum with the provider's metadata, and it publishes the object only if they match.

Direct transfers are limited to endpoints that live provider tests cover:

- AWS S3, with its default endpoint or an HTTPS endpoint under `amazonaws.com` or `amazonaws.com.cn`.
- Cloudflare R2, with an HTTPS endpoint under `r2.cloudflarestorage.com`.
- Google Cloud Storage, through the native API at `storage.googleapis.com`.

LoonFS doesn't issue direct-transfer capabilities for other S3-compatible endpoints, and no configuration setting overrides this. The service-proxied upload path is still available for them.

LoonFS represents a checksum as an algorithm and a lowercase hexadecimal value. The direct-PUT begin response returns the required algorithm. The client calculates that checksum while it uploads, and it sends the checksum at completion. AWS S3 and R2 use CRC-64/NVME, and GCS uses CRC-32C.

The capability document also returns the provider's single-request limit, as `upload.direct_put.max_content_bytes`. The current limits are 5 GiB for AWS S3, 5 GiB minus 5 MiB for R2, and 5 TiB for GCS.[^1]

Every provider that supports direct writes also supports direct reads.

| Provider | `direct_get` | `direct_put` | `direct_multipart` | Direct PUT contract | Proven endpoints |
| --- | --- | --- | --- | --- | --- |
| AWS S3 | Yes | Yes | Yes | SigV4, `if-none-match: *`, stored CRC-64/NVME | Default endpoint, `amazonaws.com`, `amazonaws.com.cn` |
| Cloudflare R2 | Yes | Yes | Yes | SigV4, `if-none-match: *`, stored CRC-64/NVME | `r2.cloudflarestorage.com` |
| Other S3-compatible endpoints | No | No | No | n/a | None (unproven) |
| Google Cloud Storage | Yes[^47] | Yes[^47] | No | GOOG4-RSA-SHA256, generation 0, stored CRC-32C | `storage.googleapis.com` (no override exists) |
| Azure Blob Storage | No | No | No | n/a | n/a |
| Local filesystem | No | No | No | n/a | n/a |

The GCS adapter uses native V4 signed URLs. It doesn't use the GCS S3-interoperability API, and it doesn't implement multipart uploads, even though GCS documents an XML multipart API.[^46] The adapter's own large creates and extensions use the JSON API, with the service account's OAuth token.

Browser clients send direct transfers to the provider. The bucket's CORS configuration MUST:

- allow the application origin,
- allow the `PUT` and `GET` methods,
- allow the `range` request header, which every download grant signs,
- for the S3 family, also allow the `if-none-match` and `x-amz-checksum-crc64nvme` request headers,
- for GCS, also allow the `x-goog-if-generation-match` request header, and
- expose the `ETag` response header.

A browser multipart upload reads each part's `ETag`, and it fails if that header isn't exposed.

Why isn't S3 API compatibility enough by itself for direct transfers? Some S3-compatible gateways ignore write preconditions.

Why does every provider with direct writes also support direct reads? That way, an object too large for a proxied upload can also be downloaded without passing through the server.

## 4. Incremental writes and where they are real

This section describes how much of a large proxied upload the server keeps in memory on each provider.

The server doesn't keep a proxied upload's whole body in memory. It hashes the payload as it forwards it to the store. Whether the store then keeps the whole payload in memory depends on the provider. The difference is visible only in memory use, so this document describes it, and LoonFS doesn't advertise it as a capability.

| Provider | Incremental write | What a large proxied upload costs the server |
| --- | --- | --- |
| AWS S3 | Yes, provider multipart | One part (8 MiB by default), whatever the object's size |
| Cloudflare R2 | Yes, provider multipart | One part, whatever the object's size |
| Other S3-compatible endpoints | Yes, provider multipart | One part, whatever the object's size |
| Google Cloud Storage | Yes, a resumable upload for a create, and provider multipart otherwise | One part, whatever the object's size |
| Azure Blob Storage | Not yet supported | n/a |
| Local filesystem | Yes, a staging file for uploads and assemblies | One chunk, and assembly reads are at most 8 MiB |

Every built-in provider has a real incremental write path. For a cloud provider, the incoming stream is regrouped into provider multipart parts, and the local filesystem provider writes chunks to a staging file. So the server keeps only one part or chunk of a proxied upload in memory, whatever the object's total size.

Azure Blob Storage isn't supported yet. A native adapter MUST:

1. Send each block's CRC-64 as the transactional checksum that the service verifies.
2. Copy source ranges with Put Block From URL, under the same check.
3. Combine the verified values.
4. Record the whole-blob checksum as a blob property at Put Block List.
5. Read the whole-blob checksum back with HEAD.

Creates use `If-None-Match: *`. Uploads are proxied only, because a SAS (shared access signature) can't require a checksum header.

## 5. Local Filesystem Provider

This section describes the local filesystem provider, which is a development and test provider supported on Unix-family platforms.

The local provider writes each replacement to a staging file in the destination directory, makes the staged bytes durable, and atomically renames the staged file over the destination. So a concurrent reader sees either the complete previous object or the complete replacement, never a missing or partial object. Constructing the provider fails on other platforms, instead of returning a provider with a weaker replacement contract.

For a HEAD request, the local provider computes the file's CRC-64/NVME. Its ETag is `local-fs-v1:crc64nvme:<hex>`. An assembly reads source ranges in chunks of at most 8 MiB, and writes the ordered tail pieces into a staging file. It computes the CRC-64/NVME while it writes, and it checks the expected checksum before it publishes the file with a create-only link. Each HEAD request or assembly buffers at most one read chunk.

## 6. LoonFS Design Implications

This section lists what the provider data above means for LoonFS's design.

1. **WAL and head flush cadence.** One update per second is the same-key CAS ceiling for GCS and R2. For more throughput, write immutable WAL objects, and update several sharded heads or a batched manifest.
2. **Immutable content path.** Content-addressed or monotonic keys can scale through multipart upload and distributed prefixes.
3. **Checksums.** LoonFS should be responsible for end-to-end integrity. Provider checksums help validate transport and storage, but ETag and checksum semantics differ sharply across providers and multipart modes.

## 7. Sources

The footnotes in this document cite these sources.

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

[^47]: A credentialed conformance run against a live bucket proved this. The provider conformance assertions and both signed direct-transfer tests (the round trip, and the scoped, bounded, single-use capability checks) passed. `GCS_DIRECT_TRANSFERS_PROVEN` in `crates/loonfs-objectstore/src/configured.rs` records the run.

[^48]: AWS S3 User Guide, "Checking object integrity for data uploads in Amazon S3": [https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)

[^49]: Google Cloud Storage, "Resumable uploads": [https://cloud.google.com/storage/docs/resumable-uploads](https://cloud.google.com/storage/docs/resumable-uploads)

[^50]: AWS S3 API Reference, `UploadPartCopy`: [https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)

[^51]: Google Cloud Storage JSON API, `objects.compose`: [https://cloud.google.com/storage/docs/json_api/v1/objects/compose](https://cloud.google.com/storage/docs/json_api/v1/objects/compose)
