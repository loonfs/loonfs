# SDK conformance tests

`loonfs-conformance` contains the shared JSON test cases that every LoonFS SDK test harness loads, and a Rust harness that runs them. If you maintain an SDK, your SDK's harness runs these same cases against its own client. This is a private crate (it isn't published to crates.io), so if you're looking for a Rust client for a LoonFS server, use `loonfs-client` instead.

## Cases

The nineteen cases cover:

- standard API errors
- repeated commit requests
- upload modes and begin-response checksums for both store shapes (`upload_modes`)
- direct PUT uploads
- multipart uploads and repeated completion requests
- repeated upload aborts
- direct downloads
- appends, and direct downloads of earlier and empty revisions (`append`)
- cursor pagination and resumption
- directory children by inode (`children_by_inode`)
- mutations by inode (`inode_mutations`)
- reads, copies, restores, and updates by inode (`inode_addressing`)
- snapshot creation, reads, extension, and deletion (`snapshots`)
- a read consistency on the trash and revision listings and the download grants (`read_consistency`)
- change feed identity fields
- change feed events of a kind that the client doesn't define (`unknown_event_kinds`)
- path entries of an inode kind that the client doesn't define (`unknown_inode_kinds`)
- an end-to-end filesystem workflow
- requests through a proxy, the routes it forwards, and its authorization hook

Two cases don't run in every harness:

- The proxy case: each SDK's test harness runs it against its own proxy implementation. The Rust harness only checks that the fixture is valid.
- `upload_modes`: this case runs in the Rust reference harness. SDK harnesses check that it's listed in the case inventory, and the Go harness logs that it skips the case.

The `unknown_event_kinds` case runs in every harness, but not against the conformance server, because the server returns only the event kinds that the API defines. Each harness serves the case's `page` from a local stub instead, and checks that its client decodes the page and returns the other events unchanged. The Python SDK leaves out the events of `unknown_kind`. The other clients return those events as values of a kind they don't define, so their harnesses skip them.

The `unknown_inode_kinds` case also runs in every harness against a local stub, because the server returns only the inode kinds that the API defines. Each harness serves the case's `entry` and `listing`, and checks that its client keeps the entry of `unknown_kind` with the fields that every entry includes, and returns the other entries unchanged. The Python SDK returns that entry as a `PathEntry_Unknown`, the Go SDK decodes its fields with `PathEntry.GetUnknown`, the TypeScript SDK returns it unchanged, and the Rust client decodes it as `PathEntryKind::Unknown`.

## JSON format

Each file in `cases/` contains:

- `name`: the case name, which must match the file name
- `intent`: the behavior being tested
- `request`: input values for the case
- `expected`: expected response fields and behavior

Each case has a Rust function that makes the calls. The JSON files contain only inputs and expected results.

A `checksum_algorithm` literal in a case stands for the store's checksum algorithm. The checked-in value, `crc64nvme`, is the algorithm of the default S3 shape that SDK suites use (see [Rust harness](#rust-harness)). The Rust harness checks that value against the S3 default, and then compares responses with the algorithm of the shape it's testing (`crc64nvme` for S3, and `crc32c` for GCS).

Large multipart cases use a repeatable byte pattern. For length `N` and modulus `M`, the byte at each zero-based offset is `offset % M`. Other payloads are UTF-8 strings.

## Rust harness

`tests/reference.rs` starts the production HTTP router with a temporary local store, and runs the corpus once for each store shape through `loonfs-client`:

- S3 uses CRC-64/NVME and offers service-proxied, direct PUT, and multipart uploads.
- GCS uses CRC-32C and offers service-proxied and direct PUT uploads.

A loopback service handles direct uploads and downloads. Only the S3 shape has a multipart store, issuer, and part-upload route, so the Rust harness skips `upload_multipart` on GCS and logs why. The `upload_modes` case compares the exact set of advertised modes, and tries every mode. Each supported begin must report the store's checksum algorithm, and each unsupported mode must fail.

`start_server` takes a `StoreShape`. The server binary reads `LOONFS_CONFORMANCE_SHAPE=s3|gcs`, and uses `s3` when the variable is unset. An invalid value makes startup fail. SDK suites use the default shape.

The typed client can't create malformed JSON or invalid query values, so the error case sends those two requests with a raw HTTP client. It also can't set a consistency on a snapshot read, so the `read_consistency` case sends that grant request with the raw client too. All other requests use `loonfs-client`.

Unit tests cover fixture loading, byte patterns, and pagination. The integration test needs local TCP listeners.
