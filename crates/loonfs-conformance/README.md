# SDK conformance tests

This private crate contains shared JSON test cases and a Rust test harness.

## Cases

The sixteen cases cover:

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
- change feed identity fields
- an end-to-end filesystem workflow
- namespace-alias-scoped requests through a proxy

Each SDK test harness runs the proxy case against its own proxy implementation.
The Rust harness only checks that the fixture is valid.
The `upload_modes` case runs in the Rust reference harness. SDK harnesses
validate its place in the inventory; Go logs a skip for this case.

## JSON format

Each file in `cases/` contains:

- `name`: the case name, which must match the file name
- `intent`: the behavior being tested
- `request`: input values for the case
- `expected`: expected response fields and behavior

Each case has a Rust function that performs the calls. The JSON files contain
only inputs and expected results.

A `checksum_algorithm` literal in a case means the store's algorithm. The
checked-in `crc64nvme` value is for the default S3 shape used by SDK suites.
The Rust harness checks that value against the S3 default, then compares
responses with the algorithm of the shape being tested: `crc64nvme` for S3
and `crc32c` for GCS.

Large multipart cases use a repeatable byte pattern. For length `N` and
modulus `M`, the byte at each zero-based offset is `offset % M`. Other payloads
are UTF-8 strings.

## Rust harness

`tests/reference.rs` starts the production HTTP router with a temporary local
store and runs the corpus once per shape through `loonfs-client`:

- S3 uses CRC-64/NVME and offers service-proxied, direct PUT, and multipart uploads.
- GCS uses CRC-32C and offers service-proxied and direct PUT uploads.

A loopback service handles direct uploads and downloads. Only the S3 shape
has a multipart store, issuer, and part-upload route. The Rust harness skips
`upload_multipart` on GCS and logs why. The `upload_modes` case compares the
exact advertised mode set and attempts every mode. Each supported begin
must name the store's checksum algorithm; unsupported modes must fail.

`start_server` takes a `StoreShape`. The server binary reads
`LOONFS_CONFORMANCE_SHAPE=s3|gcs`, defaulting to `s3` when it is unset.
Invalid values fail startup. SDK suites keep using the default shape.

The typed client cannot create malformed JSON or invalid query values, so the
error case sends those two requests with a raw HTTP client. All other requests
use `loonfs-client`.

Unit tests cover fixture loading, byte patterns, and pagination. The
integration test requires local TCP listeners.
