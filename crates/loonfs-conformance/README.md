# SDK conformance tests

This private crate contains shared JSON test cases and a Rust test harness.

## Cases

The ten cases cover:

- standard API errors
- repeated commit requests
- direct PUT uploads
- multipart uploads and repeated completion requests
- repeated upload aborts
- direct downloads
- cursor pagination and resumption
- change feed identity fields
- an end-to-end filesystem workflow
- namespace-alias-scoped requests through a proxy

Each SDK test harness runs the proxy case against its own proxy implementation.
The Rust harness only checks that the fixture is valid.

## JSON format

Each file in `cases/` contains:

- `name`: the case name, which must match the file name
- `intent`: the behavior being tested
- `request`: input values for the case
- `expected`: expected response fields and behavior

Each case has a Rust function that performs the calls. The JSON files contain
only inputs and expected results.

Large multipart cases use a repeatable byte pattern. For length `N` and
modulus `M`, the byte at each zero-based offset is `offset % M`. Other payloads
are UTF-8 strings.

## Rust harness

`tests/reference.rs` starts the production HTTP router with a temporary local
store and runs every case through `loonfs-client`. A loopback service handles
direct uploads, multipart part uploads, and direct downloads.

The typed client cannot create malformed JSON or invalid query values, so the
error case sends those two requests with a raw HTTP client. All other requests
use `loonfs-client`.

Unit tests cover fixture loading, byte patterns, and pagination. The
integration test requires local TCP listeners.
