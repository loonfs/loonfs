# LoonFS SDK conformance corpus

This crate holds version one of the language-neutral SDK conformance corpus
and the Rust reference harness. It is not published.

## Version-one inventory

The corpus contains nine server-executable cases:

- the standard error contract;
- commit replay;
- direct PUT upload;
- multipart upload and completion replay;
- upload abort and repeated abort;
- granted direct download;
- cursor pagination and resumption;
- change-feed identity fields;
- the complete namespace and filesystem workflow.

Proxy cases are not part of version one.

## Fixture format

Each file in `cases/` is one JSON object with these fields:

- `version`: the fixture format version, currently `1`.
- `name`: a stable case name that matches the file stem.
- `intent`: the behavior under test. Retry cases name the API retry class.
- `family`: the Rust harness branch that owns the sequence.
- `operations`: the ordered wire operations for a reader of the fixture.
- `request`: family-specific input values.
- `expected`: stable response fields and wire-visible invariants.

The `operations` array documents order. It does not dispatch generic actions.
The harness has one Rust branch per family, and each branch contains its own
sequence. This keeps the files as data and expectations rather than an
execution language.

Large multipart content uses a deterministic byte pattern. A pattern with
length `N` and modulus `M` contains byte `offset % M` at each zero-based
offset. Other payloads use UTF-8 strings directly.

## Harness

`tests/reference.rs` loads every JSON file, starts the production HTTP router,
and sends typed operations through `loonfs-client`. The server uses a temporary
local filesystem store. A loopback transfer provider handles direct PUT,
multipart part PUT, and direct GET capabilities against the same store.

Malformed JSON values and invalid query text cannot be constructed by the
typed client. Those two requests use a raw HTTP client, then decode and check
the standard `ApiError` envelope. All ordinary operations use
`loonfs-client`.

The fixture loader, deterministic pattern, and pagination completeness checks
have unit coverage without a server. The full reference test needs loopback
listeners and runs in the outside gate.
