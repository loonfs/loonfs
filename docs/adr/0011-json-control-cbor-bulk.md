# ADR 0011: JSON for control objects, CBOR+zstd for immutable bulk metadata

Status: accepted

Small mutable control objects and manifests will use JSON. Immutable WAL entries and checkpoint segments will use versioned CBOR (Concise Binary Object Representation) payloads compressed with zstd.

Consequences:
- production debugging stays readable
- durable binary formats remain explicit and portable
- Rust-specific encodings such as `bincode` are disallowed for durable storage
