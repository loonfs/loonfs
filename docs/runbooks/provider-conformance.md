# Runbook: provider conformance

LoonDB is allowed to depend only on the provider behavior that this suite verifies.

## Required behaviors

- create-if-absent for immutable objects
- compare-and-swap update on small mutable control objects
- strong visibility after write and delete
- range reads
- overwrite behavior
- multipart behavior for large immutable blobs

## Why this runbook exists

“S3-compatible” is not a correctness proof.

The provider contract must be tested directly for local FS, AWS S3, and Cloudflare R2.
