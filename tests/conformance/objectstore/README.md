# Object-store conformance

This directory holds the readable provider-agnostic case list for the active `loon-objectstore`
contract.

Current providers under test:
- local FS
- AWS S3
- Cloudflare R2

Future providers:
- Google Cloud Storage
- Azure Blob Storage

Core rule:
semantic crates may rely only on behavior that appears here and is proven by the matching
conformance suite.

Current active case matrix is recorded in:

```text
tests/conformance/objectstore/cases.yaml
```
