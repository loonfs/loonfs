# ADR 0006: AWS S3 and Cloudflare R2 are the first provider targets

Status: accepted

LoonDB will target AWS S3 and Cloudflare R2 first.

Consequences:
- provider assumptions must be expressed as a conformance contract
- provider ETags are not canonical content identity (due, in part, to caveats from most vendors against using ETag as a canonical content hash)
- multipart differences and control-plane caveats must stay isolated in `loon-objectstore`
