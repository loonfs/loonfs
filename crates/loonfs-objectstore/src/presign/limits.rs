//! Lifetime bounds for presigned requests.

/// Maximum lifetime supported by both signed-URL implementations.
pub(super) const MAX_PRESIGN_TTL: std::time::Duration =
    std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Lifetime of a request that LoonFS signs for its own use, such as a
/// checksum readback or a multipart control request. Each is sent at once and
/// never handed out.
pub(crate) const INTERNAL_REQUEST_TTL: std::time::Duration = std::time::Duration::from_secs(60);
