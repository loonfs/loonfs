//! Lifetime bounds for presigned requests.

/// Maximum lifetime supported by both signed-URL implementations.
pub(super) const MAX_PRESIGN_TTL: std::time::Duration =
    std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Lifetime of a signed checksum-readback request.
pub(crate) const CHECKSUM_HEAD_TTL: std::time::Duration = std::time::Duration::from_secs(60);
