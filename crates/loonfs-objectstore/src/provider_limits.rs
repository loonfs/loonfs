//! The timeouts, part sizes, and HTTP client options that every provider
//! store shares.

use object_store as provider_store;
use std::time::Duration;

/// Timeout for one control-plane HTTP attempt's request phase.
///
/// An attempt that makes no progress for this long fails and counts against the
/// operation deadline instead of consuming it invisibly.
pub const PROVIDER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout between two frames of a response body, for every request.
///
/// A response body that sends no frame for this long fails, and its attempt
/// counts against the operation deadline.
pub(crate) const PROVIDER_RESPONSE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// One HTTP attempt's connect timeout.
pub const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Deadline shared by all retries of one logical object-store operation.
///
/// The retrying provider client applies it to each request it sends, as its
/// retry timeout. LoonFS applies it to each of its own retry loops: one
/// verified immutable write, or one request it signs, such as a part, so one
/// part's retries cannot outlast it. A new attempt may start only while time
/// remains. The garbage-collection grace period is longer than the maximum
/// publication operation.
pub const PROVIDER_OPERATION_DEADLINE: Duration = Duration::from_secs(120);

/// Maximum retry delay after the provider client admits a final retry.
pub const PROVIDER_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(15);

/// Minimum payload size for a multipart upload.
///
/// A `put` is one conditional request at every size. A verified immutable
/// write at or above this size uses the provider's own conditional
/// multipart path.
pub const PROVIDER_MULTIPART_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

/// Fixed size of every multipart part except the last.
///
/// Cloudflare R2 requires all non-final parts to share one size, and every
/// supported provider requires at least 5 MiB per non-final part; 8 MiB matches
/// the part size mainstream storage clients default to, and keeps every part a
/// cheap retry that fits comfortably inside one flat attempt bound.
pub const PROVIDER_MULTIPART_PART_BYTES: u64 = 8 * 1024 * 1024;

/// Concurrent in-flight parts of one verified immutable write.
///
/// A `put_streamed` write sends one part at a time.
pub const PROVIDER_MULTIPART_PARTS_IN_FLIGHT: usize = 4;

/// The smallest part every supported provider accepts anywhere but last, and
/// so the shortest base prefix a provider copies instead of receiving it
/// again.
pub const PROVIDER_MIN_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024;

/// The largest part every supported provider accepts.
pub const PROVIDER_MAX_MULTIPART_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Parts one provider multipart upload accepts.
///
/// Every supported provider stops at 10,000, which with the part size sets the
/// largest object a multipart write can produce.
pub const PROVIDER_MAX_MULTIPART_PARTS: u32 = 10_000;

/// Request-phase timeout for one HTTP attempt that carries a payload.
///
/// Upload progress is not observable while a request body is being sent, so
/// this fixed timeout treats an excessively slow part as stalled. Multipart
/// parts are bounded by [`PROVIDER_MULTIPART_PART_BYTES`].
pub const PROVIDER_TRANSFER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(120);

/// Request-phase allowance for a single-request publication.
///
/// The provider client checks its retry deadline before backoff, so a
/// resent request may finish one backoff and one payload attempt after that
/// deadline. A conditional WAL or manifest put is one attempt, even with a
/// payload-sized body, so the bound still holds for it. This does not bound
/// response-body consumption or prove that a timed-out request cannot take
/// effect remotely.
pub const PROVIDER_PUBLICATION_REQUEST_BOUND: Duration = PROVIDER_OPERATION_DEADLINE
    .saturating_add(PROVIDER_MAX_RETRY_BACKOFF)
    .saturating_add(PROVIDER_TRANSFER_ATTEMPT_TIMEOUT);

/// Request bodies at least this large are payload transfers and get
/// [`PROVIDER_TRANSFER_ATTEMPT_TIMEOUT`] as their request-phase timeout;
/// smaller bodies are control-plane traffic bounded by
/// [`PROVIDER_ATTEMPT_TIMEOUT`]. Sits well below the part size so multipart
/// tail parts classify with their siblings.
pub(crate) const PROVIDER_MIN_TRANSFER_BODY_BYTES: u64 = 1024 * 1024;

/// Returns the timeout for one HTTP attempt's request phase (connect,
/// request-body upload, response headers), by request body size: flat and
/// small for control-plane requests, flat and generous for payload transfers.
pub(crate) fn request_phase_timeout_for(request_body_bytes: u64) -> Duration {
    if request_body_bytes >= PROVIDER_MIN_TRANSFER_BODY_BYTES {
        PROVIDER_TRANSFER_ATTEMPT_TIMEOUT
    } else {
        PROVIDER_ATTEMPT_TIMEOUT
    }
}

/// Client options every provider builder applies: an explicit per-attempt
/// total-request timeout and connect timeout, so a client built from these
/// options alone is bounded by named constants instead of upstream defaults.
/// [`crate::transfer_timeouts::TransferTimeoutConnector`] strips the
/// total-request timeout and replaces it with payload-aware request timeouts
/// and response-body idle timeouts.
pub(crate) fn provider_client_options() -> provider_store::ClientOptions {
    provider_store::ClientOptions::new()
        .with_timeout(PROVIDER_ATTEMPT_TIMEOUT)
        .with_connect_timeout(PROVIDER_CONNECT_TIMEOUT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_phase_timeout_has_two_flat_tiers() {
        assert_eq!(request_phase_timeout_for(0), PROVIDER_ATTEMPT_TIMEOUT);
        assert_eq!(
            request_phase_timeout_for(PROVIDER_MIN_TRANSFER_BODY_BYTES - 1),
            PROVIDER_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            request_phase_timeout_for(PROVIDER_MIN_TRANSFER_BODY_BYTES),
            PROVIDER_TRANSFER_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            request_phase_timeout_for(PROVIDER_MULTIPART_PART_BYTES),
            PROVIDER_TRANSFER_ATTEMPT_TIMEOUT
        );
    }
}
