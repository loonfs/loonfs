//! Store request concurrency for read and write operations.

/// Bounds store requests kept in flight by one read operation.
pub(crate) const STORE_READS_IN_FLIGHT: usize = 16;

/// Bounds store requests kept in flight by one write operation.
pub(crate) const STORE_WRITES_IN_FLIGHT: usize = 32;

/// Bounds the members whose roots one pool collection reads at once. Each
/// member's read keeps its own requests within [`STORE_READS_IN_FLIGHT`].
pub(crate) const POOL_MEMBER_READS_IN_FLIGHT: usize = STORE_READS_IN_FLIGHT;
