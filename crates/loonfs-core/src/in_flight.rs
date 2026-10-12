//! Store request concurrency for read and write operations.

/// Bounds store requests kept in flight by one read operation.
pub(crate) const STORE_READS_IN_FLIGHT: usize = 16;

/// Bounds store requests kept in flight by one write operation.
pub(crate) const STORE_WRITES_IN_FLIGHT: usize = 16;
