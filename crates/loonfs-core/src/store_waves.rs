//! Store request concurrency for read and write operations.

/// Bounds store requests kept in flight by one read operation.
pub(crate) const STORE_READ_WAVE: usize = 16;

/// Bounds store requests kept in flight by one write operation.
pub(crate) const STORE_WRITE_WAVE: usize = 16;
