//! The write-ahead log: numbered segment objects, their framing, publication,
//! discovery, loading, replay, and reclamation.

mod discover;
mod frame;
mod publish;
mod reader;
mod reclaim;
mod replay;
mod writer;

pub(crate) use self::discover::discover_tip;
pub use self::discover::probe_namespace_wal;
use self::frame::WalTailLoadRequest;
use self::frame::{
    DecodedWalRecord, PreparedWalSegment, ReplayedWalTail, ValidatedWalSegment, ValidatedWalTail,
};
pub(crate) use self::frame::{WalSegmentError, WalTailLoadError};
pub(crate) use self::publish::publish_segment;
pub(crate) use self::reader::{load_replayed_wal_tail, load_retained_wal_tail};
pub(crate) use self::reclaim::{object_is_required, required_from};
pub(crate) use self::writer::{prepare_fence_segment, prepare_wal_segment, resulting_head_after};

#[cfg(test)]
pub(crate) mod tests;
