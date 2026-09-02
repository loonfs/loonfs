//! The write-ahead log: segment framing, chain loading, replay onto
//! metadata state, and segment preparation for publication.

mod frame;
mod reader;
mod replay;
mod writer;

pub(crate) use self::frame::{
    DecodedWalRecord, PreparedWalSegment, ReplayedWalTail, ValidatedWalChain, ValidatedWalSegment,
    WalChainLoadError, WalChainLoadRequest, WalSegmentError,
};
pub(crate) use self::reader::{count_visible_wal_tail_segments, load_wal_chain, WalChainLoad};
pub(crate) use self::replay::{ensure_replayed_head_matches, project_validated_wal_tail};
pub(crate) use self::writer::prepare_wal_segment;

#[cfg(test)]
mod tests;
