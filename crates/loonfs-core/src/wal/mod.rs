//! The write-ahead log: segment framing, tail loading, replay onto
//! metadata state, and segment preparation for publication.

mod frame;
mod reader;
mod replay;
mod writer;

pub(crate) use self::frame::{
    DecodedWalRecord, PreparedWalSegment, ReplayedWalTail, ValidatedWalSegment, ValidatedWalTail,
    WalSegmentError, WalTailLoadError, WalTailLoadRequest,
};
pub(crate) use self::reader::{load_wal_segment, load_wal_tail};
pub(crate) use self::replay::{
    ensure_replayed_head_matches, project_validated_wal_tail, validate_wal_segment_for_replay,
};
pub(crate) use self::writer::prepare_wal_segment;

#[cfg(test)]
mod tests;
