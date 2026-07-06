mod frame;
mod reader;
mod replay;
mod writer;

pub(crate) use self::frame::{
    DecodedWalRecord, PreparedWalSegment, ReplayedWalTail, ValidatedWalChain, ValidatedWalSegment,
    WalBuildError, WalChainLoadError, WalChainLoadRequest, WalReplayError,
};
pub(crate) use self::reader::load_validated_wal_chain;
pub(crate) use self::replay::project_validated_wal_tail;
pub(crate) use self::writer::prepare_wal_segment;

#[cfg(test)]
mod tests;
