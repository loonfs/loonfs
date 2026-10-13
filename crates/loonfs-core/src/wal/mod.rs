//! The write-ahead log: numbered WAL objects, their framing, publication,
//! discovery, loading, replay, and reclamation.

mod discover;
mod frame;
mod projected_tail;
mod publish;
mod publish_error;
mod reader;
mod reclaim;
mod replay;
mod writer;

pub use self::discover::probe_namespace_wal;
pub(crate) use self::discover::{discover_tail, DiscoveredTail};
pub(crate) use self::frame::ValidatedWalTail;
use self::frame::WalTailLoadRequest;
use self::frame::{PreparedWalObject, ReplayedWalTail, ValidatedWalObject};
pub(crate) use self::frame::{WalObjectError, WalTailLoadError};
pub use self::projected_tail::ProjectedWalTail;
pub(crate) use self::projected_tail::{
    inline_content_refs, ProjectedContent, ProjectedPiece, TailChunk,
};
pub(crate) use self::publish::publish_wal_object;
pub use self::publish_error::{is_retryable_wal_publish, settle_publish_attempt, WalPublishError};
pub(crate) use self::reader::{load_replayed_wal_tail, replay_discovered_tail, wal_object_exists};
pub(crate) use self::reclaim::{live_folded_wal_no, object_is_required};
pub(crate) use self::writer::prepare_wal_object;

#[cfg(test)]
pub(crate) mod tests;

/// The key of the WAL object at `wal_no`.
#[expect(
    clippy::disallowed_methods,
    reason = "the wal module is the physical WAL boundary, so only it builds WAL object keys"
)]
fn wal_object_key(
    namespace_id: &loonfs_types::NamespaceId,
    wal_no: &loonfs_types::WalNo,
) -> String {
    loonfs_objectstore::keys::wal_object(namespace_id, wal_no)
}
