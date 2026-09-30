//! The write-ahead log: numbered WAL objects, their framing, publication,
//! discovery, loading, replay, and reclamation.

mod discover;
mod frame;
mod projected_tail;
mod publish;
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
pub(crate) use self::publish::publish_wal_object;
pub(crate) use self::reader::{load_replayed_wal_tail, replay_discovered_tail};
pub(crate) use self::reclaim::{live_folded_wal_no, object_is_required};
pub(crate) use self::writer::prepare_wal_object;

#[cfg(test)]
pub(crate) mod tests;
