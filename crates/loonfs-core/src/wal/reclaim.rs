//! The rule for a WAL number that no fold has covered.

use loonfs_types::WalNo;

/// Returns whether `wal_no` is above `folded_wal_no`, the folded WAL number
/// of the newest manifest, so that no fold has covered it.
///
/// A collection deletes WAL objects only at or below a published manifest's
/// `folded_wal_no`, so a WAL object above the newest manifest's
/// `folded_wal_no` is still needed for replay, and a collection keeps it. The
/// same fact decides whether a fence is live (format spec, "Writer
/// ownership"). A writer may commit at the number that a fence's put uses, and a fold and a
/// collection may empty that number before the put, so the put can succeed
/// where no reader replays it. A number above the `folded_wal_no` of the
/// newest manifest after the put wasn't emptied. When the manifest that the
/// put was planned from is still the newest, the object is live without
/// another load, because the tail was read above that manifest's
/// `folded_wal_no`.
pub(crate) fn wal_no_is_unfolded(wal_no: WalNo, folded_wal_no: WalNo) -> bool {
    wal_no > folded_wal_no
}
