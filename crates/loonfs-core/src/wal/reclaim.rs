//! Identifies numbered WAL objects still required for recovery.

use loonfs_objectstore::layout::wal_no_of;
use loonfs_types::WalNo;

pub(crate) fn object_is_required(key: &str, folded_wal_no: WalNo) -> bool {
    wal_no_of(key).is_none_or(|number| number > folded_wal_no)
}
