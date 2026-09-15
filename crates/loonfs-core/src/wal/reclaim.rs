//! Identifies numbered WAL objects still required by retention.

use crate::namespace::state::NamespaceReadState;
use loonfs_api::WalNo;
use loonfs_objectstore::layout::wal_no_of;

pub(crate) fn required_from(head: &NamespaceReadState) -> Option<WalNo> {
    (!head.status.is_deleted()).then_some(head.last_folded_wal_no.min(head.retention_floor_wal_no))
}

pub(crate) fn object_is_required(key: &str, required_from: WalNo) -> bool {
    wal_no_of(key).is_none_or(|number| number > required_from)
}
