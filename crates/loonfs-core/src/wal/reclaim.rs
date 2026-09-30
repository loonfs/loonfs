//! Identifies numbered WAL objects still required for recovery.

use crate::namespace::state::NamespaceReadState;
use loonfs_api::WalNo;
use loonfs_objectstore::layout::wal_no_of;

pub(crate) fn live_folded_wal_no(head: &NamespaceReadState) -> Option<WalNo> {
    (!head.status.is_deleted()).then_some(head.folded_wal_no)
}

pub(crate) fn object_is_required(key: &str, folded_wal_no: WalNo) -> bool {
    wal_no_of(key).is_none_or(|number| number > folded_wal_no)
}
