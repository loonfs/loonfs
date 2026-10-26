//! The change feed: `GET /changes`.

use crate::error::{MountSimError, Result};
use crate::query::{Query, ReadPoint};
use crate::reply::Reply;
use crate::state::ServerState;
use loonfs_types::api::v0::ListChangesResponse;
use loonfs_types::{ChangeSeq, ErrorCode, NamespaceId};

const CHANGES_PARAMS: &[&str] = &[
    "after_seq",
    "limit",
    "snapshot_id",
    "consistency",
    "max_staleness_ms",
];

/// Returns the commits after `after_seq`, bounded by the commit `limit` and the page budget.
///
/// `horizon` is the last sequence that the feed shows while a fault keeps a commit's echo out of
/// the feed.
pub(crate) fn list_changes(
    state: &ServerState,
    namespace_id: &NamespaceId,
    query: &Query,
    horizon: Option<ChangeSeq>,
) -> Result<Reply> {
    query.ensure_only(CHANGES_PARAMS)?;
    let after_seq = ChangeSeq(query.required_u64("after_seq")?);
    let limit = query.limit()?;
    let at = query.read_point()?;
    let namespace = state.namespace(namespace_id)?;
    let end_seq = match &at {
        ReadPoint::Live => {
            let head = namespace.head_seq();
            horizon.map_or(head, |horizon| horizon.min(head))
        }
        ReadPoint::Snapshot(_) => namespace.read_seq(&at, state.now_ms())?,
    };
    if after_seq < namespace.retention_floor_seq {
        return Err(MountSimError::new(
            ErrorCode::RebootstrapRequired,
            format!(
                "`after_seq` {after_seq} is below the retention floor {}",
                namespace.retention_floor_seq
            ),
        )
        .with_details(|details| {
            details.after_seq = Some(after_seq);
            details.retention_floor_seq = Some(namespace.retention_floor_seq);
        }));
    }
    if after_seq > end_seq {
        return Err(MountSimError::invalid_request(format!(
            "`after_seq` {after_seq} is above the head {end_seq} that the feed reads"
        ))
        .with_param("after_seq"));
    }
    let mut changes = Vec::new();
    let mut page_bytes = 0_usize;
    for record in namespace.commits_between(after_seq, end_seq) {
        let fits_budget = page_bytes + record.encoded_bytes <= state.config.feed_page_budget_bytes;
        if changes.len() == limit || (!changes.is_empty() && !fits_budget) {
            break;
        }
        page_bytes += record.encoded_bytes;
        changes.push(record.commit.clone());
    }
    let last_seq = changes
        .last()
        .map_or(after_seq, |commit| commit.committed_seq);
    Ok(Reply::json(&ListChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        head_seq: end_seq,
        next_after_seq: (last_seq < end_seq).then_some(last_seq),
        changes,
    }))
}
