//! Narrow verified change-feed and checkpoint-manifest reads for `loonfs-grep`.

use super::error::ManifestLoadError;
use super::load::load_verified_manifest_tables;
use super::record::read_checkpoint_record;
use super::scan::{string_prefix_upper_bound, VerifiedMetadataTables};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::metadata::RevisionRecord;
use crate::namespace::control::{load_namespace_head_control, read_wal_floor_seq_or_zero};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use loonfs_api::wire::control::{CheckpointRecordLifecycle, NamespaceState};
use loonfs_api::wire::manifest::{MetadataRow, MetadataTableFamily};
use loonfs_api::wire::wal::WalCommitPayload;
use loonfs_api::{ChangeSeq, CheckpointId, NamespaceId};
use loonfs_objectstore::ObjectStore;

/// Validated incremental commits after a grep watermark, or an explicit
/// signal that retention has removed part of the required history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepChangeFeed {
    /// Every returned record is later than the requested watermark.
    Records {
        /// Validated commits in sequence order.
        records: Vec<WalCommitPayload>,
    },
    /// The requested watermark precedes the retained WAL history.
    RebootstrapRequired,
}

/// One verified page from the immutable manifest pinned by a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepCheckpointRevisionPage {
    /// Sequence covered by the checkpoint manifest.
    pub checkpoint_seq: ChangeSeq,
    /// Revision rows in durable row-key order.
    pub revisions: Vec<(String, RevisionRecord)>,
    /// True when no later revision-family row remains.
    pub exhausted: bool,
}

/// Loads the validated WAL change feed after `after_seq`.
///
/// Retention gaps are data, not errors: grep rebuilds from a fresh checkpoint
/// instead of holding the namespace retention floor behind its watermark.
pub async fn load_grep_change_feed<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
) -> Result<GrepChangeFeed> {
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        });
    }
    let floor_seq = read_wal_floor_seq_or_zero(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if after_seq < floor_seq {
        return Ok(GrepChangeFeed::RebootstrapRequired);
    }
    if after_seq >= head.seq {
        return Ok(GrepChangeFeed::Records {
            records: Vec::new(),
        });
    }

    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: floor_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip,
            stop_after_seq: Some(after_seq),
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let records = wal_chain
        .segments()
        .iter()
        .flat_map(|segment| segment.records())
        .filter(|record| record.seq > after_seq)
        .cloned()
        .collect();
    Ok(GrepChangeFeed::Records { records })
}

/// Loads one revision page from an active, unexpired checkpoint.
///
/// `None` means the record or its immutable basis vanished, was released, or
/// expired. Callers restart from a newly created checkpoint in every case.
pub async fn load_grep_checkpoint_revision_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    now_ms: u64,
    cursor: &str,
    limit: usize,
) -> Result<Option<GrepCheckpointRevisionPage>> {
    let Some(record) = read_checkpoint_record(store, namespace_id, checkpoint_id)
        .await?
        .map(|loaded| loaded.state)
    else {
        return Ok(None);
    };
    if record.state != CheckpointRecordLifecycle::Active
        || record
            .expires_at_ms
            .is_some_and(|expires_at_ms| now_ms >= expires_at_ms)
    {
        return Ok(None);
    }

    let tables = match load_verified_manifest_tables(
        store,
        namespace_id,
        &record.manifest_object_id,
    )
    .await
    {
        Ok(tables) => tables,
        Err(ManifestLoadError::MissingManifest { .. }) => return Ok(None),
        Err(error) => {
            return Err(CoreError::MetadataProjection(
                MetadataProjectionLoadError::ManifestLoad(error),
            ))
        }
    };
    let manifest = tables.manifest();
    if manifest.payload_checksum != record.manifest_payload_checksum
        || manifest.payload.head_seq != record.manifest_head_seq
    {
        return Err(CoreError::NamespaceCorrupt(format!(
            "checkpoint `{checkpoint_id}` basis does not match its manifest"
        )));
    }
    let limit = limit.max(1);
    let rows = scan_checkpoint_revisions(&tables, cursor, limit).await?;
    let exhausted = rows.len() < limit;
    let mut revisions = Vec::with_capacity(rows.len());
    for (row_key, row) in rows {
        let MetadataRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            committed_at_ms,
            revision_delta_index,
            content_ref,
        } = row
        else {
            return Err(CoreError::NamespaceCorrupt(format!(
                "revisions family returned a non-revision row at `{row_key}`"
            )));
        };
        revisions.push((
            row_key,
            RevisionRecord {
                inode_id,
                revision_no,
                committed_seq,
                committed_at_ms,
                revision_delta_index,
                content_ref,
            },
        ));
    }
    Ok(Some(GrepCheckpointRevisionPage {
        checkpoint_seq: record.manifest_head_seq,
        revisions,
        exhausted,
    }))
}

async fn scan_checkpoint_revisions<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    cursor: &str,
    limit: usize,
) -> Result<Vec<(String, MetadataRow)>> {
    let lower_bound = if cursor.is_empty() {
        "revision-".to_owned()
    } else {
        format!("{cursor}\0")
    };
    tables
        .scan_range_page_with_keys(
            MetadataTableFamily::Revisions,
            &lower_bound,
            string_prefix_upper_bound("revision-").as_deref(),
            limit,
        )
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })
}
