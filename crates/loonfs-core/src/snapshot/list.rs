//! Lists existing snapshots: every snapshot record, or only the user
//! snapshots.

use super::record::load_snapshot_record_at_key;
use crate::control_object::ControlObjectLoadError;
use crate::engine::NamespaceEngine;
use crate::error::{CoreError, Result};
use futures::StreamExt;
use loonfs_objectstore::keys::snapshot_prefix;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_types::format::control::SnapshotPayload;
use loonfs_types::{
    NamespaceId, Page, PageCursor, PageRequest, Snapshot, SnapshotId, SnapshotRecord,
};
use serde::{Deserialize, Serialize};

/// Resume position for a listing of the snapshot inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRecordPageCursor {
    namespace_id: NamespaceId,
    pub(super) last_key: String,
}

impl PageCursor for SnapshotRecordPageCursor {
    const KIND: &'static str = "snapshot_record_inventory";
}

/// Resume position for a snapshot listing.
///
/// It records the same position as a [`SnapshotRecordPageCursor`] under its own
/// kind, so a cursor from one listing can't resume the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPageCursor {
    namespace_id: NamespaceId,
    last_key: String,
}

impl PageCursor for SnapshotPageCursor {
    const KIND: &'static str = "snapshot_inventory";
}

impl From<SnapshotRecordPageCursor> for SnapshotPageCursor {
    fn from(cursor: SnapshotRecordPageCursor) -> Self {
        Self {
            namespace_id: cursor.namespace_id,
            last_key: cursor.last_key,
        }
    }
}

impl From<SnapshotPageCursor> for SnapshotRecordPageCursor {
    fn from(cursor: SnapshotPageCursor) -> Self {
        Self {
            namespace_id: cursor.namespace_id,
            last_key: cursor.last_key,
        }
    }
}

impl SnapshotRecordPageCursor {
    fn validate_for(&self, namespace_id: &NamespaceId) -> Result<()> {
        if &self.namespace_id != namespace_id {
            return Err(CoreError::InvalidCursor(
                "cursor belongs to a different namespace".to_owned(),
            ));
        }
        if !self.last_key.starts_with(&snapshot_prefix(namespace_id)) {
            return Err(CoreError::InvalidCursor(
                "cursor refers to a key outside the snapshot listing".to_owned(),
            ));
        }
        Ok(())
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Loads one existing snapshot of any owner by id, as the snapshot inventory
    /// returns it. An expired or deleted record stays readable until garbage
    /// collection deletes it. A missing snapshot returns `snapshot_not_found`.
    pub async fn get_snapshot_record(&self, snapshot_id: &SnapshotId) -> Result<SnapshotRecord> {
        self.namespace.load_current_manifest().await?;
        let loaded = self
            .namespace
            .load_snapshot_record(snapshot_id)
            .await?
            .ok_or_else(|| CoreError::SnapshotNotFound {
                snapshot_id: snapshot_id.clone(),
            })?;
        Ok(super::snapshot_record_summary(loaded.state))
    }

    /// Lists one page of existing snapshots, fork snapshots included, in
    /// ascending id order. Expired and deleted records remain visible until
    /// garbage collection deletes them.
    pub async fn list_snapshot_records_page(
        &self,
        request: PageRequest<SnapshotRecordPageCursor>,
    ) -> Result<Page<SnapshotRecord, SnapshotRecordPageCursor>> {
        self.list_mapped_snapshot_records_page(request, |record| {
            Some(super::snapshot_record_summary(record))
        })
        .await
    }

    /// Lists one page of snapshots (the user snapshots) in ascending ID order,
    /// including expired snapshots that garbage collection hasn't deleted. A
    /// deleted snapshot is left out.
    pub async fn list_snapshots_page(
        &self,
        request: PageRequest<SnapshotPageCursor>,
    ) -> Result<Page<Snapshot, SnapshotPageCursor>> {
        let request = PageRequest {
            limit: request.limit,
            cursor: request.cursor.map(SnapshotRecordPageCursor::from),
        };
        let page = self
            .list_mapped_snapshot_records_page(request, super::user_snapshot)
            .await?;
        Ok(Page {
            items: page.items,
            next_cursor: page.next_cursor.map(SnapshotPageCursor::from),
        })
    }

    /// Lists the snapshots after the request's cursor and returns up to its limit of
    /// the rows that `row` returns for them. A snapshot for which `row` returns
    /// `None` doesn't count toward the limit.
    async fn list_mapped_snapshot_records_page<T>(
        &self,
        request: PageRequest<SnapshotRecordPageCursor>,
        row: impl Fn(SnapshotPayload) -> Option<T>,
    ) -> Result<Page<T, SnapshotRecordPageCursor>> {
        self.namespace.load_current_manifest().await?;

        if let Some(cursor) = &request.cursor {
            cursor.validate_for(&self.namespace.namespace_id)?;
        }

        let limit = request.limit;
        let prefix = snapshot_prefix(&self.namespace.namespace_id);
        let start_after = request
            .cursor
            .as_ref()
            .map(|cursor| cursor.last_key.as_str());
        let keys = self
            .namespace
            .store
            .list_prefix_from_stream(&prefix, start_after)
            .peekable();
        futures::pin_mut!(keys);
        let mut rows = Vec::with_capacity(limit.as_usize());
        let mut last_inspected_key = None;
        while rows.len() < limit.as_usize() {
            let Some(item) = keys.next().await else {
                break;
            };
            let key = item.map_err(|error| CoreError::store(&prefix, &error))?;
            last_inspected_key = Some(key.clone());
            // Skip records deleted after the key listing was read.
            let loaded = match load_snapshot_record_at_key(&self.namespace.store, &key).await {
                Ok(loaded) => loaded,
                Err(ControlObjectLoadError::MissingObject { .. }) => continue,
                Err(error) => return Err(CoreError::ControlObjectLoad(error)),
            };
            rows.extend(row(loaded.state));
        }
        let has_more = if rows.len() == limit.as_usize() {
            match keys.as_mut().peek().await {
                Some(Ok(_)) => true,
                Some(Err(_)) => {
                    let error = keys
                        .next()
                        .await
                        .expect("a peeked listing item should still be present")
                        .expect_err("the peeked listing item should still be an error");
                    return Err(CoreError::store(&prefix, &error));
                }
                None => false,
            }
        } else {
            false
        };
        let next_cursor = has_more.then(|| SnapshotRecordPageCursor {
            namespace_id: self.namespace.namespace_id.clone(),
            last_key: last_inspected_key.expect("a full page should inspect at least one key"),
        });
        Ok(Page {
            items: rows,
            next_cursor,
        })
    }
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use loonfs_types::{decode_cursor, encode_cursor, DirectoryPageCursor, NameKey};

    #[test]
    fn cursor_decode_rejects_unknown_fields() {
        let token = |extra: Option<(&str, serde_json::Value)>| {
            let mut cursor = serde_json::json!({
                "namespace_id": "demo",
                "last_key": "namespaces/demo/snapshots/snp_00000000000000000001-00000000000000000000000001.json",
            });
            if let Some((name, value)) = extra {
                cursor[name] = value;
            }
            loonfs_types::encode_token_bytes(
                &serde_json::to_vec(&serde_json::json!({
                    "format_version": 1,
                    "kind": "snapshot_record_inventory",
                    "token": cursor,
                }))
                .expect("encode cursor"),
            )
        };

        let cursor =
            decode_cursor::<SnapshotRecordPageCursor>(&token(None)).expect("decode cursor");
        assert_eq!(
            cursor.last_key,
            "namespaces/demo/snapshots/snp_00000000000000000001-00000000000000000000000001.json"
        );
        decode_cursor::<SnapshotRecordPageCursor>(&token(Some((
            "future_field",
            serde_json::json!({"ignored": true}),
        ))))
        .expect_err("a cursor with an unknown field should be rejected");
    }

    #[test]
    fn every_cursor_kind_matches_its_golden_bytes() {
        use crate::test_support::assert_matches_golden_token;
        use loonfs_types::{
            CommitSeq, ContentId, FileRevisionsPageCursor, GrepCursorPosition, GrepPageCursor,
            InodeId, RevisionNo, TrashPageCursor,
        };
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let snapshot = SnapshotRecordPageCursor {
            namespace_id: namespace_id.clone(),
            last_key:
                "namespaces/demo/snapshots/snp_00000000000000000001-00000000000000000000000001.json"
                    .to_owned(),
        };
        let cursors = [
            (
                "directory.txt",
                encode_cursor(&DirectoryPageCursor {
                    head_seq: CommitSeq(12),
                    snapshot_id: None,
                    directory_inode_id: InodeId(1),
                    last_name_key: NameKey::parse("report.txt").expect("name key"),
                }),
            ),
            (
                "file_revisions.txt",
                encode_cursor(&FileRevisionsPageCursor {
                    head_seq: CommitSeq(12),
                    inode_id: InodeId(42),
                    last_revision_no: RevisionNo(3),
                    last_committed_seq: CommitSeq(9),
                    last_revision_delta_index: 2,
                }),
            ),
            (
                "trash.txt",
                encode_cursor(&TrashPageCursor {
                    head_seq: CommitSeq(12),
                    last_deletion_seq: CommitSeq(8),
                    last_root_inode_id: InodeId(42),
                }),
            ),
            (
                "grep.txt",
                encode_cursor(&GrepPageCursor {
                    head_seq: CommitSeq(12),
                    index_captured_seq: CommitSeq(10),
                    cut_seq: CommitSeq(11),
                    last_content_id: ContentId::parse("con_3s0drmj8em84yb4mnetqkz2j0x")
                        .expect("content id"),
                    last_size_bytes: 4096,
                    last_inode_id: InodeId(42),
                    resume: GrepCursorPosition::AfterMatch { byte_offset: 1024 },
                    fingerprint: 7,
                }),
            ),
            ("snapshot_record_inventory.txt", encode_cursor(&snapshot)),
            (
                "snapshot_inventory.txt",
                encode_cursor(&SnapshotPageCursor::from(snapshot.clone())),
            ),
        ];
        for (name, token) in cursors {
            assert_matches_golden_token(name, &token.expect("encode cursor"));
        }
    }

    #[test]
    fn cursor_is_operation_bound() {
        let directory = DirectoryPageCursor {
            head_seq: loonfs_types::CommitSeq(1),
            snapshot_id: None,
            directory_inode_id: loonfs_types::InodeId(1),
            last_name_key: NameKey::parse("entry").expect("name key"),
        };
        let token = encode_cursor(&directory).expect("encode directory cursor");

        decode_cursor::<SnapshotRecordPageCursor>(&token)
            .expect_err("a directory cursor should not resume a snapshot inventory listing");
    }
}
