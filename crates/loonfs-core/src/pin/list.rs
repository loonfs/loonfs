//! Lists existing pins: every pin as a checkpoint, or the user pins as
//! snapshots.

use super::record::{load_pin, load_pin_at_key};
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::control::load_current_manifest;
use futures::StreamExt;
use loonfs_objectstore::keys::pin_prefix;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::PinPayload;
use loonfs_types::{Checkpoint, NamespaceId, Page, PageCursor, PageRequest, PinId, Snapshot};
use serde::{Deserialize, Serialize};

/// Resume position for a checkpoint listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointPageCursor {
    namespace_id: NamespaceId,
    pub(super) last_key: String,
}

impl PageCursor for CheckpointPageCursor {
    const KIND: &'static str = "checkpoint_inventory";
}

/// Resume position for a snapshot listing.
///
/// It records the same position as a [`CheckpointPageCursor`] under its own
/// kind, so a cursor from one listing can't resume the other.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotPageCursor {
    namespace_id: NamespaceId,
    last_key: String,
}

impl PageCursor for SnapshotPageCursor {
    const KIND: &'static str = "snapshot_inventory";
}

impl From<CheckpointPageCursor> for SnapshotPageCursor {
    fn from(cursor: CheckpointPageCursor) -> Self {
        Self {
            namespace_id: cursor.namespace_id,
            last_key: cursor.last_key,
        }
    }
}

impl From<SnapshotPageCursor> for CheckpointPageCursor {
    fn from(cursor: SnapshotPageCursor) -> Self {
        Self {
            namespace_id: cursor.namespace_id,
            last_key: cursor.last_key,
        }
    }
}

impl CheckpointPageCursor {
    fn validate_for(&self, namespace_id: &NamespaceId) -> Result<()> {
        if &self.namespace_id != namespace_id {
            return Err(CoreError::InvalidCursor(
                "cursor belongs to a different namespace".to_owned(),
            ));
        }
        if !self.last_key.starts_with(&pin_prefix(namespace_id)) {
            return Err(CoreError::InvalidCursor(
                "cursor refers to a key outside the pin listing".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Loads one existing pin of any owner as the checkpoint row a listing
/// returns for it. A missing pin returns `snapshot_not_found`.
pub(crate) async fn get_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &PinId,
) -> Result<Checkpoint> {
    load_current_manifest(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let loaded = load_pin(store, namespace_id, checkpoint_id)
        .await?
        .ok_or_else(|| CoreError::SnapshotNotFound {
            snapshot_id: checkpoint_id.clone(),
        })?;
    Ok(super::checkpoint_summary(loaded.state))
}

/// Lists one page of every pin, fork pins included, in ascending ID order.
pub(crate) async fn list_checkpoints_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: PageRequest<CheckpointPageCursor>,
) -> Result<Page<Checkpoint, CheckpointPageCursor>> {
    list_pins_page(store, namespace_id, request, |record| {
        Some(super::checkpoint_summary(record))
    })
    .await
}

/// Lists one page of snapshots (the user pins) in ascending ID order,
/// including expired snapshots that garbage collection hasn't deleted.
pub(crate) async fn list_snapshots_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: PageRequest<SnapshotPageCursor>,
) -> Result<Page<Snapshot, SnapshotPageCursor>> {
    let request = PageRequest {
        limit: request.limit,
        cursor: request.cursor.map(CheckpointPageCursor::from),
    };
    let page = list_pins_page(store, namespace_id, request, super::user_pin_snapshot).await?;
    Ok(Page {
        items: page.items,
        next_cursor: page.next_cursor.map(SnapshotPageCursor::from),
    })
}

/// Lists the pins after the request's cursor and returns up to its limit of
/// the rows that `row` returns for them. A pin for which `row` returns
/// `None` doesn't count toward the limit.
async fn list_pins_page<S: ObjectStore + ?Sized, T>(
    store: &S,
    namespace_id: &NamespaceId,
    request: PageRequest<CheckpointPageCursor>,
    row: impl Fn(PinPayload) -> Option<T>,
) -> Result<Page<T, CheckpointPageCursor>> {
    load_current_manifest(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;

    if let Some(cursor) = &request.cursor {
        cursor.validate_for(namespace_id)?;
    }

    let limit = request.limit;
    let prefix = pin_prefix(namespace_id);
    let start_after = request
        .cursor
        .as_ref()
        .map(|cursor| cursor.last_key.as_str());
    let keys = store
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
        let loaded = match load_pin_at_key(store, &key).await {
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
    let next_cursor = has_more.then(|| CheckpointPageCursor {
        namespace_id: namespace_id.clone(),
        last_key: last_inspected_key.expect("a full page should inspect at least one key"),
    });
    Ok(Page {
        items: rows,
        next_cursor,
    })
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use loonfs_types::{decode_cursor, encode_cursor, DirectoryPageCursor, NameKey};

    #[test]
    fn cursor_decode_tolerates_additive_fields() {
        let token = loonfs_types::encode_token_bytes(
            &serde_json::to_vec(&serde_json::json!({
                "format_version": 1,
                "kind": "checkpoint_inventory",
                "namespace_id": "demo",
                "last_key": "namespaces/demo/pins/pin_00000000000000000001-0000000000000001.json",
                "future_field": {"ignored": true}
            }))
            .expect("encode cursor"),
        );

        let cursor = decode_cursor::<CheckpointPageCursor>(&token)
            .expect("decode cursor with additive field");
        assert_eq!(
            cursor.last_key,
            "namespaces/demo/pins/pin_00000000000000000001-0000000000000001.json"
        );
    }

    #[test]
    fn every_cursor_kind_matches_its_golden_bytes() {
        use crate::test_support::assert_matches_golden_token;
        use loonfs_types::{
            ChangeSeq, FileRevisionsPageCursor, GrepPageCursor, InodeId, RevisionNo,
            TrashPageCursor,
        };
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let checkpoint = CheckpointPageCursor {
            namespace_id: namespace_id.clone(),
            last_key: "namespaces/demo/pins/pin_00000000000000000001-0000000000000001.json"
                .to_owned(),
        };
        let cursors = [
            (
                "directory.txt",
                encode_cursor(&DirectoryPageCursor {
                    head_seq: ChangeSeq(12),
                    pin_id: None,
                    directory_inode_id: InodeId(1),
                    last_name_key: NameKey::parse("report.txt").expect("name key"),
                }),
            ),
            (
                "file_revisions.txt",
                encode_cursor(&FileRevisionsPageCursor {
                    head_seq: ChangeSeq(12),
                    inode_id: InodeId(42),
                    last_revision_no: RevisionNo(3),
                    last_committed_seq: ChangeSeq(9),
                    last_revision_delta_index: 2,
                }),
            ),
            (
                "trash.txt",
                encode_cursor(&TrashPageCursor {
                    head_seq: ChangeSeq(12),
                    last_deletion_seq: ChangeSeq(8),
                    last_root_inode_id: InodeId(42),
                }),
            ),
            (
                "grep.txt",
                encode_cursor(&GrepPageCursor {
                    head_seq: ChangeSeq(12),
                    last_inode_id: InodeId(42),
                    last_byte_offset: 1024,
                    fingerprint: 7,
                }),
            ),
            ("checkpoint_inventory.txt", encode_cursor(&checkpoint)),
            (
                "snapshot_inventory.txt",
                encode_cursor(&SnapshotPageCursor::from(checkpoint.clone())),
            ),
        ];
        for (name, token) in cursors {
            assert_matches_golden_token(name, &token.expect("encode cursor"));
        }
    }

    #[test]
    fn cursor_is_operation_bound() {
        let directory = DirectoryPageCursor {
            head_seq: loonfs_types::ChangeSeq(1),
            pin_id: None,
            directory_inode_id: loonfs_types::InodeId(1),
            last_name_key: NameKey::parse("entry").expect("name key"),
        };
        let token = encode_cursor(&directory).expect("encode directory cursor");

        decode_cursor::<CheckpointPageCursor>(&token)
            .expect_err("a directory cursor should not resume a checkpoint listing");
    }
}
