//! Lists active checkpoint records for a namespace.
//!
//! The listing reads the `checkpoints/` prefix and excludes released records.

use super::record::load_checkpoint_record_at_key;
use crate::control_object::{core_control_load_error, ControlObjectLoadError};
use crate::error::{CoreError, Result};
use crate::namespace::control::load_head_object;
use futures::StreamExt;
use loonfs_api::wire::control::CheckpointStatus;
use loonfs_api::{Checkpoint, NamespaceCursor, NamespaceId, Page, PageCursor, PageRequest};
use loonfs_objectstore::keys::checkpoint_prefix;
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};

/// Resume position for a checkpoint listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointPageCursor {
    namespace_id: NamespaceId,
    last_key: String,
}

impl PageCursor for CheckpointPageCursor {
    const KIND: &'static str = "checkpoint_inventory";
}

impl NamespaceCursor for CheckpointPageCursor {
    fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    fn last_key(&self) -> Option<&str> {
        Some(&self.last_key)
    }

    fn key_prefix(&self) -> String {
        checkpoint_prefix(&self.namespace_id)
    }
}

impl CheckpointPageCursor {
    fn after(namespace_id: &NamespaceId, last_key: String) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            last_key,
        }
    }

    fn validate_for(&self, namespace_id: &NamespaceId) -> Result<()> {
        if &self.namespace_id != namespace_id {
            return Err(CoreError::InvalidCursor(
                "cursor belongs to a different namespace".to_owned(),
            ));
        }
        if !self.last_key.starts_with(&checkpoint_prefix(namespace_id)) {
            return Err(CoreError::InvalidCursor(
                "cursor names a key outside the checkpoint inventory".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Lists active checkpoints in ascending checkpoint-id order.
///
/// Expired checkpoints remain active until collection releases them, so they
/// are included. Released checkpoints are omitted. Fork-owned records are
/// included because they also pin data. The namespace head is read first so
/// a missing namespace does not look like an empty checkpoint list.
pub(crate) async fn list_checkpoints_page<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: PageRequest<CheckpointPageCursor>,
) -> Result<Page<Checkpoint, CheckpointPageCursor>> {
    load_head_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?;

    if let Some(cursor) = &request.cursor {
        cursor.validate_for(namespace_id)?;
    }

    let prefix = checkpoint_prefix(namespace_id);
    let start_after = request
        .cursor
        .as_ref()
        .map(|cursor| cursor.last_key.as_str());
    let keys = store
        .list_prefix_from_stream(&prefix, start_after)
        .peekable();
    futures::pin_mut!(keys);
    let mut checkpoints = Vec::with_capacity(request.limit.as_usize());
    let mut last_inspected_key = None;
    while checkpoints.len() < request.limit.as_usize() {
        let Some(item) = keys.next().await else {
            break;
        };
        let key = item.map_err(|error| CoreError::store(&prefix, &error))?;
        last_inspected_key = Some(key.clone());
        // Skip records deleted after the key listing was read.
        let loaded = match load_checkpoint_record_at_key(store, &key).await {
            Ok(loaded) => loaded,
            Err(ControlObjectLoadError::MissingObject { .. }) => continue,
            Err(error) => return Err(core_control_load_error(error)),
        };
        if loaded.state.status != (CheckpointStatus::Active {}) {
            continue;
        }
        let record = loaded.state;
        checkpoints.push(super::checkpoint_summary(record));
    }
    let has_more = if checkpoints.len() == request.limit.as_usize() {
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
    let next_cursor = has_more.then(|| {
        CheckpointPageCursor::after(
            namespace_id,
            last_inspected_key.expect("a full page should inspect at least one key"),
        )
    });

    Ok(Page {
        items: checkpoints,
        next_cursor,
    })
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use loonfs_api::{decode_namespace_cursor, encode_cursor, DirectoryPageCursor, NameKey};

    #[test]
    fn cursor_decode_tolerates_additive_fields() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let token = loonfs_api::wire::hex::hex_encode_bytes(
            &serde_json::to_vec(&serde_json::json!({
                "format_version": 1,
                "kind": "checkpoint_inventory",
                "namespace_id": "demo",
                "last_key": "namespaces/demo/checkpoints/chk_00000000000000000000000000000001.json",
                "future_field": {"ignored": true}
            }))
            .expect("encode cursor"),
        );

        let cursor = decode_namespace_cursor::<CheckpointPageCursor>(&token, &namespace_id)
            .expect("decode cursor with additive field");
        assert_eq!(
            cursor.last_key(),
            Some("namespaces/demo/checkpoints/chk_00000000000000000000000000000001.json")
        );
    }

    #[test]
    fn cursor_is_operation_bound() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let directory = DirectoryPageCursor {
            head_seq: loonfs_api::ChangeSeq(1),
            directory_inode_id: loonfs_api::InodeId(1),
            last_name_key: NameKey::parse("entry").expect("name key"),
        };
        let token = encode_cursor(&directory).expect("encode directory cursor");

        assert!(decode_namespace_cursor::<CheckpointPageCursor>(&token, &namespace_id).is_err());
    }
}
