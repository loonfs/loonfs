//! Snapshot and checkpoint shapes for the v0 HTTP API.

use crate::{ChangeSeq, ManifestNo, NamespaceId, PinId};
use serde::{Deserialize, Serialize};

/// A request to create a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateSnapshotRequest {
    /// The snapshot's label, which doesn't need to be unique.
    pub name: String,
    /// The snapshot's lifetime in milliseconds, counted from the server's current time. Omit it to
    /// keep the snapshot until it's deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub ttl_ms: Option<u64>,
}

/// A request to extend a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ExtendSnapshotRequest {
    /// The new lifetime in milliseconds, counted from the server's current time.
    pub ttl_ms: u64,
}

/// The owner of a checkpoint record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointOwner {
    /// A snapshot, which a caller created. It's deleted by its ID or after it expires.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerUser"))]
    User {
        /// The label that the creator recorded. Labels don't need to be unique.
        name: String,
    },
    /// A pin that keeps the source state that one fork attempt starts from.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerFork"))]
    Fork {
        /// The fork's target namespace. Garbage collection keeps this checkpoint while that
        /// namespace exists.
        target_namespace_id: NamespaceId,
    },
    /// An owner kind from a newer server. This variant doesn't include the kind's own fields.
    #[serde(other)]
    Unknown,
}

/// One checkpoint, as its durable record stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Checkpoint {
    /// The namespace that the checkpoint belongs to.
    pub namespace_id: NamespaceId,
    /// The checkpoint's durable ID. For a user pin, it's also the snapshot's `snapshot_id`.
    pub checkpoint_id: PinId,
    /// The checkpoint's owner. For a user pin, the owner includes the label.
    pub owner: CheckpointOwner,
    /// The time the checkpoint record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The time the checkpoint expires, in Unix milliseconds. Garbage collection waits one more
    /// grace window before it deletes the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expires_at_ms: Option<u64>,
    /// The namespace sequence that the checkpoint captured.
    pub captured_seq: ChangeSeq,
    /// The manifest that the checkpoint pins.
    pub manifest_no: ManifestNo,
}

/// A snapshot, which keeps one version of a namespace readable until it's deleted or expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Snapshot {
    /// The snapshot ID.
    pub snapshot_id: PinId,
    /// The namespace whose state the snapshot captured.
    pub namespace_id: NamespaceId,
    /// The snapshot's label.
    pub name: String,
    /// The namespace sequence that the snapshot captured.
    pub captured_seq: ChangeSeq,
    /// The time the snapshot record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The time the snapshot expires, in Unix milliseconds. A snapshot without it doesn't expire.
    /// An expired snapshot can't be read, and it's listed until garbage collection deletes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expires_at_ms: Option<u64>,
}

impl Snapshot {
    /// Reports whether the snapshot can still be read at `now_ms`.
    pub fn is_live(&self, now_ms: u64) -> bool {
        self.expires_at_ms
            .is_none_or(|expires_at_ms| expires_at_ms > now_ms)
    }
}

/// One page of checkpoint records, including expired records that garbage collection hasn't deleted
/// yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListCheckpointsResponse {
    /// The namespace that the records belong to.
    pub namespace_id: NamespaceId,
    /// The records, in ascending checkpoint ID order.
    pub checkpoints: Vec<Checkpoint>,
    /// An opaque cursor for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// One page of snapshots, including expired snapshots that garbage collection hasn't deleted yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListSnapshotsResponse {
    /// The namespace that the snapshots belong to.
    pub namespace_id: NamespaceId,
    /// The snapshot records, in ascending snapshot ID order.
    pub snapshots: Vec<Snapshot>,
    /// An opaque cursor for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// The snapshot record that was deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteSnapshotResponse {
    /// The namespace that the snapshot belonged to.
    pub namespace_id: NamespaceId,
    /// The ID of the deleted snapshot record.
    pub snapshot_id: PinId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_responses_use_one_checkpoint_wire_object() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let checkpoint = Checkpoint {
            namespace_id: namespace_id.clone(),
            checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000001")
                .expect("checkpoint id"),
            owner: CheckpointOwner::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: Some(1_752_626_600_000),
            captured_seq: ChangeSeq(12),
            manifest_no: ManifestNo(9),
        };
        let checkpoint_json = serde_json::json!({
            "namespace_id": "demo",
            "checkpoint_id": "pin_00000000000000000001-0000000000000001",
            "owner": {"kind": "user", "name": "release"},
            "created_at_ms": 1_752_623_000_000_u64,
            "expires_at_ms": 1_752_626_600_000_u64,
            "captured_seq": 12,
            "manifest_no": 9,
        });
        assert_eq!(
            serde_json::to_value(checkpoint.clone()).expect("serialize checkpoint"),
            checkpoint_json,
        );
        assert_eq!(
            serde_json::to_value(ListCheckpointsResponse {
                namespace_id: namespace_id.clone(),
                checkpoints: vec![checkpoint.clone()],
                next_cursor: None,
            })
            .expect("serialize list checkpoints response"),
            serde_json::json!({
                "namespace_id": "demo",
                "checkpoints": [checkpoint_json],
            }),
        );
    }

    #[test]
    fn optional_checkpoint_fields_are_omitted_and_default_when_absent() {
        let checkpoint_json = serde_json::to_value(Checkpoint {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            checkpoint_id: PinId::parse("pin_00000000000000000001-0000000000000001")
                .expect("checkpoint id"),
            owner: CheckpointOwner::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: None,
            captured_seq: ChangeSeq(3),
            manifest_no: ManifestNo(3),
        })
        .expect("serialize checkpoint");
        assert!(checkpoint_json.get("expires_at_ms").is_none());
        let checkpoint: Checkpoint = serde_json::from_value(checkpoint_json)
            .expect("decode checkpoint without optional fields");
        assert_eq!(checkpoint.expires_at_ms, None);
    }

    #[test]
    fn a_checkpoint_owner_from_a_newer_server_decodes_as_unknown() {
        let checkpoint: Checkpoint = serde_json::from_value(serde_json::json!({
            "namespace_id": "demo",
            "checkpoint_id": "pin_00000000000000000001-0000000000000001",
            "owner": {"kind": "retention_policy", "policy_id": "nightly"},
            "created_at_ms": 1,
            "captured_seq": 3,
            "manifest_no": 3
        }))
        .expect("a newer owner decodes");
        assert_eq!(checkpoint.owner, CheckpointOwner::Unknown);
    }

    #[test]
    fn create_snapshot_requests_are_strict() {
        serde_json::from_value::<CreateSnapshotRequest>(
            serde_json::json!({"name": "nightly", "ttl_ms": 60_000}),
        )
        .expect("the same snapshot body without a typo decodes");
        serde_json::from_value::<CreateSnapshotRequest>(serde_json::json!({"name": "nightly"}))
            .expect("a snapshot body without a lifetime decodes");
        serde_json::from_value::<CreateSnapshotRequest>(
            serde_json::json!({"name": "nightly", "ttlMs": 60_000}),
        )
        .expect_err("a camel-case field should be rejected");
    }
}
