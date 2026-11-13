//! Snapshot and snapshot record shapes for the v0 HTTP API.

use crate::{CommitSeq, ManifestNo, NamespaceId, SnapshotId};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

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
    #[cfg_attr(
        feature = "openapi",
        schema(value_type = Option<u64>, nullable = false, minimum = 1)
    )]
    pub ttl_ms: Option<NonZeroU64>,
}

/// A request to extend a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ExtendSnapshotRequest {
    /// The new lifetime in milliseconds, counted from the server's current time.
    #[cfg_attr(feature = "openapi", schema(value_type = u64, minimum = 1))]
    pub ttl_ms: NonZeroU64,
}

/// The owner of a snapshot record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotRecordOwner {
    /// A snapshot, which a caller created. It's deleted by its ID or after it expires.
    #[cfg_attr(feature = "openapi", schema(title = "SnapshotRecordOwnerUser"))]
    User {
        /// The label that the creator recorded. Labels don't need to be unique.
        name: String,
    },
    /// A snapshot that keeps the source state that one fork attempt starts from.
    #[cfg_attr(feature = "openapi", schema(title = "SnapshotRecordOwnerFork"))]
    Fork {
        /// The fork's target namespace. Garbage collection keeps this snapshot while that
        /// namespace exists.
        target_namespace_id: NamespaceId,
    },
    /// An owner kind from a newer server. This variant doesn't include the kind's own fields.
    #[serde(other)]
    Unknown,
}

/// One snapshot, as its durable record stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SnapshotRecord {
    /// The namespace that the snapshot belongs to.
    pub namespace_id: NamespaceId,
    /// The snapshot's durable ID.
    pub snapshot_id: SnapshotId,
    /// The snapshot's owner. For a user snapshot, the owner includes the label.
    pub owner: SnapshotRecordOwner,
    /// The time the snapshot record was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The time the snapshot expires, in Unix milliseconds. Garbage collection waits one more
    /// grace window before it deletes the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub expires_at_ms: Option<u64>,
    /// The namespace sequence that the snapshot captured.
    pub captured_seq: CommitSeq,
    /// The manifest that the snapshot keeps.
    pub manifest_no: ManifestNo,
}

/// A snapshot, which keeps one version of a namespace readable until it's deleted or expires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Snapshot {
    /// The snapshot ID.
    pub snapshot_id: SnapshotId,
    /// The namespace whose state the snapshot captured.
    pub namespace_id: NamespaceId,
    /// The snapshot's label.
    pub name: String,
    /// The namespace sequence that the snapshot captured.
    pub captured_seq: CommitSeq,
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

/// One page of snapshot records, including expired records that garbage collection hasn't deleted
/// yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListSnapshotRecordsResponse {
    /// The namespace that the records belong to.
    pub namespace_id: NamespaceId,
    /// The records, in ascending snapshot ID order.
    pub snapshots: Vec<SnapshotRecord>,
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
    pub snapshot_id: SnapshotId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_record_responses_use_one_snapshot_record_wire_object() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let snapshot = SnapshotRecord {
            namespace_id: namespace_id.clone(),
            snapshot_id: SnapshotId::parse("snp_00000000000000000001-00000000000000000000000001")
                .expect("snapshot id"),
            owner: SnapshotRecordOwner::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: Some(1_752_626_600_000),
            captured_seq: CommitSeq(12),
            manifest_no: ManifestNo(9),
        };
        let snapshot_json = serde_json::json!({
            "namespace_id": "demo",
            "snapshot_id": "snp_00000000000000000001-00000000000000000000000001",
            "owner": {"kind": "user", "name": "release"},
            "created_at_ms": 1_752_623_000_000_u64,
            "expires_at_ms": 1_752_626_600_000_u64,
            "captured_seq": 12,
            "manifest_no": 9,
        });
        assert_eq!(
            serde_json::to_value(snapshot.clone()).expect("serialize snapshot"),
            snapshot_json,
        );
        assert_eq!(
            serde_json::to_value(ListSnapshotRecordsResponse {
                namespace_id: namespace_id.clone(),
                snapshots: vec![snapshot.clone()],
                next_cursor: None,
            })
            .expect("serialize list snapshots response"),
            serde_json::json!({
                "namespace_id": "demo",
                "snapshots": [snapshot_json],
            }),
        );
    }

    #[test]
    fn optional_snapshot_record_fields_are_omitted_and_default_when_absent() {
        let snapshot_json = serde_json::to_value(SnapshotRecord {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            snapshot_id: SnapshotId::parse("snp_00000000000000000001-00000000000000000000000001")
                .expect("snapshot id"),
            owner: SnapshotRecordOwner::User {
                name: "release".to_owned(),
            },
            created_at_ms: 1_752_623_000_000,
            expires_at_ms: None,
            captured_seq: CommitSeq(3),
            manifest_no: ManifestNo(3),
        })
        .expect("serialize snapshot");
        assert!(snapshot_json.get("expires_at_ms").is_none());
        let snapshot: SnapshotRecord =
            serde_json::from_value(snapshot_json).expect("decode snapshot without optional fields");
        assert_eq!(snapshot.expires_at_ms, None);
    }

    #[test]
    fn a_snapshot_record_owner_from_a_newer_server_decodes_as_unknown() {
        let snapshot: SnapshotRecord = serde_json::from_value(serde_json::json!({
            "namespace_id": "demo",
            "snapshot_id": "snp_00000000000000000001-00000000000000000000000001",
            "owner": {"kind": "retention_policy", "policy_id": "nightly"},
            "created_at_ms": 1,
            "captured_seq": 3,
            "manifest_no": 3
        }))
        .expect("a newer owner decodes");
        assert_eq!(snapshot.owner, SnapshotRecordOwner::Unknown);
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
