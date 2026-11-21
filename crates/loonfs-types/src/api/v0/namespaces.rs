//! Namespace requests and responses for the v0 HTTP API.

use crate::{ActorId, CommitSeq, ManifestNo, NamespaceId, PoolId, SnapshotId};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// A request to create a namespace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateNamespaceRequest {
    /// The durable ID of the new namespace. Without this field, LoonFS generates an ID. The
    /// response's `namespace_id` is the new namespace's ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub namespace_id: Option<NamespaceId>,
    /// The namespace's retention window, in milliseconds, which must be at least the grace window
    /// (86400000, which is 24 hours). The reference server's maintenance sweep uses this window
    /// for the namespace. Without this field, the namespace's window is the server's default
    /// window, if the server has one, which the capability document reports as
    /// `namespace.default_retention_window_ms`. A `retention_window` maintenance job can change
    /// or clear the window later.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "openapi",
        schema(value_type = Option<u64>, nullable = false, minimum = 86400000)
    )]
    pub retention_window_ms: Option<NonZeroU64>,
}

// The schema declares the smallest retention window as a literal, because the
// `ToSchema` derive takes only a literal bound.
const _: () = assert!(crate::limits::GC_GRACE_WINDOW_MS == 86_400_000);

/// A request to fork a namespace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ForkNamespaceRequest {
    /// The durable ID of the new namespace that the fork creates. Without this field, LoonFS
    /// generates an ID. The response's `namespace_id` is the new namespace's ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub new_namespace_id: Option<NamespaceId>,
    /// An unexpired snapshot to fork from. Without it, the fork starts from the current head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub snapshot_id: Option<SnapshotId>,
}

/// The current state of one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceMetadata {
    /// The namespace ID.
    pub namespace_id: NamespaceId,
    /// The time the namespace was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The actor that created the namespace, as the application supplied it.
    pub created_by: ActorId,
    /// The namespace and sequence this namespace was forked from. Only a forked namespace has this
    /// field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fork_basis: Option<NamespaceForkBasis>,
    /// The pool that stores the namespace's content objects and upload records. A fork has the
    /// same pool as its source.
    pub pool_id: PoolId,
    /// The sequence of the namespace's newest visible commit.
    pub head_seq: CommitSeq,
    /// The namespace's retention floor. A change feed request can set `after_seq` to this sequence
    /// or any later one, and the feed returns the changes committed after it.
    pub retention_floor_seq: CommitSeq,
    /// The namespace's retention window, in milliseconds. Only a namespace with a window has this
    /// field. The reference server's maintenance sweep doesn't advance the floor of a namespace
    /// without a window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention_window_ms: Option<u64>,
}

/// One page of the namespaces in a store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListNamespacesResponse {
    /// The namespaces, in object-key order, which is namespace ID order except that an ID sorts
    /// after the longer IDs that extend it with `-` or `.`. Deleted namespaces and namespaces whose
    /// creation hasn't finished are left out, so a page can have fewer namespaces than its limit,
    /// even none, and still have a `next_cursor`.
    pub namespaces: Vec<NamespaceMetadata>,
    /// An opaque cursor for the next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// The source namespace and sequence that a forked namespace started from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceForkBasis {
    /// The namespace that the fork was taken from.
    pub source_namespace_id: NamespaceId,
    /// The source namespace's sequence that the fork captured.
    pub fork_point_seq: CommitSeq,
    /// The fork point snapshot, which keeps this namespace's state at `fork_point_seq` readable.
    /// That state is the source's state at the fork point. A read of this namespace accepts the
    /// ID as `snapshot_id`.
    pub fork_point_snapshot_id: SnapshotId,
    /// The fork whose unmerged commits a rebase replays into this namespace. Only a namespace that
    /// a rebase created has this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub rebased_from: Option<NamespaceRebasedFrom>,
}

/// The fork whose unmerged commits a rebase replays into the namespace that the rebase created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceRebasedFrom {
    /// The fork whose commits the namespace's replay commits apply.
    pub source_namespace_id: NamespaceId,
    /// The fork sequence that the fork's next merge would have started after when the rebase
    /// created the namespace. The namespace's first replay commit starts after it.
    pub resume_after_seq: CommitSeq,
}

/// A namespace's state and the storage details that maintenance uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceDiagnostics {
    /// The namespace ID.
    pub namespace_id: NamespaceId,
    /// The time the namespace was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The actor that created the namespace, as the application supplied it.
    pub created_by: ActorId,
    /// The namespace and sequence this namespace was forked from. Only a forked namespace has this
    /// field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub fork_basis: Option<NamespaceForkBasis>,
    /// The sequence of the namespace's newest visible commit.
    pub head_seq: CommitSeq,
    /// The namespace's retention floor. A change feed request can set `after_seq` to this sequence
    /// or any later one, and the feed returns the changes committed after it.
    pub retention_floor_seq: CommitSeq,
    /// The number of the namespace's current manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub current_manifest_no: Option<ManifestNo>,
    /// The number of visible WAL objects after the current manifest.
    pub wal_tail_objects: u64,
    /// The number of snapshots that hadn't expired when the diagnostics read began.
    pub live_snapshots: u64,
}

/// The result of deleting a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteNamespaceResponse {
    /// The deleted namespace.
    pub namespace_id: NamespaceId,
    /// The last committed sequence before the namespace was deleted.
    pub head_seq: CommitSeq,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_wire_shape_has_only_core_state() {
        let namespace = NamespaceMetadata {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
            pool_id: crate::PoolId::parse("pool_3s0drmj8em84yb4mnetqkz2j0x").expect("pool id"),
            head_seq: CommitSeq(11),
            retention_floor_seq: CommitSeq(4),
            retention_window_ms: None,
        };
        assert_eq!(
            serde_json::to_value(namespace.clone()).expect("serialize namespace"),
            serde_json::json!({
                "namespace_id": "demo",
                "created_at_ms": 1000,
                "created_by": "test",
                "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x",
                "head_seq": 11,
                "retention_floor_seq": 4
            })
        );
        let windowed = NamespaceMetadata {
            retention_window_ms: Some(86_400_000),
            ..namespace
        };
        assert_eq!(
            serde_json::to_value(windowed).expect("serialize namespace")["retention_window_ms"],
            86_400_000,
            "the metadata of a namespace with its own window includes retention_window_ms"
        );
    }

    #[test]
    fn a_namespace_listing_includes_next_cursor_only_when_another_page_exists() {
        let namespace = NamespaceMetadata {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
            pool_id: crate::PoolId::parse("pool_3s0drmj8em84yb4mnetqkz2j0x").expect("pool id"),
            head_seq: CommitSeq(11),
            retention_floor_seq: CommitSeq(4),
            retention_window_ms: None,
        };
        let last_page = ListNamespacesResponse {
            namespaces: vec![namespace.clone()],
            next_cursor: None,
        };
        assert_eq!(
            serde_json::to_value(&last_page).expect("serialize listing"),
            serde_json::json!({
                "namespaces": [serde_json::to_value(&namespace).expect("serialize namespace")]
            })
        );
        let page = ListNamespacesResponse {
            next_cursor: Some("c".to_owned()),
            ..last_page
        };
        assert_eq!(
            serde_json::to_value(page).expect("serialize listing")["next_cursor"],
            "c"
        );
    }

    #[test]
    fn namespace_diagnostics_wire_shape_keeps_storage_fields() {
        let diagnostics = NamespaceDiagnostics {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
            head_seq: CommitSeq(11),
            retention_floor_seq: CommitSeq(4),
            current_manifest_no: Some(ManifestNo(8)),
            wal_tail_objects: 3,
            live_snapshots: 2,
        };
        assert_eq!(
            serde_json::to_value(diagnostics).expect("serialize namespace diagnostics"),
            serde_json::json!({
                "namespace_id": "demo",
                "created_at_ms": 1000,
                "created_by": "test",
                "head_seq": 11,
                "retention_floor_seq": 4,
                "current_manifest_no": 8,
                "wal_tail_objects": 3,
                "live_snapshots": 2
            })
        );
    }

    #[test]
    fn create_and_fork_namespace_requests_are_strict() {
        serde_json::from_value::<CreateNamespaceRequest>(serde_json::json!({
            "namespace_id": "demo",
        }))
        .expect("the same create body without a typo decodes");
        assert_eq!(
            serde_json::from_value::<CreateNamespaceRequest>(serde_json::json!({
                "namespace_id": "demo",
                "retention_window_ms": 86_400_000
            }))
            .expect("a create body with a retention window decodes")
            .retention_window_ms,
            NonZeroU64::new(86_400_000)
        );
        serde_json::from_value::<CreateNamespaceRequest>(serde_json::json!({
            "namespace_id": "demo",
            "fork_of": "other"
        }))
        .expect_err("a fork field on a create request should be rejected");
        serde_json::from_value::<ForkNamespaceRequest>(serde_json::json!({
            "new_namespace_id": "demo",
            "source_namespace_id": "other"
        }))
        .expect_err("a source namespace field on a fork request should be rejected");
    }
}
