//! Namespace requests and responses for the v0 HTTP API.

use crate::{ActorId, ChangeSeq, ManifestNo, NamespaceAccess, NamespaceId, PinId, PrincipalScope};
use serde::{Deserialize, Serialize};

/// A request to create a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateNamespaceRequest {
    /// The namespace's access mode, which can't be changed after the namespace is created. The
    /// default is unrestricted.
    #[serde(default = "NamespaceAccess::unrestricted")]
    pub access: NamespaceAccess,
    /// The durable ID of the new namespace.
    pub namespace_id: NamespaceId,
    /// The namespace's retention window, in milliseconds, which must be positive. The reference
    /// server's maintenance sweep then uses this window for the namespace instead of the server's
    /// `retention_window_ms` setting. A `retention_window` maintenance run can change or clear the
    /// window later.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention_window_ms: Option<u64>,
}

/// A request to fork a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ForkNamespaceRequest {
    /// The durable ID of the new namespace that the fork creates.
    pub new_namespace_id: NamespaceId,
    /// An unexpired snapshot to fork from. Without it, the fork starts from the current head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub snapshot_id: Option<PinId>,
}

/// The current state of one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceMetadata {
    /// The namespace's access mode.
    pub access: NamespaceAccessMode,
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
    pub head_seq: ChangeSeq,
    /// The namespace's retention floor. A change feed request can set `after_seq` to this sequence
    /// or any later one, and the feed returns the changes committed after it.
    pub retention_floor_seq: ChangeSeq,
    /// The namespace's retention window, in milliseconds. Only a namespace with its own window has
    /// this field. The reference server's maintenance sweep uses the server's `retention_window_ms`
    /// setting for a namespace that has no window of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention_window_ms: Option<u64>,
}

/// A namespace's access mode, without the grants that the namespace was created with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NamespaceAccessMode {
    /// Every caller with the deployment credential can perform every operation.
    Unrestricted {},
    /// Access rows control the operations that principals in this identity domain can perform.
    Acl {
        /// The identity domain that the namespace's principal IDs belong to.
        principal_scope: PrincipalScope,
    },
}

impl From<&NamespaceAccess> for NamespaceAccessMode {
    fn from(access: &NamespaceAccess) -> Self {
        match access {
            NamespaceAccess::Unrestricted {} => Self::Unrestricted {},
            NamespaceAccess::Acl {
                principal_scope, ..
            } => Self::Acl {
                principal_scope: principal_scope.clone(),
            },
        }
    }
}

/// The source namespace and sequence that a forked namespace started from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceForkBasis {
    /// The namespace that the fork was taken from.
    pub source_namespace_id: NamespaceId,
    /// The source namespace's sequence that the fork captured.
    pub source_head_seq: ChangeSeq,
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
    pub head_seq: ChangeSeq,
    /// The namespace's retention floor. A change feed request can set `after_seq` to this sequence
    /// or any later one, and the feed returns the changes committed after it.
    pub retention_floor_seq: ChangeSeq,
    /// The highest retention floor that a WAL object folded into the current manifest was
    /// validated against. It's at most `retention_floor_seq`, and it stays below a new floor until
    /// a WAL object validated against that floor is folded.
    pub acknowledged_floor_seq: ChangeSeq,
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
    pub head_seq: ChangeSeq,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_wire_shape_has_only_core_state() {
        let namespace = NamespaceMetadata {
            access: NamespaceAccessMode::Unrestricted {},
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
            head_seq: ChangeSeq(11),
            retention_floor_seq: ChangeSeq(4),
            retention_window_ms: None,
        };
        assert_eq!(
            serde_json::to_value(namespace.clone()).expect("serialize namespace"),
            serde_json::json!({
                "namespace_id": "demo",
                "access": {"kind": "unrestricted"},
                "created_at_ms": 1000,
                "created_by": "test",
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
    fn a_namespace_access_mode_from_a_newer_server_decodes_with_an_unknown_field() {
        let mode: NamespaceAccessMode = serde_json::from_value(serde_json::json!({
            "kind": "acl",
            "principal_scope": "corp",
            "added_by_a_newer_server": true
        }))
        .expect("a response accepts unknown fields");
        assert_eq!(
            mode,
            NamespaceAccessMode::Acl {
                principal_scope: PrincipalScope::parse("corp").expect("principal scope"),
            }
        );
    }

    #[test]
    fn namespace_diagnostics_wire_shape_keeps_storage_fields() {
        let diagnostics = NamespaceDiagnostics {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            fork_basis: None,
            head_seq: ChangeSeq(11),
            retention_floor_seq: ChangeSeq(4),
            acknowledged_floor_seq: ChangeSeq(2),
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
                "acknowledged_floor_seq": 2,
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
            Some(86_400_000)
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
