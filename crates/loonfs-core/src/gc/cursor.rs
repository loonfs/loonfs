//! Opaque, namespace-bound cursors for bounded GC enumeration.

use crate::error::{CoreError, Result};
use loonfs_api::{
    decode_namespace_cursor, encode_cursor, NamespaceCursor, NamespaceCursorError, NamespaceId,
    PageCursor,
};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_compaction_prefix, metadata_manifest_prefix, metadata_table_prefix,
    upload_session_prefix, wal_segment_prefix,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CandidateFamily {
    WalSegments,
    MetadataTables,
    /// Everything one streaming compaction job owns: the segments it wrote
    /// and the lease that says it is still running. Its own family because
    /// its objects are decided by that lease rather than by age alone — a job
    /// outlives the ordinary grace window, so its output would otherwise read
    /// as an aged orphan while it was still being written.
    CompactionStaging,
    Manifests,
    Checkpoints,
    UploadSessions,
}

impl CandidateFamily {
    pub(super) const ALL: [Self; 6] = [
        Self::WalSegments,
        Self::MetadataTables,
        Self::CompactionStaging,
        Self::Manifests,
        Self::Checkpoints,
        Self::UploadSessions,
    ];

    /// This family's position in [`Self::ALL`], which is the sweep order.
    pub(super) fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|family| *family == self)
            .expect("every family is listed in ALL")
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId) -> String {
        match self {
            Self::WalSegments => wal_segment_prefix(namespace_id.as_str()),
            Self::MetadataTables => metadata_table_prefix(namespace_id.as_str()),
            Self::CompactionStaging => metadata_compaction_prefix(namespace_id.as_str()),
            Self::Manifests => metadata_manifest_prefix(namespace_id.as_str()),
            Self::Checkpoints => checkpoint_prefix(namespace_id.as_str()),
            Self::UploadSessions => upload_session_prefix(namespace_id.as_str()),
        }
    }
}

/// Cursor payloads are short-lived API tokens, not durable objects. Serde's
/// default unknown-field handling makes decoding tolerant of additive fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct GcCursor {
    namespace_id: NamespaceId,
    pub(super) family: CandidateFamily,
    #[serde(default)]
    pub(super) last_key: Option<String>,
}

impl PageCursor for GcCursor {
    const KIND: &'static str = "core_gc";
}

impl NamespaceCursor for GcCursor {
    fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    fn last_key(&self) -> Option<&str> {
        self.last_key.as_deref()
    }

    fn key_prefix(&self) -> String {
        self.family.prefix(&self.namespace_id)
    }
}

impl GcCursor {
    pub(super) fn initial(namespace_id: &NamespaceId) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            family: CandidateFamily::WalSegments,
            last_key: None,
        }
    }

    pub(super) fn after(namespace_id: &NamespaceId, family: CandidateFamily, key: String) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            family,
            last_key: Some(key),
        }
    }

    pub(super) fn decode(token: &str, namespace_id: &NamespaceId) -> Result<Self> {
        decode_namespace_cursor(token, namespace_id).map_err(|error| match error {
            NamespaceCursorError::ForeignNamespace => CoreError::InvalidGcConfig(error.to_string()),
            NamespaceCursorError::Malformed(_) | NamespaceCursorError::OutsideKeyspace => {
                invalid_cursor()
            }
        })
    }

    pub(super) fn encode(&self) -> Result<String> {
        encode_cursor(self)
            .map_err(|error| CoreError::Internal(format!("failed to encode GC cursor: {error}")))
    }
}

fn invalid_cursor() -> CoreError {
    CoreError::InvalidGcConfig("cursor is malformed".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::wire::hex::hex_encode_bytes;

    fn token_over(payload: &serde_json::Value) -> String {
        hex_encode_bytes(&serde_json::to_vec(payload).expect("encode payload"))
    }

    #[test]
    fn cursor_decode_tolerates_additive_fields() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let token = token_over(&serde_json::json!({
            "v": 1,
            "kind": "core_gc",
            "namespace_id": "demo",
            "family": "metadata_tables",
            "last_key": "namespaces/demo/metadata/tables/table.sst.zst",
            "future_field": {"ignored": true}
        }));

        let cursor = GcCursor::decode(&token, &namespace_id).expect("decode cursor");
        assert_eq!(cursor.family, CandidateFamily::MetadataTables);
        assert_eq!(
            cursor.last_key.as_deref(),
            Some("namespaces/demo/metadata/tables/table.sst.zst")
        );
    }

    #[test]
    fn cursor_is_bound_to_its_namespace_and_family_prefix() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let other_namespace_id = NamespaceId::parse("other").expect("namespace id");
        let cursor = GcCursor::after(
            &namespace_id,
            CandidateFamily::WalSegments,
            "namespaces/demo/wal/segments/segment.wal.zst".to_owned(),
        );
        let token = cursor.encode().expect("encode cursor");

        assert!(GcCursor::decode(&token, &namespace_id).is_ok());
        assert!(GcCursor::decode(&token, &other_namespace_id).is_err());

        let wrong_family_prefix = token_over(&serde_json::json!({
            "v": 1,
            "kind": "core_gc",
            "namespace_id": "demo",
            "family": "wal_segments",
            "last_key": "namespaces/demo/checkpoints/checkpoint.json"
        }));
        assert!(GcCursor::decode(&wrong_family_prefix, &namespace_id).is_err());
    }

    /// Core and grep collect the same namespace under two cursors of the
    /// same shape. The kind is what stops one job resuming the other's
    /// position.
    #[test]
    fn a_cursor_from_another_job_is_refused() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let foreign_kind = token_over(&serde_json::json!({
            "v": 1,
            "kind": "grep_gc",
            "namespace_id": "demo",
            "family": "wal_segments",
            "last_key": "namespaces/demo/wal/segments/segment.wal.zst"
        }));

        assert!(GcCursor::decode(&foreign_kind, &namespace_id).is_err());
    }
}
