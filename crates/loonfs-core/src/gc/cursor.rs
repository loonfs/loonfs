//! Opaque, namespace-bound cursors for bounded GC enumeration.

use crate::error::{CoreError, Result};
use loonfs_api::{
    decode_namespace_cursor, encode_cursor, NamespaceCursor, NamespaceCursorError, NamespaceId,
    PageCursor,
};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_compaction_prefix, metadata_manifest_prefix,
    metadata_segment_prefix, upload_session_prefix, wal_segment_prefix,
};
use loonfs_objectstore::layout::manifest_object_id_of;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use serde::{Deserialize, Serialize};

/// Defines the namespace key prefix and token kind for a GC cursor.
pub trait GcCursorKeyspace {
    const CURSOR_KIND: &'static str;

    fn prefix(&self, namespace_id: &NamespaceId) -> String;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CandidateFamily {
    WalSegments,
    MetadataSegments,
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
        Self::MetadataSegments,
        Self::CompactionStaging,
        Self::Manifests,
        Self::Checkpoints,
        Self::UploadSessions,
    ];

    /// This family's position in [`Self::ALL`], which is the sweep order.
    pub(super) fn index(self) -> usize {
        match self {
            Self::WalSegments => 0,
            Self::MetadataSegments => 1,
            Self::CompactionStaging => 2,
            Self::Manifests => 3,
            Self::Checkpoints => 4,
            Self::UploadSessions => 5,
        }
    }

    /// Returns whether `key` matches this family's durable grammar.
    pub(super) fn recognizes(self, key: &str) -> bool {
        let Some(family) = parse_object_key(key).map(|parsed| parsed.family()) else {
            return false;
        };
        match self {
            Self::WalSegments => family == DurableObjectFamily::WalSegment,
            Self::MetadataSegments => family == DurableObjectFamily::MetadataSegment,
            Self::CompactionStaging => matches!(
                family,
                DurableObjectFamily::MetadataCompactionStaging
                    | DurableObjectFamily::MetadataCompactionLease
            ),
            Self::Manifests => matches!(manifest_object_id_of(key), Some(Ok(_))),
            Self::Checkpoints => family == DurableObjectFamily::CheckpointRecord,
            Self::UploadSessions => family == DurableObjectFamily::UploadSession,
        }
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId) -> String {
        match self {
            Self::WalSegments => wal_segment_prefix(namespace_id),
            Self::MetadataSegments => metadata_segment_prefix(namespace_id),
            Self::CompactionStaging => metadata_compaction_prefix(namespace_id),
            Self::Manifests => metadata_manifest_prefix(namespace_id),
            Self::Checkpoints => checkpoint_prefix(namespace_id),
            Self::UploadSessions => upload_session_prefix(namespace_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "Keyspace: Serialize",
    deserialize = "Keyspace: Deserialize<'de>"
))]
pub struct NamespaceGcCursor<Keyspace> {
    namespace_id: NamespaceId,
    #[serde(flatten)]
    keyspace: Keyspace,
    #[serde(default)]
    last_key: Option<String>,
}

impl<Keyspace> PageCursor for NamespaceGcCursor<Keyspace>
where
    Keyspace: GcCursorKeyspace + Serialize + for<'de> Deserialize<'de>,
{
    const KIND: &'static str = Keyspace::CURSOR_KIND;
}

impl<Keyspace> NamespaceCursor for NamespaceGcCursor<Keyspace>
where
    Keyspace: GcCursorKeyspace + Serialize + for<'de> Deserialize<'de>,
{
    fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    fn last_key(&self) -> Option<&str> {
        self.last_key.as_deref()
    }

    fn key_prefix(&self) -> String {
        self.keyspace.prefix(&self.namespace_id)
    }
}

impl<Keyspace> NamespaceGcCursor<Keyspace>
where
    Keyspace: GcCursorKeyspace + Serialize + for<'de> Deserialize<'de>,
{
    pub fn initial(namespace_id: &NamespaceId, keyspace: Keyspace) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            keyspace,
            last_key: None,
        }
    }

    pub fn after(namespace_id: &NamespaceId, keyspace: Keyspace, key: String) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            keyspace,
            last_key: Some(key),
        }
    }

    pub fn decode(token: &str, namespace_id: &NamespaceId) -> Result<Self> {
        decode_namespace_cursor(token, namespace_id).map_err(|error| {
            if matches!(error, NamespaceCursorError::ForeignNamespace) {
                CoreError::InvalidGcConfig(error.to_string())
            } else {
                invalid_cursor()
            }
        })
    }

    pub fn encode(&self) -> Result<String> {
        encode_cursor(self)
            .map_err(|error| CoreError::Internal(format!("failed to encode gc cursor: {error}")))
    }

    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    pub fn last_key(&self) -> Option<&str> {
        self.last_key.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CoreGcKeyspace {
    pub(super) family: CandidateFamily,
}

impl GcCursorKeyspace for CoreGcKeyspace {
    const CURSOR_KIND: &'static str = "core_gc";

    fn prefix(&self, namespace_id: &NamespaceId) -> String {
        self.family.prefix(namespace_id)
    }
}

pub(super) type GcCursor = NamespaceGcCursor<CoreGcKeyspace>;

pub(super) fn initial_cursor(namespace_id: &NamespaceId) -> GcCursor {
    GcCursor::initial(
        namespace_id,
        CoreGcKeyspace {
            family: CandidateFamily::WalSegments,
        },
    )
}

pub(super) fn cursor_after(
    namespace_id: &NamespaceId,
    family: CandidateFamily,
    key: String,
) -> GcCursor {
    GcCursor::after(namespace_id, CoreGcKeyspace { family }, key)
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
            "format_version": 1,
            "kind": "core_gc",
            "namespace_id": "demo",
            "family": "metadata_segments",
            "last_key": "namespaces/demo/metadata/segments/segment.sst.zst",
            "future_field": {"ignored": true}
        }));

        let cursor = GcCursor::decode(&token, &namespace_id).expect("decode cursor");
        assert_eq!(cursor.keyspace().family, CandidateFamily::MetadataSegments);
        assert_eq!(
            cursor.last_key(),
            Some("namespaces/demo/metadata/segments/segment.sst.zst")
        );
    }

    #[test]
    fn cursor_is_bound_to_its_namespace_and_family_prefix() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let other_namespace_id = NamespaceId::parse("other").expect("namespace id");
        let cursor = cursor_after(
            &namespace_id,
            CandidateFamily::WalSegments,
            "namespaces/demo/wal/segments/segment.wal.zst".to_owned(),
        );
        let token = cursor.encode().expect("encode cursor");

        assert!(GcCursor::decode(&token, &namespace_id).is_ok());
        assert!(GcCursor::decode(&token, &other_namespace_id).is_err());

        let wrong_family_prefix = token_over(&serde_json::json!({
            "format_version": 1,
            "kind": "core_gc",
            "namespace_id": "demo",
            "family": "wal_segments",
            "last_key": "namespaces/demo/checkpoints/checkpoint.json"
        }));
        assert!(GcCursor::decode(&wrong_family_prefix, &namespace_id).is_err());
    }

    #[test]
    fn a_cursor_from_another_job_is_refused() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let foreign_kind = token_over(&serde_json::json!({
            "format_version": 1,
            "kind": "grep_gc",
            "namespace_id": "demo",
            "family": "wal_segments",
            "last_key": "namespaces/demo/wal/segments/segment.wal.zst"
        }));

        assert!(GcCursor::decode(&foreign_kind, &namespace_id).is_err());
    }
}
