//! Opaque, namespace-bound cursors for bounded GC enumeration.

use crate::error::{CoreError, Result};
use base64::Engine as _;
use loonfs_api::NamespaceId;
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_prefix, metadata_table_prefix, upload_session_prefix,
    wal_segment_prefix,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CandidateFamily {
    WalSegments,
    MetadataTables,
    Manifests,
    Checkpoints,
    UploadSessions,
}

impl CandidateFamily {
    pub(super) const ALL: [Self; 5] = [
        Self::WalSegments,
        Self::MetadataTables,
        Self::Manifests,
        Self::Checkpoints,
        Self::UploadSessions,
    ];

    pub(super) fn index(self) -> usize {
        match self {
            Self::WalSegments => 0,
            Self::MetadataTables => 1,
            Self::Manifests => 2,
            Self::Checkpoints => 3,
            Self::UploadSessions => 4,
        }
    }

    pub(super) fn prefix(self, namespace_id: &NamespaceId) -> String {
        match self {
            Self::WalSegments => wal_segment_prefix(namespace_id.as_str()),
            Self::MetadataTables => metadata_table_prefix(namespace_id.as_str()),
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
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| invalid_cursor())?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())?;
        if cursor.namespace_id != *namespace_id {
            return Err(CoreError::InvalidGcConfig(
                "cursor belongs to a different namespace".to_owned(),
            ));
        }
        if cursor
            .last_key
            .as_ref()
            .is_some_and(|key| !key.starts_with(&cursor.family.prefix(namespace_id)))
        {
            return Err(invalid_cursor());
        }
        Ok(cursor)
    }

    pub(super) fn encode(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| CoreError::Internal(format!("failed to encode GC cursor: {error}")))?;
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }
}

fn invalid_cursor() -> CoreError {
    CoreError::InvalidGcConfig("cursor is malformed".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_decode_tolerates_additive_fields() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let payload = serde_json::json!({
            "namespace_id": "demo",
            "family": "metadata_tables",
            "last_key": "namespaces/demo/metadata/tables/table.sst.zst",
            "future_field": {"ignored": true}
        });
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).expect("encode payload"));

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

        let malformed = serde_json::json!({
            "namespace_id": "demo",
            "family": "wal_segments",
            "last_key": "namespaces/demo/checkpoints/checkpoint.json"
        });
        let malformed = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&malformed).expect("encode malformed payload"));
        assert!(GcCursor::decode(&malformed, &namespace_id).is_err());
    }
}
