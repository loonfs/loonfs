use crate::keys::DerivedWorkClass;
use crate::ObjectStoreError;
use loonfs_api::ManifestId;

#[derive(Debug, Clone, Copy, Default)]
pub struct ObjectLayout;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableObjectFamily {
    NamespaceDescriptor,
    NamespaceHead,
    NamespaceForkState,
    UploadSession,
    ConflictArtifact,
    NamespaceManifest,
    WalSegment,
    NamespaceCompactions,
    CompactedMetadataSst,
    CompactedIndexSst,
    IndexManifest,
    IndexGcBoundary,
    NamespaceGcBoundary,
    GcPin,
    ContentStoreDescriptor,
    ContentBlob,
    DerivedProgress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedObjectKey<'a> {
    family: DurableObjectFamily,
    owner_namespace_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectKey(String);

macro_rules! typed_key {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(ObjectKey);

        impl $name {
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }

            pub fn into_string(self) -> String {
                self.0.into_string()
            }
        }

        impl From<$name> for String {
            fn from(key: $name) -> Self {
                key.into_string()
            }
        }
    };
}

typed_key!(NamespaceDescriptorKey);
typed_key!(NamespaceHeadKey);
typed_key!(NamespaceForkStateKey);
typed_key!(WalSegmentKey);
typed_key!(ContentStoreDescriptorKey);
typed_key!(ContentBlobKey);
typed_key!(ConflictArtifactKey);
typed_key!(UploadSessionKey);
typed_key!(NamespaceManifestKey);
typed_key!(NamespaceCompactionsKey);
typed_key!(CompactedMetadataSstKey);
typed_key!(CompactedIndexSstKey);
typed_key!(IndexManifestKey);
typed_key!(IndexGcBoundaryKey);
typed_key!(NamespaceGcBoundaryKey);
typed_key!(GcPinKey);
typed_key!(DerivedProgressKey);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceGcBoundaryKind {
    Wal,
    Manifest,
    Compaction,
}

impl NamespaceGcBoundaryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Manifest => "manifest",
            Self::Compaction => "compaction",
        }
    }
}

impl<'a> ParsedObjectKey<'a> {
    pub fn family(&self) -> DurableObjectFamily {
        self.family
    }

    pub fn owner_namespace_id(&self) -> Option<&'a str> {
        self.owner_namespace_id
    }
}

impl ObjectKey {
    fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    fn as_str(&self) -> &str {
        &self.0
    }

    fn into_string(self) -> String {
        self.0
    }
}

impl ObjectLayout {
    pub fn new() -> Self {
        Self
    }

    pub fn namespace_root_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/")
    }

    pub fn namespace_descriptor(&self, namespace: &str) -> NamespaceDescriptorKey {
        NamespaceDescriptorKey(ObjectKey::new(format!(
            "namespaces/{namespace}/descriptor.json"
        )))
    }

    pub fn namespace_head(&self, namespace: &str) -> NamespaceHeadKey {
        NamespaceHeadKey(ObjectKey::new(format!(
            "namespaces/{namespace}/control/head.json"
        )))
    }

    pub fn namespace_fork_state(&self, namespace: &str) -> NamespaceForkStateKey {
        NamespaceForkStateKey(ObjectKey::new(format!(
            "namespaces/{namespace}/control/fork.json"
        )))
    }

    pub fn wal_segment(&self, namespace: &str, wal_file_id: &str) -> WalSegmentKey {
        WalSegmentKey(ObjectKey::new(format!(
            "namespaces/{namespace}/wal/{wal_file_id}.wal.zst"
        )))
    }

    /// Listing prefix that contains every WAL segment of `namespace`.
    ///
    /// Segment file names start with the segment's 20-digit `start_seq`, so
    /// a listing under this prefix orders by history position.
    pub fn wal_segment_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/wal/")
    }

    /// Extracts the WAL segment id from a listed object key.
    ///
    /// Returns `None` for keys that are not current-format WAL segments, so
    /// listings can skip boundary markers and foreign objects.
    pub fn wal_segment_id_from_key<'a>(&self, key: &'a str) -> Option<&'a str> {
        let (_, file_name) = key.rsplit_once('/')?;
        file_name.strip_suffix(".wal.zst")
    }

    pub fn content_store_descriptor(&self, content_store: &str) -> ContentStoreDescriptorKey {
        ContentStoreDescriptorKey(ObjectKey::new(format!(
            "content-stores/{content_store}/descriptor.json"
        )))
    }

    pub fn content_blob(
        &self,
        content_store: &str,
        digest: &str,
    ) -> Result<ContentBlobKey, ObjectStoreError> {
        let hex = sha256_hex_from_digest(digest)?;
        Ok(ContentBlobKey(ObjectKey::new(format!(
            "content-stores/{content_store}/blobs/sha256/{}/{}/{}",
            &hex[0..2],
            &hex[2..4],
            hex
        ))))
    }

    pub fn conflict_artifact(&self, namespace: &str, conflict_id: &str) -> ConflictArtifactKey {
        ConflictArtifactKey(ObjectKey::new(format!(
            "namespaces/{namespace}/conflicts/{conflict_id}.json"
        )))
    }

    pub fn conflict_artifact_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/conflicts/")
    }

    pub fn upload_session(&self, namespace: &str, upload_id: &str) -> UploadSessionKey {
        UploadSessionKey(ObjectKey::new(format!(
            "namespaces/{namespace}/uploads/{upload_id}.json"
        )))
    }

    pub fn upload_session_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/uploads/")
    }

    pub fn namespace_manifest(
        &self,
        namespace: &str,
        manifest_id: ManifestId,
    ) -> NamespaceManifestKey {
        NamespaceManifestKey(ObjectKey::new(format!(
            "namespaces/{namespace}/manifest/{:020}.manifest.json",
            manifest_id.0
        )))
    }

    pub fn namespace_compactions(
        &self,
        namespace: &str,
        compactions_id: u64,
    ) -> NamespaceCompactionsKey {
        NamespaceCompactionsKey(ObjectKey::new(format!(
            "namespaces/{namespace}/compaction/{compactions_id:020}.plan.json"
        )))
    }

    pub fn metadata_sst(&self, namespace: &str, table_id: &str) -> CompactedMetadataSstKey {
        CompactedMetadataSstKey(ObjectKey::new(format!(
            "namespaces/{namespace}/tables/metadata/{table_id}.sst.zst"
        )))
    }

    pub fn compacted_metadata_sst(
        &self,
        namespace: &str,
        table_id: &str,
    ) -> CompactedMetadataSstKey {
        CompactedMetadataSstKey(ObjectKey::new(format!(
            "namespaces/{namespace}/tables/metadata/{table_id}.sst.zst"
        )))
    }

    pub fn compacted_index_sst(
        &self,
        namespace: &str,
        family: &str,
        instance: &str,
        table_id: &str,
    ) -> CompactedIndexSstKey {
        CompactedIndexSstKey(ObjectKey::new(format!(
            "namespaces/{namespace}/indexes/{family}/{instance}/compacted/{table_id}.sst.zst"
        )))
    }

    pub fn index_manifest(
        &self,
        namespace: &str,
        family: &str,
        instance: &str,
        manifest_id: ManifestId,
    ) -> IndexManifestKey {
        IndexManifestKey(ObjectKey::new(format!(
            "namespaces/{namespace}/indexes/{family}/{instance}/manifest/{:020}.manifest.json",
            manifest_id.0
        )))
    }

    pub fn index_gc_boundary(
        &self,
        namespace: &str,
        family: &str,
        instance: &str,
    ) -> IndexGcBoundaryKey {
        IndexGcBoundaryKey(ObjectKey::new(format!(
            "namespaces/{namespace}/indexes/{family}/{instance}/gc/manifest.boundary.json"
        )))
    }

    pub fn namespace_gc_boundary(
        &self,
        namespace: &str,
        boundary: NamespaceGcBoundaryKind,
    ) -> NamespaceGcBoundaryKey {
        NamespaceGcBoundaryKey(ObjectKey::new(format!(
            "namespaces/{namespace}/gc/{}.boundary.json",
            boundary.as_str()
        )))
    }

    pub fn gc_pin(&self, source_namespace: &str, pin_id: &str) -> GcPinKey {
        GcPinKey(ObjectKey::new(format!(
            "namespaces/{source_namespace}/gc/pins/{pin_id}.json"
        )))
    }

    pub fn derived_progress(
        &self,
        namespace: &str,
        work_class: DerivedWorkClass,
    ) -> DerivedProgressKey {
        let work_class = work_class.as_str();
        DerivedProgressKey(ObjectKey::new(format!(
            "namespaces/{namespace}/derived/{work_class}/progress.json"
        )))
    }
}

pub fn parse_object_key(key: &str) -> Option<ParsedObjectKey<'_>> {
    let segments: Vec<_> = key.split('/').collect();
    match segments.as_slice() {
        ["content-stores", _, "descriptor.json"] => {
            parsed(DurableObjectFamily::ContentStoreDescriptor, None)
        }
        ["content-stores", _, "blobs", "sha256", _, _, _] => {
            parsed(DurableObjectFamily::ContentBlob, None)
        }
        ["namespaces", namespace, "descriptor.json"] => {
            parsed(DurableObjectFamily::NamespaceDescriptor, Some(namespace))
        }
        ["namespaces", namespace, "control", "head.json"] => {
            parsed(DurableObjectFamily::NamespaceHead, Some(namespace))
        }
        ["namespaces", namespace, "control", "fork.json"] => {
            parsed(DurableObjectFamily::NamespaceForkState, Some(namespace))
        }
        ["namespaces", namespace, "uploads", upload] if upload.ends_with(".json") => {
            parsed(DurableObjectFamily::UploadSession, Some(namespace))
        }
        ["namespaces", namespace, "conflicts", conflict] if conflict.ends_with(".json") => {
            parsed(DurableObjectFamily::ConflictArtifact, Some(namespace))
        }
        ["namespaces", namespace, "manifest", manifest] if manifest.ends_with(".manifest.json") => {
            parsed(DurableObjectFamily::NamespaceManifest, Some(namespace))
        }
        ["namespaces", namespace, "wal", wal] if wal.ends_with(".wal.zst") => {
            parsed(DurableObjectFamily::WalSegment, Some(namespace))
        }
        ["namespaces", namespace, "compaction", compactions]
            if compactions.ends_with(".plan.json") =>
        {
            parsed(DurableObjectFamily::NamespaceCompactions, Some(namespace))
        }
        ["namespaces", namespace, "tables", "metadata", table] if table.ends_with(".sst.zst") => {
            parsed(DurableObjectFamily::CompactedMetadataSst, Some(namespace))
        }
        ["namespaces", namespace, "indexes", _, _, "compacted", table]
            if table.ends_with(".sst.zst") =>
        {
            parsed(DurableObjectFamily::CompactedIndexSst, Some(namespace))
        }
        ["namespaces", namespace, "indexes", _, _, "manifest", manifest]
            if manifest.ends_with(".manifest.json") =>
        {
            parsed(DurableObjectFamily::IndexManifest, Some(namespace))
        }
        ["namespaces", namespace, "indexes", _, _, "gc", "manifest.boundary.json"] => {
            parsed(DurableObjectFamily::IndexGcBoundary, Some(namespace))
        }
        ["namespaces", namespace, "gc", boundary]
            if matches!(
                *boundary,
                "wal.boundary.json" | "manifest.boundary.json" | "compaction.boundary.json"
            ) =>
        {
            parsed(DurableObjectFamily::NamespaceGcBoundary, Some(namespace))
        }
        ["namespaces", namespace, "gc", "pins", pin] if pin.ends_with(".json") => {
            parsed(DurableObjectFamily::GcPin, Some(namespace))
        }
        ["namespaces", namespace, "derived", .., "progress.json"] => {
            parsed(DurableObjectFamily::DerivedProgress, Some(namespace))
        }
        _ => None,
    }
}

fn parsed(
    family: DurableObjectFamily,
    owner_namespace_id: Option<&str>,
) -> Option<ParsedObjectKey<'_>> {
    Some(ParsedObjectKey {
        family,
        owner_namespace_id,
    })
}

pub fn sha256_hex_from_digest(digest: &str) -> Result<&str, ObjectStoreError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(ObjectStoreError::InvalidKey(digest.to_owned()));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ObjectStoreError::InvalidKey(digest.to_owned()));
    }
    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(ObjectStoreError::InvalidKey(digest.to_owned()));
    }
    Ok(hex)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_object_key, sha256_hex_from_digest, DurableObjectFamily, NamespaceGcBoundaryKind,
        ObjectLayout,
    };
    use crate::keys::DerivedWorkClass;
    use loonfs_api::ManifestId;

    #[test]
    fn layout_golden_tree_matches_expected_paths() {
        let layout = ObjectLayout::new();

        assert_eq!(layout.namespace_root_prefix("ns-1"), "namespaces/ns-1/");
        assert_eq!(
            layout.namespace_descriptor("ns-1").as_str(),
            "namespaces/ns-1/descriptor.json"
        );
        assert_eq!(
            layout.namespace_head("ns-1").as_str(),
            "namespaces/ns-1/control/head.json"
        );
        assert_eq!(
            layout.namespace_fork_state("ns-1").as_str(),
            "namespaces/ns-1/control/fork.json"
        );
        assert_eq!(
            layout
                .content_store_descriptor("cs_00000000000000000000000000000001")
                .as_str(),
            "content-stores/cs_00000000000000000000000000000001/descriptor.json"
        );
        assert_eq!(
            layout
                .wal_segment("ns-1", "seg_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/wal/seg_00000000000000000000000000000001.wal.zst"
        );
        assert_eq!(
            layout.namespace_manifest("ns-1", ManifestId(400)).as_str(),
            "namespaces/ns-1/manifest/00000000000000000400.manifest.json"
        );
        assert_eq!(
            layout.namespace_manifest("ns-1", ManifestId(42)).as_str(),
            "namespaces/ns-1/manifest/00000000000000000042.manifest.json"
        );
        assert_eq!(
            layout.namespace_compactions("ns-1", 42).as_str(),
            "namespaces/ns-1/compaction/00000000000000000042.plan.json"
        );
        assert_eq!(
            layout
                .metadata_sst("ns-1", "tbl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/tables/metadata/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            layout
                .compacted_metadata_sst("ns-1", "tbl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/tables/metadata/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            layout
                .compacted_index_sst(
                    "ns-1",
                    "grep",
                    "default",
                    "tbl_00000000000000000000000000000002",
                )
                .as_str(),
            "namespaces/ns-1/indexes/grep/default/compacted/tbl_00000000000000000000000000000002.sst.zst"
        );
        assert_eq!(
            layout
                .index_manifest("ns-1", "grep", "default", ManifestId(17))
                .as_str(),
            "namespaces/ns-1/indexes/grep/default/manifest/00000000000000000017.manifest.json"
        );
        assert_eq!(
            layout.index_gc_boundary("ns-1", "grep", "default").as_str(),
            "namespaces/ns-1/indexes/grep/default/gc/manifest.boundary.json"
        );
        assert_eq!(
            layout
                .namespace_gc_boundary("ns-1", NamespaceGcBoundaryKind::Manifest)
                .as_str(),
            "namespaces/ns-1/gc/manifest.boundary.json"
        );
        assert_eq!(
            layout
                .gc_pin("source-ns", "pin_00000000000000000000000000000001")
                .as_str(),
            "namespaces/source-ns/gc/pins/pin_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .derived_progress("ns-1", DerivedWorkClass::ManifestBuilder)
                .as_str(),
            "namespaces/ns-1/derived/manifest-builder/progress.json"
        );
        assert_eq!(
            layout
                .upload_session("ns-1", "upl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .content_blob(
                    "cs_00000000000000000000000000000001",
                    "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                )
                .expect("content key")
                .as_str(),
            "content-stores/cs_00000000000000000000000000000001/blobs/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn parse_build_round_trips_for_namespace_key_families() {
        let layout = ObjectLayout::new();
        let cases = [
            (
                layout.namespace_descriptor("ns-1").into_string(),
                DurableObjectFamily::NamespaceDescriptor,
            ),
            (
                layout.namespace_head("ns-1").into_string(),
                DurableObjectFamily::NamespaceHead,
            ),
            (
                layout.namespace_fork_state("ns-1").into_string(),
                DurableObjectFamily::NamespaceForkState,
            ),
            (
                layout
                    .upload_session("ns-1", "upl_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::UploadSession,
            ),
            (
                layout
                    .conflict_artifact("ns-1", "conflict-deadbeef")
                    .into_string(),
                DurableObjectFamily::ConflictArtifact,
            ),
            (
                layout
                    .namespace_manifest("ns-1", ManifestId(1))
                    .into_string(),
                DurableObjectFamily::NamespaceManifest,
            ),
            (
                layout
                    .wal_segment("ns-1", "seg_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::WalSegment,
            ),
            (
                layout.namespace_compactions("ns-1", 1).into_string(),
                DurableObjectFamily::NamespaceCompactions,
            ),
            (
                layout
                    .compacted_metadata_sst("ns-1", "tbl_abc")
                    .into_string(),
                DurableObjectFamily::CompactedMetadataSst,
            ),
            (
                layout
                    .compacted_index_sst("ns-1", "grep", "default", "tbl_abc")
                    .into_string(),
                DurableObjectFamily::CompactedIndexSst,
            ),
            (
                layout
                    .index_manifest("ns-1", "grep", "default", ManifestId(1))
                    .into_string(),
                DurableObjectFamily::IndexManifest,
            ),
            (
                layout
                    .index_gc_boundary("ns-1", "grep", "default")
                    .into_string(),
                DurableObjectFamily::IndexGcBoundary,
            ),
            (
                layout
                    .namespace_gc_boundary("ns-1", NamespaceGcBoundaryKind::Compaction)
                    .into_string(),
                DurableObjectFamily::NamespaceGcBoundary,
            ),
            (
                layout
                    .gc_pin("ns-1", "pin_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::GcPin,
            ),
            (
                layout
                    .derived_progress("ns-1", DerivedWorkClass::ManifestBuilder)
                    .into_string(),
                DurableObjectFamily::DerivedProgress,
            ),
        ];

        for (key, family) in cases {
            let parsed = parse_object_key(&key).expect("known namespace key parses");
            assert_eq!(parsed.family(), family);
            assert_eq!(parsed.owner_namespace_id(), Some("ns-1"));
        }
    }

    #[test]
    fn parse_wal_segment_requires_current_wal_suffix() {
        let parsed =
            parse_object_key("namespaces/ns-1/wal/seg_00000000000000000000000000000001.wal.zst")
                .expect("current WAL key parses");
        assert_eq!(parsed.family(), DurableObjectFamily::WalSegment);
        assert_eq!(parsed.owner_namespace_id(), Some("ns-1"));

        assert!(
            parse_object_key("namespaces/ns-1/wal/seg_00000000000000000000000000000001.sst")
                .is_none()
        );
        assert!(parse_object_key("namespaces/ns-1/wal/random.tmp").is_none());
    }

    #[test]
    fn parse_build_round_trips_for_global_key_families() {
        let layout = ObjectLayout::new();
        let content_key = layout
            .content_blob(
                "cs_00000000000000000000000000000001",
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
            )
            .expect("content key")
            .into_string();
        let cases = [
            (
                layout
                    .content_store_descriptor("cs_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::ContentStoreDescriptor,
            ),
            (content_key, DurableObjectFamily::ContentBlob),
        ];

        for (key, family) in cases {
            let parsed = parse_object_key(&key).expect("known global key parses");
            assert_eq!(parsed.family(), family);
            assert_eq!(parsed.owner_namespace_id(), None);
        }

        assert!(parse_object_key("namespaces/ns-1/unknown/file").is_none());
    }

    #[test]
    fn content_blob_rejects_invalid_sha256_digest() {
        assert!(sha256_hex_from_digest("sha1:abcdef").is_err());
        assert!(sha256_hex_from_digest("sha256:abcd").is_err());
        assert!(sha256_hex_from_digest(
            "sha256:ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )
        .is_err());
        assert!(ObjectLayout::new()
            .content_blob("ns-1", "sha256:not-hex")
            .is_err());
    }
}
