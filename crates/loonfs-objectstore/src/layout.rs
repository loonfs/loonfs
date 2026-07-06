use crate::ObjectStoreError;
use loonfs_api::{ManifestId, ManifestObjectId};

#[derive(Debug, Clone, Copy, Default)]
pub struct ObjectLayout;

/// Durable object families under the subsystem-owned namespace grammar:
///
/// ```text
/// {subsystem}/{role}.json                    small control/pointer objects
/// {subsystem}/{collection}/{id}.json         per-id JSON records
/// {subsystem}/{collection}/{id}.{kind}.zst   compressed immutable payloads
/// ```
///
/// Paths express ownership, not authority: envelopes still validate ids,
/// families, and checksums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableObjectFamily {
    NamespaceConfig,
    WalHead,
    WalFloor,
    WalIndex,
    WalIndexRun,
    WalSegment,
    MetadataRoot,
    MetadataManifest,
    MetadataTable,
    CheckpointRecord,
    Pin,
    UploadSession,
    ContentStoreDescriptor,
    ContentBlob,
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

typed_key!(NamespaceConfigKey);
typed_key!(WalHeadKey);
typed_key!(WalFloorKey);
typed_key!(WalIndexKey);
typed_key!(WalIndexRunKey);
typed_key!(WalSegmentKey);
typed_key!(MetadataRootKey);
typed_key!(MetadataManifestKey);
typed_key!(MetadataTableKey);
typed_key!(CheckpointRecordKey);
typed_key!(PinKey);
typed_key!(UploadSessionKey);
typed_key!(ContentStoreDescriptorKey);
typed_key!(ContentBlobKey);

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

    /// Stable namespace identity and immutable configuration; written last
    /// during bootstrap as the namespace completion marker.
    pub fn namespace_config(&self, namespace: &str) -> NamespaceConfigKey {
        NamespaceConfigKey(ObjectKey::new(format!(
            "namespaces/{namespace}/namespace.json"
        )))
    }

    /// Hot head of the semantic commit stream: the only object whose CAS
    /// gates user-write throughput.
    pub fn wal_head(&self, namespace: &str) -> WalHeadKey {
        WalHeadKey(ObjectKey::new(format!(
            "namespaces/{namespace}/wal/head.json"
        )))
    }

    /// Cold lower bound of retained WAL/change history.
    pub fn wal_floor(&self, namespace: &str) -> WalFloorKey {
        WalFloorKey(ObjectKey::new(format!(
            "namespaces/{namespace}/wal/floor.json"
        )))
    }

    /// Optional mutable pointer to the newest WAL index run (accelerator,
    /// never authority). Reserved: parseable and buildable ahead of the
    /// accelerator subsystem landing.
    pub fn wal_index(&self, namespace: &str) -> WalIndexKey {
        WalIndexKey(ObjectKey::new(format!(
            "namespaces/{namespace}/wal/index.json"
        )))
    }

    /// Optional immutable run of visible-chain segment pointers. Reserved.
    pub fn wal_index_run(&self, namespace: &str, index_id: &str) -> WalIndexRunKey {
        WalIndexRunKey(ObjectKey::new(format!(
            "namespaces/{namespace}/wal/indexes/{index_id}.json"
        )))
    }

    pub fn wal_segment(&self, namespace: &str, segment_id: &str) -> WalSegmentKey {
        WalSegmentKey(ObjectKey::new(format!(
            "namespaces/{namespace}/wal/segments/{segment_id}.wal.zst"
        )))
    }

    /// Listing prefix that contains every WAL segment of `namespace` and
    /// nothing else: `wal/head.json`, `wal/floor.json`, and index objects
    /// live outside it, so a GC listing yields only segment keys.
    ///
    /// Segment file names start with the segment's 20-digit `start_seq` as
    /// an operator/GC convenience; no protocol depends on listing order.
    pub fn wal_segment_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/wal/segments/")
    }

    /// Extracts the WAL segment id from a listed object key.
    ///
    /// Returns `None` for keys that are not current-format WAL segments, so
    /// listings can skip foreign objects.
    pub fn wal_segment_id_from_key<'a>(&self, key: &'a str) -> Option<&'a str> {
        let (_, file_name) = key.rsplit_once('/')?;
        file_name.strip_suffix(".wal.zst")
    }

    /// Cold pointer to the best known materialized metadata root.
    pub fn metadata_root(&self, namespace: &str) -> MetadataRootKey {
        MetadataRootKey(ObjectKey::new(format!(
            "namespaces/{namespace}/metadata/root.json"
        )))
    }

    pub fn metadata_manifest(
        &self,
        namespace: &str,
        manifest_id: ManifestId,
    ) -> MetadataManifestKey {
        MetadataManifestKey(ObjectKey::new(format!(
            "namespaces/{namespace}/metadata/manifests/{:020}.json",
            manifest_id.0
        )))
    }

    pub fn metadata_manifest_object(
        &self,
        namespace: &str,
        manifest_object_id: &ManifestObjectId,
    ) -> MetadataManifestKey {
        MetadataManifestKey(ObjectKey::new(format!(
            "namespaces/{namespace}/metadata/manifests/{}.manifest.json",
            manifest_object_id.as_str()
        )))
    }

    pub fn metadata_manifest_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/metadata/manifests/")
    }

    pub fn metadata_table(&self, namespace: &str, table_id: &str) -> MetadataTableKey {
        MetadataTableKey(ObjectKey::new(format!(
            "namespaces/{namespace}/metadata/tables/{table_id}.sst.zst"
        )))
    }

    pub fn metadata_table_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/metadata/tables/")
    }

    /// Durable stable-view pin to a metadata manifest.
    pub fn checkpoint_record(&self, namespace: &str, checkpoint_id: &str) -> CheckpointRecordKey {
        CheckpointRecordKey(ObjectKey::new(format!(
            "namespaces/{namespace}/checkpoints/{checkpoint_id}.json"
        )))
    }

    pub fn checkpoint_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/checkpoints/")
    }

    /// Explicit reachability pin, stored under the **source** namespace —
    /// the namespace whose objects it protects.
    pub fn pin(&self, source_namespace: &str, pin_id: &str) -> PinKey {
        PinKey(ObjectKey::new(format!(
            "namespaces/{source_namespace}/pins/{pin_id}.json"
        )))
    }

    pub fn pin_prefix(&self, source_namespace: &str) -> String {
        format!("namespaces/{source_namespace}/pins/")
    }

    pub fn upload_session(&self, namespace: &str, upload_id: &str) -> UploadSessionKey {
        UploadSessionKey(ObjectKey::new(format!(
            "namespaces/{namespace}/uploads/{upload_id}.json"
        )))
    }

    pub fn upload_session_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/uploads/")
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
        ["namespaces", namespace, "namespace.json"] => {
            parsed(DurableObjectFamily::NamespaceConfig, Some(namespace))
        }
        ["namespaces", namespace, "wal", "head.json"] => {
            parsed(DurableObjectFamily::WalHead, Some(namespace))
        }
        ["namespaces", namespace, "wal", "floor.json"] => {
            parsed(DurableObjectFamily::WalFloor, Some(namespace))
        }
        ["namespaces", namespace, "wal", "index.json"] => {
            parsed(DurableObjectFamily::WalIndex, Some(namespace))
        }
        ["namespaces", namespace, "wal", "indexes", run] if run.ends_with(".json") => {
            parsed(DurableObjectFamily::WalIndexRun, Some(namespace))
        }
        ["namespaces", namespace, "wal", "segments", segment] if segment.ends_with(".wal.zst") => {
            parsed(DurableObjectFamily::WalSegment, Some(namespace))
        }
        ["namespaces", namespace, "metadata", "root.json"] => {
            parsed(DurableObjectFamily::MetadataRoot, Some(namespace))
        }
        ["namespaces", namespace, "metadata", "manifests", manifest]
            if manifest.ends_with(".json") =>
        {
            parsed(DurableObjectFamily::MetadataManifest, Some(namespace))
        }
        ["namespaces", namespace, "metadata", "tables", table] if table.ends_with(".sst.zst") => {
            parsed(DurableObjectFamily::MetadataTable, Some(namespace))
        }
        ["namespaces", namespace, "checkpoints", checkpoint] if checkpoint.ends_with(".json") => {
            parsed(DurableObjectFamily::CheckpointRecord, Some(namespace))
        }
        ["namespaces", namespace, "pins", pin] if pin.ends_with(".json") => {
            parsed(DurableObjectFamily::Pin, Some(namespace))
        }
        ["namespaces", namespace, "uploads", upload] if upload.ends_with(".json") => {
            parsed(DurableObjectFamily::UploadSession, Some(namespace))
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
        return Err(ObjectStoreError::InvalidKey {
            object_key: digest.to_owned(),
            message: "digest must start with `sha256:`".to_owned(),
        });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ObjectStoreError::InvalidKey {
            object_key: digest.to_owned(),
            message: "digest must be 64 hex characters".to_owned(),
        });
    }
    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(ObjectStoreError::InvalidKey {
            object_key: digest.to_owned(),
            message: "digest hex must be lowercase".to_owned(),
        });
    }
    Ok(hex)
}

#[cfg(test)]
mod tests {
    use super::{parse_object_key, sha256_hex_from_digest, DurableObjectFamily, ObjectLayout};
    use loonfs_api::{ManifestId, ManifestObjectId};

    #[test]
    fn layout_golden_tree_matches_target_paths() {
        let layout = ObjectLayout::new();

        assert_eq!(layout.namespace_root_prefix("ns-1"), "namespaces/ns-1/");
        assert_eq!(
            layout.namespace_config("ns-1").as_str(),
            "namespaces/ns-1/namespace.json"
        );
        assert_eq!(
            layout.wal_head("ns-1").as_str(),
            "namespaces/ns-1/wal/head.json"
        );
        assert_eq!(
            layout.wal_floor("ns-1").as_str(),
            "namespaces/ns-1/wal/floor.json"
        );
        assert_eq!(
            layout.wal_index("ns-1").as_str(),
            "namespaces/ns-1/wal/index.json"
        );
        assert_eq!(
            layout
                .wal_index_run("ns-1", "idx_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/wal/indexes/idx_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .wal_segment("ns-1", "seg_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/wal/segments/seg_00000000000000000000000000000001.wal.zst"
        );
        assert_eq!(
            layout.metadata_root("ns-1").as_str(),
            "namespaces/ns-1/metadata/root.json"
        );
        assert_eq!(
            layout.metadata_manifest("ns-1", ManifestId(400)).as_str(),
            "namespaces/ns-1/metadata/manifests/00000000000000000400.json"
        );
        let manifest_object_id = ManifestObjectId::parse("00000000000000000400-0123456789abcdef")
            .expect("valid manifest object id");
        assert_eq!(
            layout
                .metadata_manifest_object("ns-1", &manifest_object_id)
                .as_str(),
            "namespaces/ns-1/metadata/manifests/00000000000000000400-0123456789abcdef.manifest.json"
        );
        assert_eq!(
            layout
                .metadata_table("ns-1", "tbl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/metadata/tables/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            layout
                .checkpoint_record("ns-1", "chk_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/checkpoints/chk_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .pin("source-ns", "pin_00000000000000000000000000000001")
                .as_str(),
            "namespaces/source-ns/pins/pin_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .upload_session("ns-1", "upl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .content_store_descriptor("cs_00000000000000000000000000000001")
                .as_str(),
            "content-stores/cs_00000000000000000000000000000001/descriptor.json"
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
    fn control_objects_live_outside_the_segment_listing_prefix() {
        let layout = ObjectLayout::new();
        let prefix = layout.wal_segment_prefix("ns-1");
        assert_eq!(prefix, "namespaces/ns-1/wal/segments/");
        assert!(!layout.wal_head("ns-1").as_str().starts_with(&prefix));
        assert!(!layout.wal_floor("ns-1").as_str().starts_with(&prefix));
        assert!(!layout.wal_index("ns-1").as_str().starts_with(&prefix));
        assert!(!layout
            .wal_index_run("ns-1", "idx_1")
            .as_str()
            .starts_with(&prefix));
        assert!(layout
            .wal_segment("ns-1", "seg_1")
            .as_str()
            .starts_with(&prefix));
    }

    #[test]
    fn parse_build_round_trips_for_namespace_key_families() {
        let layout = ObjectLayout::new();
        let cases = [
            (
                layout.namespace_config("ns-1").into_string(),
                DurableObjectFamily::NamespaceConfig,
            ),
            (
                layout.wal_head("ns-1").into_string(),
                DurableObjectFamily::WalHead,
            ),
            (
                layout.wal_floor("ns-1").into_string(),
                DurableObjectFamily::WalFloor,
            ),
            (
                layout.wal_index("ns-1").into_string(),
                DurableObjectFamily::WalIndex,
            ),
            (
                layout
                    .wal_index_run("ns-1", "idx_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::WalIndexRun,
            ),
            (
                layout
                    .wal_segment("ns-1", "seg_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::WalSegment,
            ),
            (
                layout.metadata_root("ns-1").into_string(),
                DurableObjectFamily::MetadataRoot,
            ),
            (
                layout
                    .metadata_manifest("ns-1", ManifestId(1))
                    .into_string(),
                DurableObjectFamily::MetadataManifest,
            ),
            (
                layout
                    .metadata_manifest_object(
                        "ns-1",
                        &ManifestObjectId::parse("00000000000000000001-0123456789abcdef")
                            .expect("valid manifest object id"),
                    )
                    .into_string(),
                DurableObjectFamily::MetadataManifest,
            ),
            (
                layout.metadata_table("ns-1", "tbl_abc").into_string(),
                DurableObjectFamily::MetadataTable,
            ),
            (
                layout
                    .checkpoint_record("ns-1", "chk_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::CheckpointRecord,
            ),
            (
                layout
                    .pin("ns-1", "pin_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::Pin,
            ),
            (
                layout
                    .upload_session("ns-1", "upl_00000000000000000000000000000001")
                    .into_string(),
                DurableObjectFamily::UploadSession,
            ),
        ];

        for (key, family) in cases {
            let parsed = parse_object_key(&key).expect("known namespace key parses");
            assert_eq!(parsed.family(), family);
            assert_eq!(parsed.owner_namespace_id(), Some("ns-1"));
        }
    }

    #[test]
    fn parser_rejects_retired_layout_paths() {
        for old in [
            "namespaces/ns-1/descriptor.json",
            "namespaces/ns-1/control/head.json",
            "namespaces/ns-1/control/lease.json",
            "namespaces/ns-1/wal/seg_00000000000000000000000000000001.wal.zst",
            "namespaces/ns-1/manifest/00000000000000000400.manifest.json",
            "namespaces/ns-1/tables/metadata/tbl_abc.sst.zst",
            "namespaces/ns-1/gc/manifest.boundary.json",
            "namespaces/ns-1/gc/pins/pin_00000000000000000000000000000001.json",
        ] {
            assert!(
                parse_object_key(old).is_none(),
                "retired path parsed: {old}"
            );
        }
    }

    #[test]
    fn parse_wal_segment_requires_current_wal_suffix() {
        let parsed = parse_object_key(
            "namespaces/ns-1/wal/segments/seg_00000000000000000000000000000001.wal.zst",
        )
        .expect("current WAL key parses");
        assert_eq!(parsed.family(), DurableObjectFamily::WalSegment);
        assert_eq!(parsed.owner_namespace_id(), Some("ns-1"));

        assert!(parse_object_key(
            "namespaces/ns-1/wal/segments/seg_00000000000000000000000000000001.sst"
        )
        .is_none());
        assert!(parse_object_key("namespaces/ns-1/wal/segments/random.tmp").is_none());
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
