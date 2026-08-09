//! The durable key grammar: object families, their path shapes, and
//! parsing keys back into classified families.

use loonfs_api::{ContentId, ManifestObjectId};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ObjectLayout;

/// One family in the [durable object key grammar].
///
/// [durable object key grammar]: ../../../docs/specs/format.md#12-durable-object-families
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableObjectFamily {
    /// Classifies the mutable visibility and fencing head.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    WalHead,
    /// Classifies the mutable retained-history floor.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    WalFloor,
    /// Classifies an immutable segment in a namespace's WAL chain.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    WalSegment,
    /// Classifies the mutable materialized metadata pointer.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    MetadataRoot,
    /// Classifies an immutable namespace-manifest candidate.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    MetadataManifest,
    /// Classifies an immutable metadata SST segment.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    MetadataTable,
    /// Classifies an immutable metadata SST segment a streaming compaction
    /// wrote before any manifest referenced it.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    MetadataCompactionStaging,
    /// Classifies a mutable checkpoint lifecycle record.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    CheckpointRecord,
    /// Classifies a mutable upload-session lifecycle record.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    UploadSession,
    /// Classifies immutable whole-file content bytes.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    ContentBlob,
}

/// Reports the durable family and namespace ownership recoverable from a recognized key.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedObjectKey<'a> {
    family: DurableObjectFamily,
    owner_namespace_id: Option<&'a str>,
}

impl<'a> ParsedObjectKey<'a> {
    /// Returns the durable family selected by the key's path shape.
    pub fn family(&self) -> DurableObjectFamily {
        self.family
    }

    /// Returns the namespace path component, or `None` for content-store-owned families.
    pub fn owner_namespace_id(&self) -> Option<&'a str> {
        self.owner_namespace_id
    }
}

impl ObjectLayout {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn namespace_root_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/")
    }

    /// Hot head of the semantic commit stream: the only object whose CAS
    /// gates user-write throughput.
    pub(crate) fn wal_head(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/wal/head.json")
    }

    /// Cold lower bound of retained WAL/change history.
    pub(crate) fn wal_floor(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/wal/floor.json")
    }

    pub(crate) fn wal_segment(&self, namespace: &str, segment_id: &str) -> String {
        format!("namespaces/{namespace}/wal/segments/{segment_id}.wal.zst")
    }

    /// Listing prefix that contains every WAL segment of `namespace` and
    /// nothing else: `wal/head.json` and `wal/floor.json` live outside it,
    /// so a GC listing yields only segment keys.
    ///
    /// Segment file names start with the segment's 20-digit `start_seq` as
    /// an operator/GC convenience; no protocol depends on listing order.
    pub(crate) fn wal_segment_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/wal/segments/")
    }

    /// Extracts the WAL segment id from a listed object key.
    ///
    /// Returns `None` for keys that are not current-format WAL segments, so
    /// listings can skip foreign objects.
    pub(crate) fn wal_segment_id_from_key<'a>(&self, key: &'a str) -> Option<&'a str> {
        let (_, file_name) = key.rsplit_once('/')?;
        file_name.strip_suffix(".wal.zst")
    }

    /// Cold pointer to the best known materialized metadata root.
    pub(crate) fn metadata_root(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/metadata/root.json")
    }

    pub(crate) fn metadata_manifest_object(
        &self,
        namespace: &str,
        manifest_object_id: &ManifestObjectId,
    ) -> String {
        format!(
            "namespaces/{namespace}/metadata/manifests/{}.manifest.json",
            manifest_object_id.as_str()
        )
    }

    pub(crate) fn metadata_manifest_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/metadata/manifests/")
    }

    pub(crate) fn metadata_table(&self, namespace: &str, table_id: &str) -> String {
        format!("namespaces/{namespace}/metadata/tables/{table_id}.sst.zst")
    }

    pub(crate) fn metadata_table_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/metadata/tables/")
    }

    /// A metadata segment a streaming compaction has written but no manifest
    /// references yet. The object is an ordinary metadata segment; only the
    /// directory differs, which is what keeps a running job's output out of
    /// the sweep that reaps unreferenced table keys.
    pub(crate) fn metadata_compaction_staging_table(
        &self,
        namespace: &str,
        table_id: &str,
    ) -> String {
        format!("namespaces/{namespace}/metadata/compaction-staging/{table_id}.sst.zst")
    }

    pub(crate) fn metadata_compaction_staging_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/metadata/compaction-staging/")
    }

    /// Durable stable-view pin to a metadata manifest.
    pub(crate) fn checkpoint_record(&self, namespace: &str, checkpoint_id: &str) -> String {
        format!("namespaces/{namespace}/checkpoints/{checkpoint_id}.json")
    }

    pub(crate) fn checkpoint_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/checkpoints/")
    }

    pub(crate) fn upload_session(&self, namespace: &str, upload_id: &str) -> String {
        format!("namespaces/{namespace}/uploads/{upload_id}.json")
    }

    pub(crate) fn upload_session_prefix(&self, namespace: &str) -> String {
        format!("namespaces/{namespace}/uploads/")
    }

    /// Content objects shard across two directory levels selected by the
    /// content id's leading characters. Those characters are random, so
    /// ingest spreads evenly and filesystem-backed stores keep bounded
    /// directory fanout.
    pub(crate) fn content_blob(&self, content_store: &str, content_id: &ContentId) -> String {
        let [first_shard, second_shard] = content_id.shard_prefixes();
        format!(
            "content-stores/{content_store}/objects/{first_shard}/{second_shard}/{}",
            content_id.as_str()
        )
    }
}

/// Classifies a current or reserved durable object key without validating identifier text.
///
/// Returns `None` for private, foreign, or unrecognized paths. See
/// [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn parse_object_key(key: &str) -> Option<ParsedObjectKey<'_>> {
    let segments: Vec<_> = key.split('/').collect();
    match segments.as_slice() {
        ["content-stores", _, "objects", _, _, _] => parsed(DurableObjectFamily::ContentBlob, None),
        ["namespaces", namespace, "wal", "head.json"] => {
            parsed(DurableObjectFamily::WalHead, Some(namespace))
        }
        ["namespaces", namespace, "wal", "floor.json"] => {
            parsed(DurableObjectFamily::WalFloor, Some(namespace))
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
        ["namespaces", namespace, "metadata", "compaction-staging", table]
            if table.ends_with(".sst.zst") =>
        {
            parsed(
                DurableObjectFamily::MetadataCompactionStaging,
                Some(namespace),
            )
        }
        ["namespaces", namespace, "checkpoints", checkpoint] if checkpoint.ends_with(".json") => {
            parsed(DurableObjectFamily::CheckpointRecord, Some(namespace))
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

#[cfg(test)]
mod tests {
    use super::{parse_object_key, DurableObjectFamily, ObjectLayout};
    use loonfs_api::{ContentId, ManifestObjectId};

    fn content_id() -> ContentId {
        ContentId::parse("con_abcdef0123456789abcdef0123456789").expect("valid content id")
    }

    #[test]
    fn layout_golden_tree_matches_target_paths() {
        let layout = ObjectLayout::new();

        assert_eq!(layout.namespace_root_prefix("ns-1"), "namespaces/ns-1/");
        assert_eq!(
            layout.wal_head("ns-1").as_str(),
            "namespaces/ns-1/wal/head.json"
        );
        assert_eq!(
            layout.wal_floor("ns-1").as_str(),
            "namespaces/ns-1/wal/floor.json"
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
                .metadata_compaction_staging_table("ns-1", "tbl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/metadata/compaction-staging/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            layout
                .checkpoint_record("ns-1", "chk_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/checkpoints/chk_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .upload_session("ns-1", "upl_00000000000000000000000000000001")
                .as_str(),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .content_blob("cs_00000000000000000000000000000001", &content_id())
                .as_str(),
            "content-stores/cs_00000000000000000000000000000001/objects/ab/cd/con_abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn control_objects_live_outside_the_segment_listing_prefix() {
        let layout = ObjectLayout::new();
        let prefix = layout.wal_segment_prefix("ns-1");
        assert_eq!(prefix, "namespaces/ns-1/wal/segments/");
        assert!(!layout.wal_head("ns-1").as_str().starts_with(&prefix));
        assert!(!layout.wal_floor("ns-1").as_str().starts_with(&prefix));
        assert!(layout
            .wal_segment("ns-1", "seg_1")
            .as_str()
            .starts_with(&prefix));
    }

    /// A streaming compaction's staged segments must not appear in the
    /// listing that enumerates referenced metadata tables. That listing is
    /// what a collector sweeps, and a running job's output is unreferenced
    /// for as long as the job runs.
    #[test]
    fn staged_compaction_segments_live_outside_the_table_listing_prefix() {
        let layout = ObjectLayout::new();
        let tables = layout.metadata_table_prefix("ns-1");
        let staging = layout.metadata_compaction_staging_prefix("ns-1");

        assert!(!staging.starts_with(&tables));
        assert!(!layout
            .metadata_compaction_staging_table("ns-1", "tbl_abc")
            .starts_with(&tables));
        assert!(layout
            .metadata_compaction_staging_table("ns-1", "tbl_abc")
            .starts_with(&staging));
        assert!(!layout
            .metadata_table("ns-1", "tbl_abc")
            .starts_with(&staging));
    }

    #[test]
    fn parse_build_round_trips_for_namespace_key_families() {
        let layout = ObjectLayout::new();
        let cases = [
            (layout.wal_head("ns-1"), DurableObjectFamily::WalHead),
            (layout.wal_floor("ns-1"), DurableObjectFamily::WalFloor),
            (
                layout.wal_segment("ns-1", "seg_00000000000000000000000000000001"),
                DurableObjectFamily::WalSegment,
            ),
            (
                layout.metadata_root("ns-1"),
                DurableObjectFamily::MetadataRoot,
            ),
            (
                layout.metadata_manifest_object(
                    "ns-1",
                    &ManifestObjectId::parse("00000000000000000001-0123456789abcdef")
                        .expect("valid manifest object id"),
                ),
                DurableObjectFamily::MetadataManifest,
            ),
            (
                layout.metadata_table("ns-1", "tbl_abc"),
                DurableObjectFamily::MetadataTable,
            ),
            (
                layout.metadata_compaction_staging_table("ns-1", "tbl_abc"),
                DurableObjectFamily::MetadataCompactionStaging,
            ),
            (
                layout.checkpoint_record("ns-1", "chk_00000000000000000000000000000001"),
                DurableObjectFamily::CheckpointRecord,
            ),
            (
                layout.upload_session("ns-1", "upl_00000000000000000000000000000001"),
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
            "namespaces/ns-1/pins/pin_00000000000000000000000000000001.json",
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
        let content_key = layout.content_blob("cs_00000000000000000000000000000001", &content_id());
        let cases = [(content_key, DurableObjectFamily::ContentBlob)];

        for (key, family) in cases {
            let parsed = parse_object_key(&key).expect("known global key parses");
            assert_eq!(parsed.family(), family);
            assert_eq!(parsed.owner_namespace_id(), None);
        }

        assert!(parse_object_key("namespaces/ns-1/unknown/file").is_none());
    }

    /// One content layout exists. Anything else under `content-stores/`
    /// classifies as nothing at all rather than as content.
    #[test]
    fn parser_admits_exactly_one_content_layout() {
        for foreign in [
            "content-stores/cs-1/blobs/ab/cd/deadbeef",
            "content-stores/cs-1/objects/ab/deadbeef",
            "content-stores/cs-1/objects/ab/cd/ef/deadbeef",
            "content-stores/cs-1/objects/deadbeef",
            "content-stores/cs-1/objects/",
        ] {
            assert!(
                parse_object_key(foreign).is_none(),
                "foreign content path parsed: {foreign}"
            );
        }
    }
}
