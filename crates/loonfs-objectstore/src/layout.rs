//! The durable key grammar: object families, their path shapes, and
//! parsing keys back into classified families.

use loonfs_api::{
    CheckpointId, ContentId, ContentStoreId, ManifestObjectId, MetadataCompactionId,
    MetadataTableId, NamespaceId, UploadId, WalSegmentId,
};

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
    /// Classifies the mutable lease one streaming compaction holds over its
    /// own staged output.
    ///
    /// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
    MetadataCompactionLease,
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

    pub(crate) fn namespace_root_prefix(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/")
    }

    /// Hot head of the semantic commit stream: the only object whose CAS
    /// gates user-write throughput.
    pub(crate) fn wal_head(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/wal/head.json")
    }

    /// Cold lower bound of retained WAL/change history.
    pub(crate) fn wal_floor(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/wal/floor.json")
    }

    pub(crate) fn wal_segment(
        &self,
        namespace_id: &NamespaceId,
        wal_segment_id: &WalSegmentId,
    ) -> String {
        format!("namespaces/{namespace_id}/wal/segments/{wal_segment_id}.wal.zst")
    }

    /// Listing prefix that contains every WAL segment of `namespace` and
    /// nothing else: `wal/head.json` and `wal/floor.json` live outside it,
    /// so a GC listing yields only segment keys.
    ///
    /// Segment file names start with the segment's 20-digit `start_seq` as
    /// an operator/GC convenience; no protocol depends on listing order.
    pub(crate) fn wal_segment_prefix(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/wal/segments/")
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
    pub(crate) fn metadata_root(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/metadata/root.json")
    }

    pub(crate) fn metadata_manifest_object(
        &self,
        namespace_id: &NamespaceId,
        manifest_object_id: &ManifestObjectId,
    ) -> String {
        format!("namespaces/{namespace_id}/metadata/manifests/{manifest_object_id}.manifest.json")
    }

    pub(crate) fn metadata_manifest_prefix(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/metadata/manifests/")
    }

    pub(crate) fn metadata_table(
        &self,
        namespace_id: &NamespaceId,
        metadata_table_id: &MetadataTableId,
    ) -> String {
        format!("namespaces/{namespace_id}/metadata/tables/{metadata_table_id}.sst.zst")
    }

    pub(crate) fn metadata_table_prefix(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/metadata/tables/")
    }

    /// A metadata segment a streaming compaction has written but no manifest
    /// references yet. The object is an ordinary metadata segment; only the
    /// directory differs, which is what keeps a running job's output out of
    /// the sweep that reaps unreferenced table keys.
    ///
    /// The job id in the middle is what groups one job's output together, so
    /// a collector can decide a whole job's objects from that job's lease
    /// rather than one object at a time.
    pub(crate) fn metadata_compaction_table(
        &self,
        namespace_id: &NamespaceId,
        metadata_compaction_id: &MetadataCompactionId,
        metadata_table_id: &MetadataTableId,
    ) -> String {
        format!(
            "namespaces/{namespace_id}/metadata/compactions/{metadata_compaction_id}/tables/{metadata_table_id}.sst.zst"
        )
    }

    /// The lease one job holds over its own prefix. It sorts before that
    /// job's `tables/` directory, so an ascending listing of the compaction
    /// prefix reads a job's lease before the objects it protects.
    pub(crate) fn metadata_compaction_lease(
        &self,
        namespace_id: &NamespaceId,
        metadata_compaction_id: &MetadataCompactionId,
    ) -> String {
        format!(
            "namespaces/{namespace_id}/metadata/compactions/{metadata_compaction_id}/lease.json"
        )
    }

    pub(crate) fn metadata_compaction_prefix(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/metadata/compactions/")
    }

    /// The job id of a key under one namespace's compaction prefix, for the
    /// collector that has to find the lease owning a staged object.
    ///
    /// Returns `None` for a key under the prefix that is neither a job's
    /// lease nor one of its staged segments; such a key belongs to no job and
    /// the collector refuses to decide it.
    pub(crate) fn metadata_compaction_job_id_from_key<'a>(&self, key: &'a str) -> Option<&'a str> {
        match key.split('/').collect::<Vec<_>>().as_slice() {
            ["namespaces", _, "metadata", "compactions", job_id, "lease.json"] => Some(job_id),
            ["namespaces", _, "metadata", "compactions", job_id, "tables", table]
                if table.ends_with(".sst.zst") =>
            {
                Some(job_id)
            }
            _ => None,
        }
    }

    /// Durable stable-view pin to a metadata manifest.
    pub(crate) fn checkpoint_record(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> String {
        format!("namespaces/{namespace_id}/checkpoints/{checkpoint_id}.json")
    }

    pub(crate) fn checkpoint_prefix(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/checkpoints/")
    }

    pub(crate) fn upload_session(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> String {
        format!("namespaces/{namespace_id}/uploads/{upload_id}.json")
    }

    pub(crate) fn upload_session_prefix(&self, namespace_id: &NamespaceId) -> String {
        format!("namespaces/{namespace_id}/uploads/")
    }

    /// Content objects shard across two directory levels selected by the
    /// content id's leading characters. Those characters are random, so
    /// ingest spreads evenly and filesystem-backed stores keep bounded
    /// directory fanout.
    pub(crate) fn content_blob(
        &self,
        content_store_id: &ContentStoreId,
        content_id: &ContentId,
    ) -> String {
        let [first_shard, second_shard] = content_id.shard_prefixes();
        format!(
            "content-stores/{content_store_id}/objects/{first_shard}/{second_shard}/{content_id}"
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
        ["namespaces", namespace, "metadata", "compactions", _job_id, "tables", table]
            if table.ends_with(".sst.zst") =>
        {
            parsed(
                DurableObjectFamily::MetadataCompactionStaging,
                Some(namespace),
            )
        }
        ["namespaces", namespace, "metadata", "compactions", _job_id, "lease.json"] => parsed(
            DurableObjectFamily::MetadataCompactionLease,
            Some(namespace),
        ),
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
    use loonfs_api::{
        CheckpointId, ContentId, ContentStoreId, ManifestObjectId, MetadataCompactionId,
        MetadataTableId, NamespaceId, UploadId, WalSegmentId,
    };

    fn content_id() -> ContentId {
        ContentId::parse("con_abcdef0123456789abcdef0123456789").expect("valid content id")
    }

    fn namespace_id() -> NamespaceId {
        NamespaceId::parse("ns-1").expect("valid namespace id")
    }

    fn content_store_id() -> ContentStoreId {
        ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id")
    }

    fn checkpoint_id() -> CheckpointId {
        CheckpointId::parse("chk_00000000000000000000000000000001").expect("valid checkpoint id")
    }

    fn metadata_compaction_id(value: u128) -> MetadataCompactionId {
        MetadataCompactionId::parse(format!("cmp_{value:032x}"))
            .expect("valid metadata compaction id")
    }

    fn metadata_table_id() -> MetadataTableId {
        MetadataTableId::parse("tbl_00000000000000000000000000000001")
            .expect("valid metadata table id")
    }

    fn upload_id() -> UploadId {
        UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id")
    }

    fn wal_segment_id(value: &str) -> WalSegmentId {
        WalSegmentId::parse(value).expect("valid WAL segment id")
    }

    #[test]
    fn layout_golden_tree_matches_target_paths() {
        let layout = ObjectLayout::new();

        assert_eq!(
            layout.namespace_root_prefix(&namespace_id()),
            "namespaces/ns-1/"
        );
        assert_eq!(
            layout.wal_head(&namespace_id()),
            "namespaces/ns-1/wal/head.json"
        );
        assert_eq!(
            layout.wal_floor(&namespace_id()),
            "namespaces/ns-1/wal/floor.json"
        );
        assert_eq!(
            layout.wal_segment(
                &namespace_id(),
                &wal_segment_id("00000000000000000001-0123456789abcdef")
            ),
            "namespaces/ns-1/wal/segments/00000000000000000001-0123456789abcdef.wal.zst"
        );
        assert_eq!(
            layout.metadata_root(&namespace_id()),
            "namespaces/ns-1/metadata/root.json"
        );
        let manifest_object_id = ManifestObjectId::parse("00000000000000000400-0123456789abcdef")
            .expect("valid manifest object id");
        assert_eq!(
            layout
                .metadata_manifest_object(&namespace_id(), &manifest_object_id),
            "namespaces/ns-1/metadata/manifests/00000000000000000400-0123456789abcdef.manifest.json"
        );
        assert_eq!(
            layout.metadata_table(&namespace_id(), &metadata_table_id()),
            "namespaces/ns-1/metadata/tables/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            layout
                .metadata_compaction_table(
                    &namespace_id(),
                    &metadata_compaction_id(1),
                    &metadata_table_id()
                ),
            "namespaces/ns-1/metadata/compactions/cmp_00000000000000000000000000000001/tables/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            layout.metadata_compaction_lease(&namespace_id(), &metadata_compaction_id(1)),
            "namespaces/ns-1/metadata/compactions/cmp_00000000000000000000000000000001/lease.json"
        );
        assert_eq!(
            layout.checkpoint_record(&namespace_id(), &checkpoint_id()),
            "namespaces/ns-1/checkpoints/chk_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout.upload_session(&namespace_id(), &upload_id()),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
        assert_eq!(
            layout
                .content_blob(&content_store_id(), &content_id()),
            "content-stores/cs_00000000000000000000000000000001/objects/ab/cd/con_abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn control_objects_live_outside_the_segment_listing_prefix() {
        let layout = ObjectLayout::new();
        let prefix = layout.wal_segment_prefix(&namespace_id());
        assert_eq!(prefix, "namespaces/ns-1/wal/segments/");
        assert!(!layout
            .wal_head(&namespace_id())
            .as_str()
            .starts_with(&prefix));
        assert!(!layout
            .wal_floor(&namespace_id())
            .as_str()
            .starts_with(&prefix));
        assert!(layout
            .wal_segment(
                &namespace_id(),
                &wal_segment_id("00000000000000000002-0123456789abcdef")
            )
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
        let tables = layout.metadata_table_prefix(&namespace_id());
        let staging = layout.metadata_compaction_prefix(&namespace_id());

        assert!(!staging.starts_with(&tables));
        assert!(!layout
            .metadata_compaction_table(
                &namespace_id(),
                &metadata_compaction_id(1),
                &metadata_table_id()
            )
            .starts_with(&tables));
        assert!(layout
            .metadata_compaction_table(
                &namespace_id(),
                &metadata_compaction_id(1),
                &metadata_table_id()
            )
            .starts_with(&staging));
        assert!(layout
            .metadata_compaction_lease(&namespace_id(), &metadata_compaction_id(1))
            .starts_with(&staging));
        assert!(!layout
            .metadata_table(&namespace_id(), &metadata_table_id())
            .starts_with(&staging));
    }

    /// A collector reads a job's lease before the objects the lease protects,
    /// which is what lets one lease read decide a whole job's output.
    #[test]
    fn a_jobs_lease_sorts_before_its_staged_segments() {
        let layout = ObjectLayout::new();
        let lease = layout.metadata_compaction_lease(&namespace_id(), &metadata_compaction_id(1));
        let table = layout.metadata_compaction_table(
            &namespace_id(),
            &metadata_compaction_id(1),
            &metadata_table_id(),
        );
        assert!(lease < table);
        // And one job's objects are contiguous: the next job's lease sorts
        // above every object of the job before it.
        assert!(
            table < layout.metadata_compaction_lease(&namespace_id(), &metadata_compaction_id(2))
        );
    }

    /// Both key shapes under the compaction prefix name their job; anything
    /// else under it names none, and the collector refuses to decide it.
    #[test]
    fn compaction_keys_report_the_job_that_owns_them() {
        let layout = ObjectLayout::new();
        assert_eq!(
            layout.metadata_compaction_job_id_from_key(
                &layout.metadata_compaction_lease(&namespace_id(), &metadata_compaction_id(1))
            ),
            Some("cmp_00000000000000000000000000000001")
        );
        assert_eq!(
            layout.metadata_compaction_job_id_from_key(&layout.metadata_compaction_table(
                &namespace_id(),
                &metadata_compaction_id(1),
                &metadata_table_id()
            )),
            Some("cmp_00000000000000000000000000000001")
        );
        for foreign in [
            "namespaces/ns-1/metadata/compactions/cmp_00000000000000000000000000000001/tables/tbl_00000000000000000000000000000001.tmp",
            "namespaces/ns-1/metadata/compactions/cmp_00000000000000000000000000000001/notes.json",
            "namespaces/ns-1/metadata/compactions/stray.json",
            "namespaces/ns-1/metadata/tables/tbl_00000000000000000000000000000001.sst.zst",
        ] {
            assert_eq!(layout.metadata_compaction_job_id_from_key(foreign), None);
        }
    }

    #[test]
    fn parse_build_round_trips_for_namespace_key_families() {
        let layout = ObjectLayout::new();
        let cases = [
            (
                layout.wal_head(&namespace_id()),
                DurableObjectFamily::WalHead,
            ),
            (
                layout.wal_floor(&namespace_id()),
                DurableObjectFamily::WalFloor,
            ),
            (
                layout.wal_segment(
                    &namespace_id(),
                    &wal_segment_id("00000000000000000001-0123456789abcdef"),
                ),
                DurableObjectFamily::WalSegment,
            ),
            (
                layout.metadata_root(&namespace_id()),
                DurableObjectFamily::MetadataRoot,
            ),
            (
                layout.metadata_manifest_object(
                    &namespace_id(),
                    &ManifestObjectId::parse("00000000000000000001-0123456789abcdef")
                        .expect("valid manifest object id"),
                ),
                DurableObjectFamily::MetadataManifest,
            ),
            (
                layout.metadata_table(&namespace_id(), &metadata_table_id()),
                DurableObjectFamily::MetadataTable,
            ),
            (
                layout.metadata_compaction_table(
                    &namespace_id(),
                    &metadata_compaction_id(1),
                    &metadata_table_id(),
                ),
                DurableObjectFamily::MetadataCompactionStaging,
            ),
            (
                layout.metadata_compaction_lease(&namespace_id(), &metadata_compaction_id(1)),
                DurableObjectFamily::MetadataCompactionLease,
            ),
            (
                layout.checkpoint_record(&namespace_id(), &checkpoint_id()),
                DurableObjectFamily::CheckpointRecord,
            ),
            (
                layout.upload_session(&namespace_id(), &upload_id()),
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
            "namespaces/ns-1/wal/00000000000000000001-0123456789abcdef.wal.zst",
            "namespaces/ns-1/manifest/00000000000000000400.manifest.json",
            "namespaces/ns-1/tables/metadata/tbl_00000000000000000000000000000001.sst.zst",
            "namespaces/ns-1/gc/manifest.boundary.json",
            "namespaces/ns-1/gc/pins/pin_00000000000000000000000000000001.json",
            "namespaces/ns-1/pins/pin_00000000000000000000000000000001.json",
            "namespaces/ns-1/metadata/compaction-staging/tbl_00000000000000000000000000000001.sst.zst",
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
            "namespaces/ns-1/wal/segments/00000000000000000001-0123456789abcdef.wal.zst",
        )
        .expect("current WAL key parses");
        assert_eq!(parsed.family(), DurableObjectFamily::WalSegment);
        assert_eq!(parsed.owner_namespace_id(), Some("ns-1"));

        assert!(parse_object_key(
            "namespaces/ns-1/wal/segments/00000000000000000001-0123456789abcdef.sst"
        )
        .is_none());
        assert!(parse_object_key("namespaces/ns-1/wal/segments/random.tmp").is_none());
    }

    #[test]
    fn parse_build_round_trips_for_global_key_families() {
        let layout = ObjectLayout::new();
        let content_key = layout.content_blob(&content_store_id(), &content_id());
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
