//! Key construction for every durable object family.

use crate::layout::ObjectLayout;
use loonfs_api::wire::manifest::MetadataSegmentRef;
use loonfs_api::{
    CheckpointId, ContentId, ContentStoreId, ManifestObjectId, MetadataCompactionId,
    MetadataSegmentId, NamespaceId, UploadId, WalSegmentId,
};

/// Builds the listing prefix containing every durable object owned by one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn namespace_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().namespace_root_prefix(namespace_id)
}

/// Builds the authoritative WAL head key for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_head(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().wal_head(namespace_id)
}

/// Builds the retained-history floor key for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_floor(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().wal_floor(namespace_id)
}

/// Builds the immutable WAL object key for a segment identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_segment(namespace_id: &NamespaceId, wal_segment_id: &WalSegmentId) -> String {
    ObjectLayout::new().wal_segment(namespace_id, wal_segment_id)
}

/// Builds the listing prefix containing only WAL segment objects for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_segment_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().wal_segment_prefix(namespace_id)
}

/// Extracts a segment identity from a current-format WAL object key.
///
/// Returns `None` for foreign or differently suffixed objects. See
/// [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_segment_id_from_key(key: &str) -> Option<&str> {
    ObjectLayout::new().wal_segment_id_from_key(key)
}

/// Builds the materialized metadata-root key for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_root(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().metadata_root(namespace_id)
}

/// Builds the listing prefix containing namespace-manifest candidates.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_manifest_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().metadata_manifest_prefix(namespace_id)
}

/// Builds the listing prefix containing metadata segment objects owned by one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_segment_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().metadata_segment_prefix(namespace_id)
}

/// Builds the immutable manifest key for one speculative manifest identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_manifest_object(
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> String {
    ObjectLayout::new().metadata_manifest_object(namespace_id, manifest_object_id)
}

/// Builds the immutable metadata segment key for one segment identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_segment(
    namespace_id: &NamespaceId,
    metadata_segment_id: &MetadataSegmentId,
) -> String {
    ObjectLayout::new().metadata_segment(namespace_id, metadata_segment_id)
}

/// Builds the immutable staging key one streaming compaction job writes a
/// metadata segment to before any manifest references it.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_compaction_segment(
    namespace_id: &NamespaceId,
    metadata_compaction_id: &MetadataCompactionId,
    metadata_segment_id: &MetadataSegmentId,
) -> String {
    ObjectLayout::new().metadata_compaction_segment(
        namespace_id,
        metadata_compaction_id,
        metadata_segment_id,
    )
}

/// Derives a metadata segment's object key from its manifest descriptor.
///
/// `compaction_job_id` selects the compaction prefix. Descriptors without a
/// job id use the namespace's `metadata/segments/` prefix.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_segment_object_key(descriptor: &MetadataSegmentRef) -> String {
    match &descriptor.compaction_job_id {
        Some(compaction_job_id) => metadata_compaction_segment(
            &descriptor.owner_namespace_id,
            compaction_job_id,
            &descriptor.segment_id,
        ),
        None => metadata_segment(&descriptor.owner_namespace_id, &descriptor.segment_id),
    }
}

/// Builds the mutable lease key one streaming compaction job holds over its
/// own prefix.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_compaction_lease(
    namespace_id: &NamespaceId,
    metadata_compaction_id: &MetadataCompactionId,
) -> String {
    ObjectLayout::new().metadata_compaction_lease(namespace_id, metadata_compaction_id)
}

/// Builds the listing prefix containing every streaming compaction job's
/// objects for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_compaction_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().metadata_compaction_prefix(namespace_id)
}

/// Extracts the job id from a key under one namespace's compaction prefix.
///
/// Returns `None` for a key under the prefix that is neither a job's lease
/// nor one of its staged segments. See
/// [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_compaction_job_id_from_key(key: &str) -> Option<&str> {
    ObjectLayout::new().metadata_compaction_job_id_from_key(key)
}

/// Builds the mutable lifecycle key for one checkpoint record.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn checkpoint_record(namespace_id: &NamespaceId, checkpoint_id: &CheckpointId) -> String {
    ObjectLayout::new().checkpoint_record(namespace_id, checkpoint_id)
}

/// Builds the listing prefix containing checkpoint records for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn checkpoint_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().checkpoint_prefix(namespace_id)
}

/// Builds the listing prefix containing durable upload sessions for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn upload_session_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().upload_session_prefix(namespace_id)
}

/// Builds the mutable lifecycle key for one upload session.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn upload_session(namespace_id: &NamespaceId, upload_id: &UploadId) -> String {
    ObjectLayout::new().upload_session(namespace_id, upload_id)
}

/// Builds the immutable content-object key for one content identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn content_blob(content_store_id: &ContentStoreId, content_id: &ContentId) -> String {
    ObjectLayout::new().content_blob(content_store_id, content_id)
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_record, content_blob, metadata_compaction_lease, metadata_compaction_prefix,
        metadata_compaction_segment, metadata_manifest_object, metadata_root, metadata_segment,
        metadata_segment_object_key, namespace_prefix, upload_session, wal_floor, wal_head,
        wal_segment, wal_segment_id_from_key, wal_segment_prefix,
    };
    use crate::layout::ObjectLayout;
    use loonfs_api::wire::manifest::{MetadataRowFamily, MetadataSegmentRef};
    use loonfs_api::wire::sst_blocks::BlockHandle;
    use loonfs_api::{
        ChangeSeq, CheckpointId, ContentId, ContentStoreId, ManifestObjectId, MetadataCompactionId,
        MetadataSegmentId, NamespaceId, RunNo, UploadId, WalSegmentId,
    };

    const CONTENT_ID: &str = "con_abcdef0123456789abcdef0123456789";

    fn content_id() -> ContentId {
        ContentId::parse(CONTENT_ID).expect("valid content id")
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

    fn metadata_compaction_id() -> MetadataCompactionId {
        MetadataCompactionId::parse("cmp_00000000000000000000000000000001")
            .expect("valid metadata compaction id")
    }

    fn metadata_segment_id() -> MetadataSegmentId {
        MetadataSegmentId::parse("seg_00000000000000000000000000000001")
            .expect("valid metadata segment id")
    }

    fn upload_id() -> UploadId {
        UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id")
    }

    fn wal_segment_id(value: &str) -> WalSegmentId {
        WalSegmentId::parse(value).expect("valid WAL segment id")
    }

    #[test]
    fn namespace_prefix_matches_layout_root_prefix() {
        let namespace_id = NamespaceId::parse("ns-1").expect("valid namespace id");

        assert_eq!(
            namespace_prefix(&namespace_id),
            ObjectLayout::new().namespace_root_prefix(&namespace_id)
        );
    }

    /// Pins every standard key pattern in the format spec's "Durable object
    /// families" table to the key this crate actually builds for that family.
    ///
    /// The table is normative: a new family must be added to the table and to
    /// this test together, and neither the spec pattern nor the builder can
    /// change without the other.
    #[test]
    fn standard_key_patterns_match_format_spec_table() {
        let spec = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/specs/format.md"
        ))
        .expect("read docs/specs/format.md");
        let section = spec
            .split_once("### 1.2 Durable object families")
            .expect("format.md section 1.2 exists")
            .1
            .split_once("\n### ")
            .expect("a section follows 1.2")
            .0;

        let mut patterns = std::collections::BTreeMap::new();
        for line in section.lines() {
            let Some(row) = line.strip_prefix("| **") else {
                continue;
            };
            let Some((family, rest)) = row.split_once("**") else {
                continue;
            };
            let Some(pattern) = rest
                .rsplit_once("| `")
                .and_then(|(_, tail)| tail.split_once('`'))
                .map(|(pattern, _)| pattern)
            else {
                continue;
            };
            patterns.insert(family.to_owned(), pattern.to_owned());
        }

        let substitute = |pattern: &str| -> String {
            pattern
                .replace("{namespace_id}", "ns-1")
                .replace("{owner_namespace_id}", "ns-1")
                .replace("{source_namespace_id}", "ns-1")
                .replace("{content_store_id}", "cs_00000000000000000000000000000001")
                .replace("{start_seq:020}", &format!("{:020}", 42))
                .replace("{suffix}", "0123456789abcdef")
                .replace(
                    "{manifest_object_id}",
                    "00000000000000000400-0123456789abcdef",
                )
                .replace("{checkpoint_id}", "chk_00000000000000000000000000000001")
                .replace("{job_id}", "cmp_00000000000000000000000000000001")
                .replace("{segment_id}", "seg_00000000000000000000000000000001")
                .replace("{upload_id}", "upl_00000000000000000000000000000001")
                .replace("{content_id[4..6]}", &CONTENT_ID[4..6])
                .replace("{content_id[6..8]}", &CONTENT_ID[6..8])
                .replace("{content_id}", CONTENT_ID)
        };

        let built = [
            ("WAL head", wal_head(&namespace_id())),
            (
                "WAL segments",
                wal_segment(
                    &namespace_id(),
                    &WalSegmentId::parse(format!("{:020}-{}", 42, "0123456789abcdef"))
                        .expect("valid WAL segment id"),
                ),
            ),
            (
                "Namespace manifests",
                metadata_manifest_object(
                    &namespace_id(),
                    &ManifestObjectId::parse("00000000000000000400-0123456789abcdef")
                        .expect("valid manifest object id"),
                ),
            ),
            (
                "Checkpoint records",
                checkpoint_record(&namespace_id(), &checkpoint_id()),
            ),
            (
                "Metadata segments",
                metadata_segment(&namespace_id(), &metadata_segment_id()),
            ),
            (
                "Compaction staging",
                metadata_compaction_segment(
                    &namespace_id(),
                    &metadata_compaction_id(),
                    &metadata_segment_id(),
                ),
            ),
            (
                "Compaction leases",
                metadata_compaction_lease(&namespace_id(), &metadata_compaction_id()),
            ),
            (
                "Upload sessions",
                upload_session(&namespace_id(), &upload_id()),
            ),
            ("Metadata root", metadata_root(&namespace_id())),
            ("WAL floor", wal_floor(&namespace_id())),
            (
                "Content objects",
                content_blob(&content_store_id(), &content_id()),
            ),
        ];

        let expected: std::collections::BTreeMap<String, String> = built
            .into_iter()
            .map(|(family, key)| (family.to_owned(), key))
            .collect();
        let actual: std::collections::BTreeMap<String, String> = patterns
            .into_iter()
            .map(|(family, pattern)| (family, substitute(&pattern)))
            .collect();
        assert_eq!(
            actual, expected,
            "the format.md durable-families table and the key builders must list \
             the same families with the same key shapes"
        );
    }

    #[test]
    fn key_builders_match_spec_examples() {
        assert_eq!(wal_head(&namespace_id()), "namespaces/ns-1/wal/head.json");
        assert_eq!(wal_floor(&namespace_id()), "namespaces/ns-1/wal/floor.json");
        assert_eq!(
            wal_segment(
                &namespace_id(),
                &wal_segment_id("00000000000000000001-0123456789abcdef")
            ),
            "namespaces/ns-1/wal/segments/00000000000000000001-0123456789abcdef.wal.zst"
        );
        assert_eq!(
            wal_segment_prefix(&namespace_id()),
            "namespaces/ns-1/wal/segments/"
        );
        assert!(wal_segment(
            &namespace_id(),
            &wal_segment_id("00000000000000000042-0123456789abcdef")
        )
        .starts_with(&wal_segment_prefix(&namespace_id())));
        assert!(!wal_head(&namespace_id()).starts_with(&wal_segment_prefix(&namespace_id())));
        assert!(!wal_floor(&namespace_id()).starts_with(&wal_segment_prefix(&namespace_id())));
        assert_eq!(
            wal_segment_id_from_key(&wal_segment(
                &namespace_id(),
                &wal_segment_id("00000000000000000042-0123456789abcdef")
            )),
            Some("00000000000000000042-0123456789abcdef")
        );
        assert_eq!(
            wal_segment_id_from_key("namespaces/ns-1/wal/segments/random.tmp"),
            None
        );
        assert_eq!(
            metadata_root(&namespace_id()),
            "namespaces/ns-1/metadata/root.json"
        );
        let manifest_object_id = ManifestObjectId::parse("00000000000000000400-0123456789abcdef")
            .expect("valid manifest object id");
        assert_eq!(
            metadata_manifest_object(&namespace_id(), &manifest_object_id),
            "namespaces/ns-1/metadata/manifests/00000000000000000400-0123456789abcdef.manifest.json"
        );
        assert_eq!(
            metadata_segment(&namespace_id(), &metadata_segment_id()),
            "namespaces/ns-1/metadata/segments/seg_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            metadata_compaction_prefix(&namespace_id()),
            "namespaces/ns-1/metadata/compactions/"
        );
        assert_eq!(
            metadata_compaction_segment(
                &namespace_id(),
                &metadata_compaction_id(),
                &metadata_segment_id(),
            ),
            "namespaces/ns-1/metadata/compactions/cmp_00000000000000000000000000000001/segments/seg_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            metadata_compaction_lease(&namespace_id(), &metadata_compaction_id()),
            "namespaces/ns-1/metadata/compactions/cmp_00000000000000000000000000000001/lease.json"
        );
        assert_eq!(
            checkpoint_record(&namespace_id(), &checkpoint_id()),
            "namespaces/ns-1/checkpoints/chk_00000000000000000000000000000001.json"
        );
        assert_eq!(
            content_blob(&content_store_id(), &content_id()),
            "content-stores/cs_00000000000000000000000000000001/objects/ab/cd/con_abcdef0123456789abcdef0123456789"
        );
        assert_eq!(
            upload_session(&namespace_id(), &upload_id()),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
    }

    fn segment_descriptor(compaction_job_id: Option<MetadataCompactionId>) -> MetadataSegmentRef {
        MetadataSegmentRef {
            owner_namespace_id: namespace_id(),
            segment_id: metadata_segment_id(),
            compaction_job_id,
            run_no: RunNo(0),
            run_seq: ChangeSeq(1),
            level: 0,
            family: MetadataRowFamily::Inodes,
            segment_index: 0,
            row_count: 0,
            min_row_key: String::new(),
            max_row_key: String::new(),
            index_block: BlockHandle {
                offset: 0,
                stored_len: 0,
                decoded_len: 0,
                crc32c: 0,
            },
            filter_block: BlockHandle {
                offset: 0,
                stored_len: 0,
                decoded_len: 0,
                crc32c: 0,
            },
            filter_inline: None,
            object_checksum: "sha256:unused".to_owned(),
        }
    }

    /// Segment descriptors derive standard and compaction object keys.
    #[test]
    fn segment_descriptors_derive_published_and_staging_keys() {
        assert_eq!(
            metadata_segment_object_key(&segment_descriptor(None)),
            metadata_segment(&namespace_id(), &metadata_segment_id())
        );
        assert_eq!(
            metadata_segment_object_key(&segment_descriptor(Some(metadata_compaction_id()))),
            metadata_compaction_segment(
                &namespace_id(),
                &metadata_compaction_id(),
                &metadata_segment_id()
            )
        );
    }

    /// Content keys shard on the id's own leading characters, so both shard
    /// directories are derivable from the id and nothing else.
    #[test]
    fn content_keys_shard_on_the_content_id_prefix() {
        let id = content_id();
        let key = content_blob(&content_store_id(), &id);
        let [first_shard, second_shard] = id.shard_prefixes();
        assert!(key.ends_with(&format!("/{first_shard}/{second_shard}/{}", id.as_str())));
    }
}
