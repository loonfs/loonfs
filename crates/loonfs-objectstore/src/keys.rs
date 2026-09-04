//! Key construction for every [durable object family].
//!
//! [durable object family]: ../../../docs/specs/format.md#12-durable-object-families

use crate::layout::{
    metadata_compaction_job_id_from_key as parse_metadata_compaction_job_id,
    metadata_compaction_lease_group_from_key as parse_metadata_compaction_lease_group,
    wal_segment_id_from_key as parse_wal_segment_id,
};
use loonfs_api::wire::manifest::MetadataSegmentRef;
use loonfs_api::{
    CheckpointId, ContentId, ContentStoreId, ManifestObjectId, MetadataCompactionId,
    MetadataFamilyGroup, MetadataSegmentId, NamespaceId, UploadId, WalSegmentId,
};

/// Builds the listing prefix containing every durable object owned by one namespace.
pub fn namespace_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/")
}

/// Builds the authoritative WAL head key for one namespace.
pub fn wal_head(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/wal/head.json")
}

/// Builds the retained-history floor key for one namespace.
pub fn wal_floor(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/wal/floor.json")
}

/// Builds the immutable WAL object key for a segment identity.
pub fn wal_segment(namespace_id: &NamespaceId, wal_segment_id: &WalSegmentId) -> String {
    format!("namespaces/{namespace_id}/wal/segments/{wal_segment_id}.wal.zst")
}

/// Builds the listing prefix containing only WAL segment objects for one namespace.
pub fn wal_segment_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/wal/segments/")
}

/// Extracts a segment identity from a current-format WAL object key.
///
/// Returns `None` for foreign or differently suffixed objects.
pub fn wal_segment_id_from_key(key: &str) -> Option<&str> {
    parse_wal_segment_id(key)
}

/// Builds the materialized metadata-root key for one namespace.
pub fn metadata_root(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/metadata/root.json")
}

/// Builds the listing prefix containing namespace-manifest candidates.
pub fn metadata_manifest_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/metadata/manifests/")
}

/// Builds the listing prefix containing metadata segment objects owned by one namespace.
pub fn metadata_segment_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/metadata/segments/")
}

/// Builds the immutable manifest key for one speculative manifest identity.
pub fn metadata_manifest_object(
    namespace_id: &NamespaceId,
    manifest_object_id: &ManifestObjectId,
) -> String {
    format!("namespaces/{namespace_id}/metadata/manifests/{manifest_object_id}.manifest.json")
}

/// Builds the immutable metadata segment key for one segment identity.
pub fn metadata_segment(
    namespace_id: &NamespaceId,
    metadata_segment_id: &MetadataSegmentId,
) -> String {
    format!("namespaces/{namespace_id}/metadata/segments/{metadata_segment_id}.sst.zst")
}

/// Builds the immutable staging key one streaming compaction job writes a
/// metadata segment to before any manifest references it.
pub fn metadata_compaction_segment(
    namespace_id: &NamespaceId,
    metadata_compaction_id: &MetadataCompactionId,
    metadata_segment_id: &MetadataSegmentId,
) -> String {
    format!(
        "namespaces/{namespace_id}/metadata/compactions/{metadata_compaction_id}/segments/{metadata_segment_id}.sst.zst"
    )
}

/// Derives a metadata segment's object key from its manifest descriptor.
///
/// `compaction_job_id` selects the compaction prefix. Descriptors without a
/// job id use the namespace's `metadata/segments/` prefix.
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

/// Builds the mutable lease key for one metadata family group.
pub fn metadata_compaction_lease(namespace_id: &NamespaceId, group: MetadataFamilyGroup) -> String {
    let group = group.as_str();
    format!("namespaces/{namespace_id}/metadata/compaction_leases/{group}.json")
}

/// Builds the listing prefix containing every metadata family-group lease.
pub fn metadata_compaction_lease_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/metadata/compaction_leases/")
}

/// Extracts the family group from a current-format compaction lease key.
pub fn metadata_compaction_lease_group_from_key(key: &str) -> Option<MetadataFamilyGroup> {
    parse_metadata_compaction_lease_group(key)
}

/// Builds the listing prefix containing every streaming compaction job's
/// objects for one namespace.
pub fn metadata_compaction_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/metadata/compactions/")
}

/// Extracts the job id from a key under one namespace's compaction prefix.
///
/// Returns `None` for a key under the prefix that is not a staged segment.
pub fn metadata_compaction_job_id_from_key(key: &str) -> Option<&str> {
    parse_metadata_compaction_job_id(key)
}

/// Builds the mutable lifecycle key for one checkpoint record.
pub fn checkpoint_record(namespace_id: &NamespaceId, checkpoint_id: &CheckpointId) -> String {
    format!("namespaces/{namespace_id}/checkpoints/{checkpoint_id}.json")
}

/// Builds the listing prefix containing checkpoint records for one namespace.
pub fn checkpoint_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/checkpoints/")
}

/// Builds the listing prefix containing durable upload sessions for one namespace.
pub fn upload_session_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/uploads/")
}

/// Builds the mutable lifecycle key for one upload session.
pub fn upload_session(namespace_id: &NamespaceId, upload_id: &UploadId) -> String {
    format!("namespaces/{namespace_id}/uploads/{upload_id}.json")
}

/// Builds the immutable content-object key for one content identity.
pub fn content_blob(content_store_id: &ContentStoreId, content_id: &ContentId) -> String {
    let [first_shard, second_shard] = content_id.shard_prefixes();
    format!("content-stores/{content_store_id}/objects/{first_shard}/{second_shard}/{content_id}")
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_record, content_blob, metadata_compaction_lease,
        metadata_compaction_lease_prefix, metadata_compaction_prefix, metadata_compaction_segment,
        metadata_manifest_object, metadata_root, metadata_segment, metadata_segment_object_key,
        upload_session, wal_floor, wal_head, wal_segment, wal_segment_id_from_key,
        wal_segment_prefix,
    };
    use loonfs_api::wire::manifest::{MetadataRowFamily, MetadataSegmentRef};
    use loonfs_api::wire::sst_blocks::BlockHandle;
    use loonfs_api::{
        CheckpointId, ContentId, ContentStoreId, ManifestObjectId, MetadataCompactionId,
        MetadataFamilyGroup, MetadataSegmentId, NamespaceId, UploadId, WalSegmentId,
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
                    "man_00000000000000000400-0123456789abcdef",
                )
                .replace("{checkpoint_id}", "chk_00000000000000000000000000000001")
                .replace("{job_id}", "cmp_00000000000000000000000000000001")
                .replace("{group}", "bindings")
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
                    &WalSegmentId::parse(format!("wal_{:020}-{}", 42, "0123456789abcdef"))
                        .expect("valid WAL segment id"),
                ),
            ),
            (
                "Namespace manifests",
                metadata_manifest_object(
                    &namespace_id(),
                    &ManifestObjectId::parse("man_00000000000000000400-0123456789abcdef")
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
                metadata_compaction_lease(&namespace_id(), MetadataFamilyGroup::Bindings),
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
    fn listing_prefixes_match_their_keys_and_wal_segment_ids_parse_back() {
        assert_eq!(
            wal_segment_prefix(&namespace_id()),
            "namespaces/ns-1/wal/segments/"
        );
        assert!(wal_segment(
            &namespace_id(),
            &wal_segment_id("wal_00000000000000000042-0123456789abcdef")
        )
        .starts_with(&wal_segment_prefix(&namespace_id())));
        assert!(!wal_head(&namespace_id()).starts_with(&wal_segment_prefix(&namespace_id())));
        assert!(!wal_floor(&namespace_id()).starts_with(&wal_segment_prefix(&namespace_id())));
        assert_eq!(
            wal_segment_id_from_key(&wal_segment(
                &namespace_id(),
                &wal_segment_id("wal_00000000000000000042-0123456789abcdef")
            )),
            Some("wal_00000000000000000042-0123456789abcdef")
        );
        assert_eq!(
            wal_segment_id_from_key("namespaces/ns-1/wal/segments/random.tmp"),
            None
        );
        assert_eq!(
            metadata_compaction_prefix(&namespace_id()),
            "namespaces/ns-1/metadata/compactions/"
        );
        assert_eq!(
            metadata_compaction_lease_prefix(&namespace_id()),
            "namespaces/ns-1/metadata/compaction_leases/"
        );
    }

    fn segment_descriptor(compaction_job_id: Option<MetadataCompactionId>) -> MetadataSegmentRef {
        MetadataSegmentRef {
            owner_namespace_id: namespace_id(),
            segment_id: metadata_segment_id(),
            compaction_job_id,
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
}
