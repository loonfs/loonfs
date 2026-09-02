//! The durable key grammar: object families and key classification.

use loonfs_api::{GeneratedIdValidationError, ManifestObjectId, UploadId};

/// One family in the [durable object key grammar].
///
/// [durable object key grammar]: ../../../docs/specs/format.md#12-durable-object-families
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableObjectFamily {
    /// Classifies the mutable visibility and fencing head.
    WalHead,
    /// Classifies the mutable retained-history floor.
    WalFloor,
    /// Classifies an immutable segment in a namespace's WAL chain.
    WalSegment,
    /// Classifies the mutable materialized metadata pointer.
    MetadataRoot,
    /// Classifies an immutable namespace-manifest candidate.
    MetadataManifest,
    /// Classifies an immutable metadata segment.
    MetadataSegment,
    /// Classifies an immutable metadata segment written by a streaming compaction.
    MetadataCompactionStaging,
    /// Classifies the mutable lease for one streaming compaction.
    MetadataCompactionLease,
    /// Classifies a mutable checkpoint lifecycle record.
    CheckpointRecord,
    /// Classifies a mutable upload-session lifecycle record.
    UploadSession,
    /// Classifies immutable whole-file content bytes.
    ContentBlob,
}

/// Reports the durable family and identifiers recoverable from a recognized key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedObjectKey<'a> {
    family: DurableObjectFamily,
    owner_namespace_id: Option<&'a str>,
    identifier: Option<&'a str>,
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

    /// Returns the family-specific identifier when the key carries one.
    pub fn identifier(&self) -> Option<&'a str> {
        self.identifier
    }
}

/// Classifies a current or reserved durable object key without validating identifier text.
///
/// Returns `None` for private, foreign, or unrecognized paths. See
/// [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn parse_object_key(key: &str) -> Option<ParsedObjectKey<'_>> {
    let segments: Vec<_> = key.split('/').collect();
    match segments.as_slice() {
        ["content-stores", _, "objects", _, _, content_id] => Some(parsed(
            DurableObjectFamily::ContentBlob,
            None,
            Some(content_id),
        )),
        ["namespaces", namespace, "wal", "head.json"] => {
            Some(parsed(DurableObjectFamily::WalHead, Some(namespace), None))
        }
        ["namespaces", namespace, "wal", "floor.json"] => {
            Some(parsed(DurableObjectFamily::WalFloor, Some(namespace), None))
        }
        ["namespaces", namespace, "wal", "segments", segment] => {
            segment.strip_suffix(".wal.zst").map(|identifier| {
                parsed(
                    DurableObjectFamily::WalSegment,
                    Some(namespace),
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "metadata", "root.json"] => Some(parsed(
            DurableObjectFamily::MetadataRoot,
            Some(namespace),
            None,
        )),
        ["namespaces", namespace, "metadata", "manifests", manifest] => {
            manifest.strip_suffix(".manifest.json").map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataManifest,
                    Some(namespace),
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "metadata", "segments", segment] => {
            segment.strip_suffix(".sst.zst").map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataSegment,
                    Some(namespace),
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "metadata", "compactions", job_id, "segments", segment]
            if segment.ends_with(".sst.zst") =>
        {
            Some(parsed(
                DurableObjectFamily::MetadataCompactionStaging,
                Some(namespace),
                Some(job_id),
            ))
        }
        ["namespaces", namespace, "metadata", "compactions", job_id, "lease.json"] => Some(parsed(
            DurableObjectFamily::MetadataCompactionLease,
            Some(namespace),
            Some(job_id),
        )),
        ["namespaces", namespace, "checkpoints", checkpoint] => {
            checkpoint.strip_suffix(".json").map(|identifier| {
                parsed(
                    DurableObjectFamily::CheckpointRecord,
                    Some(namespace),
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "uploads", upload] => {
            upload.strip_suffix(".json").map(|identifier| {
                parsed(
                    DurableObjectFamily::UploadSession,
                    Some(namespace),
                    Some(identifier),
                )
            })
        }
        _ => None,
    }
}

pub(crate) fn wal_segment_id_from_key(key: &str) -> Option<&str> {
    parse_object_key(key)
        .filter(|parsed| parsed.family() == DurableObjectFamily::WalSegment)
        .and_then(|parsed| parsed.identifier())
}

/// Extracts and validates a manifest object identity from its durable key.
pub fn manifest_object_id_of(
    key: &str,
) -> Option<Result<ManifestObjectId, GeneratedIdValidationError>> {
    parse_object_key(key)
        .filter(|parsed| parsed.family() == DurableObjectFamily::MetadataManifest)
        .and_then(|parsed| parsed.identifier())
        .map(ManifestObjectId::parse)
}

/// Extracts and validates an upload identity from its durable key.
pub fn upload_id_of(key: &str) -> Option<UploadId> {
    parse_object_key(key)
        .filter(|parsed| parsed.family() == DurableObjectFamily::UploadSession)
        .and_then(|parsed| parsed.identifier())
        .and_then(|identifier| UploadId::parse(identifier).ok())
}

pub(crate) fn metadata_compaction_job_id_from_key(key: &str) -> Option<&str> {
    parse_object_key(key)
        .filter(|parsed| {
            matches!(
                parsed.family(),
                DurableObjectFamily::MetadataCompactionStaging
                    | DurableObjectFamily::MetadataCompactionLease
            )
        })
        .and_then(|parsed| parsed.identifier())
}

fn parsed<'a>(
    family: DurableObjectFamily,
    owner_namespace_id: Option<&'a str>,
    identifier: Option<&'a str>,
) -> ParsedObjectKey<'a> {
    ParsedObjectKey {
        family,
        owner_namespace_id,
        identifier,
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_object_key, DurableObjectFamily};
    use crate::keys::{
        checkpoint_record, content_blob, metadata_compaction_lease, metadata_compaction_segment,
        metadata_manifest_object, metadata_root, metadata_segment, metadata_segment_prefix,
        upload_session, wal_floor, wal_head, wal_segment, wal_segment_prefix,
    };
    use loonfs_api::{
        CheckpointId, ContentId, ContentStoreId, ManifestObjectId, MetadataCompactionId,
        MetadataSegmentId, NamespaceId, UploadId, WalSegmentId,
    };

    #[test]
    fn built_keys_parse_to_their_family_owner_and_identifier() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let wal_segment_id = WalSegmentId::parse("wal_00000000000000000001-0123456789abcdef")
            .expect("WAL segment id");
        let manifest_object_id =
            ManifestObjectId::parse("man_00000000000000000400-0123456789abcdef")
                .expect("manifest object id");
        let metadata_segment_id = MetadataSegmentId::parse("seg_00000000000000000000000000000001")
            .expect("metadata segment id");
        let compaction_id = MetadataCompactionId::parse("cmp_00000000000000000000000000000001")
            .expect("compaction id");
        let checkpoint_id =
            CheckpointId::parse("chk_00000000000000000000000000000001").expect("checkpoint id");
        let upload_id = UploadId::parse("upl_00000000000000000000000000000001").expect("upload id");
        let content_store_id =
            ContentStoreId::parse("cs_00000000000000000000000000000001").expect("content store id");
        let content_id =
            ContentId::parse("con_abcdef0123456789abcdef0123456789").expect("content id");
        let cases = [
            (wal_head(&namespace_id), DurableObjectFamily::WalHead, None),
            (
                wal_floor(&namespace_id),
                DurableObjectFamily::WalFloor,
                None,
            ),
            (
                wal_segment(&namespace_id, &wal_segment_id),
                DurableObjectFamily::WalSegment,
                Some(wal_segment_id.as_str()),
            ),
            (
                metadata_root(&namespace_id),
                DurableObjectFamily::MetadataRoot,
                None,
            ),
            (
                metadata_manifest_object(&namespace_id, &manifest_object_id),
                DurableObjectFamily::MetadataManifest,
                Some(manifest_object_id.as_str()),
            ),
            (
                metadata_segment(&namespace_id, &metadata_segment_id),
                DurableObjectFamily::MetadataSegment,
                Some(metadata_segment_id.as_str()),
            ),
            (
                metadata_compaction_segment(&namespace_id, &compaction_id, &metadata_segment_id),
                DurableObjectFamily::MetadataCompactionStaging,
                Some(compaction_id.as_str()),
            ),
            (
                metadata_compaction_lease(&namespace_id, &compaction_id),
                DurableObjectFamily::MetadataCompactionLease,
                Some(compaction_id.as_str()),
            ),
            (
                checkpoint_record(&namespace_id, &checkpoint_id),
                DurableObjectFamily::CheckpointRecord,
                Some(checkpoint_id.as_str()),
            ),
            (
                upload_session(&namespace_id, &upload_id),
                DurableObjectFamily::UploadSession,
                Some(upload_id.as_str()),
            ),
            (
                content_blob(&content_store_id, &content_id),
                DurableObjectFamily::ContentBlob,
                Some(content_id.as_str()),
            ),
        ];

        for (key, family, identifier) in cases {
            let parsed = parse_object_key(&key).expect("built key should parse");
            assert_eq!(parsed.family(), family);
            assert_eq!(
                parsed.owner_namespace_id(),
                (family != DurableObjectFamily::ContentBlob).then_some("ns-1")
            );
            assert_eq!(parsed.identifier(), identifier);
        }
    }

    #[test]
    fn listing_prefixes_hold_only_their_family_and_a_lease_sorts_first() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let job = MetadataCompactionId::parse("cmp_00000000000000000000000000000001")
            .expect("compaction id");
        let next_job = MetadataCompactionId::parse("cmp_00000000000000000000000000000002")
            .expect("compaction id");
        let segment_id =
            MetadataSegmentId::parse("seg_00000000000000000000000000000001").expect("segment id");
        let lease = metadata_compaction_lease(&namespace_id, &job);
        let staged = metadata_compaction_segment(&namespace_id, &job, &segment_id);

        assert!(lease < staged);
        assert!(staged < metadata_compaction_lease(&namespace_id, &next_job));
        assert!(!staged.starts_with(&metadata_segment_prefix(&namespace_id)));
        let wal_segments = wal_segment_prefix(&namespace_id);
        assert!(!wal_head(&namespace_id).starts_with(&wal_segments));
        assert!(!wal_floor(&namespace_id).starts_with(&wal_segments));
    }

    #[test]
    fn parser_rejects_retired_and_malformed_paths() {
        for key in [
            "namespaces/ns-1/descriptor.json",
            "namespaces/ns-1/control/head.json",
            "namespaces/ns-1/wal/wal_00000000000000000001-0123456789abcdef.wal.zst",
            "namespaces/ns-1/wal/segments/random.tmp",
            "namespaces/ns-1/metadata/compactions/cmp_1/segments/seg_1.tmp",
            "content-stores/cs-1/objects/ab/deadbeef",
        ] {
            assert!(
                parse_object_key(key).is_none(),
                "unexpected key parsed: {key}"
            );
        }
    }
}
