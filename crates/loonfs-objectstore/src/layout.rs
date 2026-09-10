//! The durable key grammar: object families and key classification.

use loonfs_api::{ManifestNo, UploadId};

/// One family in the [durable object key grammar].
///
/// [durable object key grammar]: ../../../docs/specs/format.md#a8-object-keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableObjectFamily {
    /// Classifies an immutable segment in a namespace's numbered WAL.
    WalSegment,
    /// Starts forward discovery of namespace manifests.
    Hint,
    /// Classifies an immutable numbered namespace manifest.
    MetadataManifest,
    /// Classifies an immutable metadata segment.
    MetadataSegment,
    /// Classifies a pin to a numbered manifest.
    CheckpointRecord,
    /// Classifies a mutable upload-session lifecycle record.
    UploadSession,
    /// Classifies immutable whole-file content bytes.
    ContentBlob,
    /// Identifies the content domain stored under a content-store prefix.
    ContentStore,
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

    /// Returns the namespace path component, or `None` for the content-store descriptor.
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
/// [durable object families](../../../docs/specs/format.md#a8-object-keys).
pub fn parse_object_key(key: &str) -> Option<ParsedObjectKey<'_>> {
    let segments: Vec<_> = key.split('/').collect();
    match segments.as_slice() {
        ["content-stores", content_store_id, "store.json"] => Some(parsed(
            DurableObjectFamily::ContentStore,
            None,
            Some(content_store_id),
        )),
        ["content-stores", _, "objects", owner_namespace_id, _, _, content_id] => Some(parsed(
            DurableObjectFamily::ContentBlob,
            Some(owner_namespace_id),
            Some(content_id),
        )),
        ["namespaces", namespace, "wal", segment] => segment
            .strip_suffix(".wal.zst")
            .filter(|identifier| parse_wal_no(identifier).is_some())
            .map(|identifier| {
                parsed(
                    DurableObjectFamily::WalSegment,
                    Some(namespace),
                    Some(identifier),
                )
            }),
        ["namespaces", namespace, "hint.json"] => {
            Some(parsed(DurableObjectFamily::Hint, Some(namespace), None))
        }
        ["namespaces", namespace, "manifests", manifest] => {
            manifest.strip_suffix(".json").map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataManifest,
                    Some(namespace),
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "segments", segment] => {
            segment.strip_suffix(".sst.zst").map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataSegment,
                    Some(namespace),
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "pins", checkpoint] => {
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

/// Parses the fixed-width WAL number in a segment key.
pub fn wal_no_of(key: &str) -> Option<loonfs_api::WalNo> {
    let parsed = parse_object_key(key)?;
    if parsed.family() != DurableObjectFamily::WalSegment {
        return None;
    }
    parse_wal_no(parsed.identifier()?)
}

fn parse_wal_no(number: &str) -> Option<loonfs_api::WalNo> {
    if number.len() != 20 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let number = loonfs_api::WalNo::parse(number.parse().ok()?).ok()?;
    (number.0 > 0).then_some(number)
}

/// Extracts a manifest number from its twenty-digit durable name.
pub fn manifest_no_of(key: &str) -> Option<ManifestNo> {
    let parsed = parse_object_key(key)?;
    if parsed.family() != DurableObjectFamily::MetadataManifest {
        return None;
    }
    let number = parsed.identifier()?;
    if number.len() != 20 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    ManifestNo::parse(number.parse().ok()?).ok()
}

/// Extracts and validates an upload identity from its durable key.
pub fn upload_id_of(key: &str) -> Option<UploadId> {
    parse_object_key(key)
        .filter(|parsed| parsed.family() == DurableObjectFamily::UploadSession)
        .and_then(|parsed| parsed.identifier())
        .and_then(|identifier| UploadId::parse(identifier).ok())
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
        checkpoint_record, content_blob, content_owner_prefix, content_store, hint,
        metadata_manifest_object, metadata_segment, metadata_segment_prefix, upload_session,
        wal_segment, wal_segment_prefix,
    };
    use loonfs_api::{
        CheckpointId, ContentId, ContentStoreId, ManifestNo, MetadataSegmentId, NamespaceId,
        UploadId, WalNo,
    };

    #[test]
    fn built_keys_parse_to_their_family_owner_and_identifier() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let wal_no = WalNo(1);
        let manifest_object_id = ManifestNo(400);
        let metadata_segment_id = MetadataSegmentId::parse("seg_00000000000000000000000000000001")
            .expect("metadata segment id");
        let checkpoint_id = CheckpointId::parse("pin_00000000000000000001-0000000000000001")
            .expect("checkpoint id");
        let upload_id = UploadId::parse("upl_00000000000000000000000000000001").expect("upload id");
        let content_store_id =
            ContentStoreId::parse("cs_00000000000000000000000000000001").expect("content store id");
        let content_id =
            ContentId::parse("con_abcdef0123456789abcdef0123456789").expect("content id");
        let cases = [
            (
                content_store(&content_store_id),
                DurableObjectFamily::ContentStore,
                Some(content_store_id.as_str()),
            ),
            (
                wal_segment(&namespace_id, &wal_no),
                DurableObjectFamily::WalSegment,
                Some("00000000000000000001"),
            ),
            (hint(&namespace_id), DurableObjectFamily::Hint, None),
            (
                metadata_manifest_object(&namespace_id, &manifest_object_id),
                DurableObjectFamily::MetadataManifest,
                Some("00000000000000000400"),
            ),
            (
                metadata_segment(&namespace_id, &metadata_segment_id),
                DurableObjectFamily::MetadataSegment,
                Some(metadata_segment_id.as_str()),
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
                content_blob(&content_store_id, &namespace_id, &content_id),
                DurableObjectFamily::ContentBlob,
                Some(content_id.as_str()),
            ),
        ];

        for (key, family, identifier) in cases {
            let parsed = parse_object_key(&key).expect("built key should parse");
            assert_eq!(parsed.family(), family);
            assert_eq!(
                parsed.owner_namespace_id(),
                (!matches!(family, DurableObjectFamily::ContentStore)).then_some("ns-1")
            );
            assert_eq!(parsed.identifier(), identifier);
        }
        let a = NamespaceId::parse("a").expect("owner");
        let ab = NamespaceId::parse("ab").expect("owner");
        let a_prefix = content_owner_prefix(&content_store_id, &a);
        let ab_prefix = content_owner_prefix(&content_store_id, &ab);
        assert_eq!(
            a_prefix,
            format!("content-stores/{content_store_id}/objects/a/")
        );
        assert_eq!(
            ab_prefix,
            format!("content-stores/{content_store_id}/objects/ab/")
        );
        assert!(!ab_prefix.starts_with(&a_prefix));
        for owner in [a, ab] {
            let key = content_blob(&content_store_id, &owner, &content_id);
            assert!(key.starts_with(&content_owner_prefix(&content_store_id, &owner)));
            assert_eq!(
                parse_object_key(&key)
                    .expect("content key")
                    .owner_namespace_id(),
                Some(owner.as_str())
            );
        }
        assert!(parse_object_key(&format!(
            "content-stores/{content_store_id}/objects/ab/cd/{content_id}"
        ))
        .is_none());
    }

    #[test]
    fn listing_prefixes_hold_only_their_family() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let segment_id =
            MetadataSegmentId::parse("seg_00000000000000000000000000000001").expect("segment id");
        let content_store_id = ContentStoreId::generate();
        assert!(!content_store(&content_store_id)
            .starts_with(&format!("content-stores/{content_store_id}/objects/")));
        let segment = metadata_segment(&namespace_id, &segment_id);

        assert!(segment.starts_with(&metadata_segment_prefix(&namespace_id)));
        let wal_segments = wal_segment_prefix(&namespace_id);
        assert!(!hint(&namespace_id).starts_with(&wal_segments));
    }

    #[test]
    fn parser_rejects_retired_and_malformed_paths() {
        for key in [
            "namespaces/ns-1/descriptor.json",
            "namespaces/ns-1/control/head.json",
            "namespaces/ns-1/wal/wal_00000000000000000001-0123456789abcdef.wal.zst",
            "namespaces/ns-1/wal/segments/random.tmp",
            "namespaces/ns-1/metadata/compactions/cmp_1/segments/seg_1.tmp",
            "namespaces/ns-1/metadata/compactions/cmp_1/lease.json",
            "namespaces/ns-1/metadata/compaction_leases/unknown.json",
            "content-stores/cs-1/objects/ab/deadbeef",
        ] {
            assert!(
                parse_object_key(key).is_none(),
                "unexpected key parsed: {key}"
            );
        }
    }
}
