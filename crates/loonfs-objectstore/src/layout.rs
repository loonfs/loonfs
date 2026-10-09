//! The durable key grammar: object families, key classification, and the
//! namespace listing built on it.

use crate::keys::namespace_prefix;
use crate::{ObjectStore, Result};
use loonfs_types::{EffectiveLimit, ManifestNo, NamespaceId, UploadId};
use std::num::NonZeroU32;

const NAMESPACES_PREFIX: &str = "namespaces/";

/// Most namespace ids one [`list_namespace_ids`] page holds: the most keys
/// S3 and Google Cloud Storage return for one list request.
pub const MAX_NAMESPACE_IDS_PAGE_LIMIT: u32 = 1_000;

/// One family in the [durable object key grammar].
///
/// [durable object key grammar]: https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a8-object-keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableObjectFamily {
    /// Classifies an immutable numbered object in a namespace's WAL.
    WalObject,
    /// Starts forward discovery of namespace manifests.
    Hint,
    /// Classifies an immutable numbered namespace manifest.
    MetadataManifest,
    /// Classifies an immutable metadata segment.
    MetadataSegment,
    /// Classifies a pin to a numbered manifest.
    Pin,
    /// Classifies a mutable upload-session lifecycle record.
    UploadSession,
    /// Classifies immutable whole-file content bytes.
    ContentBlob,
    /// Classifies a temporary object that an extension writes and deletes.
    ScratchObject,
}

/// Reports the durable family and identifiers recoverable from a recognized key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedObjectKey<'a> {
    family: DurableObjectFamily,
    owner_namespace_id: &'a str,
    identifier: Option<&'a str>,
}

impl<'a> ParsedObjectKey<'a> {
    /// Returns the durable family selected by the key's path shape.
    pub fn family(&self) -> DurableObjectFamily {
        self.family
    }

    /// Returns the namespace path component.
    pub fn owner_namespace_id(&self) -> &'a str {
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
/// [durable object families](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a8-object-keys).
pub fn parse_object_key(key: &str) -> Option<ParsedObjectKey<'_>> {
    let segments: Vec<_> = key.split('/').collect();
    match segments.as_slice() {
        ["namespaces", owner_namespace_id, "content", content_id] => Some(parsed(
            DurableObjectFamily::ContentBlob,
            owner_namespace_id,
            Some(content_id),
        )),
        ["namespaces", namespace, "wal", object] => object
            .strip_suffix(".wal.zst")
            .filter(|identifier| parse_wal_no(identifier).is_some())
            .map(|identifier| parsed(DurableObjectFamily::WalObject, namespace, Some(identifier))),
        ["namespaces", namespace, "hint.json"] => {
            Some(parsed(DurableObjectFamily::Hint, namespace, None))
        }
        ["namespaces", namespace, "manifests", manifest] => {
            manifest.strip_suffix(".json").map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataManifest,
                    namespace,
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "segments", segment] => {
            segment.strip_suffix(".sst.zst").map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataSegment,
                    namespace,
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "pins", pin] => pin
            .strip_suffix(".json")
            .map(|identifier| parsed(DurableObjectFamily::Pin, namespace, Some(identifier))),
        ["namespaces", namespace, "uploads", upload] => {
            upload.strip_suffix(".json").map(|identifier| {
                parsed(
                    DurableObjectFamily::UploadSession,
                    namespace,
                    Some(identifier),
                )
            })
        }
        ["namespaces", namespace, "scratch", scratch] => Some(parsed(
            DurableObjectFamily::ScratchObject,
            namespace,
            Some(scratch),
        )),
        _ => None,
    }
}

/// Parses the fixed-width WAL number in a WAL object key.
pub fn wal_no_of(key: &str) -> Option<loonfs_types::WalNo> {
    let parsed = parse_object_key(key)?;
    if parsed.family() != DurableObjectFamily::WalObject {
        return None;
    }
    parse_wal_no(parsed.identifier()?)
}

fn parse_wal_no(number: &str) -> Option<loonfs_types::WalNo> {
    if number.len() != 20 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let number = loonfs_types::WalNo::parse(number.parse().ok()?).ok()?;
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

/// One page of namespace ids from [`list_namespace_ids`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceIdPage {
    /// Namespace ids in ascending key order.
    pub namespace_ids: Vec<NamespaceId>,
    /// Children of `namespaces/` on this page whose names are not namespace ids.
    pub skipped_entries: usize,
    /// Where the next page starts, or `None` when this page ends the listing.
    pub next_cursor: Option<NamespacePageCursor>,
}

/// A position in the namespace listing that [`list_namespace_ids`] resumes after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePageCursor(String);

impl NamespacePageCursor {
    /// Builds the position just after `namespace_id`.
    pub fn after(namespace_id: &NamespaceId) -> Self {
        Self(namespace_prefix(namespace_id))
    }
}

/// Lists one page of the namespace ids in `store`, in ascending key order.
///
/// The page starts after `start_after` and holds at most `limit` ids, or
/// [`MAX_NAMESPACE_IDS_PAGE_LIMIT`] when `limit` is larger. One page costs
/// one store list request, however many objects each namespace holds; see
/// [`ObjectStore::list_child_prefixes`]. A child of `namespaces/` whose name
/// is not a namespace id is skipped and counted in `skipped_entries`. A
/// deleted namespace is still listed: its tombstone manifest stays after
/// deletion and after garbage collection. The listing sees only keys inside
/// the store's key prefix.
pub async fn list_namespace_ids<S: ObjectStore + ?Sized>(
    store: &S,
    start_after: Option<&NamespacePageCursor>,
    limit: EffectiveLimit,
) -> Result<NamespaceIdPage> {
    let max_limit =
        EffectiveLimit::new(const { NonZeroU32::new(MAX_NAMESPACE_IDS_PAGE_LIMIT).unwrap() });
    let page = store
        .list_child_prefixes(
            NAMESPACES_PREFIX,
            start_after.map(|cursor| cursor.0.as_str()),
            limit.min(max_limit),
        )
        .await?;
    let namespace_ids: Vec<NamespaceId> = page
        .items
        .iter()
        .filter_map(|child| {
            let name = child.strip_prefix(NAMESPACES_PREFIX)?.strip_suffix('/')?;
            NamespaceId::parse(name).ok()
        })
        .collect();
    Ok(NamespaceIdPage {
        skipped_entries: page.items.len() - namespace_ids.len(),
        namespace_ids,
        next_cursor: page.next_cursor.map(NamespacePageCursor),
    })
}

fn parsed<'a>(
    family: DurableObjectFamily,
    owner_namespace_id: &'a str,
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
    // The key builder is tested where it is defined.
    #![allow(clippy::disallowed_methods)]

    use super::{parse_object_key, DurableObjectFamily};
    use crate::keys::{
        content_blob, hint, metadata_manifest_object, metadata_segment, metadata_segment_prefix,
        pin, scratch_object, upload_session, wal_object, wal_prefix,
    };
    use loonfs_types::{
        ContentId, ManifestNo, MetadataSegmentId, NamespaceId, PinId, UploadId, WalNo,
    };

    #[test]
    fn built_keys_parse_to_their_family_owner_and_identifier() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let wal_no = WalNo(1);
        let manifest_object_id = ManifestNo(400);
        let metadata_segment_id = MetadataSegmentId::parse("seg_00000000000000000000000000000001")
            .expect("metadata segment id");
        let pin_id = PinId::parse("pin_00000000000000000001-0000000000000001").expect("pin id");
        let upload_id = UploadId::parse("upl_00000000000000000000000000000001").expect("upload id");
        let content_id =
            ContentId::parse("con_abcdef0123456789abcdef0123456789").expect("content id");
        let scratch = scratch_object(&namespace_id);
        let scratch_id = scratch.rsplit('/').next();
        let cases = [
            (
                wal_object(&namespace_id, &wal_no),
                DurableObjectFamily::WalObject,
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
                pin(&namespace_id, &pin_id),
                DurableObjectFamily::Pin,
                Some(pin_id.as_str()),
            ),
            (
                upload_session(&namespace_id, &upload_id),
                DurableObjectFamily::UploadSession,
                Some(upload_id.as_str()),
            ),
            (
                content_blob(&namespace_id, &content_id),
                DurableObjectFamily::ContentBlob,
                Some(content_id.as_str()),
            ),
            (
                scratch.clone(),
                DurableObjectFamily::ScratchObject,
                scratch_id,
            ),
        ];

        for (key, family, identifier) in cases {
            let parsed = parse_object_key(&key).expect("built key should parse");
            assert_eq!(parsed.family(), family);
            assert_eq!(parsed.owner_namespace_id(), "ns-1");
            assert_eq!(parsed.identifier(), identifier);
        }
        for owner in ["a", "ab"] {
            let owner = NamespaceId::parse(owner).expect("owner");
            let key = content_blob(&owner, &content_id);
            assert_eq!(key, format!("namespaces/{owner}/content/{content_id}"));
            assert_eq!(
                parse_object_key(&key)
                    .expect("content key")
                    .owner_namespace_id(),
                owner.as_str()
            );
        }
    }

    #[test]
    fn listing_prefixes_hold_only_their_family() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let segment_id =
            MetadataSegmentId::parse("seg_00000000000000000000000000000001").expect("segment id");
        let segment = metadata_segment(&namespace_id, &segment_id);

        assert!(segment.starts_with(&metadata_segment_prefix(&namespace_id)));
        let wal_objects = wal_prefix(&namespace_id);
        assert!(!hint(&namespace_id).starts_with(&wal_objects));
    }

    #[test]
    fn parser_rejects_malformed_paths() {
        for key in [
            "namespaces/ns-1/wal/00000000000000000000.wal.zst",
            "namespaces/ns-1/wal/1.wal.zst",
            "namespaces/ns-1/wal/99999999999999999999.wal.zst",
            "namespaces/ns-1/wal/0000000000000000000x.wal.zst",
            "namespaces/ns-1/wal/00000000000000000001.tmp",
            "namespaces/ns-1/segments/seg_1.tmp",
            "namespaces/ns-1/manifests/00000000000000000001.tmp",
            "namespaces/ns-1/pins/pin_1.tmp",
            "namespaces/ns-1/uploads/upl_1.tmp",
            "private/random.json",
            "namespaces/ns-1/unknown/file.json",
            "../namespaces/ns-1/hint.json",
            "namespaces/ns-1/../hint.json",
            "namespaces/ns-1/content/nested/object",
        ] {
            assert!(
                parse_object_key(key).is_none(),
                "unexpected key parsed: {key}"
            );
        }
    }
}
