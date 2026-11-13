//! The durable key grammar: object families, key classification, and the
//! namespace and pool listings built on it.

use crate::keys::{
    namespace_prefix, pool_prefix, BOUNDARY_NAME, CONTENT_DIRECTORY, EXTENSIONS_DIRECTORY,
    MANIFEST_DIRECTORY, MANIFEST_SUFFIX, NAMESPACES_DIRECTORY, POOLS_DIRECTORY, POOL_RECORD_NAME,
    SEGMENT_DIRECTORY, SEGMENT_SUFFIX, SNAPSHOT_DIRECTORY, SNAPSHOT_SUFFIX, TEMPORARY_DIRECTORY,
    UPLOAD_DIRECTORY, UPLOAD_OUTCOME_SUFFIX, UPLOAD_SESSION_SUFFIX, WAL_DIRECTORY, WAL_SUFFIX,
};
use crate::{ObjectStore, Result};
use loonfs_types::{ContentId, EffectiveLimit, ManifestNo, NamespaceId, PoolId, UploadId};
use std::num::NonZeroU32;

const NAMESPACES_PREFIX: &str = "namespaces/";
const POOLS_PREFIX: &str = "pools/";

/// Length of a content ID: `con_` and a generated ID body.
const CONTENT_ID_BYTES: usize = "con_".len() + loonfs_types::GENERATED_ID_BODY_BYTES;

/// Length of a span object name: the content ID, then `-` and a twenty-digit
/// offset, twice.
const CONTENT_SPAN_NAME_BYTES: usize = CONTENT_ID_BYTES + 2 * 21;

/// Most ids one [`list_namespace_ids`] or [`list_pool_ids`] page contains: the
/// most keys S3 and Google Cloud Storage return for one list request.
pub const MAX_NAMESPACE_IDS_PER_PAGE: NonZeroU32 = NonZeroU32::new(1_000).unwrap();

/// One family in the [durable object key grammar].
///
/// [durable object key grammar]: https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a8-object-keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableObjectFamily {
    /// Classifies an immutable numbered object in a namespace's WAL.
    WalObject,
    /// Records the manifest number at or below which every manifest of a
    /// namespace is replaced.
    Boundary,
    /// Classifies an immutable numbered namespace manifest.
    MetadataManifest,
    /// Classifies an immutable metadata segment.
    MetadataSegment,
    /// Classifies an immutable segment that an extension entry lists.
    ExtensionSegment,
    /// Classifies a snapshot to a numbered manifest.
    Snapshot,
    /// Classifies the record that starts an upload session.
    UploadSession,
    /// Classifies the record of how an upload session ended.
    UploadOutcome,
    /// Classifies the record that a pool's create call writes.
    Pool,
    /// Classifies immutable whole-file content bytes.
    ContentBlob,
    /// Classifies a temporary object that an assembly writes and deletes.
    TemporaryObject,
}

/// The prefix that contains a recognized key, with the unvalidated ID text
/// from the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyPrefix<'a> {
    /// The prefix of a namespace (`namespaces/{namespace_id}/`), which
    /// contains the namespace's WAL objects, manifests, manifest
    /// boundary, snapshots, and the metadata and extension segments that the
    /// namespace owns.
    Namespace(&'a str),
    /// The prefix of a pool (`pools/{pool_id}/`), which contains the pool's
    /// pool record, content objects, upload records, and temporary objects.
    Pool(&'a str),
}

/// Reports the durable family and identifiers recoverable from a recognized key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedObjectKey<'a> {
    family: DurableObjectFamily,
    prefix: KeyPrefix<'a>,
    identifier: Option<&'a str>,
}

impl<'a> ParsedObjectKey<'a> {
    /// Returns the durable family selected by the key's path shape.
    pub fn family(&self) -> DurableObjectFamily {
        self.family
    }

    /// Returns the prefix that contains the key: the pool for a pool record,
    /// a content object, an upload record, or a temporary object, the owner
    /// namespace for a metadata or extension segment, and the namespace
    /// itself for every other family.
    pub fn prefix(&self) -> KeyPrefix<'a> {
        self.prefix
    }

    /// Returns the family-specific identifier when the key has one.
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
        [POOLS_DIRECTORY, pool_id, POOL_RECORD_NAME] => Some(parsed(
            DurableObjectFamily::Pool,
            KeyPrefix::Pool(pool_id),
            None,
        )),
        [POOLS_DIRECTORY, pool_id, CONTENT_DIRECTORY, content_id]
            if content_id.len() == CONTENT_ID_BYTES =>
        {
            Some(parsed(
                DurableObjectFamily::ContentBlob,
                KeyPrefix::Pool(pool_id),
                Some(content_id),
            ))
        }
        [POOLS_DIRECTORY, pool_id, CONTENT_DIRECTORY, object]
            if object.len() == CONTENT_SPAN_NAME_BYTES
                && object.is_ascii()
                && object.as_bytes()[CONTENT_ID_BYTES] == b'-'
                && object.as_bytes()[CONTENT_ID_BYTES + 21] == b'-'
                && object[CONTENT_ID_BYTES + 1..CONTENT_ID_BYTES + 21]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit())
                && object[CONTENT_ID_BYTES + 22..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit()) =>
        {
            Some(parsed(
                DurableObjectFamily::ContentBlob,
                KeyPrefix::Pool(pool_id),
                Some(&object[..CONTENT_ID_BYTES]),
            ))
        }
        [NAMESPACES_DIRECTORY, namespace, WAL_DIRECTORY, object] => object
            .strip_suffix(WAL_SUFFIX)
            .filter(|identifier| parse_wal_no(identifier).is_some())
            .map(|identifier| {
                parsed(
                    DurableObjectFamily::WalObject,
                    KeyPrefix::Namespace(namespace),
                    Some(identifier),
                )
            }),
        [NAMESPACES_DIRECTORY, namespace, BOUNDARY_NAME] => Some(parsed(
            DurableObjectFamily::Boundary,
            KeyPrefix::Namespace(namespace),
            None,
        )),
        [NAMESPACES_DIRECTORY, namespace, MANIFEST_DIRECTORY, manifest] => {
            manifest.strip_suffix(MANIFEST_SUFFIX).map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataManifest,
                    KeyPrefix::Namespace(namespace),
                    Some(identifier),
                )
            })
        }
        [NAMESPACES_DIRECTORY, namespace, SEGMENT_DIRECTORY, segment] => {
            segment.strip_suffix(SEGMENT_SUFFIX).map(|identifier| {
                parsed(
                    DurableObjectFamily::MetadataSegment,
                    KeyPrefix::Namespace(namespace),
                    Some(identifier),
                )
            })
        }
        [NAMESPACES_DIRECTORY, namespace, EXTENSIONS_DIRECTORY, _, SEGMENT_DIRECTORY, segment] => {
            segment.strip_suffix(SEGMENT_SUFFIX).map(|identifier| {
                parsed(
                    DurableObjectFamily::ExtensionSegment,
                    KeyPrefix::Namespace(namespace),
                    Some(identifier),
                )
            })
        }
        [NAMESPACES_DIRECTORY, namespace, SNAPSHOT_DIRECTORY, snapshot] => {
            snapshot.strip_suffix(SNAPSHOT_SUFFIX).map(|identifier| {
                parsed(
                    DurableObjectFamily::Snapshot,
                    KeyPrefix::Namespace(namespace),
                    Some(identifier),
                )
            })
        }
        [POOLS_DIRECTORY, pool_id, UPLOAD_DIRECTORY, upload] => {
            match upload.strip_suffix(UPLOAD_OUTCOME_SUFFIX) {
                Some(identifier) => Some(parsed(
                    DurableObjectFamily::UploadOutcome,
                    KeyPrefix::Pool(pool_id),
                    Some(identifier),
                )),
                None => upload
                    .strip_suffix(UPLOAD_SESSION_SUFFIX)
                    .map(|identifier| {
                        parsed(
                            DurableObjectFamily::UploadSession,
                            KeyPrefix::Pool(pool_id),
                            Some(identifier),
                        )
                    }),
            }
        }
        [POOLS_DIRECTORY, pool_id, TEMPORARY_DIRECTORY, temporary] => Some(parsed(
            DurableObjectFamily::TemporaryObject,
            KeyPrefix::Pool(pool_id),
            Some(temporary),
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
    let number = loonfs_types::WalNo::parse(parse_fixed_width_no(number)?).ok()?;
    (number.0 > 0).then_some(number)
}

/// Parses the twenty-digit decimal number that WAL and manifest keys contain.
fn parse_fixed_width_no(text: &str) -> Option<u64> {
    if text.len() != 20 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Extracts a manifest number from its twenty-digit durable name.
pub fn manifest_no_of(key: &str) -> Option<ManifestNo> {
    let parsed = parse_object_key(key)?;
    if parsed.family() != DurableObjectFamily::MetadataManifest {
        return None;
    }
    ManifestNo::parse(parse_fixed_width_no(parsed.identifier()?)?).ok()
}

/// Extracts and validates an upload identity from the key of its session
/// record.
pub fn upload_id_of(key: &str) -> Option<UploadId> {
    upload_id_in(key, DurableObjectFamily::UploadSession)
}

/// Extracts and validates an upload identity from the key of its outcome
/// record.
pub fn upload_outcome_id_of(key: &str) -> Option<UploadId> {
    upload_id_in(key, DurableObjectFamily::UploadOutcome)
}

fn upload_id_in(key: &str, family: DurableObjectFamily) -> Option<UploadId> {
    parse_object_key(key)
        .filter(|parsed| parsed.family() == family)
        .and_then(|parsed| parsed.identifier())
        .and_then(|identifier| UploadId::parse(identifier).ok())
}

/// Extracts and validates a content identity from its durable key.
pub fn content_id_of(key: &str) -> Option<ContentId> {
    parse_object_key(key)
        .filter(|parsed| parsed.family() == DurableObjectFamily::ContentBlob)
        .and_then(|parsed| parsed.identifier())
        .and_then(|identifier| ContentId::parse(identifier).ok())
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
/// The page starts after `start_after` and contains at most `limit` ids, or
/// [`MAX_NAMESPACE_IDS_PER_PAGE`] when `limit` is larger. One page costs
/// one store list request, however many objects each namespace has; see
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
    let page = list_child_ids(
        store,
        NAMESPACES_PREFIX,
        start_after.map(|cursor| cursor.0.as_str()),
        limit,
        |name| NamespaceId::parse(name).ok(),
    )
    .await?;
    Ok(NamespaceIdPage {
        namespace_ids: page.ids,
        skipped_entries: page.skipped_entries,
        next_cursor: page.next_cursor.map(NamespacePageCursor),
    })
}

/// One page of pool ids from [`list_pool_ids`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolIdPage {
    /// Pool ids in ascending key order.
    pub pool_ids: Vec<PoolId>,
    /// Children of `pools/` on this page whose names are not pool ids.
    pub skipped_entries: usize,
    /// Where the next page starts, or `None` when this page ends the listing.
    pub next_cursor: Option<PoolPageCursor>,
}

/// A position in the pool listing that [`list_pool_ids`] resumes after.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolPageCursor(String);

impl PoolPageCursor {
    /// Builds the position just after `pool_id`.
    pub fn after(pool_id: &PoolId) -> Self {
        Self(pool_prefix(pool_id))
    }
}

/// Lists one page of the pool ids in `store`, in ascending key order, with
/// the paging of [`list_namespace_ids`].
///
/// A child of `pools/` whose name is not a pool id is skipped and counted in
/// `skipped_entries`. A pool whose members are all retired is still listed,
/// because its pool record stays.
pub async fn list_pool_ids<S: ObjectStore + ?Sized>(
    store: &S,
    start_after: Option<&PoolPageCursor>,
    limit: EffectiveLimit,
) -> Result<PoolIdPage> {
    let page = list_child_ids(
        store,
        POOLS_PREFIX,
        start_after.map(|cursor| cursor.0.as_str()),
        limit,
        |name| PoolId::parse(name).ok(),
    )
    .await?;
    Ok(PoolIdPage {
        pool_ids: page.ids,
        skipped_entries: page.skipped_entries,
        next_cursor: page.next_cursor.map(PoolPageCursor),
    })
}

/// One page of the ids that name the children of one prefix.
struct ChildIdPage<T> {
    ids: Vec<T>,
    skipped_entries: usize,
    next_cursor: Option<String>,
}

/// Lists one page of the children of `prefix` after `start_after`, at most
/// `limit` or [`MAX_NAMESPACE_IDS_PER_PAGE`], and parses each child's name
/// with `parse`.
async fn list_child_ids<S: ObjectStore + ?Sized, T>(
    store: &S,
    prefix: &str,
    start_after: Option<&str>,
    limit: EffectiveLimit,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<ChildIdPage<T>> {
    let max_limit = EffectiveLimit::new(MAX_NAMESPACE_IDS_PER_PAGE);
    let page = store
        .list_child_prefixes(prefix, start_after, limit.min(max_limit))
        .await?;
    let ids: Vec<T> = page
        .items
        .iter()
        .filter_map(|child| parse(child.strip_prefix(prefix)?.strip_suffix('/')?))
        .collect();
    Ok(ChildIdPage {
        skipped_entries: page.items.len() - ids.len(),
        ids,
        next_cursor: page.next_cursor,
    })
}

fn parsed<'a>(
    family: DurableObjectFamily,
    prefix: KeyPrefix<'a>,
    identifier: Option<&'a str>,
) -> ParsedObjectKey<'a> {
    ParsedObjectKey {
        family,
        prefix,
        identifier,
    }
}

#[cfg(test)]
mod tests {
    // The key builder is tested where it is defined.
    #![allow(clippy::disallowed_methods)]

    use super::{parse_object_key, DurableObjectFamily, KeyPrefix};
    use crate::keys::{
        boundary, content_blob, extension_segment, metadata_manifest_object, metadata_segment,
        metadata_segment_prefix, pool_record, snapshot_record, temporary_object, upload_outcome,
        upload_session, wal_object, wal_prefix,
    };
    use loonfs_test_support::ids::numbered_pool_id;
    use loonfs_types::{
        ContentId, ExtensionName, IndexSegmentId, ManifestNo, MetadataSegmentId, NamespaceId,
        SnapshotId, UploadId, WalNo,
    };

    #[test]
    fn spans_use_the_chain_id_and_require_exact_decimal_bounds() {
        let pool_id = numbered_pool_id(1);
        let content_id = ContentId::generate();
        let own_key = content_blob(&pool_id, &content_id);
        let key = crate::keys::content_span(&pool_id, &content_id, 2, 19);
        assert_eq!(
            key,
            format!("{own_key}-00000000000000000002-00000000000000000019")
        );
        assert_eq!(super::content_id_of(&key), Some(content_id.clone()));
        assert_eq!(super::content_id_of(&own_key), Some(content_id));
        assert!(own_key < key);
        assert!(parse_object_key(&format!(
            "{own_key}/00000000000000000002-00000000000000000019"
        ))
        .is_none());
        for suffix in [
            "2-19",
            "00000000000000000002_00000000000000000019",
            "00000000000000000002-0000000000000000001x",
            "00000000000000000002-00000000000000000019/extra",
        ] {
            assert!(parse_object_key(&format!("{own_key}-{suffix}")).is_none());
        }
    }

    #[test]
    fn built_keys_parse_to_their_family_prefix_and_identifier() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let pool_id = numbered_pool_id(1);
        let namespace = KeyPrefix::Namespace("ns-1");
        let pool = KeyPrefix::Pool(pool_id.as_str());
        let wal_no = WalNo(1);
        let manifest_object_id = ManifestNo(400);
        let metadata_segment_id = MetadataSegmentId::parse("seg_00000000000000000000000001")
            .expect("metadata segment id");
        let snapshot_id = SnapshotId::parse("snp_00000000000000000001-00000000000000000000000001")
            .expect("snapshot id");
        let upload_id = UploadId::parse("upl_00000000000000000000000001").expect("upload id");
        let content_id = ContentId::parse("con_5bsqqg28t5cy4tqkff04hmasw9").expect("content id");
        let index_segment_id =
            IndexSegmentId::parse("idx_00000000000000000000000001").expect("index segment id");
        let temporary = temporary_object(&pool_id);
        let temporary_id = temporary.rsplit('/').next();
        let cases = [
            (
                wal_object(&namespace_id, &wal_no),
                DurableObjectFamily::WalObject,
                namespace,
                Some("00000000000000000001"),
            ),
            (
                boundary(&namespace_id),
                DurableObjectFamily::Boundary,
                namespace,
                None,
            ),
            (
                metadata_manifest_object(&namespace_id, &manifest_object_id),
                DurableObjectFamily::MetadataManifest,
                namespace,
                Some("00000000000000000400"),
            ),
            (
                metadata_segment(&namespace_id, &metadata_segment_id),
                DurableObjectFamily::MetadataSegment,
                namespace,
                Some(metadata_segment_id.as_str()),
            ),
            (
                extension_segment(
                    &namespace_id,
                    &ExtensionName::parse("grep").expect("extension name"),
                    &index_segment_id,
                ),
                DurableObjectFamily::ExtensionSegment,
                namespace,
                Some(index_segment_id.as_str()),
            ),
            (
                snapshot_record(&namespace_id, &snapshot_id),
                DurableObjectFamily::Snapshot,
                namespace,
                Some(snapshot_id.as_str()),
            ),
            (pool_record(&pool_id), DurableObjectFamily::Pool, pool, None),
            (
                upload_session(&pool_id, &upload_id),
                DurableObjectFamily::UploadSession,
                pool,
                Some(upload_id.as_str()),
            ),
            (
                upload_outcome(&pool_id, &upload_id),
                DurableObjectFamily::UploadOutcome,
                pool,
                Some(upload_id.as_str()),
            ),
            (
                content_blob(&pool_id, &content_id),
                DurableObjectFamily::ContentBlob,
                pool,
                Some(content_id.as_str()),
            ),
            (
                crate::keys::content_span(&pool_id, &content_id, 2, 19),
                DurableObjectFamily::ContentBlob,
                pool,
                Some(content_id.as_str()),
            ),
            (
                temporary.clone(),
                DurableObjectFamily::TemporaryObject,
                pool,
                temporary_id,
            ),
        ];

        for (key, family, prefix, identifier) in cases {
            let parsed = parse_object_key(&key).expect("built key should parse");
            assert_eq!(parsed.family(), family);
            assert_eq!(parsed.prefix(), prefix);
            assert_eq!(parsed.identifier(), identifier);
        }
        assert_eq!(
            content_blob(&pool_id, &content_id),
            format!("pools/{pool_id}/content/{content_id}")
        );
    }

    #[test]
    fn an_upload_outcome_key_sorts_directly_after_its_session_key() {
        // Garbage collection pairs a session's two records by listing order.
        let pool_id = numbered_pool_id(1);
        let first = UploadId::parse("upl_00000000000000000000000001").expect("upload id");
        let second = UploadId::parse("upl_0000000000000000000000000g").expect("upload id");
        let mut keys = [
            upload_outcome(&pool_id, &second),
            upload_session(&pool_id, &second),
            upload_outcome(&pool_id, &first),
            upload_session(&pool_id, &first),
        ];
        keys.sort();
        assert_eq!(
            keys,
            [
                upload_session(&pool_id, &first),
                upload_outcome(&pool_id, &first),
                upload_session(&pool_id, &second),
                upload_outcome(&pool_id, &second),
            ]
        );
        assert_eq!(super::upload_id_of(&keys[0]), Some(first.clone()));
        assert_eq!(super::upload_outcome_id_of(&keys[1]), Some(first));
        assert_eq!(super::upload_id_of(&keys[1]), None);
    }

    #[test]
    fn listing_prefixes_hold_only_their_family() {
        let namespace_id = NamespaceId::parse("ns-1").expect("namespace id");
        let segment_id =
            MetadataSegmentId::parse("seg_00000000000000000000000001").expect("segment id");
        let segment = metadata_segment(&namespace_id, &segment_id);

        assert!(segment.starts_with(&metadata_segment_prefix(&namespace_id)));
        let wal_objects = wal_prefix(&namespace_id);
        assert!(!boundary(&namespace_id).starts_with(&wal_objects));
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
            "namespaces/ns-1/manifests/00000000000000000001.json",
            "namespaces/ns-1/snapshots/snp_1.tmp",
            "namespaces/ns-1/uploads/upl_1.tmp",
            "namespaces/ns-1/content/con_5bsqqg28t5cy4tqkff04hmasw9",
            "namespaces/ns-1/content/con_5bsqqg28t5cy4tqkff04hmasw9-00000000000000000002-00000000000000000019",
            "namespaces/ns-1/uploads/upl_00000000000000000000000001.json",
            "namespaces/ns-1/uploads/upl_00000000000000000000000001.outcome.json",
            "namespaces/ns-1/temporary/tmp_00000000000000000000000001",
            "pools/pool_00000000000000000000000001/content/nested/object",
            "pools/pool_00000000000000000000000001/boundary.json",
            "pools/pool_00000000000000000000000001/wal/00000000000000000001.wal.zst",
            "private/random.json",
            "namespaces/ns-1/unknown/file.json",
            "../namespaces/ns-1/boundary.json",
            "namespaces/ns-1/../boundary.json",
            "namespaces/ns-1/content/nested/object",
            "namespaces/ns-1/extensions/grep/boundary.json",
            "namespaces/ns-1/extensions/grep/manifests/00000000000000000001.json",
            "namespaces/ns-1/extensions/grep/segments/idx_1.tmp",
            "namespaces/ns-1/extensions/grep/segments/nested/idx_1.sst",
        ] {
            assert!(
                parse_object_key(key).is_none(),
                "unexpected key parsed: {key}"
            );
        }
    }
}
