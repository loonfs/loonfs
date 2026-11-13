//! Key construction for every [durable object family].
//!
//! [durable object family]: https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a8-object-keys

use loonfs_types::format::manifest::{ExtensionSegmentRef, MetadataSegmentRef};
use loonfs_types::{
    ContentId, ExtensionName, IndexSegmentId, ManifestNo, MetadataSegmentId, NamespaceId, PoolId,
    ProbeId, SnapshotId, UploadId, WalNo,
};

/// Prefix for objects created by store probes.
pub const PROBE_RUN_PREFIX: &str = "probe-runs";

pub(crate) const NAMESPACES_DIRECTORY: &str = "namespaces";
pub(crate) const POOLS_DIRECTORY: &str = "pools";
pub(crate) const POOL_RECORD_NAME: &str = "pool.json";
pub(crate) const HINT_NAME: &str = "hint.json";
pub(crate) const WAL_DIRECTORY: &str = "wal";
pub(crate) const WAL_SUFFIX: &str = ".wal.zst";
pub(crate) const MANIFEST_DIRECTORY: &str = "manifests";
pub(crate) const MANIFEST_SUFFIX: &str = ".json.zst";
pub(crate) const SEGMENT_DIRECTORY: &str = "segments";
pub(crate) const SEGMENT_SUFFIX: &str = ".sst";
pub(crate) const SNAPSHOT_DIRECTORY: &str = "snapshots";
pub(crate) const SNAPSHOT_SUFFIX: &str = ".json";
pub(crate) const UPLOAD_DIRECTORY: &str = "uploads";
pub(crate) const UPLOAD_SESSION_SUFFIX: &str = ".json";
pub(crate) const UPLOAD_OUTCOME_SUFFIX: &str = ".outcome.json";
pub(crate) const CONTENT_DIRECTORY: &str = "content";
pub(crate) const TEMPORARY_DIRECTORY: &str = "temporary";
pub(crate) const EXTENSIONS_DIRECTORY: &str = "extensions";

/// Builds the listing prefix containing every durable object owned by one namespace.
pub fn namespace_prefix(namespace_id: &NamespaceId) -> String {
    format!("{NAMESPACES_DIRECTORY}/{namespace_id}/")
}

/// Builds the listing prefix containing every extension's objects in one
/// namespace.
pub fn extensions_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}{EXTENSIONS_DIRECTORY}/", namespace_prefix(namespace_id))
}

/// Builds the listing prefix containing every object one extension owns in
/// one namespace.
pub fn extension_prefix(namespace_id: &NamespaceId, name: &ExtensionName) -> String {
    format!("{}{name}/", extensions_prefix(namespace_id))
}

/// Builds the listing prefix containing one extension's segment objects
/// owned by one namespace.
pub fn extension_segment_prefix(namespace_id: &NamespaceId, name: &ExtensionName) -> String {
    format!(
        "{}{SEGMENT_DIRECTORY}/",
        extension_prefix(namespace_id, name)
    )
}

/// Builds the immutable key of one extension segment.
pub fn extension_segment(
    namespace_id: &NamespaceId,
    name: &ExtensionName,
    segment_id: &IndexSegmentId,
) -> String {
    format!(
        "{}{segment_id}{SEGMENT_SUFFIX}",
        extension_segment_prefix(namespace_id, name)
    )
}

/// Derives the key of a segment that the entry of extension `name` lists,
/// from the segment's owner and generated identity.
pub fn extension_segment_object_key(
    name: &ExtensionName,
    descriptor: &ExtensionSegmentRef,
) -> String {
    extension_segment(&descriptor.owner_namespace_id, name, &descriptor.segment_id)
}

/// Builds the immutable object key for a numbered WAL object.
pub fn wal_object(namespace_id: &NamespaceId, wal_no: &WalNo) -> String {
    format!("{}{:020}{WAL_SUFFIX}", wal_prefix(namespace_id), wal_no.0)
}

/// Builds the listing prefix for numbered WAL objects.
pub fn wal_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}{WAL_DIRECTORY}/", namespace_prefix(namespace_id))
}

/// Builds the starting point for numbered manifest discovery.
pub fn hint(namespace_id: &NamespaceId) -> String {
    format!("{}{HINT_NAME}", namespace_prefix(namespace_id))
}

/// Builds the listing prefix containing numbered namespace manifests.
pub fn metadata_manifest_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}{MANIFEST_DIRECTORY}/", namespace_prefix(namespace_id))
}

/// Builds the listing prefix containing metadata segment objects owned by one namespace.
pub fn metadata_segment_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}{SEGMENT_DIRECTORY}/", namespace_prefix(namespace_id))
}

/// Builds the immutable manifest key for one namespace manifest number.
pub fn metadata_manifest_object(namespace_id: &NamespaceId, manifest_no: &ManifestNo) -> String {
    format!(
        "{}{:020}{MANIFEST_SUFFIX}",
        metadata_manifest_prefix(namespace_id),
        manifest_no.0
    )
}

/// Builds the immutable metadata segment key for one segment identity.
pub fn metadata_segment(
    namespace_id: &NamespaceId,
    metadata_segment_id: &MetadataSegmentId,
) -> String {
    format!(
        "{}{metadata_segment_id}{SEGMENT_SUFFIX}",
        metadata_segment_prefix(namespace_id)
    )
}

/// Derives a segment key from its owner and generated identity.
pub fn metadata_segment_object_key(descriptor: &MetadataSegmentRef) -> String {
    metadata_segment(&descriptor.owner_namespace_id, &descriptor.segment_id)
}

/// Builds a snapshot key with its manifest number in the id.
pub fn snapshot_record(namespace_id: &NamespaceId, snapshot_id: &SnapshotId) -> String {
    format!(
        "{}{snapshot_id}{SNAPSHOT_SUFFIX}",
        snapshot_prefix(namespace_id)
    )
}

/// Builds the listing prefix containing snapshots for one namespace.
pub fn snapshot_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}{SNAPSHOT_DIRECTORY}/", namespace_prefix(namespace_id))
}

/// Builds the listing prefix containing every object that one pool stores.
pub fn pool_prefix(pool_id: &PoolId) -> String {
    format!("{POOLS_DIRECTORY}/{pool_id}/")
}

/// Builds the key of the record that the create call of one pool's first
/// member writes.
pub fn pool_record(pool_id: &PoolId) -> String {
    format!("{}{POOL_RECORD_NAME}", pool_prefix(pool_id))
}

/// Builds the listing prefix containing the upload session and outcome
/// records of every member of one pool.
pub fn upload_session_prefix(pool_id: &PoolId) -> String {
    format!("{}{UPLOAD_DIRECTORY}/", pool_prefix(pool_id))
}

/// Builds the key of the record that starts one upload session, under the
/// prefix of the pool of the namespace that created the session.
pub fn upload_session(pool_id: &PoolId, upload_id: &UploadId) -> String {
    format!(
        "{}{upload_id}{UPLOAD_SESSION_SUFFIX}",
        upload_session_prefix(pool_id)
    )
}

/// Builds the key of the record of how one upload session ended, under the
/// prefix of the pool of the namespace that created the session.
///
/// The key sorts directly after the session record's key, so a listing of
/// the uploads prefix returns a session's two records next to each other.
pub fn upload_outcome(pool_id: &PoolId, upload_id: &UploadId) -> String {
    format!(
        "{}{upload_id}{UPLOAD_OUTCOME_SUFFIX}",
        upload_session_prefix(pool_id)
    )
}

/// Builds the listing prefix containing every content object of one pool.
pub fn content_prefix(pool_id: &PoolId) -> String {
    format!("{}{CONTENT_DIRECTORY}/", pool_prefix(pool_id))
}

/// Builds the immutable key of a chain's whole object, under the prefix of
/// the reading namespace's pool.
pub fn content_blob(pool_id: &PoolId, content_id: &ContentId) -> String {
    format!(
        "{}{}",
        content_prefix(pool_id),
        content_blob_name(content_id)
    )
}

/// Builds the name of a chain's whole object: its key with the pool's
/// content prefix removed.
pub fn content_blob_name(content_id: &ContentId) -> String {
    content_id.to_string()
}

/// Builds the immutable key of the span object that stores one run of chain
/// bytes, under the prefix of the reading namespace's pool.
pub fn content_span(pool_id: &PoolId, content_id: &ContentId, start: u64, end: u64) -> String {
    format!(
        "{}{}",
        content_prefix(pool_id),
        content_span_name(content_id, start, end)
    )
}

/// Builds the name of a span object: its key with the pool's content prefix
/// removed.
pub fn content_span_name(content_id: &ContentId, start: u64, end: u64) -> String {
    format!("{content_id}-{start:020}-{end:020}")
}

/// Builds a fresh key for a temporary object that an assembly of one of a
/// pool's content objects writes and deletes.
pub fn temporary_object(pool_id: &PoolId) -> String {
    format!(
        "{}{}",
        temporary_prefix(pool_id),
        loonfs_types::generated_id("tmp")
    )
}

/// Builds the listing prefix for the temporary objects that assemblies of a
/// pool's content objects write and delete.
pub fn temporary_prefix(pool_id: &PoolId) -> String {
    format!("{}{TEMPORARY_DIRECTORY}/", pool_prefix(pool_id))
}

/// Builds the listing prefix containing the objects of every store probe run.
pub fn probe_runs_prefix() -> String {
    format!("{PROBE_RUN_PREFIX}/")
}

/// Builds the listing prefix containing the objects one store probe run writes
/// and deletes.
pub fn probe_run_prefix(probe_id: &ProbeId) -> String {
    format!("{PROBE_RUN_PREFIX}/{probe_id}/")
}

#[cfg(test)]
mod tests {
    // The key builder is tested where it is defined.
    #![allow(clippy::disallowed_methods)]

    use super::{
        content_blob, content_blob_name, content_prefix, content_span, content_span_name,
        extension_segment, hint, metadata_manifest_object, metadata_segment,
        metadata_segment_object_key, pool_prefix, pool_record, probe_run_prefix, probe_runs_prefix,
        snapshot_record, temporary_object, upload_outcome, upload_session, wal_object, wal_prefix,
    };
    use loonfs_types::format::manifest::{
        MetadataRowFamily, MetadataSegmentRef, METADATA_SEGMENT_ENCODING,
    };
    use loonfs_types::format::segment_blocks::BlockHandle;
    use loonfs_types::{
        ContentId, ExtensionName, IndexSegmentId, ManifestNo, MetadataSegmentId, NamespaceId,
        PoolId, ProbeId, SnapshotId, UploadId, WalNo,
    };

    const CONTENT_ID: &str = "con_5bsqqg28t5cy4tqkff04hmasw9";
    const PROBE_RUN_ID: &str = "probe_00000000000000000000000001";

    fn content_id() -> ContentId {
        ContentId::parse(CONTENT_ID).expect("valid content id")
    }

    fn namespace_id() -> NamespaceId {
        loonfs_test_support::ids::namespace_id("ns-1")
    }

    fn owner_namespace_id() -> NamespaceId {
        loonfs_test_support::ids::namespace_id("owner-1")
    }

    fn pool_id() -> PoolId {
        loonfs_test_support::ids::numbered_pool_id(1)
    }

    fn snapshot_id() -> SnapshotId {
        SnapshotId::parse("snp_00000000000000000001-00000000000000000000000001")
            .expect("valid snapshot id")
    }

    fn metadata_segment_id() -> MetadataSegmentId {
        MetadataSegmentId::parse("seg_00000000000000000000000001")
            .expect("valid metadata segment id")
    }

    fn upload_id() -> UploadId {
        UploadId::parse("upl_00000000000000000000000001").expect("valid upload id")
    }

    #[test]
    fn standard_key_patterns_match_format_spec_table() {
        let spec = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/specs/format.md"
        ))
        .expect("read docs/specs/format.md");
        let section = spec
            .split_once("### A.8 Object keys")
            .expect("format.md object-key appendix exists")
            .1
            .split_once("\n## ")
            .expect("an appendix follows the object-key table")
            .0;

        let mut patterns = std::collections::BTreeMap::new();
        for line in section.lines() {
            let Some(row) = line.strip_prefix("| ") else {
                continue;
            };
            let Some((family, rest)) = row.split_once(" | ") else {
                continue;
            };
            let Some(pattern) = rest
                .strip_prefix('`')
                .and_then(|tail| tail.split_once('`'))
                .map(|(pattern, _)| pattern)
            else {
                continue;
            };
            patterns.insert(family.to_owned(), pattern.to_owned());
        }

        let temporary = temporary_object(&pool_id());
        let temporary_id = temporary
            .rsplit('/')
            .next()
            .expect("a temporary key ends in its id")
            .to_owned();
        // Each namespace placeholder has its own ID, so a builder that takes
        // the wrong one fails the comparison. An extension segment's ID has
        // its own prefix.
        let substitute = |family: &str, pattern: &str| -> String {
            let segment_id = if family == "Extension segments" {
                "idx_00000000000000000000000001"
            } else {
                "seg_00000000000000000000000001"
            };
            pattern
                .replace("{namespace_id}", "ns-1")
                .replace("{owner_namespace_id}", "owner-1")
                .replace("{pool_id}", pool_id().as_str())
                .replace("{wal_no:020}", &format!("{:020}", 42))
                .replace("{manifest_no:020}", "00000000000000000400")
                .replace(
                    "{snapshot_id}",
                    "snp_00000000000000000001-00000000000000000000000001",
                )
                .replace("{extension_name}", "grep")
                .replace("{segment_id}", segment_id)
                .replace("{upload_id}", "upl_00000000000000000000000001")
                .replace("{start:020}", "00000000000000000002")
                .replace("{end:020}", "00000000000000000019")
                .replace("{content_id}", CONTENT_ID)
                .replace("{temporary_id}", &temporary_id)
        };

        let built = [
            ("WAL objects", wal_object(&namespace_id(), &WalNo(42))),
            (
                "Namespace manifests",
                metadata_manifest_object(&namespace_id(), &ManifestNo(400)),
            ),
            (
                "Snapshot records",
                snapshot_record(&namespace_id(), &snapshot_id()),
            ),
            (
                "Metadata segments",
                metadata_segment(&owner_namespace_id(), &metadata_segment_id()),
            ),
            (
                "Extension segments",
                extension_segment(
                    &owner_namespace_id(),
                    &ExtensionName::parse("grep").expect("valid extension name"),
                    &IndexSegmentId::parse("idx_00000000000000000000000001")
                        .expect("valid index segment id"),
                ),
            ),
            ("Upload sessions", upload_session(&pool_id(), &upload_id())),
            ("Upload outcomes", upload_outcome(&pool_id(), &upload_id())),
            ("Hint", hint(&namespace_id())),
            ("Pool records", pool_record(&pool_id())),
            ("Content objects", content_blob(&pool_id(), &content_id())),
            (
                "Content span objects",
                content_span(&pool_id(), &content_id(), 2, 19),
            ),
            ("Temporary objects", temporary),
        ];

        let expected: std::collections::BTreeMap<String, String> = built
            .into_iter()
            .map(|(family, key)| (family.to_owned(), key))
            .collect();
        let actual: std::collections::BTreeMap<String, String> = patterns
            .into_iter()
            .map(|(family, pattern)| {
                let key = substitute(&family, &pattern);
                (family, key)
            })
            .collect();
        assert_eq!(
            actual, expected,
            "the format.md durable-families table and the key builders must list \
             the same families with the same key shapes"
        );
    }

    #[test]
    fn listing_prefixes_match_their_keys() {
        assert_eq!(wal_prefix(&namespace_id()), "namespaces/ns-1/wal/");
        assert!(wal_object(&namespace_id(), &WalNo(42)).starts_with(&wal_prefix(&namespace_id())));
        assert_eq!(probe_runs_prefix(), "probe-runs/");
        let probe_id = ProbeId::parse(PROBE_RUN_ID).expect("valid probe run id");
        assert!(probe_run_prefix(&probe_id).starts_with(&probe_runs_prefix()));
    }

    #[test]
    fn content_keys_are_the_content_prefix_and_a_name() {
        let prefix = content_prefix(&pool_id());
        assert_eq!(pool_prefix(&pool_id()), format!("pools/{}/", pool_id()));
        assert_eq!(
            pool_record(&pool_id()),
            format!("{}pool.json", pool_prefix(&pool_id()))
        );
        assert_eq!(prefix, format!("{}content/", pool_prefix(&pool_id())));
        assert_eq!(
            content_blob(&pool_id(), &content_id()),
            format!("{prefix}{}", content_blob_name(&content_id()))
        );
        assert_eq!(
            content_span(&pool_id(), &content_id(), 2, 19),
            format!("{prefix}{}", content_span_name(&content_id(), 2, 19))
        );
    }

    fn segment_descriptor() -> MetadataSegmentRef {
        MetadataSegmentRef {
            owner_namespace_id: namespace_id(),
            segment_id: metadata_segment_id(),
            family: MetadataRowFamily::Inodes,
            encoding: METADATA_SEGMENT_ENCODING,
            row_count: 0,
            min_row_key: String::new(),
            max_row_key: String::new(),
            index_block: BlockHandle {
                offset: 0,
                stored_bytes: 0,
                decoded_bytes: 0,
                crc32c: 0,
            },
            filter_block: BlockHandle {
                offset: 0,
                stored_bytes: 0,
                decoded_bytes: 0,
                crc32c: 0,
            },
            filter_inline: None,
            extent_filter_block: None,
        }
    }

    #[test]
    fn segment_descriptors_derive_owner_keys() {
        assert_eq!(
            metadata_segment_object_key(&segment_descriptor()),
            metadata_segment(&namespace_id(), &metadata_segment_id())
        );
    }
}
