//! Key construction for every [durable object family].
//!
//! [durable object family]: https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a8-object-keys

use loonfs_types::format::manifest::MetadataSegmentRef;
use loonfs_types::{
    ContentId, ManifestNo, MetadataSegmentId, NamespaceId, PinId, ProbeRunId, UploadId, WalNo,
};

/// Prefix for objects created by store probes.
pub const PROBE_RUN_PREFIX: &str = "probe-runs";

pub(crate) const NAMESPACES_DIRECTORY: &str = "namespaces";
pub(crate) const HINT_NAME: &str = "hint.json";
pub(crate) const WAL_DIRECTORY: &str = "wal";
pub(crate) const WAL_SUFFIX: &str = ".wal.zst";
pub(crate) const MANIFEST_DIRECTORY: &str = "manifests";
pub(crate) const MANIFEST_SUFFIX: &str = ".json.zst";
pub(crate) const SEGMENT_DIRECTORY: &str = "segments";
pub(crate) const SEGMENT_SUFFIX: &str = ".sst.zst";
pub(crate) const PIN_DIRECTORY: &str = "pins";
pub(crate) const PIN_SUFFIX: &str = ".json";
pub(crate) const UPLOAD_DIRECTORY: &str = "uploads";
pub(crate) const UPLOAD_SESSION_SUFFIX: &str = ".json";
pub(crate) const UPLOAD_OUTCOME_SUFFIX: &str = ".outcome.json";
pub(crate) const CONTENT_DIRECTORY: &str = "content";
pub(crate) const TEMPORARY_DIRECTORY: &str = "temporary";

// Each key is one `format!` whose text is literal, so the String is sized
// once. A key is built on every segment block lookup, cache hit or not, so a
// key that went through the prefix builders cost three formats and about
// eight allocator calls. `layout.rs` parses keys back with the constants
// above, and its tests cover the round trip.

/// Builds the listing prefix containing every durable object owned by one namespace.
pub fn namespace_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/")
}

/// Builds the listing prefix containing every object one extension owns in
/// one namespace.
pub fn extension_prefix(namespace_id: &NamespaceId, name: &str) -> String {
    format!("namespaces/{namespace_id}/extensions/{name}/")
}

/// Builds the immutable object key for a numbered WAL object.
pub fn wal_object(namespace_id: &NamespaceId, wal_no: &WalNo) -> String {
    format!("namespaces/{namespace_id}/wal/{:020}.wal.zst", wal_no.0)
}

/// Builds the listing prefix for numbered WAL objects.
pub fn wal_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/wal/")
}

/// Builds the starting point for numbered manifest discovery.
pub fn hint(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/hint.json")
}

/// Builds the listing prefix containing numbered namespace manifests.
pub fn metadata_manifest_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/manifests/")
}

/// Builds the listing prefix containing metadata segment objects owned by one namespace.
pub fn metadata_segment_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/segments/")
}

/// Builds the immutable manifest key for one namespace manifest number.
pub fn metadata_manifest_object(namespace_id: &NamespaceId, manifest_no: &ManifestNo) -> String {
    format!(
        "namespaces/{namespace_id}/manifests/{:020}.json.zst",
        manifest_no.0
    )
}

/// Builds the immutable metadata segment key for one segment identity.
pub fn metadata_segment(
    namespace_id: &NamespaceId,
    metadata_segment_id: &MetadataSegmentId,
) -> String {
    format!("namespaces/{namespace_id}/segments/{metadata_segment_id}.sst.zst")
}

/// Derives a segment key from its owner and generated identity.
pub fn metadata_segment_object_key(descriptor: &MetadataSegmentRef) -> String {
    metadata_segment(&descriptor.owner_namespace_id, &descriptor.segment_id)
}

/// Builds a pin key with its manifest number in the id.
pub fn pin(namespace_id: &NamespaceId, pin_id: &PinId) -> String {
    format!("namespaces/{namespace_id}/pins/{pin_id}.json")
}

/// Builds the listing prefix containing pins for one namespace.
pub fn pin_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/pins/")
}

/// Builds the listing prefix containing the upload session and outcome
/// records for one namespace.
pub fn upload_session_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/uploads/")
}

/// Builds the key of the record that starts one upload session.
pub fn upload_session(namespace_id: &NamespaceId, upload_id: &UploadId) -> String {
    format!("namespaces/{namespace_id}/uploads/{upload_id}.json")
}

/// Builds the key of the record of how one upload session ended.
///
/// The key sorts directly after the session record's key, so a listing of
/// the uploads prefix returns a session's two records next to each other.
pub fn upload_outcome(namespace_id: &NamespaceId, upload_id: &UploadId) -> String {
    format!("namespaces/{namespace_id}/uploads/{upload_id}.outcome.json")
}

/// Builds the listing prefix containing every content object one namespace owns.
pub fn content_prefix(owner_namespace_id: &NamespaceId) -> String {
    format!("namespaces/{owner_namespace_id}/content/")
}

/// Builds the immutable content-object key for one content identity.
pub fn content_blob(owner_namespace_id: &NamespaceId, content_id: &ContentId) -> String {
    format!("namespaces/{owner_namespace_id}/content/{content_id}")
}

/// Builds the immutable object key for one run of chain bytes.
pub fn content_span(
    owner_namespace_id: &NamespaceId,
    content_id: &ContentId,
    start: u64,
    end: u64,
) -> String {
    format!("namespaces/{owner_namespace_id}/content/{content_id}-{start:020}-{end:020}")
}

/// Builds a fresh key for a temporary object that an assembly writes and
/// deletes under one namespace.
pub fn temporary_object(namespace_id: &NamespaceId) -> String {
    format!(
        "namespaces/{namespace_id}/temporary/{}",
        loonfs_types::generated_id("tmp")
    )
}

/// Builds the listing prefix for the temporary objects that an assembly writes
/// and deletes under one namespace.
pub fn temporary_prefix(namespace_id: &NamespaceId) -> String {
    format!("namespaces/{namespace_id}/temporary/")
}

/// Builds the listing prefix containing the objects of every store probe run.
pub fn probe_runs_prefix() -> String {
    format!("{PROBE_RUN_PREFIX}/")
}

/// Builds the listing prefix containing the objects one store probe run writes
/// and deletes.
pub fn probe_run_prefix(run_id: &ProbeRunId) -> String {
    format!("{PROBE_RUN_PREFIX}/{run_id}/")
}

#[cfg(test)]
mod tests {
    // The key builder is tested where it is defined.
    #![allow(clippy::disallowed_methods)]

    use super::{
        content_blob, hint, metadata_manifest_object, metadata_segment,
        metadata_segment_object_key, pin, probe_run_prefix, probe_runs_prefix, temporary_object,
        upload_outcome, upload_session, wal_object, wal_prefix,
    };
    use loonfs_types::format::manifest::{
        MetadataRowFamily, MetadataSegmentRef, METADATA_SEGMENT_ENCODING,
    };
    use loonfs_types::format::segment_blocks::BlockHandle;
    use loonfs_types::{
        ContentId, ManifestNo, MetadataSegmentId, NamespaceId, PinId, ProbeRunId, UploadId, WalNo,
    };

    const CONTENT_ID: &str = "con_5bsqqg28t5cy4tqkff04hmasw9";
    const PROBE_RUN_ID: &str = "probe_00000000000000000000000001";

    fn content_id() -> ContentId {
        ContentId::parse(CONTENT_ID).expect("valid content id")
    }

    fn namespace_id() -> NamespaceId {
        loonfs_test_support::ids::namespace_id("ns-1")
    }

    fn pin_id() -> PinId {
        PinId::parse("pin_00000000000000000001-0000000000000001").expect("valid pin id")
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

        let temporary = temporary_object(&namespace_id());
        let temporary_id = temporary
            .rsplit('/')
            .next()
            .expect("a temporary key ends in its id")
            .to_owned();
        let substitute = |pattern: &str| -> String {
            pattern
                .replace("{namespace_id}", "ns-1")
                .replace("{owner_namespace_id}", "ns-1")
                .replace("{source_namespace_id}", "ns-1")
                .replace("{wal_no:020}", &format!("{:020}", 42))
                .replace("{suffix}", "0123456789abcdef")
                .replace("{manifest_no:020}", "00000000000000000400")
                .replace("{pin_id}", "pin_00000000000000000001-0000000000000001")
                .replace("{job_id}", "cmp_00000000000000000000000001")
                .replace("{group}", "bindings")
                .replace("{segment_id}", "seg_00000000000000000000000001")
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
            ("Pin records", pin(&namespace_id(), &pin_id())),
            (
                "Metadata segments",
                metadata_segment(&namespace_id(), &metadata_segment_id()),
            ),
            (
                "Upload sessions",
                upload_session(&namespace_id(), &upload_id()),
            ),
            (
                "Upload outcomes",
                upload_outcome(&namespace_id(), &upload_id()),
            ),
            ("Hint", hint(&namespace_id())),
            (
                "Content objects",
                content_blob(&namespace_id(), &content_id()),
            ),
            (
                "Content span objects",
                super::content_span(&namespace_id(), &content_id(), 2, 19),
            ),
            ("Temporary objects", temporary),
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
    fn listing_prefixes_match_their_keys() {
        assert_eq!(wal_prefix(&namespace_id()), "namespaces/ns-1/wal/");
        assert!(wal_object(&namespace_id(), &WalNo(42)).starts_with(&wal_prefix(&namespace_id())));
        assert_eq!(probe_runs_prefix(), "probe-runs/");
        let run_id = ProbeRunId::parse(PROBE_RUN_ID).expect("valid probe run id");
        assert!(probe_run_prefix(&run_id).starts_with(&probe_runs_prefix()));
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
