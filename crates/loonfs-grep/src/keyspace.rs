//! Grep-owned durable object keys and their strict parser.

use crate::root::GrepManifestObjectId;
use loonfs_api::{IndexSegmentId, NamespaceId};

const NAMESPACE_KEYSPACE_PREFIX: &str = "namespaces/";
const GREP_EXTENSION_PREFIX: &str = "extensions/grep/";

/// The grep-owned object kind named by a parsed key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepKeyKind {
    Root,
    Manifest {
        manifest_object_id: GrepManifestObjectId,
    },
    Segment {
        segment_id: IndexSegmentId,
    },
}

/// A recognized grep key split into its namespace and object kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedGrepKey {
    pub namespace_id: NamespaceId,
    pub kind: GrepKeyKind,
}

/// Prefix containing every grep object for one namespace.
pub fn grep_prefix(namespace_id: &NamespaceId) -> String {
    format!("{NAMESPACE_KEYSPACE_PREFIX}{namespace_id}/{GREP_EXTENSION_PREFIX}")
}

/// Key of one namespace's atomic grep root pointer.
pub fn root_key(namespace_id: &NamespaceId) -> String {
    format!("{}root.json", grep_prefix(namespace_id))
}

/// Prefix containing one namespace's immutable grep manifests.
pub fn manifests_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}manifests/", grep_prefix(namespace_id))
}

/// Key of one immutable grep manifest, under the id its publisher minted.
pub fn manifest_key(
    namespace_id: &NamespaceId,
    manifest_object_id: &GrepManifestObjectId,
) -> String {
    format!(
        "{}{manifest_object_id}.manifest.json",
        manifests_prefix(namespace_id)
    )
}

/// Prefix containing one namespace's immutable grep segments.
pub fn segments_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}segments/", grep_prefix(namespace_id))
}

/// Key of one immutable grep segment.
pub fn segment_key(namespace_id: &NamespaceId, segment_id: &IndexSegmentId) -> String {
    format!("{}{segment_id}.sst.zst", segments_prefix(namespace_id))
}

/// Parses exactly the grep root, manifest, and segment key grammar.
///
/// Prefixes, malformed ids, unknown object families, temporary suffixes,
/// and keys with trailing path components are rejected.
pub fn parse_key(key: &str) -> Option<ParsedGrepKey> {
    let suffix = key.strip_prefix(NAMESPACE_KEYSPACE_PREFIX)?;
    let (namespace, suffix) = suffix.split_once('/')?;
    let namespace_id = NamespaceId::parse(namespace).ok()?;
    let object = suffix.strip_prefix(GREP_EXTENSION_PREFIX)?;
    let kind = if object == "root.json" {
        GrepKeyKind::Root
    } else if let Some(manifest) = object.strip_prefix("manifests/") {
        let manifest = manifest.strip_suffix(".manifest.json")?;
        if manifest.contains('/') {
            return None;
        }
        GrepKeyKind::Manifest {
            manifest_object_id: GrepManifestObjectId::parse(manifest).ok()?,
        }
    } else {
        let segment = object.strip_prefix("segments/")?.strip_suffix(".sst.zst")?;
        if segment.contains('/') {
            return None;
        }
        GrepKeyKind::Segment {
            segment_id: IndexSegmentId::parse(segment).ok()?,
        }
    };
    Some(ParsedGrepKey { namespace_id, kind })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn namespace_id() -> NamespaceId {
        NamespaceId::parse("docs").expect("valid namespace id")
    }

    fn segment_id() -> IndexSegmentId {
        IndexSegmentId::parse("idx_00000000000000000000000000000001").expect("valid segment id")
    }

    #[test]
    fn key_builders_pin_the_extension_grammar() {
        let namespace_id = namespace_id();
        let segment_id = segment_id();

        let manifest_object_id =
            GrepManifestObjectId::parse("gmf_0123456789abcdef0123456789abcdef")
                .expect("valid manifest object id");

        assert_eq!(
            grep_prefix(&namespace_id),
            "namespaces/docs/extensions/grep/"
        );
        assert_eq!(
            root_key(&namespace_id),
            "namespaces/docs/extensions/grep/root.json"
        );
        assert_eq!(
            manifests_prefix(&namespace_id),
            "namespaces/docs/extensions/grep/manifests/"
        );
        assert_eq!(
            manifest_key(&namespace_id, &manifest_object_id),
            "namespaces/docs/extensions/grep/manifests/gmf_0123456789abcdef0123456789abcdef.manifest.json"
        );
        assert_eq!(
            segments_prefix(&namespace_id),
            "namespaces/docs/extensions/grep/segments/"
        );
        assert_eq!(
            segment_key(&namespace_id, &segment_id),
            "namespaces/docs/extensions/grep/segments/idx_00000000000000000000000000000001.sst.zst"
        );
    }

    #[test]
    fn built_keys_round_trip_through_the_parser() {
        let namespace_id = namespace_id();
        let segment_id = segment_id();
        let manifest_object_id =
            GrepManifestObjectId::parse("gmf_0123456789abcdef0123456789abcdef")
                .expect("valid manifest object id");

        assert_eq!(
            parse_key(&root_key(&namespace_id)),
            Some(ParsedGrepKey {
                namespace_id: namespace_id.clone(),
                kind: GrepKeyKind::Root,
            })
        );
        assert_eq!(
            parse_key(&manifest_key(&namespace_id, &manifest_object_id)),
            Some(ParsedGrepKey {
                namespace_id: namespace_id.clone(),
                kind: GrepKeyKind::Manifest { manifest_object_id },
            })
        );
        assert_eq!(
            parse_key(&segment_key(&namespace_id, &segment_id)),
            Some(ParsedGrepKey {
                namespace_id,
                kind: GrepKeyKind::Segment { segment_id },
            })
        );
    }

    #[test]
    fn parser_rejects_non_keys_and_malformed_keys() {
        let rejected = [
            "",
            "namespaces/",
            "namespaces/docs/extensions/grep/",
            "namespaces/docs/extensions/grep/root.json/extra",
            "namespaces/docs/extensions/grep/root.json.tmp",
            "namespaces/docs/extensions/grep/manifests/",
            "namespaces/docs/extensions/grep/manifests/not-a-digest.manifest.json",
            "namespaces/docs/extensions/grep/segments/",
            "namespaces/docs/extensions/grep/segments/idx_00000000000000000000000000000001.sst",
            "namespaces/docs/extensions/grep/segments/not-an-index-id.sst.zst",
            "namespaces/docs/extensions/grep/segments/idx_00000000000000000000000000000001.sst.zst.tmp",
            "namespaces/docs/extensions/grep/segments/idx_00000000000000000000000000000001.sst.zst/extra",
            "namespaces/docs/extensions/grep/other/object",
            "grep/v1/namespaces/docs/root.json",
            "namespaces/docs/metadata/root.json",
        ];

        for key in rejected {
            assert_eq!(parse_key(key), None, "parser accepted `{key}`");
        }
    }

    #[test]
    fn core_key_parser_ignores_grep_extension_objects() {
        let namespace_id = namespace_id();
        let segment_id = segment_id();
        let manifest_object_id =
            GrepManifestObjectId::parse("gmf_0123456789abcdef0123456789abcdef")
                .expect("valid manifest object id");

        for key in [
            root_key(&namespace_id),
            manifest_key(&namespace_id, &manifest_object_id),
            segment_key(&namespace_id, &segment_id),
        ] {
            assert!(
                loonfs_objectstore::layout::parse_object_key(&key).is_none(),
                "core key parser accepted grep extension key `{key}`"
            );
        }
    }
}
