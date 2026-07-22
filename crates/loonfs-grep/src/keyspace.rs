//! Grep-owned durable object keys and their strict parser.

use loonfs_api::{IndexSegmentId, NamespaceId};

const ALL_NAMESPACES_PREFIX: &str = "grep/v0/namespaces/";

/// The grep-owned object kind named by a parsed key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepKeyKind {
    Root,
    Segment { segment_id: IndexSegmentId },
}

/// A recognized grep key split into its namespace and object kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedGrepKey {
    pub namespace_id: NamespaceId,
    pub kind: GrepKeyKind,
}

/// Prefix containing grep state for every namespace.
///
/// Grep-owned garbage collection uses this prefix to find state belonging
/// to namespaces that core has deleted.
pub const fn all_namespaces_prefix() -> &'static str {
    ALL_NAMESPACES_PREFIX
}

/// Prefix containing every grep object for one namespace.
pub fn namespace_prefix(namespace_id: &NamespaceId) -> String {
    format!("{ALL_NAMESPACES_PREFIX}{namespace_id}/")
}

/// Key of one namespace's atomic grep root.
pub fn root_key(namespace_id: &NamespaceId) -> String {
    format!("{}root.json", namespace_prefix(namespace_id))
}

/// Prefix containing one namespace's immutable grep segments.
pub fn segments_prefix(namespace_id: &NamespaceId) -> String {
    format!("{}segments/", namespace_prefix(namespace_id))
}

/// Key of one immutable grep segment.
pub fn segment_key(namespace_id: &NamespaceId, segment_id: &IndexSegmentId) -> String {
    format!("{}{segment_id}.sst", segments_prefix(namespace_id))
}

/// Parses exactly the grep root and segment key grammar.
///
/// Prefixes, malformed ids, unknown object families, temporary suffixes,
/// and keys with trailing path components are rejected.
pub fn parse_key(key: &str) -> Option<ParsedGrepKey> {
    let suffix = key.strip_prefix(ALL_NAMESPACES_PREFIX)?;
    let (namespace, object) = suffix.split_once('/')?;
    let namespace_id = NamespaceId::parse(namespace).ok()?;
    let kind = if object == "root.json" {
        GrepKeyKind::Root
    } else {
        let segment = object.strip_prefix("segments/")?.strip_suffix(".sst")?;
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
    fn key_builders_pin_the_v0_grammar() {
        let namespace_id = namespace_id();
        let segment_id = segment_id();

        assert_eq!(all_namespaces_prefix(), "grep/v0/namespaces/");
        assert_eq!(namespace_prefix(&namespace_id), "grep/v0/namespaces/docs/");
        assert_eq!(root_key(&namespace_id), "grep/v0/namespaces/docs/root.json");
        assert_eq!(
            segments_prefix(&namespace_id),
            "grep/v0/namespaces/docs/segments/"
        );
        assert_eq!(
            segment_key(&namespace_id, &segment_id),
            "grep/v0/namespaces/docs/segments/idx_00000000000000000000000000000001.sst"
        );
    }

    #[test]
    fn built_keys_round_trip_through_the_parser() {
        let namespace_id = namespace_id();
        let segment_id = segment_id();

        assert_eq!(
            parse_key(&root_key(&namespace_id)),
            Some(ParsedGrepKey {
                namespace_id: namespace_id.clone(),
                kind: GrepKeyKind::Root,
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
            "grep/v0/namespaces/",
            "grep/v0/namespaces/docs/",
            "grep/v0/namespaces/docs/root.json/extra",
            "grep/v0/namespaces/docs/root.json.tmp",
            "grep/v0/namespaces/docs/segments/",
            "grep/v0/namespaces/docs/segments/not-an-index-id.sst",
            "grep/v0/namespaces/docs/segments/idx_00000000000000000000000000000001.sst.tmp",
            "grep/v0/namespaces/docs/segments/idx_00000000000000000000000000000001.sst/extra",
            "grep/v0/namespaces/docs/other/object",
            "grep/v1/namespaces/docs/root.json",
            "namespaces/docs/metadata/root.json",
        ];

        for key in rejected {
            assert_eq!(parse_key(key), None, "parser accepted `{key}`");
        }
    }
}
