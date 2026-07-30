//! Key construction for every durable object family.

use crate::layout::ObjectLayout;
use loonfs_api::{ContentId, ManifestObjectId, NamespaceId};

/// Builds the listing prefix containing every durable object owned by one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn namespace_prefix(namespace_id: &NamespaceId) -> String {
    ObjectLayout::new().namespace_root_prefix(namespace_id.as_str())
}

/// Builds the authoritative WAL head key for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_head(namespace: &str) -> String {
    ObjectLayout::new().wal_head(namespace)
}

/// Builds the retained-history floor key for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_floor(namespace: &str) -> String {
    ObjectLayout::new().wal_floor(namespace)
}

/// Builds the immutable WAL object key for a segment identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_segment(namespace: &str, segment_id: &str) -> String {
    ObjectLayout::new().wal_segment(namespace, segment_id)
}

/// Builds the listing prefix containing only WAL segment objects for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn wal_segment_prefix(namespace: &str) -> String {
    ObjectLayout::new().wal_segment_prefix(namespace)
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
pub fn metadata_root(namespace: &str) -> String {
    ObjectLayout::new().metadata_root(namespace)
}

/// Builds the listing prefix containing namespace-manifest candidates.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_manifest_prefix(namespace: &str) -> String {
    ObjectLayout::new().metadata_manifest_prefix(namespace)
}

/// Builds the listing prefix containing metadata SST objects owned by one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_table_prefix(namespace: &str) -> String {
    ObjectLayout::new().metadata_table_prefix(namespace)
}

/// Builds the immutable manifest key for one speculative manifest identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_manifest_object(namespace: &str, manifest_object_id: &ManifestObjectId) -> String {
    ObjectLayout::new().metadata_manifest_object(namespace, manifest_object_id)
}

/// Builds the immutable metadata SST key for one table identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn metadata_table(namespace: &str, table_id: &str) -> String {
    ObjectLayout::new().metadata_table(namespace, table_id)
}

/// Builds the mutable lifecycle key for one checkpoint record.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn checkpoint_record(namespace: &str, checkpoint_id: &str) -> String {
    ObjectLayout::new().checkpoint_record(namespace, checkpoint_id)
}

/// Builds the listing prefix containing checkpoint records for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn checkpoint_prefix(namespace: &str) -> String {
    ObjectLayout::new().checkpoint_prefix(namespace)
}

/// Builds the listing prefix containing durable upload sessions for one namespace.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn upload_session_prefix(namespace: &str) -> String {
    ObjectLayout::new().upload_session_prefix(namespace)
}

/// Builds the mutable lifecycle key for one upload session.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn upload_session(namespace: &str, upload_id: &str) -> String {
    ObjectLayout::new().upload_session(namespace, upload_id)
}

/// Builds the immutable content-object key for one content identity.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
pub fn content_blob(content_store: &str, content_id: &ContentId) -> String {
    ObjectLayout::new().content_blob(content_store, content_id)
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_record, content_blob, metadata_manifest_object, metadata_root, metadata_table,
        namespace_prefix, upload_session, wal_floor, wal_head, wal_segment,
        wal_segment_id_from_key, wal_segment_prefix,
    };
    use crate::layout::ObjectLayout;
    use loonfs_api::{ContentId, ManifestObjectId, NamespaceId};

    const CONTENT_ID: &str = "con_abcdef0123456789abcdef0123456789";

    fn content_id() -> ContentId {
        ContentId::parse(CONTENT_ID).expect("valid content id")
    }

    #[test]
    fn namespace_prefix_matches_layout_root_prefix() {
        let namespace_id = NamespaceId::parse("ns-1").expect("valid namespace id");

        assert_eq!(
            namespace_prefix(&namespace_id),
            ObjectLayout::new().namespace_root_prefix(namespace_id.as_str())
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
                .replace("{content_store_id}", "cs-1")
                .replace("{start_seq:020}", &format!("{:020}", 42))
                .replace("{suffix}", "0123456789abcdef")
                .replace(
                    "{manifest_object_id}",
                    "00000000000000000400-0123456789abcdef",
                )
                .replace("{checkpoint_id}", "chk-1")
                .replace("{table_id}", "tbl-1")
                .replace("{upload_id}", "up-1")
                .replace("{content_id[4..6]}", &CONTENT_ID[4..6])
                .replace("{content_id}", CONTENT_ID)
        };

        let built = [
            ("WAL head", wal_head("ns-1")),
            (
                "WAL segments",
                wal_segment("ns-1", &format!("{:020}-{}", 42, "0123456789abcdef")),
            ),
            (
                "Namespace manifests",
                metadata_manifest_object(
                    "ns-1",
                    &ManifestObjectId::parse("00000000000000000400-0123456789abcdef")
                        .expect("valid manifest object id"),
                ),
            ),
            ("Checkpoint records", checkpoint_record("ns-1", "chk-1")),
            ("Metadata tables", metadata_table("ns-1", "tbl-1")),
            ("Upload sessions", upload_session("ns-1", "up-1")),
            ("Metadata root", metadata_root("ns-1")),
            ("WAL floor", wal_floor("ns-1")),
            ("Content objects", content_blob("cs-1", &content_id())),
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
        assert_eq!(wal_head("ns-1"), "namespaces/ns-1/wal/head.json");
        assert_eq!(wal_floor("ns-1"), "namespaces/ns-1/wal/floor.json");
        assert_eq!(
            wal_segment("ns-1", "seg_00000000000000000000000000000001"),
            "namespaces/ns-1/wal/segments/seg_00000000000000000000000000000001.wal.zst"
        );
        assert_eq!(wal_segment_prefix("ns-1"), "namespaces/ns-1/wal/segments/");
        assert!(wal_segment("ns-1", "00000000000000000042-0123456789abcdef")
            .starts_with(&wal_segment_prefix("ns-1")));
        assert!(!wal_head("ns-1").starts_with(&wal_segment_prefix("ns-1")));
        assert!(!wal_floor("ns-1").starts_with(&wal_segment_prefix("ns-1")));
        assert_eq!(
            wal_segment_id_from_key(&wal_segment(
                "ns-1",
                "00000000000000000042-0123456789abcdef"
            )),
            Some("00000000000000000042-0123456789abcdef")
        );
        assert_eq!(
            wal_segment_id_from_key("namespaces/ns-1/wal/segments/random.tmp"),
            None
        );
        assert_eq!(metadata_root("ns-1"), "namespaces/ns-1/metadata/root.json");
        let manifest_object_id = ManifestObjectId::parse("00000000000000000400-0123456789abcdef")
            .expect("valid manifest object id");
        assert_eq!(
            metadata_manifest_object("ns-1", &manifest_object_id),
            "namespaces/ns-1/metadata/manifests/00000000000000000400-0123456789abcdef.manifest.json"
        );
        assert_eq!(
            metadata_table("ns-1", "tbl_00000000000000000000000000000001"),
            "namespaces/ns-1/metadata/tables/tbl_00000000000000000000000000000001.sst.zst"
        );
        assert_eq!(
            checkpoint_record("ns-1", "chk_00000000000000000000000000000001"),
            "namespaces/ns-1/checkpoints/chk_00000000000000000000000000000001.json"
        );
        assert_eq!(
            content_blob("cs_00000000000000000000000000000001", &content_id()),
            "content-stores/cs_00000000000000000000000000000001/objects/ab/con_abcdef0123456789abcdef0123456789"
        );
        assert_eq!(
            upload_session("ns-1", "upl_00000000000000000000000000000001"),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
    }

    /// Content keys shard on the id's own leading characters, so the shard a
    /// key lands in is derivable from the id and nothing else.
    #[test]
    fn content_keys_shard_on_the_content_id_prefix() {
        let id = content_id();
        let key = content_blob("cs-1", &id);
        assert!(key.ends_with(&format!("/{}/{}", id.shard_prefix(), id.as_str())));
    }
}
