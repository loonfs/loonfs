//! Key construction for every durable object family.

use crate::layout::ObjectLayout;
use crate::object_store::Result;
use loonfs_api::ManifestObjectId;

pub fn namespace_config(namespace: &str) -> String {
    ObjectLayout::new().namespace_config(namespace)
}

pub fn wal_head(namespace: &str) -> String {
    ObjectLayout::new().wal_head(namespace)
}

pub fn wal_floor(namespace: &str) -> String {
    ObjectLayout::new().wal_floor(namespace)
}

pub fn wal_segment(namespace: &str, segment_id: &str) -> String {
    ObjectLayout::new().wal_segment(namespace, segment_id)
}

pub fn wal_segment_prefix(namespace: &str) -> String {
    ObjectLayout::new().wal_segment_prefix(namespace)
}

pub fn wal_segment_id_from_key(key: &str) -> Option<&str> {
    ObjectLayout::new().wal_segment_id_from_key(key)
}

pub fn metadata_root(namespace: &str) -> String {
    ObjectLayout::new().metadata_root(namespace)
}

pub fn metadata_manifest_prefix(namespace: &str) -> String {
    ObjectLayout::new().metadata_manifest_prefix(namespace)
}

pub fn metadata_table_prefix(namespace: &str) -> String {
    ObjectLayout::new().metadata_table_prefix(namespace)
}

pub fn metadata_manifest_object(namespace: &str, manifest_object_id: &ManifestObjectId) -> String {
    ObjectLayout::new().metadata_manifest_object(namespace, manifest_object_id)
}

pub fn metadata_table(namespace: &str, table_id: &str) -> String {
    ObjectLayout::new().metadata_table(namespace, table_id)
}

pub fn index_segment(namespace: &str, segment_id: &str) -> String {
    ObjectLayout::new().index_segment(namespace, segment_id)
}

pub fn index_segment_prefix(namespace: &str) -> String {
    ObjectLayout::new().index_segment_prefix(namespace)
}

pub fn checkpoint_record(namespace: &str, checkpoint_id: &str) -> String {
    ObjectLayout::new().checkpoint_record(namespace, checkpoint_id)
}

pub fn checkpoint_prefix(namespace: &str) -> String {
    ObjectLayout::new().checkpoint_prefix(namespace)
}

pub fn upload_session_prefix(namespace: &str) -> String {
    ObjectLayout::new().upload_session_prefix(namespace)
}

pub fn upload_session(namespace: &str, upload_id: &str) -> String {
    ObjectLayout::new().upload_session(namespace, upload_id)
}

pub fn content_store_descriptor(content_store: &str) -> String {
    ObjectLayout::new().content_store_descriptor(content_store)
}

pub fn content_blob(content_store: &str, digest: &str) -> Result<String> {
    ObjectLayout::new().content_blob(content_store, digest)
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_record, content_blob, content_store_descriptor, index_segment,
        metadata_manifest_object, metadata_root, metadata_table, namespace_config, upload_session,
        wal_floor, wal_head, wal_segment, wal_segment_id_from_key, wal_segment_prefix,
    };
    use loonfs_api::ManifestObjectId;

    /// Pins every standard key pattern in the format spec's "Durable object
    /// families" table to the key this crate actually builds for that family.
    ///
    /// The table is normative: a new family must be added to the table and to
    /// this test together, and neither the spec pattern nor the builder can
    /// change without the other.
    #[test]
    fn standard_key_patterns_match_format_spec_table() {
        const HEX: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
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
                .replace("{segment_id}", "idx-1")
                .replace("{upload_id}", "up-1")
                .replace("{hex[0..2]}", &HEX[0..2])
                .replace("{hex[2..4]}", &HEX[2..4])
                .replace("{hex}", HEX)
        };

        let built = [
            ("Namespace config", namespace_config("ns-1")),
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
            ("Index segments", index_segment("ns-1", "idx-1")),
            ("Upload sessions", upload_session("ns-1", "up-1")),
            ("Content-store descriptor", content_store_descriptor("cs-1")),
            ("Metadata root", metadata_root("ns-1")),
            ("WAL floor", wal_floor("ns-1")),
            (
                "Content objects",
                content_blob("cs-1", &format!("sha256:{HEX}")).expect("content key"),
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
    fn key_builders_match_spec_examples() {
        assert_eq!(namespace_config("ns-1"), "namespaces/ns-1/namespace.json");
        assert_eq!(wal_head("ns-1"), "namespaces/ns-1/wal/head.json");
        assert_eq!(wal_floor("ns-1"), "namespaces/ns-1/wal/floor.json");
        assert_eq!(
            content_store_descriptor("cs_00000000000000000000000000000001"),
            "content-stores/cs_00000000000000000000000000000001/descriptor.json"
        );
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
            index_segment("ns-1", "idx_00000000000000000000000000000001"),
            "namespaces/ns-1/metadata/indexes/idx_00000000000000000000000000000001.idx.zst"
        );
        assert_eq!(
            checkpoint_record("ns-1", "chk_00000000000000000000000000000001"),
            "namespaces/ns-1/checkpoints/chk_00000000000000000000000000000001.json"
        );
        assert_eq!(
            content_blob(
                "cs_00000000000000000000000000000001",
                "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            )
            .expect("content key"),
            "content-stores/cs_00000000000000000000000000000001/blobs/sha256/ab/cd/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
        assert_eq!(
            upload_session("ns-1", "upl_00000000000000000000000000000001"),
            "namespaces/ns-1/uploads/upl_00000000000000000000000000000001.json"
        );
    }

    #[test]
    fn content_blob_rejects_invalid_sha256_digest() {
        assert!(content_blob("ns-1", "sha1:abcdef").is_err());
        assert!(content_blob("ns-1", "sha256:abcd").is_err());
        assert!(content_blob(
            "ns-1",
            "sha256:ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        )
        .is_err());
        assert!(content_blob("ns-1", "sha256:not-hex").is_err());
    }
}
