//! Pins the capability registry's three copies to each other: the constants
//! in `loonfs-api`, the document the runtime handles advertise, and the
//! normative text in `docs/specs/api.md`. If any copy drifts, this fails.
//!
//! The registry describes the reference deployment, which serves one API group
//! this crate does not implement: `query/v0` and every grep key come from
//! `loonfs-grep`, which a host composes on top of these handles. Both grep
//! keys are composed — `query.grep` for searching and `maintenance.grep.index`
//! for maintaining the index — even though the second one is parented by
//! an API group these handles do advertise. So the embedded document is the
//! spec's example minus the grep extension, and `loonfs-server`'s
//! `grep_modes` test pins the merged document a composed deployment answers
//! with.
#![allow(clippy::panic)]
// Spec parsing panics with precise messages when a section is missing.

use loonfs::{CapabilityDocument, FsReader, SharedObjectStore};
use loonfs_api::API_GROUP_QUERY_V0;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::block_on::block_on;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

const API_SPEC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/api.md");

fn embedded_capabilities() -> CapabilityDocument {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let reader = block_on(FsReader::builder_with_store(store).build()).expect("build reader");
    reader.get_capabilities()
}

/// The composed grep extension, dropped from a document so what remains is
/// what this crate is responsible for.
fn without_the_grep_extension(mut document: CapabilityDocument) -> CapabilityDocument {
    document
        .api_groups
        .retain(|api_group| api_group != API_GROUP_QUERY_V0);
    document.features.retain(|key, _| !is_grep_key(key));
    document.limits.retain(|key, _| !is_grep_key(key));
    document
}

/// A key the composed grep extension owns: the whole query API group, plus the
/// maintenance API group key that gates index maintenance. A host that composes no
/// extension advertises none of them, whatever API groups it does advertise.
fn is_grep_key(key: &str) -> bool {
    key.starts_with("query.") || key.starts_with("maintenance.grep.")
}

fn is_host_transfer_key(key: &str) -> bool {
    matches!(
        key,
        "filesystem.uploads.direct_put"
            | "filesystem.uploads.direct_multipart"
            | "filesystem.downloads.direct_get"
    )
}

fn spec_section<'a>(spec: &'a str, start: &str, end: &str) -> &'a str {
    spec.split(start)
        .nth(1)
        .unwrap_or_else(|| panic!("api.md section {start} not found"))
        .split(end)
        .next()
        .expect("section end")
}

#[test]
fn embedded_capability_document_is_the_spec_example_without_the_composed_grep_extension() {
    let spec = std::fs::read_to_string(API_SPEC_PATH).expect("read docs/specs/api.md");
    let example = spec_section(&spec, "### 2.1", "### 2.2")
        .split("```json")
        .nth(1)
        .expect("capability example block")
        .split("```")
        .next()
        .expect("fenced block end");
    let expected: CapabilityDocument =
        serde_json::from_str(example).expect("spec capability example parses");
    assert!(
        expected
            .api_groups
            .iter()
            .any(|api_group| api_group == API_GROUP_QUERY_V0),
        "the section 2.1 example describes the reference server, which serves the query API group"
    );

    let document = embedded_capabilities();
    document.validate().expect("document is well-formed");
    assert_eq!(
        document,
        without_the_grep_extension(expected),
        "the advertised capability document drifted from the api.md section 2.1 example"
    );
}

#[test]
fn advertised_features_match_the_spec_feature_registry() {
    let spec = std::fs::read_to_string(API_SPEC_PATH).expect("read docs/specs/api.md");
    let registry: BTreeSet<String> = spec_section(&spec, "### 2.2", "### 2.3")
        .lines()
        .filter_map(|line| line.strip_prefix("| `"))
        .filter_map(|rest| rest.split('`').next())
        .map(str::to_owned)
        .collect();
    assert!(
        !registry.is_empty(),
        "no feature keys parsed from the api.md registry table"
    );
    for composed in ["query.grep", "maintenance.grep.index"] {
        assert!(
            registry.contains(composed),
            "the registry must keep `{composed}`, which the reference server composes"
        );
    }

    let runtime_registry: BTreeSet<String> = registry
        .into_iter()
        .filter(|key| !is_grep_key(key) && !is_host_transfer_key(key))
        .collect();
    let advertised: BTreeSet<String> = embedded_capabilities().features.into_keys().collect();
    assert_eq!(
        advertised, runtime_registry,
        "advertised feature keys drifted from the api.md section 2.2 registry"
    );
}
