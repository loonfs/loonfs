//! Pins the capability registry's three copies to each other: the constants
//! in `loonfs-api`, the document the runtime handles advertise, and the
//! normative text in `docs/specs/api.md`. If any copy drifts, this fails.
#![allow(clippy::panic)]
// Spec parsing panics with precise messages when a section is missing.

use loonfs::{CapabilityDocument, FsReader, SharedObjectStore};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use tempfile::tempdir;

const API_SPEC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/api.md");

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

fn embedded_capabilities() -> CapabilityDocument {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let reader = block_on(FsReader::builder_with_store(store).build()).expect("build reader");
    reader.capabilities()
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
fn embedded_capability_document_matches_the_spec_example() {
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

    let document = embedded_capabilities();
    document.validate().expect("document is well-formed");
    assert_eq!(
        document, expected,
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

    let advertised: BTreeSet<String> = embedded_capabilities().features.into_keys().collect();
    assert_eq!(
        advertised, registry,
        "advertised feature keys drifted from the api.md section 2.2 registry"
    );
}
