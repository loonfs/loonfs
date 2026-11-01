//! The family root that every manifest of a fork family records.

use super::*;
use loonfs_types::format::control::{ForkBasis, ManifestRef};
use std::assert_matches;

#[tokio::test]
async fn every_member_of_a_fork_family_records_the_family_root() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = test_context();
    let [root, fork, grandchild] =
        ["root", "fork", "grandchild"].map(|name| NamespaceId::parse(name).expect("namespace"));
    let created = crate::test_support::NamespaceFixture::new(&store, &root, &setup)
        .create()
        .await
        .expect("create the root");
    assert_eq!(created.family_root_id, root);
    for (source, target) in [(&root, &fork), (&fork, &grandchild)] {
        let forked = crate::test_support::NamespaceFixture::new(&store, source, &setup)
            .engine()
            .fork_namespace(target, &loonfs_test_support::test_actor(), None)
            .await
            .expect("fork")
            .response;
        assert_eq!(forked.family_root_id, root, "the fork `{target}`");
    }
    // A successor of the first manifest keeps the value.
    write_test_file(&store, &grandchild, "/file", "write", &setup).await;
    crate::test_support::namespace_store(&store, &grandchild)
        .fold_wal()
        .await
        .expect("fold");
    for namespace_id in [&root, &fork, &grandchild] {
        let namespace = crate::test_support::namespace_store(&store, namespace_id);
        let current = namespace
            .load_current_manifest()
            .await
            .expect("current manifest");
        assert_eq!(current.payload().family_root_id, root, "`{namespace_id}`");
        assert_eq!(
            namespace
                .load_metadata()
                .await
                .expect("status")
                .family_root_id,
            root
        );
    }
}

#[tokio::test]
async fn a_successor_that_changes_the_family_root_is_refused() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let setup = test_context();
    let [root, fork] = ["root", "fork"].map(|name| NamespaceId::parse(name).expect("namespace"));
    crate::test_support::NamespaceFixture::new(&store, &root, &setup)
        .create()
        .await
        .expect("create the root");
    crate::test_support::NamespaceFixture::new(&store, &root, &setup)
        .engine()
        .fork_namespace(&fork, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    let first = crate::test_support::namespace_store(&store, &fork)
        .load_current_manifest()
        .await
        .expect("the fork's first manifest");
    // A fork's manifest passes validation with any family root but its own,
    // so only the chain rule refuses this successor.
    write_namespace_manifest(
        &store,
        NamespaceManifestPayload {
            family_root_id: NamespaceId::parse("another").expect("namespace"),
            ..first
                .payload()
                .successor(first.payload().runs.clone())
                .expect("successor")
        },
    )
    .await
    .expect("write the successor");
    let error = crate::test_support::namespace_store(&store, &fork)
        .load_current_manifest()
        .await
        .expect_err("the successor changes the family root");
    assert!(error.to_string().contains("family_root_id"), "{error}");
}

#[test]
fn a_manifest_records_itself_as_its_family_root_exactly_when_it_has_no_fork_basis() {
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let created = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        1_000,
        loonfs_test_support::test_actor(),
        loonfs_types::NamespaceAccess::Unrestricted {},
    );
    let fork_basis = ForkBasis {
        manifest: ManifestRef {
            owner_namespace_id: NamespaceId::parse("source").expect("namespace"),
            manifest_no: ManifestNo(1),
            head_seq: ChangeSeq(0),
            payload_checksum: loonfs_types::sha256_digest(b"source"),
        },
        source_pin_id: PinId::parse("pin_00000000000000000001-0000000000000001").expect("pin"),
    };
    for (manifest, expected) in [
        (
            NamespaceManifestPayload {
                family_root_id: NamespaceId::parse("source").expect("namespace"),
                ..created.clone()
            },
            "records `source` as its family root instead of itself",
        ),
        (
            NamespaceManifestPayload {
                fork_basis: Some(fork_basis),
                ..created
            },
            "fork provenance disagrees with namespace identity, family root",
        ),
    ] {
        let (_, bytes) = encode_namespace_manifest_json(manifest)
            .expect("encode the manifest")
            .into_parts();
        let error = load::decode_manifest_at(&namespace_id, ManifestNo(1), "manifest", &bytes)
            .expect_err("the manifest fails validation");
        assert_matches!(
            &error,
            ManifestLoadError::RunManifestMismatch { message, .. } if message.contains(expected),
            "{error}"
        );
    }
}
