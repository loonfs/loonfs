//! Binding visibility through manifests and retention rebuilds.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::namespace::read_anchor::load_read_anchor;
use crate::path::read::{load_metadata_view, LoadedMetadataView, ReadLoadContext};
use loonfs_api::{AttributeInclusion, DirectoryPageCursor, PageRequest};

async fn assert_names<S: ObjectStore + ?Sized>(
    view: &LoadedMetadataView<'_, S>,
    expected: &[(&str, InodeId)],
) {
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let page = view
        .list_path_page(
            "/",
            PageRequest::<DirectoryPageCursor> {
                cursor: None,
                limit: EffectiveLimit::new(NonZeroU32::new(16).expect("nonzero limit")),
            },
            AttributeInclusion::Omit,
            &access,
        )
        .await
        .expect("list root");
    assert_eq!(
        page.items
            .iter()
            .map(|entry| (entry.path.as_str(), entry.inode_id))
            .collect::<Vec<_>>(),
        expected
    );
    for name in ["/a", "/b"] {
        let entry = view
            .resolve_path(name, AttributeInclusion::Omit, &access)
            .await;
        match expected.iter().find(|(path, _)| *path == name) {
            Some((_, inode_id)) => assert_eq!(entry.expect("bound path").inode_id, *inode_id),
            None => assert_eq!(
                entry.expect_err("unbound path").code(),
                ErrorCode::PathNotFound
            ),
        }
    }
}

#[tokio::test]
async fn slot_versions_preserve_moves_name_reuse_and_pinned_reads() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("binding-values").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    write_file_bytes(&store, &namespace_id, "/a", b"first", &context, None)
        .await
        .expect("bind a");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("pin first binding");
    let original = load_read_anchor(&store, &namespace_id)
        .await
        .expect("first anchor");
    let first = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("first view");
    let inode_id = first
        .resolve_path(
            "/a",
            AttributeInclusion::Omit,
            &ReadAccess::live(Authorizer::Unrestricted),
        )
        .await
        .expect("first path")
        .inode_id;
    assert_names(&first, &[("/a", inode_id)]).await;

    let operations = [("/a", "/b"), ("/b", "/a"), ("/a", "/b")]
        .into_iter()
        .map(|(source, destination)| FilesystemOperation::MovePath {
            source_path: AbsolutePath::parse(source).expect("source"),
            destination_path: AbsolutePath::parse(destination).expect("destination"),
            precondition: loonfs_api::DestinationPrecondition {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        })
        .collect();
    crate::commit_engine::publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("repeat-move").expect("commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations,
            preconditions: Vec::new(),
        })],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("one result")
    .expect("three moves in one commit");
    let moved_tail = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("moved tail");
    assert_names(&moved_tail, &[("/b", inode_id)]).await;
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("pin moved binding");
    let moved = load_read_anchor(&store, &namespace_id)
        .await
        .expect("moved anchor");
    advance_retention_floor(&store, &namespace_id)
        .await
        .expect("floor at move");
    let (manifest_no, _) = drain_reorganization(
        &store,
        &namespace_id,
        MetadataLsmPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
    )
    .await;
    let rebuilt = load_manifest_materialization_for_inspection(&store, &namespace_id, manifest_no)
        .await
        .expect("index row counts and digests agree");
    let forward =
        manifest_rows_for_family(&rebuilt.metadata_state, ApiMetadataRowFamily::DirentryBinds);
    let reverse = manifest_rows_for_family(
        &rebuilt.metadata_state,
        ApiMetadataRowFamily::DirentryChildBinds,
    );
    assert_eq!(forward, reverse);
    assert_eq!(forward.len(), 1);
    assert!(
        matches!(&forward[0], MetadataRow::DirentryBinding(binding) if binding.is_bound() && binding.name_key.as_str() == "b" && binding.committed_seq == moved.read_state.seq && binding.delta_index == 5)
    );

    write_file_bytes(&store, &namespace_id, "/a", b"replacement", &context, None)
        .await
        .expect("reuse a");
    let current = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("reused tail");
    let replacement = current
        .resolve_path(
            "/a",
            AttributeInclusion::Omit,
            &ReadAccess::live(Authorizer::Unrestricted),
        )
        .await
        .expect("replacement")
        .inode_id;
    assert_ne!(replacement, inode_id);
    assert_names(&current, &[("/a", replacement), ("/b", inode_id)]).await;
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("flush reuse");
    let current = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("reused manifest");
    assert_names(&current, &[("/a", replacement), ("/b", inode_id)]).await;
    for (anchor, path) in [(&original, "/a"), (&moved, "/b")] {
        let basis = anchor.basis();
        let pinned = load_metadata_view(
            &store,
            &namespace_id,
            ReadLoadContext::pinned_head(&anchor.read_state, &basis, None, None),
        )
        .await
        .expect("pinned manifest");
        assert_names(&pinned, &[(path, inode_id)]).await;
    }
}

#[tokio::test]
async fn a_listing_reads_only_the_slot_binding_family() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("binding-reads").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context)
        .await
        .expect("bootstrap");
    write_file_bytes(&inner, &namespace_id, "/a", b"first", &context, None)
        .await
        .expect("bind");
    move_path(&inner, &namespace_id, "/a", "/b", &context, None)
        .await
        .expect("unbind and bind");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("flush");
    let manifest = load_current_manifest(&inner, &namespace_id)
        .await
        .expect("manifest");
    let families = manifest
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(|descriptor| (metadata_segment_object_key(descriptor), descriptor.family))
        .collect::<BTreeMap<_, _>>();
    let store = RecordingStore::metadata_segments(inner);
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    store.reset();
    let children = view
        .metadata_view()
        .session()
        .visible_children_page_by_name_key(InodeId(1), None, 16)
        .await
        .expect("list");
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].binding.name_key.as_str(), "b");
    let reads = store.take_gets();
    assert!(reads
        .iter()
        .any(|(key, _)| families[key] == ApiMetadataRowFamily::DirentryBinds));
    assert!(reads
        .iter()
        .all(|(key, _)| families[key] != ApiMetadataRowFamily::DirentryChildBinds));
    assert_eq!(
        MetadataFamilyGroup::Bindings.families(),
        &[
            ApiMetadataRowFamily::DirentryBinds,
            ApiMetadataRowFamily::DirentryChildBinds
        ]
    );
}
