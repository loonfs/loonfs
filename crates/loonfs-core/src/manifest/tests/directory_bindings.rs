//! Binding visibility through manifests and retention rebuilds.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::metadata::LeafRevisionPrefetch;
use crate::namespace::read_anchor::load_read_anchor;
use crate::path::read::{load_metadata_view, LoadedMetadataView, ReadLoadContext};
use crate::store_waves::STORE_READ_WAVE;
use loonfs_test_support::stores::ConcurrencyWatchStore;
use loonfs_types::{AttributeInclusion, DirectoryPageCursor, PageRequest};

#[tokio::test]
async fn a_cold_path_walk_reads_only_the_leaf_inode() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("path-binding-reads").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &inner,
        &namespace_id,
        "/docs/reports/summary.txt",
        b"summary",
        &context,
        None,
    )
    .await
    .expect("create depth-three path");
    checkpoint_then_compact(
        &inner,
        &namespace_id,
        &context,
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
    )
    .await;
    let manifest = load_current_manifest(&inner, &namespace_id)
        .await
        .expect("manifest");
    let inode_segments = manifest
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|descriptor| descriptor.family == ApiMetadataRowFamily::Inodes)
        .map(|descriptor| {
            assert_eq!(descriptor.row_count, 1);
            (
                metadata_segment_object_key(descriptor),
                descriptor.min_row_key.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(inode_segments.len(), 4);
    let store = RecordingStore::metadata_segments(inner);
    for prefetch in [LeafRevisionPrefetch::Skip, LeafRevisionPrefetch::Prefetch] {
        let view = load_current_metadata_view(&store, &namespace_id)
            .await
            .expect("cold view");
        store.reset();
        let resolved = view
            .metadata_view()
            .session()
            .resolve_visible_path(
                &AbsolutePath::parse("/DOCS/REPORTS/SUMMARY.TXT").expect("path"),
                prefetch,
            )
            .await
            .expect("resolve path");
        assert_eq!(resolved.absolute_path, "/docs/reports/summary.txt");
        assert_eq!(resolved.inode_kind, loonfs_types::InodeKind::File);
        let inode_reads = store
            .take_gets()
            .into_iter()
            .filter_map(|(key, _)| inode_segments.get(&key).cloned())
            .collect::<Vec<_>>();
        assert_eq!(inode_reads, vec![lookup_keys::inode_key(resolved.inode_id)]);
    }
}

#[tokio::test]
async fn a_cold_checkpoint_files_page_bounds_its_parent_binding_reads() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("checkpoint-file-reads").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let files = 4 * STORE_READ_WAVE;
    for file in 0..files {
        write_file_bytes(
            &inner,
            &namespace_id,
            &format!("/file-{file:03}"),
            b"file",
            &context,
            None,
        )
        .await
        .expect("create file");
    }
    checkpoint_then_compact(
        &inner,
        &namespace_id,
        &context,
        MetadataLsmPolicy {
            max_rows_per_segment: NonZeroUsize::MIN,
            ..MetadataLsmPolicy::default()
        },
    )
    .await;
    let checkpoint = create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("pin the compacted manifest");
    let binding_segments: BTreeSet<_> = load_current_manifest(&inner, &namespace_id)
        .await
        .expect("manifest")
        .state
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|descriptor| descriptor.family == ApiMetadataRowFamily::DirentryChildBinds)
        .map(metadata_segment_object_key)
        .collect();
    assert!(binding_segments.len() >= files, "{binding_segments:?}");
    let store = ConcurrencyWatchStore::new(
        inner,
        KeyPredicate::new(move |key| binding_segments.contains(key)),
    );
    let page = crate::pin::list_checkpoint_files_page(
        &store,
        None,
        &load_namespace_read_state(&store, &namespace_id)
            .await
            .expect("head"),
        &checkpoint.checkpoint_id,
        PageRequest {
            cursor: None,
            limit: EffectiveLimit::new(NonZeroU32::new(1000).expect("nonzero limit")),
        },
        crate::pin::ListCheckpointFilesOptions::default(),
    )
    .await
    .expect("checkpoint files page");
    assert_eq!(page.files.len(), files);
    let concurrency = store.reads();
    assert!(concurrency.peak_in_flight > 1, "{concurrency:?}");
    assert!(
        concurrency.peak_in_flight <= STORE_READ_WAVE,
        "{concurrency:?}"
    );
}

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
    for name in ["/a", "/b", "/c"] {
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
            precondition: loonfs_types::DestinationPrecondition {
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
        std::sync::Arc::new(tokio::sync::Semaphore::new(32 * 1024 * 1024)),
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
    advance_retention_floor(&store, None, &namespace_id, RetentionTarget::Head)
        .await
        .expect("floor at move");
    let (manifest_no, _) = drain_compaction(
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
        .expect("fold reuse");
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
async fn a_lower_numbered_child_reusing_a_slot_survives_a_base_compaction() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("slot-reuse").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    for path in ["/b", "/c"] {
        write_file_bytes(&store, &namespace_id, path, b"body", &context, None)
            .await
            .expect("create");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the creates");
    let created = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("created view");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let mut inode_ids = Vec::new();
    for path in ["/b", "/c"] {
        let entry = created
            .resolve_path(path, AttributeInclusion::Omit, &access)
            .await
            .expect("created path");
        inode_ids.push(entry.inode_id);
    }
    let (moved, replaced) = (inode_ids[0], inode_ids[1]);
    assert!(
        moved < replaced,
        "the moved child must sort before the slot's earlier child"
    );

    delete_path(&store, &namespace_id, "/c", &context, None)
        .await
        .expect("delete c");
    move_path(&store, &namespace_id, "/b", "/c", &context, None)
        .await
        .expect("move b to c");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the move");
    advance_retention_floor(&store, None, &namespace_id, RetentionTarget::Head)
        .await
        .expect("floor at the move");
    let (manifest_no, _) = drain_compaction(
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
    assert_eq!(forward.len(), 1, "{forward:?}");
    assert!(
        matches!(&forward[0], MetadataRow::DirentryBinding(binding) if binding.is_bound() && binding.name_key.as_str() == "c" && binding.child_inode_id == moved)
    );
    let compacted = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("compacted view");
    assert_names(&compacted, &[("/c", moved)]).await;
}

#[tokio::test]
async fn a_child_moved_through_several_parents_keeps_its_current_edge_after_a_base_compaction() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("child-moves").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&store, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::commit_engine::publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::new(CommitRequest {
            commit_id: CommitId::parse("parents").expect("commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations: ["/a", "/b", "/c"]
                .into_iter()
                .map(|path| FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse(path).expect("path"),
                    parents: false,
                })
                .collect(),
            preconditions: Vec::new(),
        })],
        &context,
        std::sync::Arc::new(tokio::sync::Semaphore::new(32 * 1024 * 1024)),
    )
    .await
    .into_iter()
    .next()
    .expect("one result")
    .expect("create the parents");
    write_file_bytes(&store, &namespace_id, "/c/report", b"body", &context, None)
        .await
        .expect("create the child");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the creates");
    let created = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("created view");
    let access = ReadAccess::live(Authorizer::Unrestricted);
    let mut inode_ids = Vec::new();
    for path in ["/a", "/b", "/c", "/c/report"] {
        let entry = created
            .resolve_path(path, AttributeInclusion::Omit, &access)
            .await
            .expect("created path");
        inode_ids.push(entry.inode_id);
    }
    let (last_parent, child) = (inode_ids[0], inode_ids[3]);
    assert!(
        inode_ids[0] < inode_ids[1] && inode_ids[1] < inode_ids[2],
        "the child's last edge must sort before its earlier edges"
    );

    for (source, destination) in [("/c/report", "/b/report"), ("/b/report", "/a/report")] {
        move_path(&store, &namespace_id, source, destination, &context, None)
            .await
            .expect("move the child");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the moves");
    let moved = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("moved view");
    let parent = moved
        .metadata_view()
        .current_parent_binding_for_child(child)
        .await
        .expect("parent lookup")
        .expect("the child is bound");
    assert_eq!(parent.parent_inode_id, last_parent);
    advance_retention_floor(&store, None, &namespace_id, RetentionTarget::Head)
        .await
        .expect("floor at the last move");
    let (manifest_no, _) = drain_compaction(
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
    assert_eq!(
        forward.len(),
        4,
        "three parents and one edge of the child: {forward:?}"
    );
    let child_rows = forward
        .iter()
        .filter(|row| matches!(row, MetadataRow::DirentryBinding(binding) if binding.child_inode_id == child))
        .collect::<Vec<_>>();
    assert!(
        matches!(child_rows.as_slice(), [MetadataRow::DirentryBinding(binding)] if binding.is_bound() && binding.parent_inode_id == last_parent && binding.name_key.as_str() == "report"),
        "{child_rows:?}"
    );
    let compacted = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("compacted view");
    let parent = compacted
        .metadata_view()
        .current_parent_binding_for_child(child)
        .await
        .expect("parent lookup")
        .expect("the child is bound");
    assert_eq!(parent.parent_inode_id, last_parent);
    for (path, bound) in [
        ("/a/report", true),
        ("/b/report", false),
        ("/c/report", false),
    ] {
        let entry = compacted
            .resolve_path(path, AttributeInclusion::Omit, &access)
            .await;
        match entry {
            Ok(entry) if bound => assert_eq!(entry.inode_id, child),
            Err(error) if !bound => assert_eq!(error.code(), ErrorCode::PathNotFound),
            other => panic!("unexpected lookup of `{path}`: {other:?}"),
        }
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
        .expect("fold");
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
