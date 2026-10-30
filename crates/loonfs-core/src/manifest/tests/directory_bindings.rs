//! Binding visibility through manifests and retention rebuilds.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::metadata::LeafRevisionPreload;
use crate::path::read::LoadedMetadataView;
use loonfs_test_support::stores::ConcurrencyWatchStore;
use loonfs_types::options::AttributeInclusion;
use loonfs_types::options::ListOptions;
use loonfs_types::{InodeKind, ListPageCursor, PageRequest};
use std::assert_matches;

#[tokio::test]
async fn a_cold_path_walk_reads_no_inode() {
    let directory = tempdir().expect("tempdir");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("path-binding-reads").expect("namespace id");
    let context = test_context();
    bootstrap_namespace(&inner, &namespace_id, &context)
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .write_file_bytes("/docs/reports/summary.txt", b"summary", None)
        .await
        .expect("create depth-three path");
    checkpoint_then_compact(
        &inner,
        &namespace_id,
        &context,
        MetadataRunPolicy {
            max_rows_per_segment: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
        },
    )
    .await;
    let manifest = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let inode_segments = manifest
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
    for preload in [LeafRevisionPreload::Skip, LeafRevisionPreload::Preload] {
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace
            .load_current_metadata_view()
            .await
            .expect("cold view");
        store.reset();
        let resolved = view
            .metadata_view()
            .session()
            .resolve_visible_path(
                &AbsolutePath::parse("/DOCS/REPORTS/SUMMARY.TXT").expect("path"),
                preload,
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
        assert!(
            inode_reads.is_empty(),
            "the walk read inode rows {inode_reads:?}"
        );
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
    let files = 4 * STORE_READS_IN_FLIGHT;
    for file in 0..files {
        crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
            .write_file_bytes(&format!("/file-{file:03}"), b"file", None)
            .await
            .expect("create file");
    }
    checkpoint_then_compact(
        &inner,
        &namespace_id,
        &context,
        MetadataRunPolicy {
            max_rows_per_segment: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
        },
    )
    .await;
    let checkpoint = create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("pin the compacted manifest");
    let binding_segments: BTreeSet<_> = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest")
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
    let page = crate::test_support::namespace_store(&store, &namespace_id)
        .list_checkpoint_files_page(
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
        concurrency.peak_in_flight <= STORE_READS_IN_FLIGHT,
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
            PageRequest::<ListPageCursor> {
                cursor: None,
                limit: EffectiveLimit::new(NonZeroU32::new(16).expect("nonzero limit")),
            },
            &ListOptions::default(),
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/a", b"first", None)
        .await
        .expect("bind a");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("pin first binding");
    let original = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("first anchor");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let first = namespace
        .load_current_metadata_view()
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .publish_candidates(vec![CommitCandidate::new(CommitIntent {
            commit_id: CommitId::parse("repeat-move").expect("commit id"),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations,
            preconditions: Vec::new(),
        })])
        .await
        .into_iter()
        .next()
        .expect("one result")
        .expect("three moves in one commit");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let moved_tail = namespace
        .load_current_metadata_view()
        .await
        .expect("moved tail");
    assert_names(&moved_tail, &[("/b", inode_id)]).await;
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("pin moved binding");
    let moved = crate::test_support::namespace_store(&store, &namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("moved anchor");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("floor at move");
    let (manifest_no, _) = drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
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
    assert_matches!(
        &forward[0],
        MetadataRow::DirentryBinding(binding) if binding.is_bound()
            && binding.name_key.as_str() == "b"
            && binding.committed_seq == moved.read_state.seq
            && binding.delta_index == 5
    );

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/a", b"replacement", None)
        .await
        .expect("reuse a");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let current = namespace
        .load_current_metadata_view()
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
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let current = namespace
        .load_current_metadata_view()
        .await
        .expect("reused manifest");
    assert_names(&current, &[("/a", replacement), ("/b", inode_id)]).await;
    for (anchor, path) in [(&original, "/a"), (&moved, "/b")] {
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let context = crate::RuntimeReadContext {
            read_basis: crate::control::ReadBasis {
                basis: anchor.basis(),
                head: anchor.read_state.clone(),
            },
            tail: None,
        };
        let pinned = namespace
            .load_metadata_view(&context)
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
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .write_file_bytes(path, b"body", None)
            .await
            .expect("create");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the creates");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let created = namespace
        .load_current_metadata_view()
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

    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .delete_path("/c", None)
        .await
        .expect("delete c");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .move_path("/b", "/c", None)
        .await
        .expect("move b to c");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the move");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("floor at the move");
    let (manifest_no, _) = drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
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
    assert_matches!(
        &forward[0],
        MetadataRow::DirentryBinding(binding) if binding.is_bound()
            && binding.name_key.as_str() == "c"
            && binding.child_inode_id == moved
    );
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let compacted = namespace
        .load_current_metadata_view()
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .publish_candidates(vec![CommitCandidate::new(CommitIntent {
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
        })])
        .await
        .into_iter()
        .next()
        .expect("one result")
        .expect("create the parents");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .write_file_bytes("/c/report", b"body", None)
        .await
        .expect("create the child");
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the creates");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let created = namespace
        .load_current_metadata_view()
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
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .move_path(source, destination, None)
            .await
            .expect("move the child");
    }
    create_checkpoint(&store, &namespace_id, &context)
        .await
        .expect("fold the moves");
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let moved = namespace
        .load_current_metadata_view()
        .await
        .expect("moved view");
    let parent = moved
        .metadata_view()
        .current_parent_binding_for_child(child)
        .await
        .expect("parent lookup")
        .expect("the child is bound");
    assert_eq!(parent.parent_inode_id, last_parent);
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(context.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("floor at the last move");
    let (manifest_no, _) = drain_compaction(
        &store,
        &namespace_id,
        MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
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
    assert_matches!(
        child_rows.as_slice(),
        [MetadataRow::DirentryBinding(binding)] if binding.is_bound()
            && binding.parent_inode_id == last_parent
            && binding.name_key.as_str() == "report"
    );
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let compacted = namespace
        .load_current_metadata_view()
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
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .write_file_bytes("/a", b"first", None)
        .await
        .expect("bind");
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context)
        .move_path("/a", "/b", None)
        .await
        .expect("unbind and bind");
    create_checkpoint(&inner, &namespace_id, &context)
        .await
        .expect("fold");
    let manifest = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    let families = manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(|descriptor| (metadata_segment_object_key(descriptor), descriptor.family))
        .collect::<BTreeMap<_, _>>();
    let store = RecordingStore::metadata_segments(inner);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace.load_current_metadata_view().await.expect("view");
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
            ApiMetadataRowFamily::DirentryChildBinds,
            ApiMetadataRowFamily::DirentryRevisions,
        ]
    );
}

#[tokio::test]
async fn a_parent_binding_cycle_is_namespace_corruption_for_visibility_and_current_paths() {
    let directory = tempdir().expect("tempdir");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("binding-cycle").expect("namespace id");
    let commit_id = CommitId::parse("c_binding_cycle").expect("commit id");
    let mut builder = MetadataStateBuilder::default();
    for inode_id in [InodeId(2), InodeId(3)] {
        builder.push_inode(crate::metadata::InodeRecord {
            inode_id,
            inode_kind: InodeKind::Directory,
            committed_seq: ChangeSeq(1),
            commit_id: commit_id.clone(),
            committed_by: loonfs_types::ActorId::loonfs(),
            committed_at_ms: 1_000,
        });
    }
    // Each directory is bound under the other, which no commit can produce.
    for (parent_inode_id, child_inode_id, name) in [
        (InodeId(2), InodeId(3), "three"),
        (InodeId(3), InodeId(2), "two"),
    ] {
        let display_name = loonfs_types::DisplayName::parse(name).expect("display name");
        builder.push_direntry_binding(crate::metadata::DirentryBindingRecord {
            parent_inode_id,
            name_key: loonfs_types::NameKey::for_display_name(&display_name),
            state: loonfs_types::format::manifest::DirentryBindingState::Bound { display_name },
            child_inode_id,
            child_kind: InodeKind::Directory,
            child_created_by: loonfs_types::ActorId::loonfs(),
            child_created_at_ms: 1_000,
            committed_seq: ChangeSeq(1),
            delta_index: 0,
        });
    }
    let segments = crate::test_support::namespace_store(&store, &namespace_id)
        .build_manifest_segments(&builder.build(), &HashMap::new())
        .await
        .expect("build segments");
    let manifest = publish_manifest_with_segments(
        &store,
        &namespace_id,
        ManifestNo(1),
        ChangeSeq(1),
        flatten_manifest_segments(segments),
    )
    .await;
    let verified = load_manifest_segments_for_inspection(&store, None, &namespace_id, &manifest)
        .await
        .expect("load manifest segments");
    let view = crate::metadata::MetadataView::over_manifest_segments(&verified, ChangeSeq(1));

    let covered = view
        .covering_subtree_tombstone(InodeId(2))
        .await
        .expect_err("a cycle is not an uncovered inode");
    let cycle = view
        .would_create_directory_cycle(InodeId(4), InodeId(2))
        .await
        .expect_err("a stored cycle is not a rename without a cycle");
    let path = crate::path::read::resolve_visible_inode(
        &mut view.session(),
        &mut HashMap::new(),
        InodeId(2),
    )
    .await
    .expect_err("a cycle has no current path");
    for error in [covered, cycle, path] {
        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt, "{error}");
    }
}
