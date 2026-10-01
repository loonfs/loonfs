//! Retained namespace views across metadata maintenance and collection.

use crate::common::*;
use loonfs::engine::UNREFERENCED_SEGMENT_MIN_AGE_MS;
use loonfs::AccessState;
use loonfs::AttributeChanges;
use loonfs::{
    current_time_ms, CompactionStepOutcome, CreateDirectoryOptions, CreateNamespaceOptions,
    DestinationBehavior, GcOptions, InlineContentPolicy, LoonFs, MetadataCompactionPolicy,
    MetadataMaintenanceOptions, PutFileOptions, WalFoldStepOutcome, GC_DEFAULT_GRACE_WINDOW_MS,
};
use loonfs_core::control::load_namespace_current_manifest;
use loonfs_core::MutationContext;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ByteRange;
use loonfs_test_support::ids::{
    attribute_key, attribute_text, first_page, namespace_id, page_limit,
};
use loonfs_test_support::test_actor;
use loonfs_types::format::manifest::{MetadataRow, MetadataRowFamily, RunTier};
use loonfs_types::format::sst_blocks::{decode_data_block, decode_index_block};
use loonfs_types::{
    AccessGrants, AccessRevisionNo, AccessRight, AccessRights, Attributes, AttributesRevisionNo,
    ChangeSeq, ErrorCode, NamespaceAccess, PageRequest, PathEntry, PrincipalId, PrincipalScope,
    PrincipalSet, RevisionNo, Subject, SubjectId,
};
use std::collections::{BTreeMap, BTreeSet};
use tempfile::tempdir;

#[tokio::test]
async fn retained_views_keep_their_meaning_across_maintenance() {
    let directory = tempdir().expect("tempdir");
    let object_store = store(directory.path());
    let runtime = open_runtime_with_async(object_store.clone(), "retained-views", |builder| {
        builder
            .min_publish_interval_ms(0)
            .inline_content(InlineContentPolicy {
                inline_content_threshold_bytes: Some(64),
                ..Default::default()
            })
    })
    .await;
    let source = namespace_id("source");
    let fork = namespace_id("fork");
    let principal = PrincipalId::parse("prn_admin").expect("principal");
    let subject = Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse("administrator").expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([principal.clone()])).expect("principals"),
    };
    let writer = runtime.writer.with_subject(subject.clone());
    let source_namespace = writer.namespace(&source);
    writer
        .create_namespace_with_options(
            &source,
            &test_actor(),
            &CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: subject.principal_scope.clone(),
                    root_grants: AccessGrants::new(BTreeMap::from([(
                        principal,
                        AccessRights::from_iter([AccessRight::Admin]),
                    )]))
                    .expect("root grants"),
                },
                ..Default::default()
            },
        )
        .await
        .expect("create ACL namespace");
    let namespace_writer = writer.open_namespace(&source).expect("open namespace");
    namespace_writer
        .create_directory_with_options(
            "/docs/nested",
            &test_actor(),
            &CreateDirectoryOptions {
                parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("create directory tree");
    for (path, bytes) in [
        ("/docs/inline.txt", b"first revision".as_slice()),
        ("/docs/deleted.txt", b"deleted".as_slice()),
        ("/docs/recover.txt", b"recovered".as_slice()),
    ] {
        let prepared = namespace_writer
            .prepare_content(bytes)
            .await
            .expect("prepare inline content");
        assert!(prepared.upload_id().is_none());
        namespace_writer
            .put_file_prepared(path, prepared, &test_actor())
            .await
            .expect("commit inline content");
    }
    let uploaded_bytes = vec![b'u'; 128 * 1024];
    let prepared = namespace_writer
        .prepare_content(&uploaded_bytes)
        .await
        .expect("stage upload");
    assert!(prepared.upload_id().is_some());
    namespace_writer
        .put_file_prepared("/docs/nested/uploaded.txt", prepared, &test_actor())
        .await
        .expect("commit uploaded content");
    namespace_writer
        .put_file_with_options(
            "/docs/inline.txt",
            b"second revision",
            &test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("replace inline file");
    namespace_writer
        .move_path("/docs/inline.txt", "/docs/renamed.txt", &test_actor())
        .await
        .expect("rename inline file");
    for (set, remove) in [
        (
            BTreeMap::from([(attribute_key("owner"), attribute_text("platform"))]),
            Vec::new(),
        ),
        (BTreeMap::new(), vec![attribute_key("owner")]),
    ] {
        namespace_writer
            .update_attributes(
                "/docs/renamed.txt",
                &test_actor(),
                AttributeChanges { set, remove },
            )
            .await
            .expect("update attributes");
    }
    for (boundary, grants) in [
        (
            true,
            AccessGrants::new(BTreeMap::from([(
                PrincipalId::parse("prn_reader").expect("principal"),
                AccessRights::from_iter([AccessRight::Read]),
            )]))
            .expect("directory grants"),
        ),
        (false, AccessGrants::default()),
    ] {
        namespace_writer
            .update_access(
                "/docs/nested",
                &test_actor(),
                AccessState { boundary, grants },
            )
            .await
            .expect("update directory access");
    }
    let deleted_inode = source_namespace
        .stat("/docs/deleted.txt")
        .await
        .expect("file to delete")
        .inode_id;
    let deleted = namespace_writer
        .delete_path("/docs/deleted.txt", &test_actor())
        .await
        .expect("delete file");
    let restored_inode = source_namespace
        .stat("/docs/recover.txt")
        .await
        .expect("file to recover")
        .inode_id;
    let recover = namespace_writer
        .delete_path("/docs/recover.txt", &test_actor())
        .await
        .expect("delete file before recovery");
    let restored = namespace_writer
        .undelete(
            restored_inode,
            recover.committed_seq,
            Some("/docs/restored.txt"),
            &test_actor(),
        )
        .await
        .expect("restore file at a new name");

    let mut head_seq = restored.committed_seq;
    let mut checkpoint = None;
    let mut fork_listing = BTreeMap::new();
    for stage in [
        "cold",
        "folded",
        "forked",
        "floor_advanced",
        "rebuilt",
        "gc",
    ] {
        match stage {
            "folded" => {
                let step = runtime
                    .maintenance
                    .maintain_metadata_with_options(&source, &metadata_options(1))
                    .await
                    .expect("fold WAL");
                assert_eq!(
                    step.wal_fold,
                    WalFoldStepOutcome::Folded {
                        manifest_head_seq: head_seq,
                    }
                );
            }
            "forked" => {
                let captured = runtime
                    .create_checkpoint(&source)
                    .await
                    .expect("checkpoint");
                assert_eq!(captured.captured_seq, head_seq);
                let target = writer
                    .fork_namespace(&source, &fork, &test_actor())
                    .await
                    .expect("fork checkpointed head");
                assert_eq!(target.head_seq, captured.captured_seq);
                checkpoint = Some(captured);
            }
            "floor_advanced" => {
                head_seq = namespace_writer
                    .put_file("/later.txt", b"source only", &test_actor())
                    .await
                    .expect("commit after fork")
                    .committed_seq;
                runtime
                    .maintenance
                    .maintain_metadata_with_options(&source, &metadata_options(1))
                    .await
                    .expect("materialize source head before advancing retention");
                let advanced = runtime
                    .maintenance
                    .advance_retention_floor(&source)
                    .await
                    .expect("advance retention");
                assert_eq!(advanced.retention_floor_seq, head_seq);
            }
            "rebuilt" => {
                let mut rebuilt = false;
                let mut finished = false;
                for _ in 0..32 {
                    let step = runtime
                        .maintenance
                        .maintain_metadata_with_options(
                            &source,
                            &MetadataMaintenanceOptions {
                                compaction_policy: MetadataCompactionPolicy::CompactImmediately,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("rebuild metadata after retention");
                    if step.compaction == (CompactionStepOutcome::NotNeeded {}) {
                        finished = true;
                        break;
                    }
                    assert_eq!(step.compaction, CompactionStepOutcome::UnitPublished {});
                    rebuilt = true;
                }
                assert!(rebuilt && finished, "metadata must rebuild and finish");
                let manifest = load_namespace_current_manifest(object_store.as_ref(), &source)
                    .await
                    .expect("rebuilt manifest");
                assert!(manifest
                    .state
                    .envelope
                    .payload()
                    .runs
                    .iter()
                    .all(|run| { run.tier == RunTier::Base }));
                assert!(manifest.state.envelope.payload().runs.iter().any(|run| {
                    run.run_seq == head_seq
                        && run
                            .segments
                            .iter()
                            .any(|segment| segment.family == MetadataRowFamily::DirentryBinds)
                }));
            }
            "gc" => {
                // Every object is fresh, so the pass runs at a clock past
                // every age gate. The checkpoint and fork pins are the only
                // roots for the pre-rebuild file set.
                let now_ms = current_time_ms().expect("wall clock")
                    + UNREFERENCED_SEGMENT_MIN_AGE_MS
                    + GC_DEFAULT_GRACE_WINDOW_MS
                    + 1;
                let collected = loonfs_core::gc_namespace(
                    object_store.as_ref(),
                    &source,
                    &GcOptions {
                        grace_window_ms: GC_DEFAULT_GRACE_WINDOW_MS,
                    },
                    &MutationContext {
                        writer_id: loonfs_types::WriterId::parse("retained-views-gc")
                            .expect("writer id"),
                        now_ms,
                    },
                )
                .await
                .expect("collect aged objects");
                assert!(collected.deleted.wal_objects > 0, "{collected:?}");
                assert!(collected.deleted.metadata_segments > 0, "{collected:?}");
                assert!(collected.deleted.manifests > 0, "{collected:?}");
            }
            _ => {}
        }

        let reader = LoonFs::builder_with_store(object_store.clone())
            .read_only()
            .build()
            .await
            .expect("fresh reader")
            .with_subject(subject.clone());
        let source_namespace = reader.namespace(&source);
        let fork_namespace = reader.namespace(&fork);
        for (path, mut expected_paths) in [
            ("/", vec!["/docs"]),
            (
                "/docs",
                vec!["/docs/nested", "/docs/renamed.txt", "/docs/restored.txt"],
            ),
            ("/docs/nested", vec!["/docs/nested/uploaded.txt"]),
        ] {
            if path == "/" && head_seq > restored.committed_seq {
                expected_paths.push("/later.txt");
            }
            let listed = collect_path_entries(&reader, &source, path)
                .await
                .expect("source listing");
            assert_eq!(listed.head_seq, head_seq, "{stage}: {path}");
            assert_eq!(
                listed
                    .entries
                    .iter()
                    .map(|entry| entry.path.as_str())
                    .collect::<Vec<_>>(),
                expected_paths,
                "{stage}: {path}"
            );
            if stage == "cold" {
                fork_listing.insert(path, listed.entries);
            }
            if let Some(checkpoint) = &checkpoint {
                let view = source_namespace
                    .read_view_at_checkpoint(&checkpoint.checkpoint_id)
                    .await
                    .expect("checkpoint view");
                let historical = view
                    .list(path)
                    .page(PageRequest {
                        limit: page_limit(16),
                        cursor: None,
                    })
                    .await
                    .expect("checkpoint listing");
                assert_eq!(historical.head_seq, checkpoint.captured_seq, "{stage}");
                assert!(historical.next_cursor.is_none());
                assert_eq!(historical.entries, fork_listing[path], "{stage}: {path}");
                let inherited = collect_path_entries(&reader, &fork, path)
                    .await
                    .expect("fork listing");
                assert_eq!(inherited.head_seq, checkpoint.captured_seq, "{stage}");
                assert!(inherited
                    .entries
                    .iter()
                    .all(|entry| entry.namespace_id == fork));
                // Binding tokens are scoped to the namespace that issued
                // them, so the fork's entries match the source's on
                // everything but the token.
                let without_tokens = |entries: &[PathEntry]| {
                    entries
                        .iter()
                        .map(|entry| (entry.inode_id, entry.path.clone(), entry.kind.clone()))
                        .collect::<Vec<_>>()
                };
                assert_eq!(
                    without_tokens(&inherited.entries),
                    without_tokens(&fork_listing[path]),
                    "{stage}: {path}"
                );
            }
        }
        for namespace in std::iter::once(&source).chain(checkpoint.as_ref().map(|_| &fork)) {
            let namespace_reader = reader.namespace(namespace);
            for (path, expected) in [
                ("/docs/renamed.txt", b"second revision".as_slice()),
                ("/docs/nested/uploaded.txt", uploaded_bytes.as_slice()),
            ] {
                assert_eq!(
                    namespace_reader
                        .read_file(path)
                        .await
                        .expect("file bytes")
                        .bytes,
                    expected,
                    "{stage}: {namespace}: {path}"
                );
            }
            let revisions = namespace_reader
                .list_file_revisions("/docs/renamed.txt")
                .page(PageRequest {
                    limit: page_limit(16),
                    cursor: None,
                })
                .await
                .expect("revision listing");
            assert_eq!(revisions.revisions.len(), 2, "{stage}: {namespace}");
            assert!(revisions.next_cursor.is_none());
            assert_eq!(
                namespace_reader
                    .read_file_revision("/docs/renamed.txt", RevisionNo(1))
                    .await
                    .expect("first revision bytes")
                    .bytes,
                b"first revision"
            );
            let entry = namespace_reader
                .stat("/docs/renamed.txt")
                .await
                .expect("cleared attributes");
            let attributes = entry.attributes.expect("attribute projection");
            assert_eq!(attributes.attributes_revision_no, AttributesRevisionNo(2));
            assert_eq!(attributes.attributes, Attributes::default(), "{stage}");
            let trash = namespace_reader
                .list_trash()
                .page(PageRequest {
                    limit: page_limit(16),
                    cursor: None,
                })
                .await
                .expect("trash listing");
            assert!(trash.next_cursor.is_none());
            assert_eq!(trash.entries.len(), 1, "{stage}: {namespace}");
            assert_eq!(trash.entries[0].inode_id, deleted_inode);
            assert_eq!(trash.entries[0].deletion_seq, deleted.committed_seq);
            assert_eq!(
                trash.entries[0].deleted_binding.display_name.as_str(),
                "deleted.txt"
            );
            assert_eq!(
                namespace_reader
                    .stat("/docs/restored.txt")
                    .await
                    .expect("restored binding")
                    .inode_id,
                restored_inode,
                "{stage}"
            );
            if stage != "cold" {
                let directory_inode = namespace_reader
                    .stat("/docs/nested")
                    .await
                    .expect("directory")
                    .inode_id;
                let manifest = load_namespace_current_manifest(object_store.as_ref(), namespace)
                    .await
                    .expect("manifest with access rows");
                let mut access_rows = Vec::new();
                for segment in manifest
                    .state
                    .envelope
                    .payload()
                    .runs
                    .iter()
                    .flat_map(|run| &run.segments)
                    .filter(|segment| segment.family == MetadataRowFamily::Access)
                {
                    let key = metadata_segment_object_key(segment);
                    let handle = segment.index_block;
                    let bytes = object_store
                        .get(
                            &key,
                            Some(ByteRange {
                                start_inclusive: handle.offset,
                                end_exclusive: handle.offset + u64::from(handle.stored_bytes),
                            }),
                        )
                        .await
                        .expect("access index")
                        .expect("index exists");
                    for index in decode_index_block(&bytes, &handle).expect("decode access index") {
                        let handle = index.block;
                        let bytes = object_store
                            .get(
                                &key,
                                Some(ByteRange {
                                    start_inclusive: handle.offset,
                                    end_exclusive: handle.offset + u64::from(handle.stored_bytes),
                                }),
                            )
                            .await
                            .expect("access block")
                            .expect("block exists");
                        for row in decode_data_block(&bytes, &handle)
                            .expect("decode access rows")
                            .rows
                        {
                            if let MetadataRow::AccessRevision(row) = row {
                                if row.inode_id == directory_inode {
                                    access_rows.push(row);
                                }
                            }
                        }
                    }
                }
                let access = access_rows
                    .iter()
                    .max_by_key(|row| row.access_revision_no)
                    .expect("retained directory access row");
                assert_eq!(access.access_revision_no, AccessRevisionNo(2));
                assert!(!access.boundary, "{stage}: {namespace}");
                assert!(access.grants.is_empty(), "{stage}: {namespace}");
            }
        }
        if head_seq > restored.committed_seq {
            expect_code(
                fork_namespace.read_file("/later.txt").await,
                ErrorCode::PathNotFound,
            );
            expect_code(
                source_namespace
                    .list_changes(ChangeSeq(0))
                    .page(first_page())
                    .await,
                ErrorCode::RebootstrapRequired,
            );
        }
    }
    writer.shutdown().await.expect("shutdown writer");
}
