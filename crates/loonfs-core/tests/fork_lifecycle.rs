//! Namespace creation, fork installation, repair, and terminal lifecycle guards.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

mod common;

use bytes::Bytes;
use common::mutation_split_support::*;
use common::namespace_engine;
use loonfs_api::{
    sha256_digest,
    wire::control::{
        decode_control_object, encode_control_object, CheckpointOwner,
        ContentStoreDescriptorEnvelope, ContentStoreDescriptorState, ControlObjectKind,
        NamespaceConfigEnvelope, NamespaceConfigState,
    },
    wire::manifest::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataTableFamily,
        NamespaceManifestEnvelope,
    },
    AbsolutePath, ChangeSeq, CommitId, ContentStoreId, DestinationBehavior, ManifestId,
    NamespaceId, RepairNamespaceOutcome,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::control::load_namespace_head_control;
use loonfs_core::publish::PathMutationIntent;
use loonfs_core::{repair_namespace, Error as CoreError, ErrorCode, MutationContext};
use loonfs_objectstore::keys::{
    content_store_descriptor, metadata_manifest_object, namespace_config, wal_head,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
use std::path::Path;
use tempfile::tempdir;

async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::NamespaceSummary, CoreError> {
    namespace_engine(store, source_namespace_id, context)
        .fork_namespace(new_namespace_id)
        .await
}

async fn load_namespace_descriptor_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> NamespaceConfigState {
    let descriptor_key = namespace_config(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .await
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    decode_control_object(&descriptor_bytes, ControlObjectKind::NamespaceConfig)
        .expect("decode namespace descriptor")
        .state
}

async fn seed_source_namespace_for_fork<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    bootstrap_namespace(store, source_namespace_id, context, false)
        .await
        .expect("bootstrap source namespace");
    write_file_bytes(
        store,
        source_namespace_id,
        "/docs/shared.txt",
        b"base",
        context,
        Some("seed-shared"),
    )
    .await
    .expect("seed shared file");
}

async fn assert_namespace_partial<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    let partial_error = bootstrap_namespace(store, namespace_id, context, false)
        .await
        .expect_err("partial namespace");
    assert!(matches!(
        partial_error,
        loonfs_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

fn metadata_table_counting_store(root: impl AsRef<Path>) -> CountingStore<LocalFsStore> {
    CountingStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::metadata_table(),
    )
}

#[tokio::test]
async fn namespace_creation_writes_descriptors_and_rejects_partial_recreation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    let duplicate_error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("complete namespace id reuse should be rejected");
    assert!(matches!(
        duplicate_error,
        loonfs_core::BootstrapNamespaceError::NamespaceAlreadyExists { .. }
    ));
    let existing = bootstrap_namespace(&store, &namespace_id, &context, true)
        .await
        .expect("allow existing");
    assert_eq!(existing.namespace_id, namespace_id);

    let descriptor_key = namespace_config(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .await
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let descriptor: NamespaceConfigEnvelope =
        decode_control_object(&descriptor_bytes, ControlObjectKind::NamespaceConfig)
            .expect("decode namespace descriptor");
    assert_eq!(descriptor.state.namespace_id, namespace_id);
    assert!(store
        .head(&content_store_descriptor(
            descriptor.state.content_store_id.as_str()
        ))
        .await
        .expect("content store descriptor head")
        .is_some());
    let manifest_root =
        loonfs_core::control::load_namespace_metadata_root_control(&store, &namespace_id)
            .await
            .expect("metadata root");
    let manifest_bytes = store
        .get(
            &metadata_manifest_object(
                namespace_id.as_str(),
                &manifest_root.state.manifest_object_id,
            ),
            None,
        )
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest =
        decode_namespace_manifest_json(&manifest_bytes).expect("decode namespace manifest");
    assert!(
        manifest.payload.fork.is_none(),
        "root namespace creation must not write fork provenance"
    );

    let content_descriptor_key =
        content_store_descriptor(descriptor.state.content_store_id.as_str());
    let content_descriptor_bytes = store
        .get(&content_descriptor_key, None)
        .await
        .expect("read content-store descriptor")
        .expect("content-store descriptor exists");
    let content_descriptor: ContentStoreDescriptorEnvelope = decode_control_object(
        &content_descriptor_bytes,
        ControlObjectKind::ContentStoreDescriptor,
    )
    .expect("decode content-store descriptor");
    assert_eq!(
        content_descriptor.state.content_store_id,
        descriptor.state.content_store_id
    );

    let content_store_descriptors = store
        .list_prefix("content-stores/")
        .await
        .expect("list content stores");
    assert_eq!(
        content_store_descriptors,
        vec![content_descriptor_key],
        "new root namespace should create exactly one content store descriptor"
    );

    store
        .put_if_absent(
            &wal_head("partial"),
            Bytes::from_static(br#"{"not":"a descriptor"}"#),
        )
        .await
        .expect("write partial namespace key");

    let partial_error = bootstrap_namespace(
        &store,
        &NamespaceId::parse("partial").expect("valid namespace id"),
        &context,
        false,
    )
    .await
    .expect_err("partial namespace should be rejected");
    assert!(matches!(
        partial_error,
        loonfs_core::BootstrapNamespaceError::NamespacePartiallyInitialized { .. }
    ));
}

#[tokio::test]
async fn bootstrap_head_lost_ack_stays_partial_until_explicit_repair() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(wal_head(namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("target head precondition should re-check partial namespace");
    // The lost ack left the head written and the descriptor absent, so the
    // shared install re-check answers partial — the same policy fork pins.
    assert!(matches!(
        &error,
        loonfs_core::BootstrapNamespaceError::Core(CoreError::NamespacePartial { .. })
    ));
    assert_eq!(error.code(), ErrorCode::NamespacePartial);
    assert!(
        store
            .list_prefix("content-stores/")
            .await
            .expect("list content stores")
            .is_empty(),
        "content-store descriptor must not be allocated before namespace head reservation"
    );
    // The injected failure wrote the head before reporting the lost ack.
    // Normal create retries are classification-only and leave it untouched.
    let retry = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("retry preserves the ack-lost partial tree");
    assert_eq!(retry.code(), ErrorCode::NamespacePartial);
    let report = repair_namespace(&store, &namespace_id, &context)
        .await
        .expect("explicit repair completes the ack-lost create");
    assert_eq!(report.outcome, RepairNamespaceOutcome::Completed);
}

#[tokio::test]
async fn bootstrap_head_conflict_rechecks_complete_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    // The injected conflict simulates a racing create that completed the
    // namespace: its head wins the reservation and its descriptor pair is
    // already published when this attempt re-checks.
    let content_store_id = ContentStoreId::generate();
    let content_store_descriptor_envelope = ContentStoreDescriptorEnvelope::from_state(
        ControlObjectKind::ContentStoreDescriptor,
        &context.writer_version,
        ContentStoreDescriptorState {
            content_store_id: content_store_id.clone(),
        },
    )
    .expect("content store descriptor envelope");
    let descriptor = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: namespace_id.clone(),
            content_store_id: content_store_id.clone(),
            name_policy: loonfs_api::NamePolicy::default(),
        },
    )
    .expect("descriptor envelope");
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(wal_head(namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: vec![
                (
                    content_store_descriptor(content_store_id.as_str()),
                    encode_control_object(&content_store_descriptor_envelope)
                        .expect("content store descriptor bytes"),
                ),
                (
                    namespace_config(namespace_id.as_str()),
                    encode_control_object(&descriptor).expect("descriptor bytes"),
                ),
            ],
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("target head conflict should re-check complete namespace");
    assert!(matches!(
        &error,
        loonfs_core::BootstrapNamespaceError::Core(CoreError::NamespaceExists { .. })
    ));
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
}

#[tokio::test]
async fn namespace_delete_is_terminal_for_reads_writes_creation_and_forks() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"will vanish")
        .await
        .expect("stage content");
    submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("before-delete").expect("valid commit id"),
            message: None,
            absolute_path: AbsolutePath::parse("/keep.txt").expect("path"),
            content_ref: content.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("commit before delete");

    // A stale precondition deletes nothing.
    let engine = namespace_engine(&store, &namespace_id, &context);
    let stale = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions {
            expected_head_seq: Some(ChangeSeq(0)),
        })
        .await
        .expect_err("stale precondition");
    assert_eq!(stale.code(), ErrorCode::StaleHead);

    let response = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    assert_eq!(response.head_seq, ChangeSeq(1));

    // Terminal: reads, commits, status, repeat deletes, re-creation, and forks
    // all observe the deleted head.
    let read = resolve_path(&store, &namespace_id, "/")
        .await
        .expect_err("read after delete");
    assert_eq!(read.code(), ErrorCode::NamespaceDeleted);
    let commit = submit_intent_async(
        &store,
        &namespace_id,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("after-delete").expect("valid commit id"),
            message: None,
            absolute_path: AbsolutePath::parse("/late.txt").expect("path"),
            content_ref: content.content_ref.clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect_err("commit after delete");
    assert_eq!(commit.code(), ErrorCode::NamespaceDeleted);
    let again = engine
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect_err("repeat delete");
    assert_eq!(again.code(), ErrorCode::NamespaceDeleted);
    let recreate = bootstrap_namespace(&store, &namespace_id, &context, false).await;
    assert!(matches!(
        recreate,
        Err(loonfs_core::BootstrapNamespaceError::NamespaceDeleted { .. })
    ));
    let fork_target = NamespaceId::parse("fork-of-deleted").expect("valid namespace id");
    let fork = fork_namespace(&store, &namespace_id, &fork_target, &context).await;
    assert_eq!(
        fork.expect_err("fork of deleted source").code(),
        ErrorCode::NamespaceDeleted
    );
}

#[tokio::test]
async fn fork_clone_survives_source_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let source = NamespaceId::parse("source").expect("valid namespace id");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    bootstrap_namespace(&store, &source, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &source, b"shared bytes")
        .await
        .expect("stage content");
    submit_intent_async(
        &store,
        &source,
        PathMutationIntent::PutFile {
            commit_id: CommitId::parse("seed-clone").expect("valid commit id"),
            message: None,
            absolute_path: AbsolutePath::parse("/shared.txt").expect("path"),
            content_ref: content.content_ref,
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("seed source");
    fork_namespace(&store, &source, &clone, &context)
        .await
        .expect("fork");

    namespace_engine(&store, &source, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete source");

    // The spec promise: the clone keeps reading through the source-owned
    // immutable metadata its manifest pins.
    let clone_head = load_namespace_head_control(&store, &clone)
        .await
        .expect("clone head survives source delete");
    assert_eq!(clone_head.state.seq, ChangeSeq(1));
    resolve_path(&store, &clone, "/shared.txt")
        .await
        .expect("clone reads forked file");
}

#[tokio::test]
async fn namespace_descriptor_checksum_is_validated() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let descriptor_key = namespace_config(namespace_id.as_str());
    let descriptor_bytes = store
        .get(&descriptor_key, None)
        .await
        .expect("read namespace descriptor")
        .expect("namespace descriptor exists");
    let descriptor: NamespaceConfigEnvelope =
        decode_control_object(&descriptor_bytes, ControlObjectKind::NamespaceConfig)
            .expect("decode namespace descriptor");
    // Corrupt the durable document at the byte level: swap the stored
    // checksum for a syntactically valid but wrong digest.
    let corrupted = String::from_utf8(descriptor_bytes.to_vec())
        .expect("descriptor is utf8")
        .replace(
            &descriptor.payload_checksum,
            &sha256_digest(b"not-the-payload"),
        );
    store
        .put_overwrite(&descriptor_key, Bytes::from(corrupted))
        .await
        .expect("overwrite descriptor");

    let error = resolve_path(&store, &namespace_id, "/")
        .await
        .expect_err("descriptor checksum");
    assert!(
        error.to_string().contains("checksum mismatch"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn fork_namespace_reuses_content_store_and_isolates_metadata() {
    let temp_dir = tempdir().expect("tempdir");
    let store = metadata_table_counting_store(temp_dir.path());
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    bootstrap_namespace(&store, &source_namespace_id, &context, false)
        .await
        .expect("bootstrap source namespace");
    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"base",
        &context,
        Some("seed-shared"),
    )
    .await
    .expect("seed shared file");
    namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint before fork");

    let source_head = load_namespace_head_control(&store, &source_namespace_id)
        .await
        .expect("source head");
    assert_eq!(source_head.state.seq, ChangeSeq(1));
    let content_store_id = load_namespace_descriptor_state(&store, &source_namespace_id)
        .await
        .content_store_id;
    let blobs_before = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .await
        .expect("list blobs before fork");

    store.reset();
    fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect("fork namespace");
    assert_eq!(
        store.count(OperationClass::Read),
        0,
        "fork should validate manifest descriptors without loading metadata SST payloads"
    );

    let blobs_after = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .await
        .expect("list blobs after fork");
    assert_eq!(blobs_after, blobs_before, "fork must not copy content");

    let clone_descriptor = load_namespace_descriptor_state(&store, &clone_namespace_id).await;
    assert_eq!(clone_descriptor.content_store_id, content_store_id);
    let clone_head = load_namespace_head_control(&store, &clone_namespace_id)
        .await
        .expect("clone head");
    assert_eq!(clone_head.state.seq, ChangeSeq(1));
    let clone_root =
        loonfs_core::control::load_namespace_metadata_root_control(&store, &clone_namespace_id)
            .await
            .expect("clone metadata root");
    assert_eq!(clone_root.state.manifest_id, ManifestId(1));
    let clone_floor =
        loonfs_core::control::load_namespace_wal_floor_control(&store, &clone_namespace_id)
            .await
            .expect("clone wal floor");
    assert_eq!(clone_floor.state.floor_seq, ChangeSeq(1));

    let target_manifest_key = metadata_manifest_object(
        clone_namespace_id.as_str(),
        &clone_root.state.manifest_object_id,
    );
    let target_manifest_bytes = store
        .get(&target_manifest_key, None)
        .await
        .expect("read target manifest")
        .expect("target manifest exists");
    let target_manifest =
        decode_namespace_manifest_json(&target_manifest_bytes).expect("decode target manifest");
    let fork_provenance = target_manifest
        .payload
        .fork
        .as_ref()
        .expect("fork provenance lives in target manifest");
    assert_eq!(fork_provenance.source_namespace_id, source_namespace_id);
    assert_eq!(fork_provenance.fork_seq, ChangeSeq(1));
    assert!(fork_provenance
        .source_checkpoint_id
        .as_str()
        .starts_with("chk_"));
    assert_eq!(fork_provenance.source_manifest_id, ManifestId(1));
    assert_eq!(fork_provenance.source_head_seq, ChangeSeq(1));
    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &fork_provenance.source_checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    assert_eq!(source_record.manifest_head_seq, ChangeSeq(1));
    assert_eq!(source_record.manifest_id, ManifestId(1));
    assert!(
        target_manifest
            .payload
            .metadata_files
            .iter()
            .all(|metadata_file| metadata_file.owner_namespace_id == source_namespace_id),
        "fork target manifest should reference source-owned metadata SSTs"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/metadata/tables/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target metadata SSTs")
            .is_empty(),
        "COW fork should not copy metadata SSTs into the target namespace"
    );

    assert!(
        matches!(
            &source_record.owner,
            CheckpointOwner::Fork { target_namespace_id }
                if *target_namespace_id == clone_namespace_id
        ),
        "fork record is owned by its target namespace"
    );
    let referenced_metadata_files = target_manifest
        .payload
        .metadata_files
        .iter()
        .map(|metadata_file| metadata_file.object_key.clone())
        .collect::<Vec<_>>();

    let duplicate_error =
        fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
            .await
            .expect_err("duplicate fork target");
    assert_eq!(duplicate_error.code(), ErrorCode::NamespaceExists);

    let source_entry = resolve_path(&store, &source_namespace_id, "/docs/shared.txt")
        .await
        .expect("source stat");
    let clone_entry = resolve_path(&store, &clone_namespace_id, "/docs/shared.txt")
        .await
        .expect("clone stat");
    assert_eq!(source_entry.content_ref, clone_entry.content_ref);
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone")
            .bytes,
        b"base"
    );
    let stale_clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(0))
        .await
        .expect_err("old cursor");
    assert_eq!(stale_clone_changes.code(), ErrorCode::RebootstrapRequired);
    let empty_clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(1))
        .await
        .expect("empty changes");
    assert!(empty_clone_changes.changes.is_empty());

    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"source-after-fork",
        &context,
        Some("source-after-fork"),
    )
    .await
    .expect("source replace");
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone after source write")
            .bytes,
        b"base"
    );

    let clone_write = write_file_bytes(
        &store,
        &clone_namespace_id,
        "/docs/shared.txt",
        b"clone-after-fork",
        &context,
        Some("clone-after-fork"),
    )
    .await
    .expect("clone replace");
    assert_eq!(clone_write.committed_seq, ChangeSeq(2));
    assert_eq!(
        read_file_bytes(&store, &source_namespace_id, "/docs/shared.txt")
            .await
            .expect("read source")
            .bytes,
        b"source-after-fork"
    );
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone")
            .bytes,
        b"clone-after-fork"
    );

    let clone_changes = list_changes_after(&store, &clone_namespace_id, ChangeSeq(1))
        .await
        .expect("clone changes");
    assert_eq!(clone_changes.changes.len(), 1);
    assert_eq!(clone_changes.changes[0].seq, ChangeSeq(2));

    for prefix in [
        format!("namespaces/{}/wal/head.json", source_namespace_id.as_str()),
        format!("namespaces/{}/wal/", source_namespace_id.as_str()),
        format!(
            "namespaces/{}/metadata/manifests/",
            source_namespace_id.as_str()
        ),
    ] {
        for key in store
            .list_prefix(&prefix)
            .await
            .expect("list source mutable keys")
        {
            store.delete(&key).await.expect("delete source mutable key");
        }
    }
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("clone remains readable")
            .bytes,
        b"clone-after-fork"
    );

    let referenced_sst = referenced_metadata_files
        .first()
        .expect("fork should reference source metadata SST")
        .clone();
    store
        .delete(&referenced_sst)
        .await
        .expect("delete referenced source metadata SST");
    let corrupt_target = read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
        .await
        .expect_err("target should fail when referenced source SST is missing");
    assert_eq!(corrupt_target.code(), ErrorCode::NamespaceCorrupt);
}

#[tokio::test]
async fn fork_namespace_rejects_corrupt_source_manifest_descriptors() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    bootstrap_namespace(&store, &source_namespace_id, &context, false)
        .await
        .expect("bootstrap source namespace");
    write_file_bytes(
        &store,
        &source_namespace_id,
        "/docs/shared.txt",
        b"base",
        &context,
        Some("seed-shared"),
    )
    .await
    .expect("seed shared file");
    let checkpoint = namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint");

    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &checkpoint.checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    let manifest_key = metadata_manifest_object(
        source_namespace_id.as_str(),
        &source_record.manifest_object_id,
    );
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read source manifest")
        .expect("source manifest exists");
    let mut manifest =
        decode_namespace_manifest_json(&manifest_bytes).expect("decode source manifest");
    manifest
        .payload
        .metadata_files
        .retain(|metadata_file| metadata_file.family != MetadataTableFamily::RevisionsByInodeDesc);
    let manifest =
        NamespaceManifestEnvelope::from_payload(manifest.writer_version, manifest.payload)
            .expect("rebuild manifest checksum");
    let corrupted = encode_namespace_manifest_json(&manifest).expect("encode corrupt manifest");
    store
        .put_overwrite(&manifest_key, Bytes::from(corrupted))
        .await
        .expect("overwrite source manifest");

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("corrupt source manifest should block fork");

    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head clone descriptor")
            .is_none(),
        "failed fork must not publish target descriptor"
    );
}

#[tokio::test]
async fn fork_target_head_reservation_failure_keeps_descriptor_unpublished() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(wal_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: Vec::new(),
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target head precondition should re-check partial namespace");
    assert_eq!(error.code(), ErrorCode::NamespacePartial);
    assert!(
        !store
            .list_prefix(&format!(
                "namespaces/{}/metadata/manifests/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target manifests")
            .is_empty(),
        "target manifest should be written before target head reservation"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context).await;
    let retry = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("normal fork retry preserves the raced partial target");
    assert_eq!(retry.code(), ErrorCode::NamespacePartial);
    let report = repair_namespace(&store, &clone_namespace_id, &context)
        .await
        .expect("explicit repair completes the raced fork target");
    assert_eq!(report.outcome, RepairNamespaceOutcome::Completed);
}

#[tokio::test]
async fn fork_source_checkpoint_failure_leaves_target_namespace_absent() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!(
            "namespaces/{}/checkpoints/",
            source_namespace_id.as_str()
        )),
        InjectedCreateFailure::Transport {
            message: "injected source checkpoint failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("source checkpoint failure should abort fork before target publication");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&wal_head(clone_namespace_id.as_str()))
            .await
            .expect("head target head")
            .is_none(),
        "target head must not be reserved before source retention is pinned"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "target descriptor must remain unpublished"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/metadata/manifests/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target manifests")
            .is_empty(),
        "target manifest must not be written before source retention is pinned"
    );
    assert!(
        store
            .head(&namespace_config(source_namespace_id.as_str()))
            .await
            .expect("head source descriptor")
            .is_some(),
        "source descriptor should remain published"
    );
}

#[tokio::test]
async fn fork_target_manifest_failure_leaves_target_namespace_absent() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Prefix(format!(
            "namespaces/{}/metadata/manifests/",
            clone_namespace_id.as_str()
        )),
        InjectedCreateFailure::Transport {
            message: "injected target manifest failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target manifest write should fail");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&wal_head(clone_namespace_id.as_str()))
            .await
            .expect("head target head")
            .is_none(),
        "target head must not be reserved before target manifest exists"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    assert!(
        store
            .list_prefix(&format!(
                "namespaces/{}/metadata/manifests/",
                clone_namespace_id.as_str()
            ))
            .await
            .expect("list target manifests")
            .is_empty(),
        "target manifest should not exist after injected manifest write failure"
    );
    assert!(
        store
            .head(&namespace_config(source_namespace_id.as_str()))
            .await
            .expect("head source descriptor")
            .is_some(),
        "source descriptor should remain published"
    );
}

#[tokio::test]
async fn fork_failure_after_target_head_before_descriptor_remains_partial() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = InjectCreateFailureStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyMatcher::Exact(namespace_config(clone_namespace_id.as_str())),
        InjectedCreateFailure::Transport {
            message: "injected target descriptor failure",
        },
    );
    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target descriptor write should fail");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(
        store
            .head(&wal_head(clone_namespace_id.as_str()))
            .await
            .expect("head target head")
            .is_some(),
        "target head should still reserve namespace"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head target descriptor")
            .is_none(),
        "descriptor must remain unpublished"
    );
    let target_manifest_keys = store
        .list_prefix(&format!(
            "namespaces/{}/metadata/manifests/",
            clone_namespace_id.as_str()
        ))
        .await
        .expect("list target manifests");
    assert!(
        !target_manifest_keys.is_empty(),
        "target manifest should have been written before descriptor failure"
    );
    assert_namespace_partial(&store, &clone_namespace_id, &context).await;
}

#[tokio::test]
async fn fork_target_control_conflict_rechecks_complete_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&inner, &source_namespace_id, &context).await;
    let content_store_id = load_namespace_descriptor_state(&inner, &source_namespace_id)
        .await
        .content_store_id;
    let descriptor = NamespaceConfigEnvelope::from_state(
        ControlObjectKind::NamespaceConfig,
        &context.writer_version,
        NamespaceConfigState {
            namespace_id: clone_namespace_id.clone(),
            content_store_id,
            name_policy: loonfs_api::NamePolicy::default(),
        },
    )
    .expect("descriptor envelope");
    let store = InjectCreateFailureStore::new(
        inner,
        KeyMatcher::Exact(wal_head(clone_namespace_id.as_str())),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: true,
            additional_writes: vec![(
                namespace_config(clone_namespace_id.as_str()),
                loonfs_api::wire::control::encode_control_object(&descriptor)
                    .expect("descriptor bytes"),
            )],
        },
    );

    let error = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect_err("target head conflict should re-check complete namespace");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    assert!(
        store
            .head(&namespace_config(source_namespace_id.as_str()))
            .await
            .expect("head source descriptor")
            .is_some(),
        "source descriptor should remain published"
    );
    assert!(
        store
            .head(&namespace_config(clone_namespace_id.as_str()))
            .await
            .expect("head clone descriptor")
            .is_some(),
        "clone descriptor should be present for the simulated complete target"
    );
}
