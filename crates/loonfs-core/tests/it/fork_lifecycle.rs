//! Namespace creation, fork installation, and terminal lifecycle guards.
//!
//! Create and fork are one conditional write of a complete head, so these
//! tests are mostly about what that single write does under contention and
//! what a namespace looks like before it has published anything of its own.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use bytes::Bytes;
use loonfs_api::{
    wire::control::{
        decode_control_object, encode_control_object, CheckpointOwner, CheckpointRecordLifecycle,
        ControlObjectKind, HeadState, HeadStateEnvelope,
    },
    wire::manifest::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataTableFamily,
        NamespaceManifestEnvelope,
    },
    AbsolutePath, ChangeSeq, CommitId, DestinationBehavior, ManifestId, NamespaceId,
};
use loonfs_core::content::store_bytes_as_content;
use loonfs_core::control::load_namespace_head_control;
use loonfs_core::publish::FilesystemOperation;
use loonfs_core::{Error as CoreError, ErrorCode, MutationContext};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_root, wal_floor, wal_head};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

async fn fork_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<loonfs_api::NamespaceStatusResponse, CoreError> {
    namespace_engine(store, source_namespace_id, context)
        .fork_namespace(new_namespace_id)
        .await
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

async fn head_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> loonfs_api::wire::control::HeadState {
    load_namespace_head_control(store, namespace_id)
        .await
        .expect("load head")
        .state
}

async fn namespace_keys<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Vec<String> {
    store
        .list_prefix(&format!("namespaces/{}/", namespace_id.as_str()))
        .await
        .expect("list namespace prefix")
}

fn metadata_table_counting_store(root: impl AsRef<Path>) -> CountingStore<LocalFsStore> {
    CountingStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::metadata_table(),
    )
}

/// A created namespace is exactly one object. It serves reads and accepts
/// writes with no root, no floor, and no manifest anywhere; the first flush
/// is what publishes a root.
#[tokio::test]
async fn a_created_namespace_is_one_object_until_its_first_flush() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");

    let created = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    assert_eq!(created.namespace_id, namespace_id);
    assert_eq!(created.head_seq, ChangeSeq(0));
    assert_eq!(created.current_manifest_id, None);
    assert_eq!(created.wal_tail_segments, 0);
    assert_eq!(created.retention_floor_seq, ChangeSeq(0));
    assert_eq!(
        namespace_keys(&store, &namespace_id).await,
        vec![wal_head(&namespace_id)],
        "creation writes the head and nothing else"
    );
    assert!(
        store
            .list_prefix("content-stores/")
            .await
            .expect("list content stores")
            .is_empty(),
        "the content store is an id in the head, not an object"
    );

    // Reads work against the built-in genesis state.
    let root_entry = resolve_path(&store, &namespace_id, "/")
        .await
        .expect("a fresh namespace serves reads");
    assert_eq!(root_entry.inode_kind(), loonfs_api::InodeKind::Directory);
    let status = loonfs_core::cache::load_namespace_head_summary(&store, &namespace_id)
        .await
        .expect("status");
    assert_eq!(status.current_manifest_id, None);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    // And so do writes.
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/first.txt",
        b"hello",
        &context,
        Some("first-write"),
    )
    .await
    .expect("a fresh namespace accepts writes");
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/first.txt")
            .await
            .expect("read back")
            .bytes,
        b"hello"
    );
    let keys = namespace_keys(&store, &namespace_id).await;
    assert!(
        keys.iter().all(|key| key == &wal_head(&namespace_id)
            || key.starts_with(&format!(
                "namespaces/{}/wal/segments/",
                namespace_id.as_str()
            ))),
        "before the first flush a namespace is a head plus its WAL: {keys:?}"
    );

    // The first flush materializes the root.
    namespace_engine(&store, &namespace_id, &context)
        .flush_wal()
        .await
        .expect("flush");
    assert!(
        store
            .head(&metadata_root(&namespace_id))
            .await
            .expect("probe root")
            .is_some(),
        "the first flush publishes metadata/root.json"
    );
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/docs/first.txt")
            .await
            .expect("read after flush")
            .bytes,
        b"hello"
    );
    let status = loonfs_core::cache::load_namespace_head_summary(&store, &namespace_id)
        .await
        .expect("status after flush");
    assert_eq!(status.current_manifest_id, Some(ManifestId(1)));
}

/// Exactly one of two concurrent creates of the same id wins; the loser is
/// told the id is taken.
#[tokio::test]
async fn concurrent_creates_of_one_id_leave_exactly_one_winner() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = namespace_id("demo");
    let first = mutation_context();
    let mut second = mutation_context();
    second.writer_id = "writer-second".to_owned();

    let (left, right) = tokio::join!(
        bootstrap_namespace(store.as_ref(), &namespace_id, &first, false),
        bootstrap_namespace(store.as_ref(), &namespace_id, &second, false),
    );
    let outcomes = [left, right];
    let winners = outcomes.iter().filter(|result| result.is_ok()).count();
    assert_eq!(winners, 1, "exactly one create may win: {outcomes:?}");
    let loser = outcomes
        .into_iter()
        .find_map(|result| result.err())
        .expect("one loser");
    assert_eq!(loser.code(), ErrorCode::NamespaceExists);
    assert_eq!(
        namespace_keys(store.as_ref(), &namespace_id).await,
        vec![wal_head(&namespace_id)]
    );
}

/// A create retry after a lost acknowledgment answers `namespace_exists`,
/// whoever made the namespace, and leaves the landed head untouched. The
/// namespace it names is complete and usable, which is the whole point: the
/// old protocol answered this case with a partial namespace nobody could
/// use. A caller that wants the retry to succeed asks for that.
#[tokio::test]
async fn a_create_retry_after_a_lost_acknowledgment_reports_the_id_as_taken() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("first create lands");
    let head_before = head_state(&store, &namespace_id).await;

    let mut retry_context = context.clone();
    retry_context.now_ms += 5_000;
    let conflict = bootstrap_namespace(&store, &namespace_id, &retry_context, false)
        .await
        .expect_err("the id is taken, whoever took it");
    assert_eq!(conflict.code(), ErrorCode::NamespaceExists);

    let mut other_writer = context.clone();
    other_writer.writer_id = "writer-other".to_owned();
    let conflict = bootstrap_namespace(&store, &namespace_id, &other_writer, false)
        .await
        .expect_err("another writer may not adopt this namespace either");
    assert_eq!(conflict.code(), ErrorCode::NamespaceExists);

    // Opting in makes the retry succeed, and the namespace it returns is
    // the one that landed.
    let adopted = bootstrap_namespace(&store, &namespace_id, &retry_context, true)
        .await
        .expect("allow_existing adopts the landed namespace");
    assert_eq!(adopted.namespace_id, namespace_id);
    assert_eq!(
        head_state(&store, &namespace_id).await,
        head_before,
        "no retry may rewrite the landed head"
    );
}

/// The same one-winner rule covers two forks racing for one target, and a
/// create racing a fork for the same id.
#[tokio::test]
async fn concurrent_installs_of_one_target_leave_exactly_one_winner() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let context = mutation_context();
    let source = namespace_id("source");
    let target = NamespaceId::parse("target").expect("valid namespace id");
    seed_source_namespace_for_fork(store.as_ref(), &source, &context).await;

    let mut second = context.clone();
    second.writer_id = "writer-second".to_owned();
    let (left, right) = tokio::join!(
        fork_namespace(store.as_ref(), &source, &target, &context),
        fork_namespace(store.as_ref(), &source, &target, &second),
    );
    let outcomes = [left, right];
    assert_eq!(
        outcomes.iter().filter(|result| result.is_ok()).count(),
        1,
        "exactly one fork may install the target: {outcomes:?}"
    );
    assert_eq!(
        outcomes
            .into_iter()
            .find_map(|result| result.err())
            .expect("one loser")
            .code(),
        ErrorCode::NamespaceExists
    );

    // A create racing a fork for a fresh id: same rule, and the loser never
    // sees a half-installed namespace.
    let contested = NamespaceId::parse("contested").expect("valid namespace id");
    let (created, forked) = tokio::join!(
        bootstrap_namespace(store.as_ref(), &contested, &context, false),
        fork_namespace(store.as_ref(), &source, &contested, &second),
    );
    assert_eq!(
        usize::from(created.is_ok()) + usize::from(forked.is_ok()),
        1,
        "exactly one of create and fork may win: {created:?} {forked:?}"
    );
    let head = head_state(store.as_ref(), &contested).await;
    assert_eq!(
        head.state,
        loonfs_api::wire::control::NamespaceState::Active
    );
    if created.is_ok() {
        assert!(head.fork_basis.is_none(), "the create won");
    } else {
        assert!(head.fork_basis.is_some(), "the fork won");
    }
}

/// A fork target is one object too: it reads through the source's manifest
/// until it flushes, and it never copies content or metadata.
#[tokio::test]
async fn fork_namespace_reuses_content_store_and_isolates_metadata() {
    let temp_dir = tempdir().expect("tempdir");
    let store = metadata_table_counting_store(temp_dir.path());
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;
    namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint before fork");

    let source_head = head_state(&store, &source_namespace_id).await;
    assert_eq!(source_head.seq, ChangeSeq(1));
    let content_store_id = source_head.content_store_id.clone();
    let blobs_before = store
        .list_prefix(&format!(
            "content-stores/{}/blobs/",
            content_store_id.as_str()
        ))
        .await
        .expect("list blobs before fork");

    store.reset();
    let forked = fork_namespace(&store, &source_namespace_id, &clone_namespace_id, &context)
        .await
        .expect("fork namespace");
    assert_eq!(forked.namespace_id, clone_namespace_id);
    assert_eq!(forked.head_seq, ChangeSeq(1));
    assert_eq!(forked.current_manifest_id, None);
    assert_eq!(forked.wal_tail_segments, 0);
    assert_eq!(forked.retention_floor_seq, ChangeSeq(1));
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
    assert_eq!(
        namespace_keys(&store, &clone_namespace_id).await,
        vec![wal_head(&clone_namespace_id)],
        "a fork target is its head and nothing else"
    );

    let clone_head = head_state(&store, &clone_namespace_id).await;
    assert_eq!(clone_head.content_store_id, content_store_id);
    assert_eq!(clone_head.seq, ChangeSeq(1));
    let fork_basis = clone_head.fork_basis.clone().expect("fork basis");
    assert_eq!(fork_basis.source_namespace_id, source_namespace_id);
    assert_eq!(fork_basis.fork_seq, ChangeSeq(1));
    assert!(fork_basis.source_checkpoint_id.as_str().starts_with("chk_"));

    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &fork_basis.source_checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    assert_eq!(source_record.manifest_head_seq, ChangeSeq(1));
    assert_eq!(
        source_record.manifest_object_id,
        fork_basis.source_manifest_object_id
    );
    assert_eq!(
        source_record.manifest_payload_checksum,
        fork_basis.source_manifest_checksum
    );
    assert!(
        matches!(
            &source_record.owner,
            CheckpointOwner::Fork { target_namespace_id }
                if *target_namespace_id == clone_namespace_id
        ),
        "fork record is owned by its target namespace"
    );

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
    assert_eq!(source_entry.content_ref(), clone_entry.content_ref());
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
    assert_eq!(clone_changes.changes[0].committed_seq, ChangeSeq(2));

    // The target's own first flush inherits the source's tables by
    // reference and adds only its own delta run.
    namespace_engine(&store, &clone_namespace_id, &context)
        .flush_wal()
        .await
        .expect("flush clone");
    let clone_root =
        loonfs_core::control::load_namespace_metadata_root_control(&store, &clone_namespace_id)
            .await
            .expect("clone metadata root");
    let clone_manifest_bytes = store
        .get(
            &metadata_manifest_object(&clone_namespace_id, &clone_root.state.manifest_object_id),
            None,
        )
        .await
        .expect("read clone manifest")
        .expect("clone manifest exists");
    let clone_manifest =
        decode_namespace_manifest_json(&clone_manifest_bytes).expect("decode clone manifest");
    assert!(
        clone_manifest
            .payload
            .metadata_files
            .iter()
            .any(|metadata_file| metadata_file.owner_namespace_id == source_namespace_id),
        "the target keeps referencing source-owned metadata SSTs"
    );
    assert_eq!(
        read_file_bytes(&store, &clone_namespace_id, "/docs/shared.txt")
            .await
            .expect("read clone after its own flush")
            .bytes,
        b"clone-after-fork"
    );
}

/// The fork target keeps reading after the source namespace is deleted: the
/// pinned basis and the source-owned tables it names both survive.
#[tokio::test]
async fn fork_clone_survives_source_delete() {
    let temp_dir = tempdir().expect("tempdir");
    let source = NamespaceId::parse("source").expect("valid namespace id");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    let context = mutation_context();
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    fork_namespace(&store, &source, &clone, &context)
        .await
        .expect("fork");

    namespace_engine(&store, &source, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete source");

    let clone_head = head_state(&store, &clone).await;
    assert_eq!(clone_head.seq, ChangeSeq(1));
    assert_eq!(
        read_file_bytes(&store, &clone, "/docs/shared.txt")
            .await
            .expect("clone reads forked file")
            .bytes,
        b"base"
    );
}

/// The head authorizes the foreign basis and nothing else does: a recorded
/// checksum that disagrees with the manifest is corruption, never a
/// fallback to some other basis.
#[tokio::test]
async fn a_fork_basis_checksum_mismatch_is_corruption() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let source = namespace_id("source");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    fork_namespace(&store, &source, &clone, &context)
        .await
        .expect("fork");
    read_file_bytes(&store, &clone, "/docs/shared.txt")
        .await
        .expect("the clone reads through its recorded basis");

    let mut head = head_state(&store, &clone).await;
    let fork_basis = head.fork_basis.as_mut().expect("fork basis");
    fork_basis.source_manifest_checksum =
        loonfs_api::sha256_digest(b"not-the-source-manifest-payload");
    let envelope =
        HeadStateEnvelope::from_state(ControlObjectKind::WalHead, head).expect("head envelope");
    store
        .put_overwrite(
            &wal_head(&clone),
            Bytes::from(encode_control_object(&envelope).expect("encode head")),
        )
        .await
        .expect("rewrite clone head");

    let error = read_file_bytes(&store, &clone, "/docs/shared.txt")
        .await
        .expect_err("a basis that fails its recorded checksum is corruption");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
}

/// Checkpointing a fork target that has never flushed materializes its
/// first own manifest, because a checkpoint record can only pin a manifest
/// of its own namespace.
#[tokio::test]
async fn checkpointing_an_unflushed_fork_materializes_its_first_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let source = namespace_id("source");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    seed_source_namespace_for_fork(&store, &source, &context).await;
    fork_namespace(&store, &source, &clone, &context)
        .await
        .expect("fork");
    assert!(
        store
            .head(&metadata_root(&clone))
            .await
            .expect("probe clone root")
            .is_none(),
        "a fresh fork target has published no manifest"
    );

    let checkpoint = namespace_engine(&store, &clone, &context)
        .create_checkpoint("clone-pin".to_owned(), None)
        .await
        .expect("checkpoint the unflushed fork target")
        .checkpoint;
    let record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &clone,
        &checkpoint.checkpoint_id,
    )
    .await
    .expect("read record")
    .expect("record exists");
    let manifest_key = metadata_manifest_object(&clone, &record.manifest_object_id);
    assert!(
        store
            .head(&manifest_key)
            .await
            .expect("probe pinned manifest")
            .is_some(),
        "the pinned basis is a manifest under the target's own prefix"
    );
    assert!(
        store
            .head(&metadata_root(&clone))
            .await
            .expect("probe clone root")
            .is_some(),
        "materializing the first manifest publishes the target's root"
    );
    assert_eq!(
        read_file_bytes(&store, &clone, "/docs/shared.txt")
            .await
            .expect("the target still reads its inherited file")
            .bytes,
        b"base"
    );
}

/// A fork that loses its source checkpoint after the target head lands
/// deletes the target it just created, rather than leaving a namespace
/// whose basis nothing protects.
#[tokio::test]
async fn a_fork_whose_source_pin_is_released_deletes_its_own_target() {
    let temp_dir = tempdir().expect("tempdir");
    let store = ReleasePinAfterHeadStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        NamespaceId::parse("clone").expect("valid namespace id"),
        ForkPinAfterHead::Released,
    );
    let context = mutation_context();
    let source = namespace_id("source");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    seed_source_namespace_for_fork(&store, &source, &context).await;

    let error = fork_namespace(&store, &source, &clone, &context)
        .await
        .expect_err("a released source pin fails the fork");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);
    assert_eq!(
        head_state(&store, &clone).await.state,
        loonfs_api::wire::control::NamespaceState::Deleted,
        "the fork deletes the target it published"
    );
    let retry = fork_namespace(&store, &source, &clone, &context)
        .await
        .expect_err("the deleted id is retired");
    assert_eq!(retry.code(), ErrorCode::NamespaceDeleted);
}

/// The guard is a margin, not a bare re-read. A source record that is still
/// active but has only the guard margin of lease left could be released by
/// a pass at any moment, so the fork refuses it and deletes the target it
/// just published rather than leaving a basis nothing protects.
#[tokio::test]
async fn a_fork_whose_source_lease_runs_out_deletes_its_own_target() {
    let temp_dir = tempdir().expect("tempdir");
    let context = mutation_context();
    let store = ReleasePinAfterHeadStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        NamespaceId::parse("clone").expect("valid namespace id"),
        ForkPinAfterHead::LeaseAtTheGuardMargin {
            now_ms: context.now_ms,
        },
    );
    let source = namespace_id("source");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    seed_source_namespace_for_fork(&store, &source, &context).await;

    let error = fork_namespace(&store, &source, &clone, &context)
        .await
        .expect_err("a lease inside the guard margin fails the fork");
    assert_eq!(error.code(), ErrorCode::CheckpointUnavailable);
    assert_eq!(
        head_state(&store, &clone).await.state,
        loonfs_api::wire::control::NamespaceState::Deleted,
        "the fork deletes the target it published"
    );
}

/// The other side of the guard: a fork with its whole lease ahead of it
/// stands, even with a garbage-collection pass running against the source
/// at the same time.
#[tokio::test]
async fn a_fork_with_lease_to_spare_survives_a_concurrent_gc_pass() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let context = mutation_context();
    let source = namespace_id("source");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");
    seed_source_namespace_for_fork(store.as_ref(), &source, &context).await;

    let gc_config = loonfs_core::GcConfig::default();
    let forking = fork_namespace(store.as_ref(), &source, &clone, &context);
    let collecting = loonfs_core::gc_namespace(store.as_ref(), &source, &gc_config, &context);
    let (forked, collected) = tokio::join!(forking, collecting);
    forked.expect("a fresh lease survives a concurrent pass");
    collected.expect("the pass finishes");
    assert_eq!(
        head_state(store.as_ref(), &clone).await.state,
        loonfs_api::wire::control::NamespaceState::Active
    );
    assert_eq!(
        read_file_bytes(store.as_ref(), &clone, "/docs/shared.txt")
            .await
            .expect("the target reads its inherited file")
            .bytes,
        b"base"
    );
}

/// A source manifest that no longer validates blocks the fork before any
/// target object exists.
#[tokio::test]
async fn fork_namespace_rejects_corrupt_source_manifest_descriptors() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let source_namespace_id = namespace_id("demo");
    let clone_namespace_id = NamespaceId::parse("clone").expect("valid namespace id");

    seed_source_namespace_for_fork(&store, &source_namespace_id, &context).await;
    let checkpoint = namespace_engine(&store, &source_namespace_id, &context)
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("create source checkpoint")
        .checkpoint;

    let source_record = loonfs_core::control::load_namespace_checkpoint_record_control(
        &store,
        &source_namespace_id,
        &checkpoint.checkpoint_id,
    )
    .await
    .expect("read source checkpoint record")
    .expect("source checkpoint record exists");
    let manifest_key =
        metadata_manifest_object(&source_namespace_id, &source_record.manifest_object_id);
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
    let manifest = NamespaceManifestEnvelope::from_payload(manifest.payload)
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
        namespace_keys(&store, &clone_namespace_id).await.is_empty(),
        "a failed fork leaves the target absent"
    );
}

/// A source checkpoint that cannot be written aborts the fork before the
/// target exists at all.
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
        namespace_keys(&store, &clone_namespace_id).await.is_empty(),
        "the target must not be installed before the source basis is pinned"
    );
    assert!(
        store
            .head(&wal_head(&source_namespace_id))
            .await
            .expect("head source head")
            .is_some(),
        "the source is untouched"
    );
}

/// A create that loses its conditional write to an unrelated namespace
/// reports the id as taken, and leaves that namespace alone.
#[tokio::test]
async fn a_create_losing_to_a_foreign_head_reports_the_id_as_taken() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    let inner = LocalFsStore::new(temp_dir.path()).expect("store");
    // Another writer's complete head for the same id, already durable.
    let foreign = HeadState::initial(
        namespace_id.clone(),
        loonfs_api::ContentStoreId::generate(),
        1_000,
    );
    let foreign_bytes = encode_control_object(
        &HeadStateEnvelope::from_state(ControlObjectKind::WalHead, foreign)
            .expect("foreign head envelope"),
    )
    .expect("encode foreign head");
    let store = InjectCreateFailureStore::new(
        inner,
        KeyMatcher::Exact(wal_head(&namespace_id)),
        InjectedCreateFailure::PreconditionFailed {
            write_attempted_object: false,
            additional_writes: vec![(wal_head(&namespace_id), foreign_bytes.clone())],
        },
    );

    let error = bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect_err("another namespace owns the id");
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
    assert_eq!(
        store
            .get(&wal_head(&namespace_id), None)
            .await
            .expect("read head")
            .expect("head exists")
            .to_vec(),
        foreign_bytes,
        "the losing create must not touch the winner's head"
    );
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
    submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("before-delete").expect("valid commit id"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/keep.txt").expect("path"),
            content_ref: content.content_ref().clone(),
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
    let commit = submit_operation(
        &store,
        &namespace_id,
        CommitId::parse("after-delete").expect("valid commit id"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/late.txt").expect("path"),
            content_ref: content.content_ref().clone(),
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
    // Even `allow_existing` cannot revive a retired id.
    let adopt = bootstrap_namespace(&store, &namespace_id, &context, true).await;
    assert!(matches!(
        adopt,
        Err(loonfs_core::BootstrapNamespaceError::NamespaceDeleted { .. })
    ));
    let fork_target = NamespaceId::parse("fork-of-deleted").expect("valid namespace id");
    let fork = fork_namespace(&store, &namespace_id, &fork_target, &context).await;
    assert_eq!(
        fork.expect_err("fork of deleted source").code(),
        ErrorCode::NamespaceDeleted
    );
}

/// A namespace with no root of its own is still a legal garbage-collection
/// subject: it roots its WAL from birth and nothing else, and a later
/// deleted namespace reclaims down to its tombstone.
#[tokio::test]
async fn gc_handles_a_namespace_with_no_root_and_then_its_tombstone() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    let namespace_id = namespace_id("demo");
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/keep.txt",
        b"keep me",
        &context,
        Some("keep"),
    )
    .await
    .expect("write");

    let mut aged = context.clone();
    aged.now_ms = u64::MAX / 2;
    let report = loonfs_core::gc_namespace(
        &store,
        &namespace_id,
        &loonfs_core::GcConfig::default(),
        &aged,
    )
    .await
    .expect("gc a namespace with no root");
    assert_eq!(report.deleted_wal_segments, 0, "{report:?}");
    assert!(!report.degraded_retention);
    assert_eq!(
        read_file_bytes(&store, &namespace_id, "/keep.txt")
            .await
            .expect("still readable after gc")
            .bytes,
        b"keep me"
    );

    namespace_engine(&store, &namespace_id, &context)
        .delete_namespace(loonfs_core::DeleteNamespaceOptions::default())
        .await
        .expect("delete");
    let report = loonfs_core::gc_namespace(
        &store,
        &namespace_id,
        &loonfs_core::GcConfig::default(),
        &aged,
    )
    .await
    .expect("gc the tombstone");
    assert!(report.deleted_wal_segments >= 1, "{report:?}");
    let surviving = namespace_keys(&store, &namespace_id).await;
    assert_eq!(
        surviving,
        vec![wal_head(&namespace_id)],
        "reclamation leaves the tombstone head and nothing else"
    );
    assert!(store
        .head(&wal_floor(&namespace_id))
        .await
        .expect("probe floor")
        .is_none());
}

/// What happens to the fork's source pin the moment the target head lands.
#[derive(Debug, Clone, Copy)]
enum ForkPinAfterHead {
    /// A garbage-collection pass released the pin while a stalled forker
    /// slept.
    Released,
    /// The forker stalled until its lease was all but gone: the record is
    /// still active, but with exactly the guard margin left, which is not
    /// margin enough to trust.
    LeaseAtTheGuardMargin { now_ms: u64 },
}

/// A store that rewrites the fork's source checkpoint the moment the target
/// head lands, standing in for the window between the two writes.
#[derive(Debug)]
struct ReleasePinAfterHeadStore {
    inner: LocalFsStore,
    target_head_key: String,
    after_head: ForkPinAfterHead,
}

impl ReleasePinAfterHeadStore {
    fn new(
        inner: LocalFsStore,
        target_namespace_id: NamespaceId,
        after_head: ForkPinAfterHead,
    ) -> Self {
        Self {
            inner,
            target_head_key: wal_head(&target_namespace_id),
            after_head,
        }
    }

    async fn rewrite_every_fork_pin(&self) {
        for key in self
            .inner
            .list_prefix("namespaces/")
            .await
            .expect("list namespaces")
            .into_iter()
            .filter(|key| key.contains("/checkpoints/"))
        {
            let Some(bytes) = self.inner.get(&key, None).await.expect("read record") else {
                continue;
            };
            let Ok(envelope) = decode_control_object::<
                loonfs_api::wire::control::CheckpointRecordState,
            >(&bytes, ControlObjectKind::CheckpointRecord) else {
                continue;
            };
            let mut record = envelope.state;
            if !matches!(record.owner, CheckpointOwner::Fork { .. }) {
                continue;
            }
            match self.after_head {
                ForkPinAfterHead::Released => {
                    record.state = CheckpointRecordLifecycle::Released {
                        released_at_ms: 2_000,
                    };
                }
                ForkPinAfterHead::LeaseAtTheGuardMargin { now_ms } => {
                    record.expires_at_ms = Some(now_ms + loonfs_core::limits::FORK_GUARD_MARGIN_MS);
                }
            }
            let rewritten = loonfs_api::wire::control::CheckpointRecordEnvelope::from_state(
                ControlObjectKind::CheckpointRecord,
                record,
            )
            .expect("rewritten record envelope");
            self.inner
                .put_overwrite(
                    &key,
                    Bytes::from(encode_control_object(&rewritten).expect("encode record")),
                )
                .await
                .expect("rewrite record");
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReleasePinAfterHeadStore {
    async fn head(
        &self,
        key: &str,
    ) -> Result<Option<loonfs_objectstore::ObjectMetadata>, loonfs_objectstore::ObjectStoreError>
    {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<loonfs_objectstore::ByteRange>,
    ) -> Result<Option<Bytes>, loonfs_objectstore::ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> Result<Option<loonfs_objectstore::ObjectBody>, loonfs_objectstore::ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: loonfs_objectstore::PutMode,
    ) -> Result<loonfs_objectstore::ObjectMetadata, loonfs_objectstore::ObjectStoreError> {
        let metadata = self.inner.put(key, bytes, mode).await?;
        if key == self.target_head_key {
            self.rewrite_every_fork_pin().await;
        }
        Ok(metadata)
    }

    async fn delete(&self, key: &str) -> Result<(), loonfs_objectstore::ObjectStoreError> {
        self.inner.delete(key).await
    }

    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, loonfs_objectstore::ObjectStoreError> {
        self.inner.list_prefix(prefix).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> futures::stream::BoxStream<'static, Result<String, loonfs_objectstore::ObjectStoreError>>
    {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}
