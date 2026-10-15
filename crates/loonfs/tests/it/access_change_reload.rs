//! Storage requests and access checks for reads and writes after this
//! runtime or another one publishes a newer manifest.

use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    AccessState, AttributeChanges, CreateNamespaceOptions, LoonFs, Maintenance,
    MaintenanceCancellation, MetadataCompactionOutcome, Namespace, ReadOnly, SharedObjectStore,
    SnapshotPolicy, Writable,
};
use loonfs_core::limits::WAL_PUBLISH_BUDGET_MS;
use loonfs_objectstore::keys::{
    hint, metadata_manifest_object, metadata_manifest_prefix, wal_prefix,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::{
    AccessGrants, AccessRight, AccessRights, ActorId, ChecksumAlgorithm, NamespaceAccess,
    NamespaceId, PrincipalId, PrincipalScope, PrincipalSet, Subject, SubjectId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn subject(principal: &str) -> Subject {
    Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(principal).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
    }
}

fn grants(principals: &[&str], right: AccessRight) -> AccessGrants {
    AccessGrants::new(
        principals
            .iter()
            .map(|principal| {
                (
                    PrincipalId::parse(*principal).expect("principal"),
                    AccessRights::from_iter([right]),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .expect("grants")
}

/// The boundary directory `/m`, readable by staff and, when granted, by the
/// user.
fn matter(user_granted: bool) -> AccessState {
    let principals: &[&str] = if user_granted {
        &["prn_staff", "prn_user"]
    } else {
        &["prn_staff"]
    };
    AccessState {
        boundary: true,
        grants: grants(principals, AccessRight::Read),
    }
}

struct Fixture {
    /// Records every request, and can park the namespace's WAL puts.
    recording: Arc<RecordingStore<BlockingStore<MemoryStore>>>,
    recorder: Arc<DefaultMetricsRecorder>,
    clock: Arc<ManualClock>,
    namespace_id: NamespaceId,
    actor: ActorId,
    admin: Namespace<Writable>,
    user: Namespace<ReadOnly>,
    maintenance: Maintenance,
}

/// Creates a namespace with folded WAL data and a reader on the writer's runtime.
///
/// The first compaction also publishes a compactor claim. When
/// `claim_compactor` is true, the fixture runs that first compaction during
/// setup, so a test's compaction publishes one manifest. Either way, the
/// publisher's cached position moves to each manifest this runtime
/// publishes.
async fn fixture(
    name: &str,
    manifest_revalidation_interval_ms: u64,
    claim_compactor: bool,
) -> Fixture {
    let namespace_id = namespace_id(name);
    let recording = Arc::new(RecordingStore::new(
        BlockingStore::new(
            MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
            KeyPredicate::prefix(wal_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
        ),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let clock = Arc::new(ManualClock::new(1_000_000));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id(name))
        .min_publish_interval_ms(0)
        .manifest_revalidation_interval_ms(manifest_revalidation_interval_ms)
        .monotonic_timer(clock.clone())
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    let options = CreateNamespaceOptions {
        access: NamespaceAccess::Acl {
            principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
            root_grants: grants(&["prn_admin"], AccessRight::Admin),
        },
        ..Default::default()
    };
    writer
        .create_namespace_with_options(&namespace_id, &actor, &options)
        .await
        .expect("namespace");
    let fixture = Fixture {
        recording,
        recorder,
        clock,
        admin: writer
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .with_subject(subject("prn_admin")),
        user: writer
            .read_only()
            .with_subject(subject("prn_user"))
            .namespace(&namespace_id),
        maintenance: writer.maintenance(writer_id(name)),
        namespace_id,
        actor,
    };
    let (admin, actor) = (&fixture.admin, &fixture.actor);
    admin.create_directory("/m", actor).await.expect("matter");
    admin
        .update_access("/m", actor, matter(true))
        .await
        .expect("grant");
    admin.put_file("/m/doc", b"doc", actor).await.expect("doc");
    for index in 0..9 {
        admin
            .put_file(&format!("/fill-{index}"), b"fill", actor)
            .await
            .expect("fill");
        fixture
            .maintenance
            .fold_wal(&fixture.namespace_id)
            .await
            .expect("fold");
        // The folds after this compaction write the runs that a test's
        // compaction merges.
        if claim_compactor && index == 4 {
            fixture.compact().await;
        }
    }
    // Each fold moves the publisher's cached position to the fold's
    // manifest, so the next commit starts from that position.
    admin
        .put_file("/settle", b"x", actor)
        .await
        .expect("settle");
    fixture.user.stat("/m/doc").await.expect("warm read");
    fixture
}

impl Fixture {
    async fn compact(&self) {
        let outcome = self
            .maintenance
            .compact_metadata(&self.namespace_id, &MaintenanceCancellation::new())
            .await
            .expect("compact");
        assert_eq!(
            outcome.compaction,
            MetadataCompactionOutcome::BoundedMergePublished
        );
    }

    /// Commits one kind of change, and returns whether the user may read.
    async fn commit(&self, kind: &str, round: usize) -> bool {
        let (admin, actor) = (&self.admin, &self.actor);
        match kind {
            "update_attributes" => {
                let changes = AttributeChanges {
                    set: BTreeMap::from([(
                        attribute_key("owner"),
                        attribute_text(&format!("o{round}")),
                    )]),
                    ..Default::default()
                };
                admin
                    .update_attributes("/m/doc", actor, changes)
                    .await
                    .map(drop)
            }
            "put_file" => admin
                .put_file(&format!("/put-{round}"), b"put", actor)
                .await
                .map(drop),
            "revoke" => admin
                .update_access("/m", actor, matter(false))
                .await
                .map(drop),
            _ => admin
                .update_access("/m", actor, matter(true))
                .await
                .map(drop),
        }
        .expect(kind);
        kind != "revoke"
    }

    /// Returns how many publishes read the WAL tail from the store.
    fn tail_replays(&self) -> u64 {
        let snapshot = self.recorder.snapshot();
        let entry = snapshot
            .by_name("loonfs.publisher.tail_replays")
            .next()
            .expect("the publisher registers its replay counter");
        match entry.value {
            MetricValue::Counter(value) => value,
            ref other => panic!("replays are reported as a counter, found {other:?}"),
        }
    }

    /// Commits a file, and checks that the publish replays no WAL tail and
    /// reloads no head: it reads no hint or WAL object, and at most one
    /// manifest (the one it plans against, the first time it plans against
    /// it).
    ///
    /// The clock first moves past the publish budget, so the publish checks
    /// its manifest for a successor. A publisher left on a manifest that
    /// this runtime replaced finds the successor and replays its tail.
    async fn assert_write_reloads_nothing(&self, round: usize, after: &str) {
        self.clock.advance_ms(WAL_PUBLISH_BUDGET_MS + 1);
        let replays = self.tail_replays();
        self.recording.reset();
        self.commit("put_file", round).await;
        let operations = self.recording.take();
        let namespace_id = &self.namespace_id;
        let gets = || {
            operations.iter().filter(|operation| {
                matches!(
                    operation,
                    RecordedOperation::Get { .. } | RecordedOperation::GetWithMetadata { .. }
                )
            })
        };
        let reloads = gets()
            .filter(|operation| {
                operation.key() == hint(namespace_id)
                    || operation.key().starts_with(&wal_prefix(namespace_id))
            })
            .count();
        let manifests = gets()
            .filter(|operation| {
                operation
                    .key()
                    .starts_with(&metadata_manifest_prefix(namespace_id))
            })
            .count();
        assert!(
            reloads == 0 && manifests <= 1 && self.tail_replays() == replays,
            "write after {after}: {reloads} hint or WAL GETs, {manifests} manifest GETs, \
             {} replays: {operations:?}",
            self.tail_replays() - replays
        );
    }

    /// Checks that the read uses at most four storage requests without
    /// reloading the hint or any manifests.
    fn assert_no_reload(&self, read: &str) {
        let operations = self.recording.take();
        let reloads = operations
            .iter()
            .filter(|operation| {
                let key = operation.key();
                key.ends_with("hint.json")
                    || (key.contains("/manifests/")
                        && !matches!(operation, RecordedOperation::Head { .. }))
            })
            .count();
        assert!(
            operations.len() <= 4 && reloads == 0,
            "{read}: {} requests, {reloads} hint or manifest GETs: {operations:?}",
            operations.len()
        );
    }
}

const COMMIT_KINDS: [&str; 4] = ["update_attributes", "put_file", "revoke", "regrant"];

/// Checks that later commits keep the newer manifest in the read cache.
#[tokio::test]
async fn commits_on_an_older_basis_keep_the_reads_newer_manifest() {
    let fixture = fixture("anchor-seed-guard", 0, false).await;
    fixture.compact().await;
    fixture.commit("put_file", 0).await;
    fixture.user.stat("/m/doc").await.expect("reload");
    for (round, kind) in COMMIT_KINDS.into_iter().enumerate() {
        let user_granted = fixture.commit(kind, round + 1).await;
        fixture.recording.reset();
        let read = fixture.user.stat("/m/doc").await;
        assert_eq!(read.is_ok(), user_granted, "{kind}: read access to /m/doc");
        fixture.assert_no_reload(&format!("user after {kind}"));
    }
}

/// Checks that reads after a compaction on the same runtime don't reload
/// metadata.
///
/// The revalidation interval is zero, so every read checks for a newer
/// manifest, as reads more than a second apart do with the default interval.
#[tokio::test]
async fn first_reads_after_commits_reload_nothing_after_a_compaction() {
    let fixture = fixture("own-compaction-every-read", 0, true).await;
    fixture.compact().await;
    for round in 0..2 {
        for kind in COMMIT_KINDS {
            let user_granted = fixture.commit(kind, round).await;
            fixture.recording.reset();
            let read = fixture.user.stat("/m/doc").await;
            assert_eq!(read.is_ok(), user_granted, "{kind}: read access to /m/doc");
            fixture.assert_no_reload(&format!("round {round}, user after {kind}"));
        }
    }
}

/// Checks that reads after a compaction on the same runtime don't reload
/// metadata with the default one-second revalidation interval.
///
/// A read checks for a newer manifest when the last check is more than a
/// second old, so the test moves the clock past that interval before some
/// commits.
#[tokio::test]
async fn reads_past_the_revalidation_interval_reload_nothing_after_a_compaction() {
    let fixture = fixture("own-compaction-interval", 1_000, true).await;
    fixture.compact().await;
    for (round, advance_ms) in [0, 1_100, 0, 1_100, 30_000].into_iter().enumerate() {
        fixture.clock.advance_ms(advance_ms);
        let kind = if round % 2 == 0 { "revoke" } else { "regrant" };
        let user_granted = fixture.commit(kind, round).await;
        fixture.recording.reset();
        let read = fixture.user.stat("/m/doc").await;
        assert_eq!(read.is_ok(), user_granted, "{kind}: read access to /m/doc");
        fixture.assert_no_reload(&format!("+{advance_ms} ms, user after {kind}"));
    }
}

/// Checks that commits on a manifest that another runtime replaced keep the
/// read's newer manifest in the read cache.
///
/// The writer's runtime doesn't learn of another runtime's compaction, so its
/// publisher keeps planning against the older manifest while its reads load
/// the newer one.
#[tokio::test]
async fn commits_on_a_manifest_another_runtime_replaced_keep_the_reads_newer_manifest() {
    let fixture = fixture("anchor-seed-guard-other", 0, false).await;
    let other_store: SharedObjectStore = fixture.recording.clone();
    let other = LoonFs::builder_with_store(other_store)
        .writer_id(writer_id("anchor-seed-guard-maintenance"))
        .monotonic_timer(fixture.clock.clone())
        .build()
        .await
        .expect("maintenance runtime");
    let outcome = other
        .maintenance(writer_id("anchor-seed-guard-maintenance"))
        .compact_metadata(&fixture.namespace_id, &MaintenanceCancellation::new())
        .await
        .expect("compact");
    assert_eq!(
        outcome.compaction,
        MetadataCompactionOutcome::BoundedMergePublished
    );
    fixture.user.stat("/m/doc").await.expect("reload");
    for (round, kind) in COMMIT_KINDS.into_iter().enumerate() {
        let user_granted = fixture.commit(kind, round).await;
        fixture.recording.reset();
        let read = fixture.user.stat("/m/doc").await;
        assert_eq!(read.is_ok(), user_granted, "{kind}: read access to /m/doc");
        fixture.assert_no_reload(&format!("user after {kind}"));
    }
}

/// The maintenance calls that a test runs on the writer's runtime, in an
/// order where each delete finds the pin an earlier call created.
const MAINTENANCE_CALLS: [&str; 7] = [
    "fold_wal",
    "create_checkpoint",
    "delete_checkpoint",
    "advance_retention_floor",
    "gc",
    "create_snapshot",
    "delete_snapshot",
];

/// Checks that a write after a maintenance call on the same runtime replays
/// no WAL tail and reloads nothing, and that the read after the write reloads
/// nothing either.
///
/// Each call follows a commit, so the folds, checkpoints, and snapshots have
/// a WAL tail to fold.
#[tokio::test]
async fn writes_after_maintenance_on_the_same_runtime_reload_nothing() {
    let fixture = fixture("own-maintenance", 0, false).await;
    let (maintenance, namespace_id) = (&fixture.maintenance, &fixture.namespace_id);
    let (mut checkpoint, mut snapshot) = (None, None);
    for (round, call) in MAINTENANCE_CALLS.into_iter().enumerate() {
        fixture.commit("put_file", 2 * round).await;
        match call {
            "fold_wal" => drop(maintenance.fold_wal(namespace_id).await.expect(call)),
            "create_checkpoint" => {
                let created = maintenance.create_checkpoint(namespace_id, "keep").await;
                checkpoint = Some(created.expect(call).checkpoint_id);
            }
            "delete_checkpoint" => {
                let checkpoint_id = checkpoint.take().expect("a checkpoint to delete");
                let deleted = maintenance.delete_checkpoint(namespace_id, &checkpoint_id);
                drop(deleted.await.expect(call));
            }
            "advance_retention_floor" => {
                let advanced = maintenance.advance_retention_floor(namespace_id).await;
                assert!(advanced.expect(call).retention_floor_seq.0 > 0, "{call}");
            }
            "gc" => drop(maintenance.gc(namespace_id).await.expect(call)),
            "create_snapshot" => {
                let policy = SnapshotPolicy::default();
                let created = fixture.admin.create_snapshot("keep", u64::MAX / 2, &policy);
                snapshot = Some(created.await.expect(call).snapshot_id);
            }
            _ => {
                let snapshot_id = snapshot.take().expect("a snapshot to delete");
                let deleted = fixture.admin.delete_snapshot(&snapshot_id);
                drop(deleted.await.expect(call));
            }
        }
        fixture
            .assert_write_reloads_nothing(2 * round + 1, call)
            .await;
        fixture.recording.reset();
        fixture.user.stat("/m/doc").await.expect("read");
        fixture.assert_no_reload(&format!("user after a write after {call}"));
    }
}

/// Checks that a publish in flight while a checkpoint folds the WAL tail
/// moves to the checkpoint's manifest before it seeds the read cache.
///
/// The publish's WAL put waits until the checkpoint has folded the commits
/// before it, so the checkpoint's manifest misses the publish's own commit.
/// The next read finds no newer manifest, and the next write replays no WAL
/// tail.
#[tokio::test]
async fn a_publish_in_flight_while_a_checkpoint_publishes_ends_on_its_manifest() {
    let fixture = fixture("checkpoint-during-publish", 0, false).await;
    fixture.commit("put_file", 0).await;
    let gate = fixture.recording.inner();
    gate.block_next();
    let (_, checkpoint) = futures::join!(fixture.commit("put_file", 1), async {
        gate.wait_until_blocked().await;
        let checkpoint = fixture
            .maintenance
            .create_checkpoint(&fixture.namespace_id, "during")
            .await;
        gate.release();
        checkpoint
    });
    let checkpoint = checkpoint.expect("checkpoint");
    fixture.recording.reset();
    fixture.user.stat("/m/doc").await.expect("read");
    // A read cache seeded with the replaced manifest would find the
    // checkpoint's manifest as its successor and reload. A read cache seeded
    // with the checkpoint's manifest checks the manifest after it, and loads
    // the checkpoint's manifest once.
    let operations = fixture.recording.take();
    let manifest = |manifest_no| metadata_manifest_object(&fixture.namespace_id, &manifest_no);
    let successor = manifest(checkpoint.manifest_no.successor().expect("next manifest"));
    let checks_successor = |operation: &RecordedOperation| matches!(operation, RecordedOperation::Head { key } if *key == successor);
    let unexpected = operations
        .iter()
        .filter(|operation| {
            let key = operation.key();
            (key == hint(&fixture.namespace_id)
                || key.starts_with(&metadata_manifest_prefix(&fixture.namespace_id)))
                && !checks_successor(operation)
                && key != manifest(checkpoint.manifest_no)
        })
        .count();
    assert!(
        unexpected == 0 && operations.iter().any(checks_successor),
        "the read starts from the checkpoint's manifest: {operations:?}"
    );
    fixture
        .assert_write_reloads_nothing(2, "a publish during a checkpoint")
        .await;
    for path in ["/put-0", "/put-1", "/put-2"] {
        fixture.admin.stat(path).await.expect(path);
    }
}
