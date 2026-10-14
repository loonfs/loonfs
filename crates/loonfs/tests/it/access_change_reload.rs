//! Store requests of a subject's first read after a commit, once a manifest
//! the publisher never heard about has been published: the publisher keeps
//! planning on its older basis, and its commits must not seed that basis
//! over the newer manifest a read already loaded.

use loonfs::{
    AccessState, AttributeChanges, CreateNamespaceOptions, LoonFs, Maintenance,
    MaintenanceCancellation, MetadataCompactionOutcome, Namespace, ReadOnly, SharedObjectStore,
    Writable,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{attribute_key, attribute_text, namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, MemoryStore, RecordedOperation, RecordingStore};
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
    recording: Arc<RecordingStore<MemoryStore>>,
    namespace_id: NamespaceId,
    actor: ActorId,
    admin: Namespace<Writable>,
    user: Namespace<ReadOnly>,
    maintenance: Maintenance,
}

/// A namespace with folded runs and a subject reader on the writer's
/// runtime. The first compaction on a runtime also publishes its compactor
/// claim, which no publisher hears about, so the test's compaction leaves
/// the publisher on an older basis than the manifest a read then loads.
async fn fixture(name: &str) -> Fixture {
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let clock = Arc::new(ManualClock::new(1_000_000));
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id(name))
        .min_publish_interval_ms(0)
        .manifest_revalidation_interval_ms(0)
        .monotonic_timer(clock)
        .build()
        .await
        .expect("writer");
    let namespace_id = namespace_id(name);
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
    }
    // Maintenance folds drop the publisher's position; this commit reloads it.
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

    /// Asserts the requests since the last call probe the manifest and the
    /// WAL tip and reload nothing.
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
            operations.len() <= 3 && reloads == 0,
            "{read}: {} requests, {reloads} hint or manifest GETs: {operations:?}",
            operations.len()
        );
    }
}

const COMMIT_KINDS: [&str; 4] = ["update_attributes", "put_file", "revoke", "regrant"];

/// The publisher misses the compactor claim, so it keeps planning on a
/// manifest older than the one the first read reloads onto. Its later
/// commits must not seed that older manifest over the read's.
#[tokio::test]
async fn commits_on_an_older_basis_keep_the_reads_newer_manifest() {
    let fixture = fixture("anchor-seed-guard").await;
    fixture.compact().await;
    fixture.commit("put_file", 0).await;
    fixture.user.stat("/m/doc").await.expect("reload");
    for (round, kind) in COMMIT_KINDS.into_iter().enumerate() {
        let user_granted = fixture.commit(kind, round + 1).await;
        fixture.recording.reset();
        let read = fixture.user.stat("/m/doc").await;
        assert_eq!(read.is_ok(), user_granted, "{kind}: access answer");
        fixture.assert_no_reload(&format!("user after {kind}"));
    }
}
