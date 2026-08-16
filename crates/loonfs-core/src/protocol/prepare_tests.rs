//! Scenario corpus for the single-pass commit preparation, converted from
//! the differential harness that proved the pass equal to the retired
//! two-pass pipeline.
//!
//! Each scenario runs its candidates through the per-candidate preparation
//! against one loaded publish view, with accepted commits folded into the
//! session so later candidates observe earlier ones. Accepted candidates
//! must encode to durable segment records that survive the codec round
//! trip, and rejected candidates must carry the exact wire error: code,
//! rendered message (which carries any `operation N:` attribution), and
//! structured details.
//!
//! The owner-decided precedence — validation first, uniformly, even for a
//! single-operation request that is also content-uncovered — is pinned by
//! the test at the bottom and at the API surface in
//! `tests/it/commit_validation.rs`.

#![allow(clippy::panic)]

use super::candidates::validate_commit_content_references;
use super::publish_view::{load_publish_metadata_view, PublishMetadataView, PublishTailOptions};
use crate::commit::{
    materialize_commit, wal_payload_from_materialized_commit, CommitPlan, MaterializedCommit,
};
use crate::commit_engine::{publish_namespace_commits_batch, CommitCandidate, ContentPreparation};
use crate::context::MutationContext;
use crate::error::{CoreError, ErrorCode, Result};
use crate::namespace::bootstrap::bootstrap_namespace;
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::write::{CommitRequest, FilesystemOperation, PublishPlanningSession};
use crate::storage::content::{store_bytes_as_content, StoredContent};
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use loonfs_api::wire::wal::{
    decode_wal_segment_envelope_zstd, encode_wal_segment_envelope_zstd, WalCommitPayload,
    WalSegmentEnvelope, WalSegmentPayload,
};
use loonfs_api::{
    AbsolutePath, AttributeKey, AttributeRevisionNo, AttributeValue, ChangeSeq, CommitId,
    ContentId, ContentRef, DeleteDirectoryBehavior, DestinationBehavior, ErrorDetails, InodeId,
    NamespaceId, RevisionNo, WalSegmentId, WriterEpoch, MAX_PUBLIC_INTEGER,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use std::collections::BTreeMap;
use tempfile::tempdir;

/// One admitted candidate, fully prepared: the plan and its materialization.
struct PreparedCandidate {
    plan: CommitPlan,
    materialized: MaterializedCommit,
}

/// The comparable surface of a rejection: everything a caller can observe.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RejectionShape {
    code: ErrorCode,
    message: String,
    details: Option<ErrorDetails>,
}

fn rejection_shape(error: &CoreError) -> RejectionShape {
    RejectionShape {
        code: error.code(),
        message: error.to_string(),
        details: error.details(),
    }
}

/// What a probe candidate must resolve to.
enum Expect {
    Accept,
    Reject {
        code: ErrorCode,
        message_contains: &'static str,
        operation_index: Option<u32>,
    },
}

/// Request-limit and content-preparation admission, in
/// `candidates::validate_new_primary` order.
fn admit_new_primary(candidate: &CommitCandidate) -> Result<()> {
    candidate.validate_request_limits()?;
    match candidate.content_preparation() {
        ContentPreparation::Ready(_) => Ok(()),
        ContentPreparation::Rejected(error) => Err(error.clone().into()),
    }
}

/// The per-candidate preparation in `batch.rs` order: merged
/// plan-and-validate, then coverage (validation first, uniformly — the
/// owner-decided precedence), then accept.
async fn prepare_candidate(
    session: &mut PublishPlanningSession,
    view: &PublishMetadataView<'_, LocalFsStore>,
    namespace_id: &NamespaceId,
    candidate: &CommitCandidate,
    committed_at_ms: u64,
) -> Result<PreparedCandidate> {
    admit_new_primary(candidate)?;
    let semantic_identity = candidate.semantic_identity(namespace_id)?;
    let mut allocation = session.begin_candidate();
    let validated = match session
        .prepare_commit(
            candidate.request(),
            semantic_identity,
            view.metadata_view(),
            committed_at_ms,
            &mut allocation,
        )
        .await
    {
        Ok(validated) => validated,
        Err(error) => {
            session.discard_candidate(allocation);
            return Err(error);
        }
    };
    if let Err(error) = validate_commit_content_references(candidate, view.content_store_id()) {
        session.discard_candidate(allocation);
        return Err(error);
    }
    let resulting_next_inode_id = session.commit_candidate(allocation)?;
    Ok(accept(
        session,
        validated.finish(resulting_next_inode_id),
        committed_at_ms,
    ))
}

fn accept(
    session: &mut PublishPlanningSession,
    plan: CommitPlan,
    committed_at_ms: u64,
) -> PreparedCandidate {
    let materialized = materialize_commit(plan.clone(), committed_at_ms);
    let payload = wal_payload_from_materialized_commit(&materialized);
    session.apply_accepted_commit(&payload, &materialized.commit);
    PreparedCandidate { plan, materialized }
}

/// Encodes the accepted records as one durable segment with a pinned segment
/// id and requires the durable codec to round-trip them unchanged.
fn assert_accepted_records_round_trip(
    namespace_id: &NamespaceId,
    writer_epoch: WriterEpoch,
    records: &[MaterializedCommit],
) {
    let payload_records: Vec<WalCommitPayload> = records
        .iter()
        .map(wal_payload_from_materialized_commit)
        .collect();
    let start_seq = payload_records.first().expect("non-empty accepted set").seq;
    let end_seq = payload_records.last().expect("non-empty accepted set").seq;
    let payload = WalSegmentPayload {
        namespace_id: namespace_id.clone(),
        segment_id: WalSegmentId::parse(format!("{:020}-00000000000000d1", start_seq.0))
            .expect("pinned segment id"),
        writer_epoch,
        prev_visible_segment: None,
        base_head_seq: ChangeSeq(start_seq.0.checked_sub(1).expect("non-zero start seq")),
        start_seq,
        end_seq,
        records: payload_records.clone(),
    };
    let envelope = WalSegmentEnvelope::from_payload(payload).expect("wal envelope");
    let encoded = encode_wal_segment_envelope_zstd(&envelope).expect("wal segment bytes");
    let decoded = decode_wal_segment_envelope_zstd(&encoded).expect("decode wal segment");
    assert_eq!(
        decoded.payload.records, payload_records,
        "accepted records must survive the durable codec unchanged"
    );
}

struct PrepareFixture {
    _temp_dir: tempfile::TempDir,
    store: LocalFsStore,
    namespace_id: NamespaceId,
    context: MutationContext,
}

impl PrepareFixture {
    async fn new() -> Self {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let context = MutationContext {
            writer_id: "prepare-corpus".to_owned(),
            now_ms: 1,
        };
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap namespace");
        Self {
            _temp_dir: temp_dir,
            store,
            namespace_id,
            context,
        }
    }

    async fn stage(&self, bytes: &[u8]) -> StoredContent {
        store_bytes_as_content(&self.store, &self.namespace_id, bytes)
            .await
            .expect("stage content")
    }

    /// Publishes setup state through the production engine so probes start
    /// from durable rows, not an in-memory shortcut.
    async fn seed(&self, candidates: Vec<CommitCandidate>) {
        for outcome in publish_namespace_commits_batch(
            &self.store,
            &self.namespace_id,
            candidates,
            &self.context,
        )
        .await
        {
            outcome.expect("seed publish");
        }
    }

    async fn load_view(&self) -> PublishMetadataView<'_, LocalFsStore> {
        let acquired_writer = acquire_writer_epoch(&self.store, &self.namespace_id, &self.context)
            .await
            .expect("acquire writer epoch");
        let (view, _projection) = load_publish_metadata_view(
            &self.store,
            None,
            &self.namespace_id,
            acquired_writer,
            None,
            &PublishTailOptions::default(),
        )
        .await
        .expect("load publish view");
        view
    }

    /// Runs every probe through the per-candidate preparation against
    /// `view`, requiring each outcome to match its expectation, then
    /// requiring the accepted set to round-trip through the durable codec.
    /// Returns the outcomes for scenario-specific assertions.
    async fn run_against_view(
        &self,
        view: &PublishMetadataView<'_, LocalFsStore>,
        probes: Vec<(CommitCandidate, Expect)>,
    ) -> Vec<std::result::Result<CommitPlan, RejectionShape>> {
        let mut session = PublishPlanningSession::new(&view.head);
        let committed_at_ms = 4_200;
        let mut accepted: Vec<MaterializedCommit> = Vec::new();
        let mut outcomes = Vec::new();
        for (index, (candidate, expect)) in probes.into_iter().enumerate() {
            let commit_id = candidate.commit_id().clone();
            let outcome = match prepare_candidate(
                &mut session,
                view,
                &self.namespace_id,
                &candidate,
                committed_at_ms,
            )
            .await
            {
                Ok(prepared) => {
                    let plan = prepared.plan.clone();
                    accepted.push(prepared.materialized);
                    Ok(plan)
                }
                Err(error) => Err(rejection_shape(&error)),
            };
            match (&outcome, expect) {
                (Ok(_), Expect::Accept) => {}
                (Err(shape), Expect::Accept) => {
                    panic!("probe {index} (`{commit_id}`): expected acceptance, got {shape:?}")
                }
                (
                    Err(shape),
                    Expect::Reject {
                        code,
                        message_contains,
                        operation_index,
                    },
                ) => {
                    assert_eq!(
                        shape.code, code,
                        "probe {index} (`{commit_id}`): unexpected code in {shape:?}"
                    );
                    assert!(
                        shape.message.contains(message_contains),
                        "probe {index} (`{commit_id}`): message {:?} lacks {message_contains:?}",
                        shape.message
                    );
                    assert_eq!(
                        shape
                            .details
                            .as_ref()
                            .and_then(|details| details.operation_index),
                        operation_index,
                        "probe {index} (`{commit_id}`): unexpected operation index in {shape:?}"
                    );
                }
                (Ok(plan), Expect::Reject { .. }) => panic!(
                    "probe {index} (`{commit_id}`): expected rejection, got acceptance at seq {}",
                    plan.assigned_seq.0
                ),
            }
            outcomes.push(outcome);
        }
        if !accepted.is_empty() {
            assert_accepted_records_round_trip(
                &self.namespace_id,
                view.acquired_writer.writer_epoch,
                &accepted,
            );
        }
        outcomes
    }

    async fn run(
        &self,
        probes: Vec<(CommitCandidate, Expect)>,
    ) -> Vec<std::result::Result<CommitPlan, RejectionShape>> {
        let view = self.load_view().await;
        self.run_against_view(&view, probes).await
    }
}

fn commit_id(value: &str) -> CommitId {
    CommitId::parse(value).expect("valid commit id")
}

fn path(value: &str) -> AbsolutePath {
    AbsolutePath::parse(value).expect("valid path")
}

fn request(id: &str, operations: Vec<FilesystemOperation>) -> CommitRequest {
    CommitRequest {
        commit_id: commit_id(id),
        actor: loonfs_test_support::test_actor(),
        message: None,
        operations,
    }
}

fn create_dir(value: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: path(value),
        parents: false,
    }
}

fn put_file(
    value: &str,
    content_ref: ContentRef,
    behavior: DestinationBehavior,
) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: path(value),
        content_ref,
        behavior,
        expected_revision_no: None,
    }
}

fn delete_path(value: &str, behavior: DeleteDirectoryBehavior) -> FilesystemOperation {
    FilesystemOperation::DeletePath {
        path: path(value),
        behavior,
        expected_inode_id: None,
    }
}

fn update_attributes(value: &str, key: &str, attribute: &str) -> FilesystemOperation {
    FilesystemOperation::UpdateAttributes {
        path: path(value),
        set: BTreeMap::from([(
            AttributeKey::parse(key).expect("valid attribute key"),
            AttributeValue::parse(attribute).expect("valid attribute value"),
        )]),
        remove: Vec::new(),
        expected_inode_id: None,
        expected_attributes_revision_no: None,
    }
}

/// A candidate whose put content has a valid in-memory admission proof.
fn covered(
    id: &str,
    operations: Vec<FilesystemOperation>,
    staged: &[&StoredContent],
) -> CommitCandidate {
    CommitCandidate::prepared(
        request(id, operations),
        staged
            .iter()
            .map(|content| {
                PreparedContent::from_admission(ContentAdmission::for_durable_content_write(
                    content.content_store_id().clone(),
                    content.content_ref().clone(),
                ))
            })
            .collect(),
    )
}

fn uncovered(id: &str, operations: Vec<FilesystemOperation>) -> CommitCandidate {
    CommitCandidate::new(request(id, operations))
}

fn uncovered_content_ref(seed: &str) -> ContentRef {
    ContentRef::blob_v1(ContentId::generate(), seed.as_bytes())
}

/// Every semantic operation accepted across a session, each candidate
/// observing the ones before it: create directory, create file, replace,
/// restore, move, copy, attribute update, file delete, subtree delete, and
/// undelete.
#[tokio::test]
async fn every_semantic_operation_prepares_and_observes_earlier_candidates() {
    let fixture = PrepareFixture::new().await;
    let seed_content = fixture.stage(b"seed").await;
    // Seed seq 1: /undel (inode 2) with gone.txt (inode 3); seq 2: delete
    // the file; seq 3: /docs (inode 4) with seed.txt (inode 5).
    fixture
        .seed(vec![
            covered(
                "seed-undel",
                vec![put_file(
                    "/undel/gone.txt",
                    seed_content.content_ref().clone(),
                    DestinationBehavior::NoReplace,
                )],
                &[&seed_content],
            ),
            uncovered(
                "seed-delete",
                vec![delete_path(
                    "/undel/gone.txt",
                    DeleteDirectoryBehavior::NonRecursive,
                )],
            ),
            covered(
                "seed-docs",
                vec![put_file(
                    "/docs/seed.txt",
                    seed_content.content_ref().clone(),
                    DestinationBehavior::NoReplace,
                )],
                &[&seed_content],
            ),
        ])
        .await;

    let new_content = fixture.stage(b"new").await;
    let replacement = fixture.stage(b"replacement").await;
    let outcomes = fixture
        .run(vec![
            (
                uncovered("probe-mkdir", vec![create_dir("/a")]),
                Expect::Accept,
            ),
            (
                covered(
                    "probe-create-under-new-dir",
                    vec![put_file(
                        "/a/new.txt",
                        new_content.content_ref().clone(),
                        DestinationBehavior::NoReplace,
                    )],
                    &[&new_content],
                ),
                Expect::Accept,
            ),
            (
                covered(
                    "probe-replace",
                    vec![put_file(
                        "/docs/seed.txt",
                        replacement.content_ref().clone(),
                        DestinationBehavior::Replace,
                    )],
                    &[&replacement],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-restore",
                    vec![FilesystemOperation::RestoreRevision {
                        path: path("/docs/seed.txt"),
                        source_revision_no: RevisionNo(1),
                    }],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-move",
                    vec![FilesystemOperation::MovePath {
                        from_path: path("/docs/seed.txt"),
                        to_path: path("/docs/renamed.txt"),
                        behavior: DestinationBehavior::NoReplace,
                    }],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-copy",
                    vec![FilesystemOperation::CopyPath {
                        from_path: path("/docs/renamed.txt"),
                        to_path: path("/docs/copy.txt"),
                        behavior: DestinationBehavior::NoReplace,
                    }],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-attributes",
                    vec![update_attributes("/docs/renamed.txt", "owner", "ada")],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-delete-file",
                    vec![delete_path(
                        "/docs/copy.txt",
                        DeleteDirectoryBehavior::NonRecursive,
                    )],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-delete-subtree",
                    vec![delete_path("/a", DeleteDirectoryBehavior::Recursive)],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-undelete",
                    vec![FilesystemOperation::Undelete {
                        inode_id: InodeId(3),
                        deletion_seq: ChangeSeq(2),
                        path: Some(path("/undel/gone.txt")),
                    }],
                ),
                Expect::Accept,
            ),
        ])
        .await;

    // The probes above consume the whole loaded head: ten candidates, ten
    // consecutive sequences.
    let last = outcomes
        .last()
        .expect("ten outcomes")
        .as_ref()
        .expect("undelete accepted");
    assert_eq!(last.assigned_seq, ChangeSeq(13));
}

/// Multi-operation compositions: parent creation then a write into it,
/// create then rename, delete then a recreate that must observe the
/// deletion, and repeated destination names.
#[tokio::test]
async fn compositions_in_one_commit_observe_their_own_operations() {
    let fixture = PrepareFixture::new().await;
    let seed_content = fixture.stage(b"seed").await;
    fixture
        .seed(vec![covered(
            "seed-docs",
            vec![put_file(
                "/docs/seed.txt",
                seed_content.content_ref().clone(),
                DestinationBehavior::NoReplace,
            )],
            &[&seed_content],
        )])
        .await;

    let staged = fixture.stage(b"staged").await;
    fixture
        .run(vec![
            (
                covered(
                    "probe-parent-then-write",
                    vec![
                        create_dir("/r"),
                        put_file(
                            "/r/a.txt",
                            staged.content_ref().clone(),
                            DestinationBehavior::NoReplace,
                        ),
                    ],
                    &[&staged],
                ),
                Expect::Accept,
            ),
            (
                covered(
                    "probe-create-then-rename",
                    vec![
                        put_file(
                            "/m/f.txt",
                            staged.content_ref().clone(),
                            DestinationBehavior::NoReplace,
                        ),
                        FilesystemOperation::MovePath {
                            from_path: path("/m/f.txt"),
                            to_path: path("/m/g.txt"),
                            behavior: DestinationBehavior::NoReplace,
                        },
                    ],
                    &[&staged],
                ),
                Expect::Accept,
            ),
            (
                covered(
                    "probe-delete-then-recreate",
                    vec![
                        delete_path("/docs/seed.txt", DeleteDirectoryBehavior::NonRecursive),
                        put_file(
                            "/docs/seed.txt",
                            staged.content_ref().clone(),
                            DestinationBehavior::NoReplace,
                        ),
                    ],
                    &[&staged],
                ),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-duplicate-creates",
                    vec![create_dir("/dup"), create_dir("/dup")],
                ),
                Expect::Reject {
                    code: ErrorCode::PathConflict,
                    message_contains: "operation 1",
                    operation_index: Some(1),
                },
            ),
            (
                covered(
                    "probe-duplicate-puts",
                    vec![
                        put_file(
                            "/x.txt",
                            staged.content_ref().clone(),
                            DestinationBehavior::NoReplace,
                        ),
                        put_file(
                            "/x.txt",
                            staged.content_ref().clone(),
                            DestinationBehavior::NoReplace,
                        ),
                    ],
                    &[&staged],
                ),
                Expect::Reject {
                    code: ErrorCode::PathConflict,
                    message_contains: "operation 1",
                    operation_index: Some(1),
                },
            ),
        ])
        .await;
}

/// A directory rename into its own subtree is refused, alone and as a named
/// operation of a longer request.
#[tokio::test]
async fn rename_cycles_are_refused() {
    let fixture = PrepareFixture::new().await;
    fixture
        .seed(vec![
            uncovered("seed-cyc", vec![create_dir("/cyc")]),
            uncovered("seed-inner", vec![create_dir("/cyc/inner")]),
        ])
        .await;

    fixture
        .run(vec![
            (
                uncovered(
                    "probe-cycle",
                    vec![FilesystemOperation::MovePath {
                        from_path: path("/cyc"),
                        to_path: path("/cyc/inner/down"),
                        behavior: DestinationBehavior::NoReplace,
                    }],
                ),
                Expect::Reject {
                    code: ErrorCode::WouldCycle,
                    message_contains: "would create a cycle",
                    operation_index: None,
                },
            ),
            (
                uncovered(
                    "probe-cycle-in-batch",
                    vec![
                        create_dir("/pad"),
                        FilesystemOperation::MovePath {
                            from_path: path("/cyc"),
                            to_path: path("/cyc/inner/down"),
                            behavior: DestinationBehavior::NoReplace,
                        },
                    ],
                ),
                Expect::Reject {
                    code: ErrorCode::WouldCycle,
                    message_contains: "operation 1",
                    operation_index: Some(1),
                },
            ),
        ])
        .await;
}

/// Revision, attribute, and binding preconditions fail identically, with
/// `operation_index` only on multi-operation requests.
#[tokio::test]
async fn precondition_failures_name_the_failing_operation() {
    let fixture = PrepareFixture::new().await;
    let seed_content = fixture.stage(b"seed").await;
    fixture
        .seed(vec![
            covered(
                "seed-file",
                vec![put_file(
                    "/docs/a.txt",
                    seed_content.content_ref().clone(),
                    DestinationBehavior::NoReplace,
                )],
                &[&seed_content],
            ),
            uncovered(
                "seed-attributes",
                vec![update_attributes("/docs/a.txt", "owner", "ada")],
            ),
        ])
        .await;

    let staged = fixture.stage(b"staged").await;
    let stale_guard_put = || FilesystemOperation::PutFile {
        path: path("/docs/a.txt"),
        content_ref: staged.content_ref().clone(),
        behavior: DestinationBehavior::Replace,
        expected_revision_no: Some(RevisionNo(99)),
    };
    fixture
        .run(vec![
            (
                covered("probe-stale-guard", vec![stale_guard_put()], &[&staged]),
                Expect::Reject {
                    code: ErrorCode::StaleRevision,
                    message_contains: "expected revision 99",
                    operation_index: None,
                },
            ),
            (
                covered(
                    "probe-stale-guard-in-batch",
                    vec![create_dir("/ok"), stale_guard_put()],
                    &[&staged],
                ),
                Expect::Reject {
                    code: ErrorCode::StaleRevision,
                    message_contains: "operation 1",
                    operation_index: Some(1),
                },
            ),
            (
                uncovered(
                    "probe-wrong-inode-delete",
                    vec![FilesystemOperation::DeletePath {
                        path: path("/docs/a.txt"),
                        behavior: DeleteDirectoryBehavior::NonRecursive,
                        expected_inode_id: Some(InodeId(99)),
                    }],
                ),
                Expect::Reject {
                    code: ErrorCode::PathConflict,
                    message_contains: "expected child inode",
                    operation_index: None,
                },
            ),
            (
                uncovered(
                    "probe-stale-attributes",
                    vec![FilesystemOperation::UpdateAttributes {
                        path: path("/docs/a.txt"),
                        set: BTreeMap::from([(
                            AttributeKey::parse("owner").expect("valid attribute key"),
                            AttributeValue::parse("grace").expect("valid attribute value"),
                        )]),
                        remove: Vec::new(),
                        expected_inode_id: None,
                        expected_attributes_revision_no: Some(AttributeRevisionNo(0)),
                    }],
                ),
                Expect::Reject {
                    code: ErrorCode::StaleAttributes,
                    message_contains: "attribute base revision mismatch",
                    operation_index: None,
                },
            ),
            (
                covered(
                    "probe-guard-on-missing-path",
                    vec![FilesystemOperation::PutFile {
                        path: path("/docs/missing.txt"),
                        content_ref: staged.content_ref().clone(),
                        behavior: DestinationBehavior::Replace,
                        expected_revision_no: Some(RevisionNo(1)),
                    }],
                    &[&staged],
                ),
                Expect::Reject {
                    code: ErrorCode::PathNotFound,
                    message_contains: "/docs/missing.txt",
                    operation_index: None,
                },
            ),
            (
                // Planning failures precede coverage on both paths: an
                // uncovered guarded put on a missing path is still path-not-
                // found.
                uncovered(
                    "probe-uncovered-guard-on-missing-path",
                    vec![FilesystemOperation::PutFile {
                        path: path("/docs/missing.txt"),
                        content_ref: uncovered_content_ref("never-staged"),
                        behavior: DestinationBehavior::Replace,
                        expected_revision_no: Some(RevisionNo(1)),
                    }],
                ),
                Expect::Reject {
                    code: ErrorCode::PathNotFound,
                    message_contains: "/docs/missing.txt",
                    operation_index: None,
                },
            ),
        ])
        .await;
}

/// Content coverage rejections are identical wherever validation passes,
/// including multi-operation requests where validation already failed first
/// on both paths.
#[tokio::test]
async fn content_coverage_is_checked_after_validation() {
    let fixture = PrepareFixture::new().await;
    let seed_content = fixture.stage(b"seed").await;
    fixture
        .seed(vec![covered(
            "seed-docs",
            vec![put_file(
                "/docs/seed.txt",
                seed_content.content_ref().clone(),
                DestinationBehavior::NoReplace,
            )],
            &[&seed_content],
        )])
        .await;

    let staged = fixture.stage(b"staged").await;
    fixture
        .run(vec![
            (
                uncovered(
                    "probe-valid-uncovered",
                    vec![put_file(
                        "/docs/new.txt",
                        uncovered_content_ref("never-staged"),
                        DestinationBehavior::NoReplace,
                    )],
                ),
                Expect::Reject {
                    code: ErrorCode::ContentNotPrepared,
                    message_contains: "not prepared for publication",
                    operation_index: None,
                },
            ),
            (
                uncovered(
                    "probe-valid-uncovered-batch",
                    vec![
                        create_dir("/cov"),
                        put_file(
                            "/cov/f.txt",
                            uncovered_content_ref("never-staged"),
                            DestinationBehavior::NoReplace,
                        ),
                    ],
                ),
                Expect::Reject {
                    code: ErrorCode::ContentNotPrepared,
                    message_contains: "not prepared for publication",
                    operation_index: None,
                },
            ),
            (
                // Multi-operation and both invalid and uncovered: validation
                // already reported first on both paths, so this stays in the
                // equality corpus.
                uncovered(
                    "probe-invalid-uncovered-batch",
                    vec![
                        create_dir("/pad"),
                        FilesystemOperation::PutFile {
                            path: path("/docs/seed.txt"),
                            content_ref: uncovered_content_ref("never-staged"),
                            behavior: DestinationBehavior::Replace,
                            expected_revision_no: Some(RevisionNo(99)),
                        },
                    ],
                ),
                Expect::Reject {
                    code: ErrorCode::StaleRevision,
                    message_contains: "operation 1",
                    operation_index: Some(1),
                },
            ),
            (
                CommitCandidate::rejected(
                    request("probe-rejected-preparation", vec![create_dir("/never")]),
                    crate::commit_engine::ContentPreparationError::ContentNotPrepared {
                        content_id: ContentId::generate(),
                    },
                ),
                Expect::Reject {
                    code: ErrorCode::ContentNotPrepared,
                    message_contains: "not prepared for publication",
                    operation_index: None,
                },
            ),
            (
                covered(
                    "probe-covered-put-still-lands",
                    vec![put_file(
                        "/docs/new.txt",
                        staged.content_ref().clone(),
                        DestinationBehavior::NoReplace,
                    )],
                    &[&staged],
                ),
                Expect::Accept,
            ),
        ])
        .await;
}

/// Rejected candidates roll their allocation back identically: an accepted
/// candidate after a rejected one reuses the discarded inode ids on both
/// paths.
#[tokio::test]
async fn allocation_rollback_reuses_discarded_inode_ids() {
    let fixture = PrepareFixture::new().await;
    let outcomes = fixture
        .run(vec![
            (
                uncovered("probe-kept", vec![create_dir("/kept")]),
                Expect::Accept,
            ),
            (
                uncovered(
                    "probe-allocates-then-fails",
                    vec![
                        create_dir("/discarded"),
                        delete_path("/missing", DeleteDirectoryBehavior::NonRecursive),
                    ],
                ),
                Expect::Reject {
                    code: ErrorCode::PathNotFound,
                    message_contains: "operation 1",
                    operation_index: Some(1),
                },
            ),
            (
                uncovered("probe-kept-2", vec![create_dir("/kept2")]),
                Expect::Accept,
            ),
        ])
        .await;

    let last = outcomes
        .last()
        .expect("three outcomes")
        .as_ref()
        .expect("second create accepted");
    // The rejected candidate's fork was discarded, so the follow-up reuses
    // inode 3 and the batch position advances past it.
    assert_eq!(last.resulting_next_inode_id, InodeId(4));
    assert_eq!(last.assigned_seq, ChangeSeq(2));
}

/// An undelete scoped to a superseded deletion generation is refused
/// identically.
#[tokio::test]
async fn undelete_scoped_to_a_superseded_generation_is_refused() {
    let fixture = PrepareFixture::new().await;
    let seed_content = fixture.stage(b"seed").await;
    fixture
        .seed(vec![
            covered(
                "seed-file",
                vec![put_file(
                    "/u/f.txt",
                    seed_content.content_ref().clone(),
                    DestinationBehavior::NoReplace,
                )],
                &[&seed_content],
            ),
            uncovered(
                "seed-delete",
                vec![delete_path(
                    "/u/f.txt",
                    DeleteDirectoryBehavior::NonRecursive,
                )],
            ),
        ])
        .await;

    fixture
        .run(vec![(
            uncovered(
                "probe-wrong-generation",
                vec![FilesystemOperation::Undelete {
                    inode_id: InodeId(3),
                    deletion_seq: ChangeSeq(1),
                    path: Some(path("/u/f.txt")),
                }],
            ),
            Expect::Reject {
                code: ErrorCode::NotDeleted,
                message_contains: "active deletion is at seq",
                operation_index: None,
            },
        )])
        .await;
}

/// A subtree delete inside a request covers the paths beneath it for the
/// operations that follow, identically on both paths.
#[tokio::test]
async fn in_commit_tombstones_cover_later_operations() {
    let fixture = PrepareFixture::new().await;
    let seed_content = fixture.stage(b"seed").await;
    fixture
        .seed(vec![covered(
            "seed-docs",
            vec![put_file(
                "/docs/seed.txt",
                seed_content.content_ref().clone(),
                DestinationBehavior::NoReplace,
            )],
            &[&seed_content],
        )])
        .await;

    fixture
        .run(vec![(
            uncovered(
                "probe-delete-then-touch",
                vec![
                    delete_path("/docs", DeleteDirectoryBehavior::Recursive),
                    update_attributes("/docs/seed.txt", "owner", "ada"),
                ],
            ),
            Expect::Reject {
                code: ErrorCode::PathNotFound,
                message_contains: "operation 1",
                operation_index: Some(1),
            },
        )])
        .await;
}

/// An empty request is refused before any planning on both paths.
#[tokio::test]
async fn an_empty_request_is_refused_before_planning() {
    let fixture = PrepareFixture::new().await;
    fixture
        .run(vec![(
            uncovered("probe-empty", Vec::new()),
            Expect::Reject {
                code: ErrorCode::InvalidRequest,
                message_contains: "carries no operations",
                operation_index: None,
            },
        )])
        .await;
}

/// An exhausted namespace sequence reports the planner's pinned shape on
/// both paths.
#[tokio::test]
async fn sequence_exhaustion_reports_the_planner_shape() {
    let fixture = PrepareFixture::new().await;
    let mut view = fixture.load_view().await;
    view.head.seq = ChangeSeq(MAX_PUBLIC_INTEGER);
    fixture
        .run_against_view(
            &view,
            vec![(
                uncovered("probe-past-cap", vec![create_dir("/blocked")]),
                Expect::Reject {
                    code: ErrorCode::ServerError,
                    message_contains: "cannot exceed",
                    operation_index: None,
                },
            )],
        )
        .await;
}

/// The owner-decided precedence: a single-operation request that is both
/// invalid and content-uncovered reports the validation error, not the
/// coverage error the retired two-pass pipeline reported. Validation-first,
/// uniformly. The API surface pins the same decision in
/// `tests/it/commit_validation.rs`.
#[tokio::test]
async fn single_op_validation_error_precedes_coverage_by_owner_decision() {
    let fixture = PrepareFixture::new().await;
    let seed_content = fixture.stage(b"seed").await;
    fixture
        .seed(vec![covered(
            "seed-docs",
            vec![put_file(
                "/docs/seed.txt",
                seed_content.content_ref().clone(),
                DestinationBehavior::NoReplace,
            )],
            &[&seed_content],
        )])
        .await;

    let view = fixture.load_view().await;
    let mut session = PublishPlanningSession::new(&view.head);
    let candidate = uncovered(
        "probe-invalid-and-uncovered",
        vec![FilesystemOperation::PutFile {
            path: path("/docs/seed.txt"),
            content_ref: uncovered_content_ref("never-staged"),
            behavior: DestinationBehavior::Replace,
            expected_revision_no: Some(RevisionNo(99)),
        }],
    );

    let rejection = prepare_candidate(
        &mut session,
        &view,
        &fixture.namespace_id,
        &candidate,
        4_200,
    )
    .await
    .err()
    .map(|error| rejection_shape(&error))
    .expect("rejection");
    assert_eq!(rejection.code, ErrorCode::StaleRevision);
    assert!(
        rejection.message.contains("expected revision 99"),
        "the validation error names the stale guard: {rejection:?}"
    );
    assert_eq!(
        rejection
            .details
            .as_ref()
            .and_then(|details| details.operation_index),
        None,
        "a one-operation request keeps its raw error shape"
    );
}
