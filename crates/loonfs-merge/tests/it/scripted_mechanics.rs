//! A scripted [`Mechanics`] for session tests, which answers each request
//! with the next answer of its script and records the requests, and the
//! helpers that drive a session to its end.

use loonfs_client::{CheckpointsPager, ClientError, Result};
use loonfs_merge::{
    ConflictReport, Mechanics, MergeSession, MergeSessionOptions, MergeSessionOutcome,
    MergeSessionState, Resolver,
};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::test_actor;
use loonfs_types::options::MergeForkOptions;
use loonfs_types::{
    ActorId, ChangeSeq, Commit, DeleteNamespaceResponse, ErrorCode, ErrorDetails, ErrorResponse,
    ListCheckpointsResponse, Merge, MergeConflict, NamespaceId, NamespaceMetadata,
    NextMergeRangeOutcome, NextMergeRangeResponse, Pager, RebaseForkResponse,
};
use serde_json::json;
use std::collections::VecDeque;
use std::error::Error;
use std::sync::{Arc, Mutex};

/// One request that the session sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Sent {
    Merge {
        fork_id: NamespaceId,
        options: MergeForkOptions,
    },
    Rebase {
        fork_id: NamespaceId,
        new_fork_id: NamespaceId,
        options: MergeForkOptions,
    },
    NextMergeRange {
        fork_id: NamespaceId,
    },
    Delete {
        namespace_id: NamespaceId,
    },
    GetNamespace {
        namespace_id: NamespaceId,
    },
    ListCheckpoints,
}

/// The answer to one request.
#[derive(Debug)]
pub(crate) enum Answer {
    /// The merge commits at this sequence, under the request's commit ID.
    Committed(u64),
    /// The replay commit commits with this conflict report, or without one,
    /// as when the request's commit ID identifies an earlier replay commit.
    Replayed(Option<Vec<MergeConflict>>),
    /// The next-range query's `open` answer, with the fork's newest merge
    /// when `merged`.
    Range {
        next_after_seq: u64,
        source_head_seq: u64,
        merged: bool,
    },
    /// The next-range query's answer with another outcome.
    RangeOutcome {
        source_head_seq: u64,
        outcome: NextMergeRangeOutcome,
    },
    /// The namespace is deleted.
    Deleted,
    /// The namespace is a fork of `main`, which a rebase of `rebased_from`
    /// created when it has one.
    Namespace { rebased_from: Option<NamespaceId> },
    /// One page of `main`'s checkpoints: a fork pin for each namespace.
    ForkPins(Vec<NamespaceId>),
    /// The server refuses the request.
    Refused(ErrorCode, Box<ErrorDetails>),
}

impl Answer {
    pub(crate) fn range(next_after_seq: u64, source_head_seq: u64) -> Self {
        Self::Range {
            next_after_seq,
            source_head_seq,
            merged: false,
        }
    }

    pub(crate) fn refused(code: ErrorCode) -> Self {
        Self::Refused(code, Box::default())
    }

    pub(crate) fn too_large(split_at_seq: u64) -> Self {
        Self::Refused(
            ErrorCode::MergeTooLarge,
            Box::new(ErrorDetails {
                split_at_seq: Some(ChangeSeq(split_at_seq)),
                ..ErrorDetails::default()
            }),
        )
    }
}

struct Script {
    answers: VecDeque<Answer>,
    sent: Vec<Sent>,
}

/// Answers each request with the next answer, and records the requests.
#[derive(Clone)]
pub(crate) struct ScriptedMechanics(Arc<Mutex<Script>>);

impl ScriptedMechanics {
    pub(crate) fn new(answers: impl IntoIterator<Item = Answer>) -> Self {
        Self(Arc::new(Mutex::new(Script {
            answers: answers.into_iter().collect(),
            sent: Vec::new(),
        })))
    }

    pub(crate) fn sent(&self) -> Vec<Sent> {
        self.lock().sent.clone()
    }

    pub(crate) fn assert_script_used(&self) {
        let script = self.lock();
        assert!(
            script.answers.is_empty(),
            "the session left answers unused: {:?}",
            script.answers
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Script> {
        self.0.lock().expect("script lock should not be poisoned")
    }

    fn answer(&self, sent: Sent) -> Answer {
        let mut script = self.lock();
        let answer = script
            .answers
            .pop_front()
            .unwrap_or_else(|| panic!("the script should answer {sent:?}"));
        script.sent.push(sent);
        answer
    }
}

impl Mechanics for ScriptedMechanics {
    async fn merge_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<Commit> {
        let sent = Sent::Merge {
            fork_id: source_namespace_id.clone(),
            options: options.clone(),
        };
        match self.answer(sent) {
            Answer::Committed(committed_seq) => Ok(Commit {
                namespace_id: namespace_id.clone(),
                commit_id: options.commit_id.clone().expect("a merge has a commit ID"),
                committed_seq: ChangeSeq(committed_seq),
                committed_by: actor.clone(),
                committed_at_ms: 0,
                message: None,
                merge: Some(Merge {
                    source_namespace_id: source_namespace_id.clone(),
                    source_after_seq: ChangeSeq(0),
                    source_through_seq: options.through_seq.expect("a merge has a sequence"),
                }),
                events: Vec::new(),
            }),
            Answer::Refused(code, details) => Err(refused(code, details)),
            answer => panic!("a merge can't answer {answer:?}"),
        }
    }

    async fn rebase_fork_with_options(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        _actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<RebaseForkResponse> {
        let sent = Sent::Rebase {
            fork_id: source_namespace_id.clone(),
            new_fork_id: new_namespace_id.clone(),
            options: options.clone(),
        };
        match self.answer(sent) {
            Answer::Replayed(conflicts) => Ok(RebaseForkResponse {
                namespace: namespace(new_namespace_id, namespace_id, Some(source_namespace_id)),
                conflicts,
            }),
            Answer::Refused(code, details) => Err(refused(code, details)),
            answer => panic!("a rebase can't answer {answer:?}"),
        }
    }

    async fn next_merge_range(
        &self,
        _namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
    ) -> Result<NextMergeRangeResponse> {
        let sent = Sent::NextMergeRange {
            fork_id: source_namespace_id.clone(),
        };
        match self.answer(sent) {
            Answer::Range {
                next_after_seq,
                source_head_seq,
                merged,
            } => Ok(NextMergeRangeResponse {
                fork_point_seq: ChangeSeq(0),
                source_head_seq: ChangeSeq(source_head_seq),
                outcome: NextMergeRangeOutcome::Open {
                    merge: merged.then(|| Merge {
                        source_namespace_id: source_namespace_id.clone(),
                        source_after_seq: ChangeSeq(0),
                        source_through_seq: ChangeSeq(next_after_seq),
                    }),
                    next_after_seq: ChangeSeq(next_after_seq),
                },
            }),
            Answer::RangeOutcome {
                source_head_seq,
                outcome,
            } => Ok(NextMergeRangeResponse {
                fork_point_seq: ChangeSeq(0),
                source_head_seq: ChangeSeq(source_head_seq),
                outcome,
            }),
            Answer::Refused(code, details) => Err(refused(code, details)),
            answer => panic!("the next-range query can't answer {answer:?}"),
        }
    }

    async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DeleteNamespaceResponse> {
        let sent = Sent::Delete {
            namespace_id: namespace_id.clone(),
        };
        match self.answer(sent) {
            Answer::Deleted => Ok(DeleteNamespaceResponse {
                namespace_id: namespace_id.clone(),
                head_seq: ChangeSeq(0),
            }),
            Answer::Refused(code, details) => Err(refused(code, details)),
            answer => panic!("a delete can't answer {answer:?}"),
        }
    }

    async fn get_namespace(&self, namespace_id: &NamespaceId) -> Result<NamespaceMetadata> {
        let sent = Sent::GetNamespace {
            namespace_id: namespace_id.clone(),
        };
        match self.answer(sent) {
            Answer::Namespace { rebased_from } => {
                Ok(namespace(namespace_id, &main(), rebased_from.as_ref()))
            }
            Answer::Refused(code, details) => Err(refused(code, details)),
            answer => panic!("a namespace read can't answer {answer:?}"),
        }
    }

    fn list_checkpoints(&self, namespace_id: &NamespaceId) -> CheckpointsPager {
        let answer = self.answer(Sent::ListCheckpoints);
        let Answer::ForkPins(targets) = answer else {
            panic!("a checkpoint listing can't answer {answer:?}");
        };
        let page: ListCheckpointsResponse = serde_json::from_value(json!({
            "namespace_id": namespace_id,
            "checkpoints": targets.iter().enumerate().map(|(index, target)| json!({
                "namespace_id": namespace_id,
                "checkpoint_id": format!("pin_{:020}-{index:016x}", index + 1),
                "owner": {"kind": "fork", "target_namespace_id": target},
                "created_at_ms": 0,
                "captured_seq": 10,
                "manifest_no": index + 1,
            })).collect::<Vec<_>>(),
        }))
        .expect("a checkpoint page should decode");
        let mut page = Some(page);
        Pager::new(move |_request| {
            let page = page.take().expect("the listing has one page");
            async move { Ok(page) }
        })
    }
}

/// Returns the namespace that sessions merge into.
pub(crate) fn main() -> NamespaceId {
    namespace_id("main")
}

/// Returns the fork that sessions start with.
pub(crate) fn sandbox() -> NamespaceId {
    namespace_id("sandbox")
}

/// Starts a session that merges `sandbox` into `main`, which first
/// recovers what an earlier run did from the script's checkpoint listing.
pub(crate) async fn recover<R: Resolver>(
    mechanics: &ScriptedMechanics,
    resolver: R,
) -> MergeSession<ScriptedMechanics, R> {
    MergeSession::recover(
        mechanics.clone(),
        resolver,
        main(),
        sandbox(),
        test_actor(),
        &MergeSessionOptions::default(),
    )
    .await
    .expect("recover the session")
}

/// Runs the session until it has no step left, and returns each step's
/// outcome and the last state. Every step saves the state, and the next step
/// resumes from the saved copy, as the CLI does.
pub(crate) async fn run_to_end<R: Resolver + Clone>(
    mechanics: &ScriptedMechanics,
    resolver: &R,
    mut session: MergeSession<ScriptedMechanics, R>,
) -> (Vec<MergeSessionOutcome>, MergeSessionState) {
    let mut outcomes = Vec::new();
    for _ in 0..64 {
        let Some(outcome) = session.step().await.expect("step") else {
            mechanics.assert_script_used();
            return (outcomes, session.state().clone());
        };
        outcomes.push(outcome);
        let saved = serde_json::to_vec(session.state()).expect("encode the state");
        let state = serde_json::from_slice(&saved).expect("decode the state");
        session = MergeSession::resume(mechanics.clone(), resolver.clone(), state);
    }
    panic!("the session should end within 64 steps");
}

/// Returns the requests of one kind that the session sent.
pub(crate) fn merges(sent: &[Sent]) -> Vec<(NamespaceId, MergeForkOptions)> {
    sent.iter()
        .filter_map(|sent| match sent {
            Sent::Merge { fork_id, options } => Some((fork_id.clone(), options.clone())),
            _ => None,
        })
        .collect()
}

/// Returns the namespaces that the session deleted, in order.
pub(crate) fn deletes(sent: &[Sent]) -> Vec<NamespaceId> {
    sent.iter()
        .filter_map(|sent| match sent {
            Sent::Delete { namespace_id } => Some(namespace_id.clone()),
            _ => None,
        })
        .collect()
}

/// Returns a `content` conflict on one file.
pub(crate) fn conflict(path: &str) -> MergeConflict {
    serde_json::from_value(json!({
        "inode_id": "ino_000000r0000013kje0d47gc8kw",
        "kind": "content",
        "base": {"path": path},
        "ours": {"path": path},
        "theirs": {"path": path},
    }))
    .expect("a conflict should decode")
}

/// The new fork and the reports of one call to a resolver.
type Resolution = (NamespaceId, Vec<ConflictReport>);

/// Records the reports that it resolves, and resolves them.
#[derive(Clone, Default)]
pub(crate) struct RecordingResolver(Arc<Mutex<Vec<Resolution>>>);

impl RecordingResolver {
    pub(crate) fn resolved(&self) -> Vec<Resolution> {
        self.0
            .lock()
            .expect("resolver lock should not be poisoned")
            .clone()
    }
}

impl Resolver for RecordingResolver {
    async fn resolve(
        &self,
        new_fork_id: &NamespaceId,
        reports: &[ConflictReport],
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        self.0
            .lock()
            .expect("resolver lock should not be poisoned")
            .push((new_fork_id.clone(), reports.to_vec()));
        Ok(())
    }
}

fn refused(code: ErrorCode, details: Box<ErrorDetails>) -> ClientError {
    ClientError::from_error_response(
        409,
        ErrorResponse {
            details: Some(details),
            ..ErrorResponse::new(code, "scripted refusal")
        },
    )
}

fn namespace(
    namespace_id: &NamespaceId,
    source_namespace_id: &NamespaceId,
    rebased_from: Option<&NamespaceId>,
) -> NamespaceMetadata {
    let mut basis = json!({
        "source_namespace_id": source_namespace_id,
        "source_head_seq": 10,
        "source_pin_id": "pin_00000000000000000001-0000000000000001",
    });
    if let Some(rebased_from) = rebased_from {
        basis["rebased_from"] =
            json!({"source_namespace_id": rebased_from, "resume_after_seq": 10});
    }
    serde_json::from_value(json!({
        "access": {"kind": "unrestricted"},
        "namespace_id": namespace_id,
        "created_at_ms": 0,
        "created_by": "test-actor",
        "fork_basis": basis,
        "pool_id": "pool_3s0drmj8em84yb4mnetqkz2j0x",
        "head_seq": 10,
        "retention_floor_seq": 10,
    }))
    .expect("a namespace should decode")
}
