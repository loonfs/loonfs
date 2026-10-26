//! Admitting a commit: request validation, the receipt check that replays a retried commit, the
//! request preconditions, planning each operation, and publishing the result.

use super::planner::{CommitContext, Planner};
use super::preconditions;
use crate::config::{InlineContent, MAX_APPEND_BYTES};
use crate::content_store::ChainKey;
use crate::error::{MountSimError, Result};
use crate::namespace::CommitRecord;
use crate::state::ServerState;
use loonfs_types::api::v0::{Commit, CommitRequest, FilesystemOperation};
use loonfs_types::{
    semantic_commit_fingerprint, ActorId, ChangeSeq, ErrorCode, NamespaceId, SubjectId,
    FEATURE_COMMIT_INLINE_CONTENT,
};
use std::collections::BTreeMap;

// The commit request limits that the capability document advertises. The reference server
// defines them in `loonfs-core`, which the simulation doesn't depend on.
pub(crate) const MAX_COMMIT_OPERATIONS: usize = 4096;
pub(crate) const MAX_COMMIT_PRECONDITIONS: usize = 1024;
pub(crate) const MAX_COMMIT_CONTENT_TOKENS: usize = 4096;
pub(crate) const MAX_COMMIT_MESSAGE_BYTES: usize = 4096;
pub(crate) const MAX_COMMIT_REQUEST_BODY_BYTES: usize = 2 * 1024 * 1024;

/// The identity headers of a commit, which are part of its fingerprint.
#[derive(Debug, Clone)]
pub(crate) struct CommitIdentity {
    pub(crate) actor: ActorId,
    pub(crate) subject: Option<SubjectId>,
}

/// How a commit request ended.
#[derive(Debug)]
pub(crate) enum CommitOutcome {
    /// The request made a new commit.
    Committed(Commit),
    /// The request repeated a commit whose receipt the namespace keeps, which returns the original
    /// commit.
    Replayed(Commit),
}

impl CommitOutcome {
    pub(crate) fn into_commit(self) -> Commit {
        match self {
            Self::Committed(commit) | Self::Replayed(commit) => commit,
        }
    }
}

/// Admits and publishes one commit request. Every error includes the request's `commit_id`.
pub(crate) fn create_commit(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    identity: &CommitIdentity,
    request: &CommitRequest,
) -> Result<CommitOutcome> {
    admit(state, namespace_id, identity, request)
        .map_err(|error| error.for_commit(&request.commit_id))
}

fn admit(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    identity: &CommitIdentity,
    request: &CommitRequest,
) -> Result<CommitOutcome> {
    validate_shape(request)?;
    let fingerprint = semantic_commit_fingerprint(
        namespace_id,
        &identity.actor,
        identity.subject.as_ref(),
        request.message.as_deref(),
        &request.operations,
        &request.preconditions,
        &BTreeMap::new(),
    )
    .map_err(|error| MountSimError::invalid_request(error.to_string()))?;
    let namespace = state.namespace(namespace_id)?;
    if let Some(&committed_seq) = namespace.receipts.get(&request.commit_id) {
        let record = namespace.commit_at(committed_seq);
        if record.fingerprint == fingerprint {
            return Ok(CommitOutcome::Replayed(record.commit.clone()));
        }
        return Err(MountSimError::new(
            ErrorCode::CommitIdReuseConflict,
            format!(
                "commit `{}` was committed at sequence {committed_seq} with different content",
                request.commit_id
            ),
        )
        .with_details(|details| {
            details.committed_seq = Some(committed_seq);
            details.committed_fingerprint = Some(record.fingerprint.clone());
        }));
    }
    validate_content(state.config.capabilities.inline_content, request)?;
    ensure_content_prepared(state, namespace_id, request)?;
    let head_seq = namespace.head_seq();
    preconditions::evaluate(&namespace.view(head_seq), &request.preconditions)?;

    let now_ms = state.now_ms();
    let seq = ChangeSeq(head_seq.0 + 1);
    let ServerState {
        namespaces,
        content,
        ids,
        ..
    } = state;
    let namespace = namespaces
        .get_mut(namespace_id)
        .expect("the namespace was found above");
    let context = CommitContext {
        namespace_id,
        naming: namespace.naming,
        seq,
        commit_id: &request.commit_id,
        actor: &identity.actor,
        committed_at_ms: now_ms,
    };
    let mut planner = Planner::new(
        context,
        namespace.metadata.clone(),
        namespace.next_inode_id,
        content,
        ids,
    );
    for (index, operation) in request.operations.iter().enumerate() {
        planner
            .plan(operation)
            .map_err(|error| error.at_operation(index))?;
    }
    let planned = planner.finish();
    let commit = Commit {
        namespace_id: namespace_id.clone(),
        commit_id: request.commit_id.clone(),
        committed_seq: seq,
        committed_by: identity.actor.clone(),
        committed_at_ms: now_ms,
        message: request.message.clone(),
        events: planned.events,
    };
    namespace.publish(
        CommitRecord::new(commit.clone(), fingerprint),
        planned.metadata,
        planned.next_inode_id,
    );
    planned.content.publish(content);
    Ok(CommitOutcome::Committed(commit))
}

/// Checks that one of the request's content tokens proves each uploaded reference that an
/// operation publishes, before the preconditions and the operations run, as the reference server
/// does. The error identifies the content, not an operation.
fn ensure_content_prepared(
    state: &ServerState,
    namespace_id: &NamespaceId,
    request: &CommitRequest,
) -> Result<()> {
    let now_ms = state.now_ms();
    for content_ref in request
        .operations
        .iter()
        .filter_map(FilesystemOperation::content_ref)
    {
        let proven =
            state
                .content_tokens
                .proves(&request.content_tokens, namespace_id, content_ref, now_ms);
        let stored = state
            .content
            .find_chain(&ChainKey::of(content_ref))
            .is_some_and(|chain| {
                u64::try_from(chain.len()).is_ok_and(|length| length >= content_ref.size_bytes)
            });
        if !(proven && stored) {
            return Err(MountSimError::new(
                ErrorCode::ContentNotPrepared,
                format!(
                    "no valid content token proves content `{}`",
                    content_ref.content_id
                ),
            ));
        }
    }
    Ok(())
}

/// Rejects a request whose shape is invalid before anything else is checked.
fn validate_shape(request: &CommitRequest) -> Result<()> {
    let over = |field: &str, count: usize, max: usize| {
        (count > max).then(|| {
            MountSimError::invalid_request(format!(
                "`{field}` has {count} entries, more than {max}"
            ))
            .with_param(format!("/{field}"))
        })
    };
    if request.operations.is_empty() {
        return Err(
            MountSimError::invalid_request("`operations` can't be empty").with_param("/operations"),
        );
    }
    let too_many = over(
        "operations",
        request.operations.len(),
        MAX_COMMIT_OPERATIONS,
    )
    .or_else(|| {
        over(
            "preconditions",
            request.preconditions.len(),
            MAX_COMMIT_PRECONDITIONS,
        )
    })
    .or_else(|| {
        over(
            "content_tokens",
            request.content_tokens.len(),
            MAX_COMMIT_CONTENT_TOKENS,
        )
    });
    if let Some(error) = too_many {
        return Err(error);
    }
    if request
        .message
        .as_ref()
        .is_some_and(|message| message.len() > MAX_COMMIT_MESSAGE_BYTES)
    {
        return Err(MountSimError::invalid_request(format!(
            "`message` is longer than {MAX_COMMIT_MESSAGE_BYTES} bytes"
        ))
        .with_param("/message"));
    }
    for (index, operation) in request.operations.iter().enumerate() {
        validate_operation_shape(operation).map_err(|error| error.at_operation(index))?;
    }
    Ok(())
}

fn validate_operation_shape(operation: &FilesystemOperation) -> Result<()> {
    use FilesystemOperation as Op;
    match operation {
        Op::PutFile {
            content_ref,
            inline_content,
            ..
        }
        | Op::CreateFileByInode {
            content_ref,
            inline_content,
            ..
        }
        | Op::PutFileRevisionByInode {
            content_ref,
            inline_content,
            ..
        } if content_ref.is_some() == inline_content.is_some() => {
            Err(MountSimError::invalid_request(
                "exactly one of `content_ref` and `inline_content` is required",
            )
            .with_param("content_ref"))
        }
        Op::AppendFile { inline_content, .. } | Op::AppendFileByInode { inline_content, .. }
            if inline_content.is_empty() =>
        {
            Err(
                MountSimError::invalid_request("an append adds at least one byte")
                    .with_param("inline_content"),
            )
        }
        _ => Ok(()),
    }
}

/// Checks inline content against the advertised feature and limits, and appends against the
/// append limit.
fn validate_content(inline: InlineContent, request: &CommitRequest) -> Result<()> {
    for (index, operation) in request.operations.iter().enumerate() {
        validate_operation_content(inline, operation).map_err(|error| error.at_operation(index))?;
    }
    Ok(())
}

fn validate_operation_content(
    inline: InlineContent,
    operation: &FilesystemOperation,
) -> Result<()> {
    use FilesystemOperation as Op;
    let length = |bytes: &[u8]| u64::try_from(bytes.len()).expect("a length fits in u64");
    match operation {
        Op::PutFile {
            inline_content: Some(bytes),
            ..
        }
        | Op::CreateFileByInode {
            inline_content: Some(bytes),
            ..
        }
        | Op::PutFileRevisionByInode {
            inline_content: Some(bytes),
            ..
        } => match inline {
            InlineContent::NotAdvertised => Err(MountSimError::new(
                ErrorCode::NotSupported,
                "the deployment doesn't accept inline content",
            )
            .with_feature(FEATURE_COMMIT_INLINE_CONTENT)),
            InlineContent::Advertised {
                max_bytes_per_operation,
            } if length(bytes) > max_bytes_per_operation => Err(MountSimError::invalid_request(
                format!("inline content is larger than {max_bytes_per_operation} bytes"),
            )
            .with_param("inline_content")),
            InlineContent::Advertised { .. } => Ok(()),
        },
        Op::AppendFile { inline_content, .. } | Op::AppendFileByInode { inline_content, .. }
            if length(inline_content) > MAX_APPEND_BYTES =>
        {
            Err(MountSimError::new(
                ErrorCode::ContentTooLarge,
                format!("an append adds at most {MAX_APPEND_BYTES} bytes"),
            )
            .with_param("inline_content"))
        }
        _ => Ok(()),
    }
}
