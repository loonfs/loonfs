//! `loonfs merge`: runs a fork's merge session as far as it goes with the
//! default resolver, and saves the session's record after every step.

use super::context::{Invocation, ProfileContext};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::MergeArgs;
use crate::error::{CliError, Result};
use crate::merge_record::{MergeAttempt, MergeRecordFile};
use crate::render::{human_merge_step, write_stderr_progress};
use loonfs_client::Client;
use loonfs_merge::{DefaultResolver, MergeSession, MergeSessionOptions, MergeSessionStatus};
use loonfs_types::ActorId;

pub(crate) async fn run_merge(
    invocation: &Invocation,
    args: MergeArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = ProfileContext::resolve(invocation).await?;
    let actor_id = context.actor(args.actor.actor_id.as_ref())?;
    let context = context.act_as_subject(&invocation.selectors, &actor_id)?;
    let record = MergeRecordFile::open(
        args.record.as_deref(),
        &context.profile_name,
        context.target.target_identity(),
        &args.namespace_id,
        &args.fork_id,
    )
    .map_err(|error| context.fail(error))?;
    let attempt = MergeAttempt {
        subject: context.target.client.subject().cloned(),
        through_seq: args.through_seq,
    };
    let (attempt, mut session) = open_session(
        context.target.client.clone(),
        &record,
        &args,
        actor_id,
        attempt,
    )
    .await
    .map_err(|error| context.fail(error))?;
    if args.abandon {
        session.abandon().map_err(|error| context.fail(error))?;
        record
            .save(&attempt, session.state())
            .map_err(|error| context.fail(error))?;
    }
    let mut steps = Vec::new();
    // The session's steps are bounded: each merge or rebase request gets at
    // most `MAX_MERGE_ATTEMPTS` attempts, each split hint shortens the range,
    // the session's rebases create at most `MAX_NEW_FORKS` new forks, and
    // each cleanup step deletes one of them.
    while let Some(outcome) = session.step().await.map_err(|error| context.fail(error))? {
        record
            .save(&attempt, session.state())
            .map_err(|error| context.fail(error))?;
        if invocation.behavior.progress.human_lines_enabled() {
            write_stderr_progress(human_merge_step(&outcome));
        }
        steps.push(outcome);
    }
    if !matches!(session.state().status, MergeSessionStatus::Failed { .. }) {
        record.remove().map_err(|error| context.fail(error))?;
    }

    Ok(context.output(CommandData::MergeSession {
        record_path: record.path().display().to_string(),
        session: session.state().clone(),
        steps,
    }))
}

/// Resumes the session that the record keeps, or recovers it from the
/// store when there's no record, and returns the inputs that the record
/// keeps with it.
///
/// Recovery is the start of a session that has nothing to recover, so a
/// first run and a run that lost its record are the same.
async fn open_session(
    client: Client,
    record: &MergeRecordFile,
    args: &MergeArgs,
    actor_id: ActorId,
    attempt: MergeAttempt,
) -> Result<(MergeAttempt, MergeSession<Client, DefaultResolver>)> {
    // Abandoning sends no merge, so it doesn't have to repeat the inputs
    // that the session's merges were sent with.
    let resumed_as = (!args.abandon).then_some((&actor_id, &attempt));
    if let Some((recorded, state)) = record.load(&args.namespace_id, &args.fork_id, resumed_as)? {
        return Ok((
            recorded,
            MergeSession::resume(client, DefaultResolver, state),
        ));
    }
    if args.abandon {
        return Err(CliError::invalid_request(format!(
            "there's no merge session of `{}` into `{}` to abandon in `{}`",
            args.fork_id,
            args.namespace_id,
            record.path().display()
        ))
        .with_param("--abandon"));
    }
    let session = MergeSession::recover(
        client,
        DefaultResolver,
        args.namespace_id.clone(),
        args.fork_id.clone(),
        actor_id,
        &MergeSessionOptions {
            through_seq: args.through_seq,
        },
    )
    .await?;
    record.save(&attempt, session.state())?;
    Ok((attempt, session))
}
