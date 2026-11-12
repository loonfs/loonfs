//! The record of a `loonfs merge` session, which the command saves after
//! every step so that a rerun resumes the session where it stopped.

use crate::error::{CliError, Result};
use crate::state_file::{keyed_path, OwnedStateFile};
use loonfs_merge::MergeSessionState;
use loonfs_types::{ActorId, CommitSeq, NamespaceId, Subject};
use serde::{Deserialize, Serialize};
use std::path::Path;

const MERGES_SUBDIR: &str = "merges";

/// The inputs of a merge session that a resumed run has to repeat, beside
/// the actor that the session's state records. The subject is part of every
/// merge's identity, and `--through` sets the range that the session merges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeAttempt {
    /// The subject that the session's requests act as, or `None` for the
    /// token holder.
    pub subject: Option<Subject>,
    /// The fork sequence that the command asked to merge the fork through.
    pub through_seq: Option<CommitSeq>,
}

/// What a merge record file stores.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MergeRecord {
    attempt: MergeAttempt,
    session: MergeSessionState,
}

/// One merge session's record file, which this command owns until it drops
/// the value.
#[derive(Debug)]
pub(crate) struct MergeRecordFile {
    file: OwnedStateFile,
}

impl MergeRecordFile {
    /// Opens the record of the session that merges `fork_id` into
    /// `namespace_id` through the profile `profile` and its target, at
    /// `explicit_path`, or under the CLI's state directory.
    pub(crate) fn open(
        explicit_path: Option<&Path>,
        profile: &str,
        target_identity: &str,
        namespace_id: &NamespaceId,
        fork_id: &NamespaceId,
    ) -> Result<Self> {
        let path = match explicit_path {
            Some(path) => path.to_owned(),
            None => keyed_path(
                MERGES_SUBDIR,
                &(profile, target_identity, namespace_id, fork_id),
            )
            .ok_or_else(|| {
                CliError::io_error(
                    "merge records require an absolute XDG_STATE_HOME or HOME, or `--record`",
                )
                .with_param("--record")
            })?,
        };
        let file = OwnedStateFile::open(path.clone(), "merge session")
            .map_err(|error| record_error(&path, error))?;
        Ok(Self { file })
    }

    pub(crate) fn path(&self) -> &Path {
        self.file.path()
    }

    /// Returns the saved session and the inputs that it was started with, or
    /// `None` when there's no record. A record of another fork's merge is an
    /// error, and so is a record that `resumed_as` doesn't repeat, when the
    /// command sends merges as `resumed_as`.
    pub(crate) fn load(
        &self,
        namespace_id: &NamespaceId,
        fork_id: &NamespaceId,
        resumed_as: Option<(&ActorId, &MergeAttempt)>,
    ) -> Result<Option<(MergeAttempt, MergeSessionState)>> {
        let Some(record) = self
            .file
            .read::<MergeRecord>()
            .map_err(|error| self.error(error))?
        else {
            return Ok(None);
        };
        let session = &record.session;
        if session.namespace_id != *namespace_id || session.original_fork_id != *fork_id {
            return Err(self.error(format!(
                "the record is for the merge of `{}` into `{}`",
                session.original_fork_id, session.namespace_id
            )));
        }
        if let Some((actor_id, attempt)) = resumed_as {
            if session.actor_id != *actor_id || record.attempt != *attempt {
                return Err(self.error(
                    "merge options changed; resume with the original actor, subject, and \
                     `--through`, or abandon the session with `--abandon`",
                ));
            }
        }
        Ok(Some((record.attempt, record.session)))
    }

    /// Saves the session's state after a step.
    pub(crate) fn save(&self, attempt: &MergeAttempt, session: &MergeSessionState) -> Result<()> {
        self.file
            .write(&MergeRecord {
                attempt: attempt.clone(),
                session: session.clone(),
            })
            .map_err(|error| self.error(error))
    }

    /// Removes the record of a session that has nothing left to do.
    pub(crate) fn remove(&self) -> Result<()> {
        self.file.remove().map_err(|error| self.error(error))
    }

    fn error(&self, error: impl std::fmt::Display) -> CliError {
        record_error(self.file.path(), error)
    }
}

fn record_error(path: &Path, error: impl std::fmt::Display) -> CliError {
    CliError::io_error(format!("merge record `{}`: {error}", path.display()))
}
