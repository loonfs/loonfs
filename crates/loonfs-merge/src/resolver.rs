//! The resolver, which writes resolutions of a rebase's conflicts into the
//! new fork, and the default resolver, which fails on a conflict.

use crate::ConflictReport;
use loonfs_types::NamespaceId;
use std::error::Error;
use std::future::Future;

/// A program or a person that resolves a rebase's conflicts in the new fork.
///
/// A resolver reads each conflict's versions through the ordinary read
/// routes: `ours` from the fork that the new fork's
/// `fork_basis.rebased_from` identifies, `theirs` from the namespace that the
/// session merges into, and the base from that namespace through the new
/// fork's fork pin (API spec, `POST /rebases`). It writes the resolved
/// content into the new fork with ordinary commits.
pub trait Resolver: Send + Sync {
    /// Writes resolutions of the changes that `reports` list into
    /// `new_fork_id`, and returns once the new fork is ready to merge.
    ///
    /// The reports list no change when the rebase applied every change. An
    /// error fails the session, which keeps the reports and the new fork for
    /// the caller to inspect. A step that fails after the resolver returned
    /// calls the resolver again.
    fn resolve(
        &self,
        new_fork_id: &NamespaceId,
        reports: &[ConflictReport],
    ) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send;
}

/// The resolver that resolves nothing, so a session whose rebase left out a
/// change fails with the conflict reports and the new fork to inspect, and
/// the conflicting changes aren't merged.
///
/// When the reports list no change, the default resolver returns, and the
/// session merges the new fork.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultResolver;

impl Resolver for DefaultResolver {
    async fn resolve(
        &self,
        new_fork_id: &NamespaceId,
        reports: &[ConflictReport],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let conflicts: usize = reports.iter().map(|report| report.conflicts.len()).sum();
        if conflicts == 0 {
            return Ok(());
        }
        let noun = if conflicts == 1 { "change" } else { "changes" };
        Err(format!(
            "the default resolver doesn't resolve conflicts, and the rebase into \
             `{new_fork_id}` left out {conflicts} conflicting {noun}"
        )
        .into())
    }
}
