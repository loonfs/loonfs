//! A run of grep steps over one namespace: the change to grep's entry that
//! the steps make, which is kept in memory, and its publication in a
//! manifest of its own.

use super::reorganize::reorganize_due;
use super::{GramIndexBuildPolicy, GrepBuildOutcome, GrepReorganizeOutcome, GrepWorker};
use crate::entry::{grep_extension_name, GrepEntry, GrepEntryError, GrepIndexStatus};
use crate::{GrepError, Result};
use loonfs::engine::{ExtensionBasis, ExtensionPublication};
use loonfs_objectstore::ObjectStore;
use loonfs_types::NamespaceId;
use std::sync::atomic::{AtomicUsize, Ordering};

/// How long a run of steps in a host without a maintenance interval of its
/// own, such as the CLI, keeps its changes in memory before it publishes them.
///
/// A crash loses at most this much of the run's work.
pub const DEFAULT_GREP_PUBLISH_INTERVAL_MS: u64 = 5_000;

/// How publishing a run's pending change ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrepPublishOutcome {
    /// The pending change is in the manifest after the one it built on, or
    /// after a manifest that followed that one with the same grep entry.
    Published,
    /// The steps changed nothing since the last publication.
    NothingPending,
    /// Another process changed grep's entry after the manifest that the
    /// change built on, so the change wasn't published, and its segments are
    /// left to the collector.
    Superseded,
}

/// How one step of a run of steps ended (see [`GrepSteps::step`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrepProgress {
    /// The step was a build step.
    Built(GrepBuildOutcome),
    /// The step was a reorganize step, because a reorganization was due.
    Reorganized(GrepReorganizeOutcome),
}

/// How a worker's publications of grep's entry ended since the worker was
/// created, as counts of the changes that its runs of steps tried to
/// publish.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GrepPublicationStats {
    /// Changes published in the manifest after the one that they built on.
    pub published: usize,
    /// Changes whose put lost to a manifest with the same grep entry, such
    /// as a fold's, and that were published over that manifest.
    pub rebuilt: usize,
    /// Changes that weren't published, because another process changed
    /// grep's entry first.
    pub superseded: usize,
}

/// The counters behind [`GrepPublicationStats`], which clones of a worker
/// share.
#[derive(Debug, Default)]
pub(super) struct PublicationCounts {
    published: AtomicUsize,
    rebuilt: AtomicUsize,
    superseded: AtomicUsize,
}

impl PublicationCounts {
    pub(super) fn stats(&self) -> GrepPublicationStats {
        GrepPublicationStats {
            published: self.published.load(Ordering::Relaxed),
            rebuilt: self.rebuilt.load(Ordering::Relaxed),
            superseded: self.superseded.load(Ordering::Relaxed),
        }
    }
}

/// The change that a run's steps made to grep's entry since the manifest
/// that the change builds on.
pub(super) struct PendingEntry {
    pub(super) basis: ExtensionBasis,
    /// The entry that the basis lists, or `None` when grep was never enabled
    /// or when a disable replaces an invalid entry (see
    /// [`Self::for_disable`]).
    published: Option<GrepEntry>,
    /// The entry after the steps since the basis, which the next step starts
    /// from. A run that a step writes is assigned its number from this entry
    /// (see [`GrepEntry::next_run_no`]).
    pub(super) entry: Option<GrepEntry>,
}

impl PendingEntry {
    /// Starts a change from `basis`, after checking that the basis's grep
    /// entry decodes.
    ///
    /// In a fork, the change clears a reorganization that the fork inherited
    /// from its source (see [`GrepEntry::without_inherited_reorganize`]), so
    /// the fork's first change publishes the entry without it.
    fn new(basis: ExtensionBasis) -> Result<Self> {
        match decode_published(&basis) {
            Ok(published) => Ok(Self::from_published(basis, published)),
            Err(error) => Err(invalid_entry(&basis, error)),
        }
    }

    /// Starts a disable's change from `basis` like [`Self::new`], except that
    /// when [`Self::new`] would reject a grep entry of this build's version
    /// ([`GREP_ENTRY_VERSION`](crate::entry::GREP_ENTRY_VERSION)), the
    /// change's entry is the disabled entry instead.
    ///
    /// That change records no published entry, so publishing it replaces the
    /// invalid entry with the disabled entry. For an entry of another
    /// version, this returns [`GrepError::UnsupportedVersion`].
    fn for_disable(basis: ExtensionBasis) -> Result<Self> {
        match decode_published(&basis) {
            Ok(published) => Ok(Self::from_published(basis, published)),
            Err(error @ GrepEntryError::UnsupportedVersion { .. }) => {
                Err(invalid_entry(&basis, error))
            }
            Err(error) => {
                tracing::warn!(
                    namespace_id = %basis.manifest().namespace_id,
                    manifest_no = %basis.manifest().manifest_no,
                    %error,
                    "disabling grep replaces an invalid grep entry"
                );
                Ok(Self {
                    entry: Some(GrepEntry::disabled()),
                    ..Self::from_published(basis, None)
                })
            }
        }
    }

    /// Starts a change from `basis`, whose grep entry is `published`.
    fn from_published(basis: ExtensionBasis, published: Option<GrepEntry>) -> Self {
        let namespace_id = &basis.manifest().namespace_id;
        let entry = published
            .clone()
            .map(|entry| entry.without_inherited_reorganize(namespace_id));
        Self {
            basis,
            entry,
            published,
        }
    }

    /// Returns the entry that the basis lists, or `None` when grep was never
    /// enabled or when disabling replaces an entry that grep rejects.
    pub(super) fn published(&self) -> Option<&GrepEntry> {
        self.published.as_ref()
    }

    /// Returns the pending entry when the index is enabled.
    pub(super) fn enabled_entry(&self) -> Option<&GrepEntry> {
        self.entry
            .as_ref()
            .filter(|entry| !matches!(entry.status(), GrepIndexStatus::Disabled {}))
    }

    /// Replaces the pending entry with `entry`.
    ///
    /// # Panics
    ///
    /// Panics if `entry` doesn't follow the pending entry: each step keeps
    /// the build that it follows, or starts a later one, and doesn't move
    /// the indexing position back within a build. A change can
    /// hold more than one build, such as a backfill's restart and its
    /// completion, so each step's entry is checked against the one before it.
    pub(super) fn set_entry(&mut self, entry: GrepEntry) {
        if let Some(current) = &self.entry {
            assert!(
                current.is_followed_by(&entry),
                "a grep step should keep the build it follows or start a later one"
            );
        }
        self.entry = Some(entry);
    }

    /// Returns whether the steps changed the entry.
    fn is_changed(&self) -> bool {
        self.entry != self.published
    }
}

/// Decodes the grep entry that `basis` lists.
fn decode_published(
    basis: &ExtensionBasis,
) -> std::result::Result<Option<GrepEntry>, GrepEntryError> {
    basis
        .find_entry(&grep_extension_name())
        .map(GrepEntry::decode)
        .transpose()
}

/// Returns the error for the invalid grep entry of `basis`, where `error`
/// says why the entry is invalid.
fn invalid_entry(basis: &ExtensionBasis, error: GrepEntryError) -> GrepError {
    GrepError::invalid_entry(
        format_args!("manifest {}", basis.manifest().manifest_no),
        error,
    )
}

/// A run of grep's bounded steps over one namespace, whose changes to grep's
/// entry are kept in memory and published together in a manifest of their own
/// ([Grep extension format](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#appendix-d-grep-extension-format)).
///
/// Each step starts from the entry that the steps before it left pending, so
/// a step doesn't index again what an earlier step indexed. A step ends by
/// publishing the pending change once `publish_interval_ms` has passed since
/// the change loaded the manifest it builds on, and [`Self::publish`]
/// publishes what's pending. When another manifest follows that manifest
/// first with the same grep entry, the change is published over it. When
/// another process changed the entry, nothing is published, the step
/// returns `Superseded`, and the next step plans from the current manifest.
/// A run that's dropped before it publishes leaves its segments to the
/// collector.
pub struct GrepSteps<'w, S> {
    pub(super) worker: &'w GrepWorker<S>,
    pub(super) namespace_id: NamespaceId,
    publish_interval_ms: u64,
    pending: Option<PendingEntry>,
}

impl<'w, S: ObjectStore + Clone> GrepSteps<'w, S> {
    pub(super) fn new(
        worker: &'w GrepWorker<S>,
        namespace_id: &NamespaceId,
        publish_interval_ms: u64,
    ) -> Self {
        Self {
            worker,
            namespace_id: namespace_id.clone(),
            publish_interval_ms,
            pending: None,
        }
    }

    /// Runs the run's next step, which is a reorganize step when the pending
    /// entry has a reorganization in progress or a level at its run limit
    /// (`max_delta_runs` delta runs, or `max_mid_runs` mid runs), and a build
    /// step otherwise.
    ///
    /// A build step looks up its content values' `texts` rows in the
    /// segments that the entry lists, and each build step that indexes
    /// something adds a delta run, so a build step's cost grows with the
    /// entry's runs. The run reorganizes a full level before its next build
    /// step, so the delta level has at most `max_delta_runs` runs however far
    /// the index is behind.
    pub async fn step(&mut self, policy: GramIndexBuildPolicy) -> Result<GrepProgress> {
        let due = self
            .pending()
            .await?
            .enabled_entry()
            .is_some_and(|entry| reorganize_due(entry, policy));
        if due {
            return Ok(GrepProgress::Reorganized(
                self.reorganize_step(policy).await?,
            ));
        }
        Ok(GrepProgress::Built(self.build_step(policy).await?))
    }

    /// Returns the status of the entry that the run's steps left pending, or
    /// `None` before the run's first step or after a superseded publication.
    pub fn pending_status(&self) -> Option<&GrepIndexStatus> {
        self.pending.as_ref()?.entry.as_ref().map(GrepEntry::status)
    }

    /// Returns the pending change, which starts from the current manifest
    /// when nothing is pending.
    pub(super) async fn pending(&mut self) -> Result<&mut PendingEntry> {
        self.pending_from(PendingEntry::new).await
    }

    /// Returns a disable's pending change, which starts from the current
    /// manifest when nothing is pending, and replaces that manifest's grep
    /// entry when the entry is invalid (see [`PendingEntry::for_disable`]).
    pub(super) async fn pending_for_disable(&mut self) -> Result<&mut PendingEntry> {
        self.pending_from(PendingEntry::for_disable).await
    }

    /// Returns the pending change, and when nothing is pending, starts one
    /// from the current manifest with `start`.
    async fn pending_from(
        &mut self,
        start: fn(ExtensionBasis) -> Result<PendingEntry>,
    ) -> Result<&mut PendingEntry> {
        if self.pending.is_none() {
            let basis = self
                .worker
                .maintenance
                .load_extension_basis(&self.namespace_id)
                .await?;
            self.pending = Some(start(basis)?);
        }
        Ok(self
            .pending
            .as_mut()
            .expect("the run should have a pending change after loading one"))
    }

    /// Ends a step: publishes the pending change when `publish_interval_ms`
    /// has passed since the change loaded its basis, and returns
    /// `superseded` instead of `outcome` when another process changed the
    /// entry first.
    pub(super) async fn finish_step<T>(&mut self, outcome: T, superseded: T) -> Result<T> {
        let due = self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.basis.elapsed_ms() >= self.publish_interval_ms);
        if due && self.publish().await? == GrepPublishOutcome::Superseded {
            return Ok(superseded);
        }
        Ok(outcome)
    }

    /// Publishes the pending change, if the steps changed anything since the
    /// last publication.
    ///
    /// After a publication, the next step starts from the published manifest.
    /// After another process changed the entry, or after an error, the next
    /// step loads the current manifest. A change that's published or
    /// superseded is recorded in
    /// [`GrepWorker::publication_stats`].
    pub async fn publish(&mut self) -> Result<GrepPublishOutcome> {
        let Some(pending) = self.pending.take() else {
            return Ok(GrepPublishOutcome::NothingPending);
        };
        if !pending.is_changed() {
            self.pending = Some(pending);
            return Ok(GrepPublishOutcome::NothingPending);
        }
        let entry = pending
            .entry
            .as_ref()
            .expect("a changed pending change should have an entry");
        let publication = self
            .worker
            .maintenance
            .publish_extension(&pending.basis, &grep_extension_name(), entry.encode())
            .await?;
        let counts = &self.worker.publications;
        match publication {
            ExtensionPublication::Published(basis) => {
                let first_put = pending.basis.manifest().manifest_no.successor().ok()
                    == Some(basis.manifest().manifest_no);
                let outcome = if first_put {
                    &counts.published
                } else {
                    &counts.rebuilt
                };
                outcome.fetch_add(1, Ordering::Relaxed);
                self.pending = Some(PendingEntry::new(basis)?);
                Ok(GrepPublishOutcome::Published)
            }
            ExtensionPublication::Superseded => {
                counts.superseded.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    namespace_id = %self.namespace_id,
                    "another process changed the grep entry; the next step plans from the current \
                     manifest"
                );
                Ok(GrepPublishOutcome::Superseded)
            }
        }
    }
}
