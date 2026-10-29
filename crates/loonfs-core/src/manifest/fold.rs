//! Folds the visible WAL tail and publishes the next manifest number.

use super::load::{load_basis_metadata_segments, metadata_basis_from_manifest};
use super::publish::ManifestChange;
use super::published::{same_head, PublicationReport, PublishedManifest};
use super::runs::flatten_manifest_segments;
use super::scan::VerifiedMetadataSegments;
use crate::commit_engine::WalFoldInput;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::CoreError;
use crate::error::Result;
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::metadata::{MetadataState, MetadataView};
use crate::namespace::basis::{MetadataBasis, PublicationBasis};
use crate::namespace::control::LoadedManifest;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::storage::tail_content::{assemble_tail_content, write_tail_content};
use crate::time::{Deadline, Observation};
use crate::wal::replay_discovered_tail;
use crate::wal::ProjectedWalTail;
use crate::wal::WalPublishError;
use futures::{stream, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::format::manifest::{MetadataRunRef, NamespaceManifestPayload, RunTier};
use loonfs_types::{ChangeSeq, ContentId, ManifestNo, NamespaceId, RunNo};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::Instrument;

/// How one WAL fold ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldWalOutcome {
    /// A current manifest already folded the WAL tail, so nothing was
    /// published.
    AlreadyCurrent,
    /// This call published the next current manifest.
    Published,
}

/// The current manifest state after one WAL fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldWalResponse {
    /// Namespace whose WAL tail was folded.
    pub namespace_id: NamespaceId,
    /// Head sequence the fold attempted to cover.
    pub target_head_seq: ChangeSeq,
    /// Current manifest number after the operation.
    pub manifest_no: ManifestNo,
    /// Sequence covered by that manifest.
    pub manifest_head_seq: ChangeSeq,
    /// Whether this call published the current manifest.
    pub outcome: FoldWalOutcome,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Folds the visible WAL tail and publishes a
    /// manifest covering the current head.
    ///
    /// This is the latest-state maintenance operation: it absorbs the visible
    /// WAL tail into a new manifest, creating no
    /// pin. Superseded manifests become garbage-collection
    /// candidates once nothing pins them. If [`Self::with_writer_position`]
    /// set a position, the fold takes the tail from that position while the
    /// position's last check is within the revalidation bound. The report
    /// includes the manifest this call published.
    pub async fn fold_wal(&self) -> Result<PublicationReport<FoldWalResponse>> {
        let folded = self
            .namespace
            .fold_wal_tail(
                self.writer_position.clone(),
                &Deadline::start(Arc::clone(&self.namespace.timer)),
            )
            .await?;
        Ok(PublicationReport {
            published: folded.published(),
            response: folded.response,
        })
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Folds the visible WAL tail into segments and publishes the next manifest.
    #[cfg(test)]
    pub(crate) async fn fold_wal(&self) -> Result<FoldWalResponse> {
        let deadline = Deadline::start(Arc::new(crate::time::StdMonotonicTimer::default()));
        self.fold_wal_tail(None, &deadline)
            .await
            .map(|folded| folded.response)
    }

    async fn load_fold_projection(
        &self,
        observed: Observation,
    ) -> Result<ManifestProjection<'_, S>> {
        self.load_manifest_projection(observed)
            .instrument(tracing::debug_span!(
                "loonfs.phase",
                phase = "scan_namespace_state"
            ))
            .await
    }

    async fn materialize_tail_content(
        &self,
        projection: &ManifestProjection<'_, S>,
    ) -> Result<HashMap<ContentId, ContentLayoutRecord>> {
        let tail = projection.tail_state.as_ref();
        let assembled = tail
            .contents()
            .into_iter()
            .map(|content| {
                let pieces = assemble_tail_content(&content)?;
                Ok((content, pieces))
            })
            .collect::<Result<Vec<_>>>()?;
        let view = projection.metadata_view();
        let layouts = Mutex::new(HashMap::new());
        // In a live namespace, failed folds leave committed content for the next fold.
        stream::iter(assembled.into_iter().map(Ok))
            .try_for_each_concurrent(STORE_WRITES_IN_FLIGHT, |(content, pieces)| {
                let layouts = &layouts;
                async move {
                    let layout = write_tail_content(
                        &self.store,
                        &view,
                        &content,
                        pieces,
                        &self.content_writes,
                    )
                    .await?;
                    {
                        layouts
                            .lock()
                            .expect("layout lock should not be poisoned")
                            .insert(content.content_ref.content_id.clone(), layout);
                    }
                    Result::Ok(())
                }
            })
            .await?;
        Ok(layouts
            .into_inner()
            .expect("layout lock should not be poisoned"))
    }

    /// Folds the WAL tail and publishes the next manifest as the maintainer.
    ///
    /// The manifest copies both epochs from its predecessor (format spec,
    /// "Publishing a materialized file set"). A writer session passes the tail
    /// it holds as `held`, and every other caller passes `None`, which loads the
    /// tail from the store. A held tail whose last check is at the revalidation
    /// bound is discarded, and the tail is loaded from the store too: garbage
    /// collection keeps the manifest that the tail was read above for one grace
    /// after a successor, and a check within the bound is younger than that
    /// grace (see `update_manifest`). The publication's first attempt starts
    /// from the manifest that the tail was read above, and each later attempt
    /// starts from the manifest that the attempt before it found (see
    /// `update_manifest`).
    /// A current manifest that folds the tail finishes the fold, and any other
    /// manifest after the fold's own predecessor makes the fold rebuild against
    /// it. The rebuild keeps the tail it read when the manifest is at the same
    /// head (see `same_head`), and loads the tail again otherwise, which
    /// folds a new writer's log after the writer's claim. A namespace tombstone
    /// stops the fold.
    pub async fn fold_wal_tail(
        &self,
        held: Option<WalFoldInput>,
        deadline: &Deadline,
    ) -> Result<FoldedWalTail> {
        let held = held.filter(|held| held.basis_checked.is_within_revalidation_bound());
        let projection = match held {
            Some(held) => ManifestProjection {
                manifest_segments: load_basis_metadata_segments(
                    &self.store,
                    Some(&self.segment_cache),
                    &held.basis,
                )
                .await?
                .segments,
                head: held.head,
                basis: held.basis,
                basis_checked: held.basis_checked,
                tail_state: held.tail_state,
            },
            None => self.load_fold_projection(deadline.observe()).await?,
        };
        let basis = projection.manifest_segments.manifest();
        if basis.payload().folded_wal_no == projection.head.wal_no {
            // No store observation confirms the basis here, so the manifest has
            // no start observation.
            return Ok(FoldedWalTail::new(
                &self.namespace_id,
                projection.head.seq,
                PublishedManifest::new(basis, None),
                FoldWalOutcome::AlreadyCurrent,
            ));
        }
        let publication_basis = PublicationBasis {
            manifest: projection.manifest_segments.loaded_manifest(),
            checked: projection.basis_checked.clone(),
        };
        // Each attempt's future borrows the fold through this lock, because a
        // future that a closure returns can't borrow the closure's own state.
        let fold = tokio::sync::Mutex::new(FoldAttempts {
            namespace: self,
            deadline,
            projection,
            layouts: None,
        });
        let folded = self
            .update_manifest(
                Some(publication_basis),
                deadline,
                MAX_CONTENTION_ATTEMPTS,
                |current: LoadedManifest| {
                    let fold = &fold;
                    async move { fold.lock().await.attempt(current).await }
                },
            )
            .await;
        let (outcome, manifest, _) = match folded {
            Ok(folded) => folded,
            // Every attempt lost its manifest number to another publication.
            Err(CoreError::StoreContention { .. }) => {
                return Err(CoreError::WalPublish(WalPublishError::NumberTaken))
            }
            Err(error) => return Err(error),
        };
        // The call published the manifest, or found it current, after the
        // deadline started.
        Ok(FoldedWalTail::new(
            &self.namespace_id,
            fold.into_inner().projection.head.seq,
            PublishedManifest::new(&manifest.state.envelope, Some(deadline.origin())),
            outcome,
        ))
    }

    pub(super) async fn load_manifest_projection(
        &self,
        observed: Observation,
    ) -> Result<ManifestProjection<'_, S>> {
        let anchor = self
            .load_read_anchor(observed)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let basis = anchor.basis();
        let basis_checked = anchor.checked.clone();
        let head = anchor.read_state;
        crate::namespace::control::ensure_namespace_live(&head)?;
        let loaded_basis = super::load::metadata_basis_from_manifest(
            &self.store,
            Some(&self.segment_cache),
            &anchor.manifest,
        );
        let manifest_head = loaded_basis.replay_head(&head);
        let manifest_segments = loaded_basis.segments;
        let replayed =
            replay_discovered_tail(&manifest_head, &loaded_basis.base_state, &anchor.tail)
                .map_err(CoreError::MetadataProjection)?;
        Ok(ManifestProjection {
            head,
            basis,
            basis_checked,
            manifest_segments,
            tail_state: Arc::new(replayed.projected_tail),
        })
    }

    async fn build_namespace_manifest_for_projection(
        &self,
        projection: &ManifestProjection<'_, S>,
        layouts: &HashMap<ContentId, ContentLayoutRecord>,
        manifest_no: ManifestNo,
    ) -> Result<NamespaceManifestPayload> {
        let activity = projection
            .manifest_segments
            .manifest()
            .payload()
            .activity
            .checked_add(projection.tail_state.activity())
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(
                    "activity counter cannot exceed 9007199254740991".to_owned(),
                )
            })?;
        let head_seq = projection.head.seq;
        let tail_state = projection.tail_with_deletion_inodes().await?;
        // A WAL fold keeps existing runs and writes the WAL delta as one new delta
        // run. Compaction merges delta runs into the base separately.
        //
        let (runs, next_run_no) = if projection
            .manifest_segments
            .manifest()
            .payload()
            .runs
            .is_empty()
        {
            let run_no = RunNo(0);
            (
                vec![MetadataRunRef {
                    run_no,
                    run_seq: head_seq,
                    tier: RunTier::Base,
                    segments: flatten_manifest_segments(
                        self.build_manifest_segments(&tail_state, layouts).await?,
                    ),
                }],
                next_run_no_after(run_no)?,
            )
        } else {
            let previous_manifest = projection.manifest_segments.manifest();
            // The manifest that first names a run allocates its number. A fold
            // takes one number for its delta, or none when the head is unchanged.
            let run_no = previous_manifest.payload().next_run_no;
            let mut runs = previous_manifest.payload().runs.clone();
            let mut next_run_no = run_no;
            if previous_manifest.payload().head_seq < head_seq {
                runs.push(MetadataRunRef {
                    run_no,
                    run_seq: head_seq,
                    tier: RunTier::Delta,
                    segments: flatten_manifest_segments(
                        self.build_manifest_delta_run_segments(
                            previous_manifest.payload().head_seq,
                            &tail_state,
                            layouts,
                        )
                        .await?,
                    ),
                });
                next_run_no = next_run_no_after(run_no)?;
            }
            (runs, next_run_no)
        };

        Ok(NamespaceManifestPayload {
            activity,
            manifest_no,
            head_seq,
            writer_epoch: projection.head.writer_epoch,
            next_inode_id: projection.head.next_inode_id,
            next_run_no,
            folded_wal_no: projection.head.wal_no,
            acknowledged_floor_seq: acknowledged_floor_after_fold(projection)?,
            runs,
            ..projection.manifest_segments.manifest().payload().clone()
        })
    }
}

/// Whether `current` already folds the WAL tail that ends at `head`.
fn covers_tail(current: &NamespaceManifestPayload, head: &NamespaceReadState) -> bool {
    current.folded_wal_no >= head.wal_no && current.head_seq >= head.seq
}

pub struct FoldedWalTail {
    pub response: FoldWalResponse,
    /// The manifest that covers the tail after the call: the one it
    /// published, or the current one that already covered it.
    pub manifest: PublishedManifest,
}

impl FoldedWalTail {
    fn new(
        namespace_id: &NamespaceId,
        target_head_seq: ChangeSeq,
        manifest: PublishedManifest,
        outcome: FoldWalOutcome,
    ) -> Self {
        Self {
            response: FoldWalResponse {
                namespace_id: namespace_id.clone(),
                target_head_seq,
                manifest_no: manifest.manifest.manifest_no,
                manifest_head_seq: manifest.manifest.head_seq,
                outcome,
            },
            manifest,
        }
    }

    /// The manifest this fold published, or `None` when it published none.
    pub(crate) fn published(&self) -> Option<PublishedManifest> {
        (self.response.outcome == FoldWalOutcome::Published).then(|| self.manifest.clone())
    }
}

/// One fold's state across its publication attempts.
struct FoldAttempts<'a, S: ObjectStore> {
    namespace: &'a NamespaceStore<S>,
    deadline: &'a Deadline,
    projection: ManifestProjection<'a, S>,
    /// The content objects written for the projection's tail, which a
    /// rebuild over a manifest at the same head uses again.
    layouts: Option<HashMap<ContentId, ContentLayoutRecord>>,
}

impl<S: ObjectStore> FoldAttempts<'_, S> {
    /// Returns the next manifest over `current`, or `Finished` when `current`
    /// already folds the tail.
    async fn attempt(&mut self, current: LoadedManifest) -> Result<ManifestChange<FoldWalOutcome>> {
        let payload = current.state.envelope.payload();
        if payload.status.is_deleted() {
            return Err(CoreError::NamespaceDeleted {
                namespace_id: self.namespace.namespace_id.clone(),
            });
        }
        if covers_tail(payload, &self.projection.head) {
            return Ok(ManifestChange::Finished(FoldWalOutcome::AlreadyCurrent));
        }
        // Another manifest followed the fold's own predecessor without
        // folding its tail, such as a compaction or a new writer's claim.
        // Over a manifest at the same head, everything the fold holds is
        // still right:
        // - Tail rows: see `same_head`.
        // - Deletion roots: the fold reads each one through the winner's
        //   runs, which hold the same view at `head_seq`. A compaction keeps
        //   every view at or above the floor, and the floor is at most
        //   `head_seq`.
        // - Activity: a successor at the same `head_seq` keeps it, so the
        //   basis activity plus the tail's is still the total.
        // - Run numbers: the retry takes its runs and run number from the
        //   winner.
        // - Content: the first attempt's extents and layouts still describe
        //   this tail.
        if payload.manifest_no != self.projection.basis.manifest_no() {
            if same_head(
                &self.projection.basis,
                &self.projection.head,
                &PublishedManifest::new(&current.state.envelope, None),
            ) {
                self.projection.basis = MetadataBasis(current.state.manifest());
                self.projection.manifest_segments = metadata_basis_from_manifest(
                    &self.namespace.store,
                    Some(&self.namespace.segment_cache),
                    &current,
                )
                .segments;
            } else {
                self.projection = self
                    .namespace
                    .load_fold_projection(self.deadline.observe())
                    .await?;
                self.layouts = None;
                // The tail was read above a manifest newer than `current`.
                if self.projection.basis.manifest_no() != payload.manifest_no {
                    return Ok(ManifestChange::Again);
                }
                if covers_tail(payload, &self.projection.head) {
                    return Ok(ManifestChange::Finished(FoldWalOutcome::AlreadyCurrent));
                }
            }
        }
        let layouts = match self.layouts.take() {
            Some(layouts) => layouts,
            None => {
                self.namespace
                    .materialize_tail_content(&self.projection)
                    .await?
            }
        };
        self.deadline
            .ensure_metadata_publication_budget(&self.namespace.namespace_id)?;
        let manifest = self
            .namespace
            .build_namespace_manifest_for_projection(
                &self.projection,
                &layouts,
                next_manifest_no_after(payload.manifest_no)?,
            )
            .await?;
        self.layouts = Some(layouts);
        Ok(ManifestChange::Next(
            Box::new(manifest),
            FoldWalOutcome::Published,
        ))
    }
}

pub(super) struct ManifestProjection<'a, S: ObjectStore + ?Sized> {
    pub(super) head: NamespaceReadState,
    pub(super) basis: MetadataBasis,
    /// When a store observation last confirmed `basis` current.
    pub(super) basis_checked: Observation,
    pub(super) manifest_segments: VerifiedMetadataSegments<'a, S>,
    /// Rows that are not in any segment yet: the genesis root inode when the
    /// basis is genesis, plus the replayed WAL tail.
    pub(super) tail_state: Arc<ProjectedWalTail>,
}

impl<S: ObjectStore + ?Sized> ManifestProjection<'_, S> {
    pub(super) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.manifest_segments, &self.tail_state)
    }

    pub(super) async fn tail_with_deletion_inodes(&self) -> Result<MetadataState> {
        let mut tail = self.tail_state.merged_rows();
        let view = self.metadata_view();
        for tombstone in self.tail_state.subtree_tombstones() {
            if tail
                .inode_at_seq(tombstone.root_inode_id, self.head.seq)
                .is_some()
            {
                continue;
            }
            let inode = view
                .inode_at_seq(tombstone.root_inode_id)
                .await?
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "deletion root inode `{}` is missing",
                        tombstone.root_inode_id
                    ))
                })?;
            tail.push_inode_record(inode);
        }
        Ok(tail)
    }
}

pub(super) fn next_manifest_no_after(current: ManifestNo) -> Result<ManifestNo> {
    current.successor().map_err(|_| {
        CoreError::NamespaceCorrupt(format!(
            "the current manifest is number {current}, the largest manifest number, so no \
             manifest can follow it"
        ))
    })
}

/// Advances the manifest's run allocator after a producer has taken
/// `current`.
pub fn next_run_no_after(current: RunNo) -> Result<RunNo> {
    current.successor().map_err(|_| {
        CoreError::NamespaceCorrupt(format!(
            "the manifest's next run number is {current}, the largest run number, so no \
             run can follow it"
        ))
    })
}

/// Returns the acknowledged floor of a fold's manifest: the predecessor's,
/// raised to the basis floor of the last WAL object the fold folds.
///
/// Basis floors never decrease along the WAL, so the last object's floor is
/// the highest one the fold folds. A floor above the predecessor's retention
/// floor is `NamespaceCorrupt`, because every folded object was validated
/// against a manifest at or before the predecessor (see the design note's
/// [Acknowledging the floor](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#acknowledging-the-floor)).
fn acknowledged_floor_after_fold<S: ObjectStore + ?Sized>(
    projection: &ManifestProjection<'_, S>,
) -> Result<ChangeSeq> {
    let predecessor = projection.manifest_segments.manifest().payload();
    let acknowledged_floor_seq = predecessor
        .acknowledged_floor_seq
        .max(projection.head.basis_floor_seq);
    if acknowledged_floor_seq > predecessor.retention_floor_seq {
        return Err(CoreError::NamespaceCorrupt(format!(
            "WAL object {} records basis floor {}, above the retention floor {} of manifest {}",
            projection.head.wal_no,
            projection.head.basis_floor_seq,
            predecessor.retention_floor_seq,
            predecessor.manifest_no
        )));
    }
    Ok(acknowledged_floor_seq)
}

#[cfg(test)]
mod ordinal_tests {
    use super::*;
    use loonfs_types::MAX_PUBLIC_INTEGER;
    use std::assert_matches;

    #[test]
    fn manifest_no_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        assert_eq!(
            next_manifest_no_after(ManifestNo(MAX_PUBLIC_INTEGER - 1))
                .expect("advance to public maximum"),
            ManifestNo(MAX_PUBLIC_INTEGER)
        );

        let error = next_manifest_no_after(ManifestNo(MAX_PUBLIC_INTEGER))
            .expect_err("manifest number must not exceed the public maximum");
        assert_matches!(
            error,
            CoreError::NamespaceCorrupt(message) if message.contains("largest manifest number")
        );
    }
}

#[cfg(test)]
#[path = "tests/segment_puts.rs"]
mod segment_puts;
