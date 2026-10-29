//! [`GrepService`]: query planning and execution over grep index state.

mod candidates;
mod page_walk;

use crate::cache::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey};
use crate::keyspace::manifest_key;
use crate::manifest::{
    load_current_grep_manifest, ChangeFeedResume, GrepIndexStatus, GrepManifestState,
};
use crate::query::{compile_pattern, CompiledPattern, GramPlanOutcome};
use crate::reads::{NamespaceReadView, NamespaceReads};
use crate::segments::GrepSegments;
use crate::{GrepError, Result};
use candidates::{scan_candidate_inodes, tail_revisions, GrepCandidates, TailScan};
use loonfs::engine::{Observation, SegmentBlockKind};
use loonfs::CoreError;
use loonfs_objectstore::ObjectStore;
use loonfs_types::MonotonicTimer;
use loonfs_types::{
    decode_cursor, ChangeSeq, GrepPageCursor, GrepRequest, GrepResponse, GrepScope, NamespaceId,
    PageRequest, PathEntry, MAX_PAGE_LIMIT,
};
use page_walk::{walk_page, GrepResume};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

/// Unindexed-tail revisions one query will scan exhaustively before
/// failing with `index_lagging` (or skipping the tail under `allow_stale`).
/// Advertised as the `query.grep.tail_budget_files` capability limit.
pub const MAX_GREP_TAIL_FILES: usize = 512;
/// Files a plan-less `allow_scan` query will scan before refusing.
/// Advertised as the `query.grep.scan_budget_files` capability limit.
pub const MAX_GREP_SCAN_FILES: usize = 4096;
/// Longest match line returned, in bytes; longer lines are truncated.
pub(crate) const GREP_LINE_CAP_BYTES: usize = 512;
/// Candidate files one page will read and verify before returning with a
/// resume cursor, so a page's cost is bounded by its own budget rather
/// than by how many false-positive candidates the plan admits.
pub(crate) const MAX_GREP_VERIFIED_FILES_PER_PAGE: usize = 256;
/// Maximum candidates examined while building one page. This bounds work
/// when scope filters reject most candidates. The cursor advances past
/// rejected candidates so the next page does not examine them again.
pub(crate) const MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE: usize = 4096;
/// Maximum concurrent `(gram, segment)` posting probes, and the data-block
/// reads that one probe keeps in flight.
///
/// Each probe may issue several small ranged reads, so this limit is lower
/// than the whole-content read limit. A query's index lookup keeps at most
/// 16 probes times 16 data-block reads, or 256 reads, in flight.
pub(crate) const GREP_READS_IN_FLIGHT: usize = 16;
/// Commits one unindexed-tail page asks the change feed for. The tail is
/// measured in files, not commits, so this is a transfer size rather than a
/// budget: paging continues until the tail is exhausted or exceeds
/// [`MAX_GREP_TAIL_FILES`].
pub(crate) const TAIL_FEED_PAGE_COMMITS: usize = 256;
/// Directory entries one plan-less scan page reads. The scan is bounded by
/// [`MAX_GREP_SCAN_FILES`]; this only sizes the reads that reach it.
pub(crate) const SCAN_DIRECTORY_PAGE_ENTRIES: usize = 1000;
const _: () = assert!(
    0 < TAIL_FEED_PAGE_COMMITS
        && TAIL_FEED_PAGE_COMMITS <= MAX_PAGE_LIMIT as usize
        && 0 < SCAN_DIRECTORY_PAGE_ENTRIES
        && SCAN_DIRECTORY_PAGE_ENTRIES <= MAX_PAGE_LIMIT as usize
);
/// Maximum concurrent content reads while verifying grep candidates.
///
/// Indexed files are size-limited, so content reads may use more concurrency
/// than posting probes. The per-page verification budget still caps the total
/// number of content reads.
pub(crate) const GREP_CONTENT_READS_IN_FLIGHT: usize = 32;

/// Namespace-independent grep execution over a decoded-block cache.
#[derive(Debug)]
pub struct GrepService {
    block_cache: Arc<GrepBlockCache>,
    /// An insert removes the entries whose manifest was dropped once the map
    /// contains more than twice the entries the last removal kept. Only the
    /// block cache and the queries in progress keep a manifest alive, so the
    /// map contains at most one more than twice the manifests those kept at the
    /// last removal.
    current_manifests: Mutex<CurrentManifests>,
    timer: Arc<dyn MonotonicTimer>,
}

/// The newest manifest the service confirmed for each namespace it queried,
/// and when it confirmed it.
#[derive(Debug, Default)]
struct CurrentManifests {
    entries: BTreeMap<NamespaceId, (Weak<GrepManifestState>, Observation)>,
    entries_after_prune: usize,
}

impl CurrentManifests {
    /// Finds the namespace's confirmed manifest, and removes an entry whose
    /// manifest was dropped.
    fn find(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Option<(Arc<GrepManifestState>, Observation)> {
        let (state, checked) = self.entries.get(namespace_id)?;
        let Some(state) = state.upgrade() else {
            self.entries.remove(namespace_id);
            return None;
        };
        Some((state, checked.clone()))
    }

    /// Records `state` as confirmed at `observed`, unless the entry has a
    /// newer manifest.
    fn insert(
        &mut self,
        namespace_id: &NamespaceId,
        state: &Arc<GrepManifestState>,
        observed: Observation,
    ) {
        if self
            .entries
            .get(namespace_id)
            .and_then(|(cached, _)| cached.upgrade())
            .is_some_and(|cached| cached.manifest_no() > state.manifest_no())
        {
            return;
        }
        self.entries
            .insert(namespace_id.clone(), (Arc::downgrade(state), observed));
        if self.entries.len() > self.entries_after_prune.saturating_mul(2) {
            self.entries
                .retain(|_, (state, _)| state.strong_count() > 0);
            self.entries_after_prune = self.entries.len();
        }
    }
}

#[derive(Debug, Clone)]
struct MaterializedGrepIndexSnapshot {
    resume: ChangeFeedResume,
    state: Arc<GrepManifestState>,
}

impl GrepService {
    /// Creates a service over a host-composed process-wide grep block cache,
    /// whose manifest checks run on `timer`, the runtime's monotonic timer.
    pub fn new(block_cache: Arc<GrepBlockCache>, timer: Arc<dyn MonotonicTimer>) -> Self {
        Self {
            block_cache,
            current_manifests: Mutex::new(CurrentManifests::default()),
            timer,
        }
    }

    async fn load_index_snapshot<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        namespace_id: &NamespaceId,
    ) -> Result<MaterializedGrepIndexSnapshot> {
        let cached = self
            .current_manifests
            .lock()
            .expect("grep manifest cache lock should not be poisoned")
            .find(namespace_id)
            .filter(|(_, checked)| checked.is_within_revalidation_bound());
        let observed = Observation::now(Arc::clone(&self.timer));
        let confirmed = match cached {
            Some((state, checked)) => {
                let successor_present = match state.manifest_no().successor() {
                    Ok(next) => {
                        let object_key = manifest_key(namespace_id, &next);
                        store
                            .head(&object_key)
                            .await
                            .map_err(|error| GrepError::store(object_key, &error))?
                            .is_some()
                    }
                    Err(_) => false,
                };
                (!successor_present && checked.is_within_revalidation_bound()).then_some(state)
            }
            None => None,
        };
        let state = match confirmed {
            Some(state) => state,
            None => {
                let current = load_current_grep_manifest(store, namespace_id, observed.clone())
                    .await?
                    .ok_or(GrepError::NotEnabled)?;
                let state = Arc::new(current.manifest_state().clone());
                let decoded_bytes = serde_json::to_vec(current.manifest_state())
                    .expect("a decoded grep manifest state should encode")
                    .len()
                    .saturating_mul(2);
                self.block_cache.insert(
                    GrepBlockCacheKey {
                        identity: manifest_key(namespace_id, &state.manifest_no()),
                        block_kind: SegmentBlockKind::Manifest,
                        block_offset: 0,
                    },
                    DecodedGrepBlock::Manifest {
                        manifest: state.clone(),
                        decoded_bytes,
                    },
                );
                state
            }
        };
        self.current_manifests
            .lock()
            .expect("grep manifest cache lock should not be poisoned")
            .insert(namespace_id, &state, observed);
        materialized_snapshot_from_state(state)
    }

    async fn plan_query<'a, S: ObjectStore>(
        &self,
        request: &GrepRequest,
        cursor: Option<&str>,
        namespace_reads: &'a NamespaceReads,
        store: &S,
    ) -> Result<QueryPlan<'a>> {
        validate_grep_request(request)?;
        let mut reads = namespace_reads.read_view().await?;
        let snapshot = self
            .load_index_snapshot(store, reads.namespace_id())
            .await?;
        if snapshot.resume.built_through_seq() > reads.head_seq() {
            reads = namespace_reads.read_view().await?;
        }
        let head_seq = reads.head_seq();
        let fingerprint = request.fingerprint();
        let resume = match cursor {
            Some(cursor) => {
                let cursor: GrepPageCursor = decode_cursor(cursor)
                    .map_err(|error| CoreError::InvalidCursor(error.to_string()))?;
                if cursor.fingerprint != fingerprint {
                    return Err(CoreError::InvalidCursor(
                        "the cursor was issued by a different request; replaying it under new criteria would silently skip results"
                            .to_owned(),
                    )
                    .into());
                }
                if cursor.head_seq > head_seq {
                    return Err(CoreError::InvalidCursor(format!(
                        "the cursor was minted at seq `{}`, ahead of the serving head `{head_seq}`",
                        cursor.head_seq
                    ))
                    .into());
                }
                Some(GrepResume::from_cursor(&cursor))
            }
            None => None,
        };
        let CompiledPattern {
            regex: pattern,
            plan: gram_plan,
        } = compile_pattern(request)?;
        let scope = match &request.scope {
            GrepScope::PathPrefix(prefix) => Some(reads.resolve_path(prefix).await?),
            GrepScope::Inode(inode_id) => Some(reads.resolve_inode(*inode_id).await?),
            GrepScope::Namespace => None,
        };
        let mut candidates = GrepCandidates::default();
        let tail_resume = match gram_plan {
            GramPlanOutcome::Indexable(plan) => {
                let segments = GrepSegments {
                    store,
                    namespace_id: reads.namespace_id(),
                    block_cache: Some(&self.block_cache),
                };
                candidates.indexed = segments
                    .indexed_candidates(snapshot.state.segments(), &plan)
                    .await?;
                Some(snapshot.resume)
            }
            GramPlanOutcome::Unindexable => {
                if !request.allow_scan {
                    return Err(GrepError::QueryUnindexable(
                        "the pattern has no run of at least 3 literal bytes for the trigram index; set `allow_scan` to search without it"
                            .to_owned(),
                    ));
                }
                candidates.unfiltered = scan_candidate_inodes(&reads, scope.as_ref()).await?;
                None
            }
        };
        let mut tail_scanned = true;
        if let Some(tail_resume) = tail_resume {
            match tail_revisions(&reads, tail_resume).await? {
                TailScan::Within(inodes) => candidates.unfiltered.extend(inodes),
                TailScan::OverBudget if request.allow_stale => {
                    tail_scanned = false;
                }
                TailScan::OverBudget => {
                    return Err(GrepError::IndexLagging {
                        behind_commits: head_seq
                            .0
                            .saturating_sub(snapshot.resume.built_through_seq().0),
                    });
                }
            }
        }
        Ok(QueryPlan {
            reads,
            head_seq,
            built_through_seq: snapshot.resume.built_through_seq(),
            fingerprint,
            resume,
            pattern,
            scope,
            candidates,
            tail_scanned,
        })
    }

    /// Content search over one namespace read view, for the page at
    /// `page.cursor`.
    ///
    /// A cursor resumes only the request whose page returned it, and the
    /// search checks it against the current namespace head.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "debug"),
        skip_all,
        fields(phase = "grep")
    )]
    pub async fn query<S: ObjectStore>(
        &self,
        request: &GrepRequest,
        page: PageRequest<String>,
        reads: &NamespaceReads,
        store: &S,
    ) -> Result<GrepResponse> {
        let plan = self
            .plan_query(request, page.cursor.as_deref(), reads, store)
            .await?;
        let page = walk_page(&plan, page.limit.as_usize()).await?;
        Ok(GrepResponse {
            namespace_id: plan.reads.namespace_id().clone(),
            head_seq: plan.head_seq,
            built_through_seq: plan.built_through_seq,
            tail_scanned: plan.tail_scanned,
            matches: page.matches,
            next_cursor: page.next_cursor,
        })
    }
}

/// Checks the shape of a grep request: the pattern length and the scope fields.
fn validate_grep_request(request: &GrepRequest) -> Result<()> {
    const MAX_GREP_PATTERN_BYTES: usize = 1024;
    if request.pattern.len() > MAX_GREP_PATTERN_BYTES {
        return Err(loonfs::Error::InvalidRequest {
            message: format!(
                "grep pattern is {} bytes; the maximum is {MAX_GREP_PATTERN_BYTES} bytes",
                request.pattern.len()
            ),
            param: "pattern",
        }
        .into());
    }
    Ok(())
}

fn materialized_snapshot_from_state(
    state: Arc<GrepManifestState>,
) -> Result<MaterializedGrepIndexSnapshot> {
    // Queries require an active index and its watermark. Disabled and
    // backfilling indexes return their corresponding errors.
    let resume = match state.status() {
        GrepIndexStatus::Disabled {} => return Err(GrepError::NotEnabled),
        GrepIndexStatus::Backfilling { .. } => return Err(GrepError::Backfilling),
        GrepIndexStatus::Active { .. } => state
            .status()
            .active_watermark()
            .expect("an active grep status should have a watermark"),
    };
    Ok(MaterializedGrepIndexSnapshot { resume, state })
}

struct QueryPlan<'a> {
    reads: NamespaceReadView<'a>,
    head_seq: ChangeSeq,
    built_through_seq: ChangeSeq,
    fingerprint: u64,
    resume: Option<GrepResume>,
    pattern: regex::bytes::Regex,
    scope: Option<PathEntry>,
    candidates: GrepCandidates,
    tail_scanned: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_cached_manifest_checked_longer_ago_than_the_bound_is_discovered_again() {
        use crate::manifest::{publish_grep_manifest, GrepIndexState};
        use loonfs::engine::READ_REVALIDATION_BOUND_MS;
        use loonfs_objectstore::local_fs_store::LocalFsStore;
        use loonfs_test_support::clock::ManualClock;
        use loonfs_types::StdMonotonicTimer;
        use loonfs_types::{ManifestNo, RunNo};

        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        let namespace_id = loonfs_test_support::ids::namespace_id("grep-revalidation");
        let deadline =
            loonfs::engine::OperationClock::start(0, Arc::new(StdMonotonicTimer::default()));
        let manifest = |manifest_no| {
            GrepManifestState::new(
                namespace_id.clone(),
                ManifestNo(manifest_no),
                GrepIndexStatus::Active {
                    built_through_seq: ChangeSeq(0),
                    next_event_index: 0,
                },
                GrepIndexState {
                    next_run_no: RunNo(0),
                    reorganize: None,
                },
                Vec::new(),
            )
            .expect("manifest")
        };
        let mut current = publish_grep_manifest(&store, None, &manifest(1), &deadline)
            .await
            .expect("publish");
        let service = GrepService::new(
            Arc::new(GrepBlockCache::new(
                loonfs::engine::DecodedBlockCacheConfig::new(
                    crate::DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
                ),
            )),
            Arc::new(StdMonotonicTimer::default()),
        );
        let snapshot = || async {
            service
                .load_index_snapshot(&store, &namespace_id)
                .await
                .expect("snapshot")
                .state
                .manifest_no()
        };
        assert_eq!(snapshot().await, ManifestNo(1));
        for manifest_no in 2..=3 {
            current =
                publish_grep_manifest(&store, Some(&current), &manifest(manifest_no), &deadline)
                    .await
                    .expect("publish");
        }
        store
            .delete(&manifest_key(&namespace_id, &ManifestNo(2)))
            .await
            .expect("collect");
        assert_eq!(snapshot().await, ManifestNo(1));
        let clock = Arc::new(ManualClock::new(0));
        service
            .current_manifests
            .lock()
            .expect("lock")
            .entries
            .get_mut(&namespace_id)
            .expect("cached manifest")
            .1 = Observation::now(clock.clone());
        clock.advance_ms(READ_REVALIDATION_BOUND_MS);
        assert_eq!(snapshot().await, ManifestNo(3));
    }
}
