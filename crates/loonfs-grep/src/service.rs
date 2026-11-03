//! [`GrepService`]: query planning and execution over grep index state.

mod candidates;
mod page_walk;
mod value_files;

use crate::cache::{DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey};
use crate::keyspace::manifest_key;
use crate::manifest::{
    load_current_grep_manifest, ChangeFeedResume, GrepIndexStatus, GrepManifestState,
};
use crate::query::{compile_pattern, CompiledPattern, GramPlanOutcome};
use crate::reads::{NamespaceReadView, NamespaceReads};
use crate::segments::GrepSegments;
use crate::{GrepError, Result};
use candidates::{add_tail_files, scan_candidate_files, tail_revisions, GrepCandidates, TailScan};
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
/// Candidate content values whose bytes one page will read and verify before
/// returning with a resume cursor, so a page's cost is bounded by its own
/// budget rather than by how many false-positive candidates the plan admits.
/// The files of each value count against
/// [`MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE`].
pub(crate) const MAX_GREP_VERIFIED_VALUES_PER_PAGE: usize = 256;
/// Maximum candidate files examined while building one page. This bounds work
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
    /// The sequence that the index's backfill captured, which identifies
    /// the build that a cursor records.
    captured_seq: ChangeSeq,
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
        &'a self,
        request: &GrepRequest,
        cursor: Option<&str>,
        namespace_reads: &'a NamespaceReads,
        store: &'a S,
    ) -> Result<QueryPlan<'a, S>> {
        validate_grep_request(request)?;
        // The index loads before the read view, so the view's head is at or
        // above every sequence that the index records, such as its build's
        // capture and a reorganization's pruning floor.
        let snapshot = self
            .load_index_snapshot(store, namespace_reads.namespace_id())
            .await?;
        let head = namespace_reads.read_view().await?;
        let head_seq = head.head_seq();
        let fingerprint = request.fingerprint();
        let (reads, resume, cursor_cut_seq, later) = match cursor {
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
                if cursor.index_captured_seq != snapshot.captured_seq {
                    return Err(CoreError::InvalidCursor(
                        "the cursor was issued by an earlier build of the grep index; start the search again"
                            .to_owned(),
                    )
                    .into());
                }
                // Every page after the first reads at the first page's
                // sequence, so a file whose content changed between pages
                // keeps its place in content order.
                let at_cursor = head.at_seq(cursor.head_seq).await?;
                let later = LaterPage {
                    members: namespace_reads.membership_view(cursor.head_seq).await?,
                    head,
                };
                (
                    at_cursor,
                    Some(GrepResume::from_cursor(&cursor)),
                    Some(cursor.cut_seq),
                    Some(later),
                )
            }
            None => (head, None, None, None),
        };
        let read_seq = reads.head_seq();
        let CompiledPattern {
            regex: pattern,
            plan: gram_plan,
        } = compile_pattern(request)?;
        // A later page decides its cut at its read sequence as the service,
        // and that includes the scope.
        let member_reads = later.as_ref().map_or(&reads, |later| &later.members);
        let scope = match &request.scope {
            GrepScope::PathPrefix(prefix) => Some(member_reads.resolve_path(prefix).await?),
            GrepScope::Inode(inode_id) => Some(member_reads.resolve_inode(*inode_id).await?),
            GrepScope::Namespace => None,
        };
        let index = GrepSegments {
            store,
            namespace_id: namespace_reads.namespace_id(),
            block_cache: Some(&self.block_cache),
        };
        let mut candidates = GrepCandidates::default();
        let mut cut_seq = read_seq;
        let mut tail_scanned = true;
        match gram_plan {
            GramPlanOutcome::Indexable(plan) => {
                candidates.indexed = index
                    .indexed_candidates(snapshot.state.segments(), &plan)
                    .await?;
                match cursor_cut_seq.filter(|cursor_cut_seq| *cursor_cut_seq < read_seq) {
                    // An earlier page of this stale search left out the
                    // commits after its cut, so this page leaves them out
                    // too, however far the index has moved since. The cut
                    // stays at or below what this page's index covers in
                    // full.
                    Some(cursor_cut_seq) => {
                        tail_scanned = false;
                        cut_seq = cursor_cut_seq.min(stale_cut_seq(snapshot.resume));
                    }
                    None => match tail_revisions(&reads, snapshot.resume).await? {
                        TailScan::Within(tail) => {
                            add_tail_files(
                                &index,
                                snapshot.state.segments(),
                                &mut candidates,
                                tail,
                            )
                            .await?;
                        }
                        TailScan::OverBudget if request.allow_stale => {
                            tail_scanned = false;
                            cut_seq = cut_seq.min(stale_cut_seq(snapshot.resume));
                        }
                        TailScan::OverBudget => {
                            return Err(GrepError::IndexLagging {
                                behind_commits: read_seq
                                    .0
                                    .saturating_sub(snapshot.resume.built_through_seq().0),
                            });
                        }
                    },
                }
            }
            GramPlanOutcome::Unindexable => {
                if !request.allow_scan {
                    return Err(GrepError::QueryUnindexable(
                        "the pattern has no run of at least 3 literal bytes for the trigram index; set `allow_scan` to search without it"
                            .to_owned(),
                    ));
                }
                candidates.files = scan_candidate_files(member_reads, scope.as_ref()).await?;
            }
        }
        Ok(QueryPlan {
            reads,
            later,
            read_seq,
            built_through_seq: snapshot.resume.built_through_seq(),
            index_captured_seq: snapshot.captured_seq,
            fingerprint,
            resume,
            pattern,
            scope,
            candidates,
            tail_scanned,
            cut_seq,
            index,
            index_state: snapshot.state,
        })
    }

    /// Content search over one namespace read view, for the page at
    /// `page.cursor`.
    ///
    /// A cursor resumes only the request whose page returned it. A first page
    /// reads at the namespace head, and a later page reads at the sequence
    /// that the first page read at, which the cursor records.
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
            head_seq: plan.read_seq,
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
    let (captured_seq, resume) = match state.status() {
        GrepIndexStatus::Disabled {} => return Err(GrepError::NotEnabled),
        GrepIndexStatus::Backfilling { .. } => return Err(GrepError::Backfilling),
        status @ GrepIndexStatus::Active { captured_seq, .. } => (
            *captured_seq,
            status
                .active_watermark()
                .expect("an active grep status should have a watermark"),
        ),
    };
    Ok(MaterializedGrepIndexSnapshot {
        captured_seq,
        resume,
        state,
    })
}

/// Everything one page of a query reads with: the read view, the candidates,
/// and the index that the candidates' files are read from.
struct QueryPlan<'a, S> {
    /// The subject's view at the read sequence. A later page reads the files
    /// that changed since its read sequence through it, which needs the
    /// subject's `history` right.
    reads: NamespaceReadView<'a>,
    /// The other views of a page after the first.
    later: Option<LaterPage<'a>>,
    /// The sequence that the page reads at: the head for a first page, and
    /// the cursor's sequence for a later page.
    read_seq: ChangeSeq,
    built_through_seq: ChangeSeq,
    /// The sequence that the index's backfill captured, which the page's
    /// cursor records.
    index_captured_seq: ChangeSeq,
    fingerprint: u64,
    resume: Option<GrepResume>,
    pattern: regex::bytes::Regex,
    scope: Option<PathEntry>,
    candidates: GrepCandidates,
    tail_scanned: bool,
    /// The newest commit whose `content_revisions` rows the page uses: the
    /// read sequence, or with `allow_stale` and a tail past its budget, the
    /// last commit that the index covers in full, if that's lower.
    cut_seq: ChangeSeq,
    index: GrepSegments<'a, S>,
    index_state: Arc<GrepManifestState>,
}

/// The views of a page after the first, which reads at its cursor's
/// sequence.
///
/// The page decides its cut at that sequence through `members`, as the
/// service. A file in the cut whose state at the head is its state at the
/// read sequence (the same revision and path) is current content, which the
/// page serves through `head` with the subject's rights there, as a first
/// page would. The page serves any other file in the cut through the
/// subject's view at the read sequence, so only to a subject with the
/// `history` right.
struct LaterPage<'a> {
    members: NamespaceReadView<'a>,
    head: NamespaceReadView<'a>,
}

/// Returns the last commit that an index at `resume` covers in full, which a
/// stale query treats as the end of what it scanned. The commit at the
/// watermark is left out while the index covers only part of it.
fn stale_cut_seq(resume: ChangeFeedResume) -> ChangeSeq {
    match resume.next_event_index() {
        0 => resume.built_through_seq(),
        _ => ChangeSeq(resume.built_through_seq().0.saturating_sub(1)),
    }
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
                    captured_seq: ChangeSeq(0),
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
