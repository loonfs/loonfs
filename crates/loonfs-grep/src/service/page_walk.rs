//! One grep page: the walk over the plan's candidates in content order, the
//! content reads, the matches, and the cursor of the next page.

use super::candidates::{CandidateValue, CandidateValues};
use super::value_files::ValueFiles;
use super::{
    QueryPlan, GREP_CONTENT_READS_IN_FLIGHT, GREP_LINE_CAP_BYTES, GREP_READS_IN_FLIGHT,
    MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE, MAX_GREP_VERIFIED_VALUES_PER_PAGE,
};
use crate::codec::{is_indexable_text_content, ContentValue, MAX_INDEX_GRAMS_FILE_BYTES};
use crate::reads::effective_batch_size;
use crate::Result;
use futures::stream::{FuturesOrdered, StreamExt};
use loonfs::{CurrentFile, CurrentFileState};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    encode_cursor, AbsolutePath, GrepCursorPosition, GrepMatch, GrepPageCursor, InodeId, RevisionNo,
};
use std::collections::VecDeque;
use std::future::Future;

/// One verified line match inside a file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LineMatch {
    line_number: u64,
    byte_offset: u64,
    line: String,
    line_truncated: bool,
}

/// Runs the pattern over content, one match per line, in offset order.
/// One forward walk: matches arrive in offset order, so the current line's
/// bounds and number advance monotonically — a file full of matches costs
/// one scan of its bytes, never a repeated scan per match.
fn line_matches(content: &[u8], pattern: &regex::bytes::Regex) -> Vec<LineMatch> {
    let mut matches = Vec::new();
    let mut line_start = 0usize;
    let mut line_number = 1u64;
    let mut emitted_line_start = usize::MAX;
    for found in pattern.find_iter(content) {
        // Advance the line window forward to the one that contains this match.
        while let Some(newline) = content[line_start..found.start()]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            line_start += newline + 1;
            line_number += 1;
        }
        if line_start == emitted_line_start {
            continue;
        }
        emitted_line_start = line_start;
        let line_end = content[found.start()..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(content.len(), |newline| found.start() + newline);
        let line_bytes = &content[line_start..line_end];
        let line_truncated = line_bytes.len() > GREP_LINE_CAP_BYTES;
        let line_bytes = &line_bytes[..line_bytes.len().min(GREP_LINE_CAP_BYTES)];
        matches.push(LineMatch {
            line_number,
            byte_offset: found.start() as u64,
            line: String::from_utf8_lossy(line_bytes).into_owned(),
            line_truncated,
        });
    }
    matches
}

/// Whether a file's current path lies inside the requested scope.
///
/// Both spellings are rendered from the same stored display names — the
/// candidate's by the current-state resolution, the scope's by resolving
/// the requested prefix — so this compares durable names rather than caller
/// input, and name-policy folding applies exactly as it does to every other
/// read.
fn path_within_scope(path: &AbsolutePath, scope: &AbsolutePath) -> bool {
    if scope.is_root() {
        return true;
    }
    let (path, scope) = (path.as_str(), scope.as_str());
    path == scope
        || (path.len() > scope.len()
            && path.starts_with(scope)
            && path.as_bytes()[scope.len()] == b'/')
}

/// Folds the page's highest examined-and-rejected candidate into the resume
/// cursor on a budget exit. Sound only when every examined candidate is
/// resolved — rejected, or read and recorded — which the budget exits
/// guarantee. Never sound on a mid-file page fill: there, later candidates
/// were examined but their content reads discarded, and the cursor must stay
/// at the last emitted match.
fn fold_rejected_frontier(
    resume_cursor: &mut Option<GrepResume>,
    rejected_frontier: Option<CandidatePosition>,
) {
    let Some(frontier) = rejected_frontier else {
        return;
    };
    let behind = resume_cursor.as_ref().is_none_or(|cursor| match cursor {
        GrepResume::InFile { position, .. } => *position <= frontier,
        GrepResume::AfterFile { position } => *position < frontier,
    });
    if behind {
        *resume_cursor = Some(GrepResume::AfterFile { position: frontier });
    }
}

/// One candidate's place in the query's order: its content value, then its
/// inode.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CandidatePosition {
    pub(super) value: ContentValue,
    pub(super) inode_id: InodeId,
}

/// Where a grep page resumes in candidate order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum GrepResume {
    /// After the match at `after_offset` in this file.
    InFile {
        position: CandidatePosition,
        after_offset: u64,
    },
    /// After the whole file.
    AfterFile { position: CandidatePosition },
}

impl GrepResume {
    pub(super) fn from_cursor(cursor: &GrepPageCursor) -> Self {
        let position = CandidatePosition {
            value: ContentValue::new(&cursor.last_content_id, cursor.last_size_bytes),
            inode_id: cursor.last_inode_id,
        };
        match cursor.resume {
            GrepCursorPosition::AfterMatch { byte_offset } => Self::InFile {
                position,
                after_offset: byte_offset,
            },
            GrepCursorPosition::AfterFile {} => Self::AfterFile { position },
        }
    }

    pub(super) fn position(&self) -> &CandidatePosition {
        match self {
            Self::InFile { position, .. } | Self::AfterFile { position } => position,
        }
    }

    /// Returns where the next page resumes in the candidate at this
    /// position, as the encoded cursor spells it.
    fn cursor_position(&self) -> GrepCursorPosition {
        match self {
            Self::InFile { after_offset, .. } => GrepCursorPosition::AfterMatch {
                byte_offset: *after_offset,
            },
            Self::AfterFile { .. } => GrepCursorPosition::AfterFile {},
        }
    }
}

/// How far a page walk has got.
enum PageProgress {
    /// The walk is still scanning, and has got past this position, if any.
    Scanning(Option<GrepResume>),
    /// The page stopped before the last candidate, and the next page resumes
    /// after this position.
    Truncated(GrepResume),
}

/// One grep candidate that survived the current-state checks and awaits its
/// content read, with everything match emission needs so its content is
/// recorded without further metadata lookups.
struct GrepContentCandidate {
    position: CandidatePosition,
    revision_no: RevisionNo,
    path: AbsolutePath,
    /// How the page serves the file.
    served: Served,
}

/// The candidates of one content value that the page reads together, in
/// inode order. Their revisions refer to the same bytes, so the page reads
/// and verifies the bytes once.
struct ValueGroup {
    value: ContentValue,
    files: Vec<GrepContentCandidate>,
}

/// What one group's content fetch produced.
enum GroupContent {
    /// The value's size exceeds the index eligibility cap, so no read was
    /// issued: the files could never pass the post-read text check, and the
    /// walk counts them as fully scanned.
    Oversized,
    /// The content read's result, reported at this group's position in the
    /// ordered walk rather than where the fan-out produced it.
    Fetched(Result<Vec<u8>>),
}

/// Reads the bytes of a group's value once, through the revision of a file
/// that the page has already checked: a file's revision at the head when the
/// page serves one there, and otherwise the first file's revision at the
/// page's read sequence.
async fn group_content<S: ObjectStore>(
    plan: &QueryPlan<'_, S>,
    group: &ValueGroup,
) -> GroupContent {
    if group.value.size_bytes > MAX_INDEX_GRAMS_FILE_BYTES {
        return GroupContent::Oversized;
    }
    let at_head = group
        .files
        .iter()
        .find(|file| file.served == Served::AtHead);
    let (file, reads) = match (at_head, &plan.later) {
        (Some(file), Some(later)) => (file, &later.head),
        _ => (
            group
                .files
                .first()
                .expect("a value group should have at least one file"),
            &plan.reads,
        ),
    };
    GroupContent::Fetched(
        reads
            .read_file_revision_by_inode(
                file.position.inode_id,
                file.revision_no,
                MAX_INDEX_GRAMS_FILE_BYTES,
            )
            .await,
    )
}

/// How a page serves a file of its cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Served {
    /// Through the subject's view at the read sequence: every file of a first
    /// page, and a file of a later page whose state changed since the read
    /// sequence.
    AtReadSequence,
    /// Through the subject's view of the head: a file of a later page whose
    /// state at the head is its state at the read sequence.
    AtHead,
    /// Not at all: a file of a later page whose state changed, which the
    /// subject's view at the read sequence doesn't admit.
    Withheld,
}

/// Returns the state of each candidate's file at the read sequence, which
/// decides whether the file is in the page's cut, and how the page serves
/// it.
///
/// A first page reads both through the subject's view. A later page reads
/// the state as the service, so the subject's rights never decide the cut,
/// and then serves each file of the cut by whether it changed: a file whose
/// state at the head (its revision and path) is its state at the read
/// sequence is current content, which the page serves with the subject's
/// rights at the head. The page serves any other file through the subject's
/// view at the read sequence, which admits it only with the `history` right.
async fn candidate_states<S: ObjectStore>(
    plan: &QueryPlan<'_, S>,
    batch: &[(CandidatePosition, RevisionNo)],
) -> Result<Vec<(CurrentFileState, Served)>> {
    let inode_ids: Vec<InodeId> = batch
        .iter()
        .map(|(position, _)| position.inode_id)
        .collect();
    let Some(later) = &plan.later else {
        let states = plan.reads.stat_current_files_by_inode(&inode_ids).await?;
        return Ok(states
            .into_iter()
            .map(|state| (state, Served::AtReadSequence))
            .collect());
    };
    let members = later
        .members
        .stat_current_files_by_inode(&inode_ids)
        .await?;
    let in_cut: Vec<usize> = members
        .iter()
        .zip(batch)
        .enumerate()
        .filter(|(_, (state, (_, revision_no)))| serves_revision(state, *revision_no))
        .map(|(position, _)| position)
        .collect();
    let mut served = vec![Served::Withheld; batch.len()];
    let at_head = later
        .head
        .stat_current_files_by_inode(&in_cut.iter().map(|at| inode_ids[*at]).collect::<Vec<_>>())
        .await?;
    let mut changed = Vec::new();
    for (at, head_state) in in_cut.into_iter().zip(at_head) {
        if serves_revision(&head_state, batch[at].1) && path(&head_state) == path(&members[at]) {
            served[at] = Served::AtHead;
        } else {
            changed.push(at);
        }
    }
    let at_read_seq = plan
        .reads
        .stat_current_files_by_inode(&changed.iter().map(|at| inode_ids[*at]).collect::<Vec<_>>())
        .await?;
    for (at, state) in changed.into_iter().zip(at_read_seq) {
        if serves_revision(&state, batch[at].1) {
            served[at] = Served::AtReadSequence;
        }
    }
    Ok(members.into_iter().zip(served).collect())
}

/// Whether `state` is a visible file whose current revision is `revision_no`.
fn serves_revision(state: &CurrentFileState, revision_no: RevisionNo) -> bool {
    matches!(
        state.file,
        CurrentFile::Visible { revision_no: Some(current), .. } if current == revision_no
    )
}

/// The path of a visible file's state.
fn path(state: &CurrentFileState) -> Option<&AbsolutePath> {
    match &state.file {
        CurrentFile::Visible { path, .. } => Some(path),
        _ => None,
    }
}

/// One page of grep matches, and the cursor of the next page when the page
/// stopped before the last candidate.
pub(super) struct GrepPage {
    pub(super) matches: Vec<GrepMatch>,
    pub(super) next_cursor: Option<String>,
}

/// Walks the plan's candidates in content order, and records their matches
/// until the page fills or one of its budgets runs out.
///
/// Consecutive candidates with the same content value form one group, whose
/// bytes the page reads once. The content reads run with at most
/// [`GREP_CONTENT_READS_IN_FLIGHT`] in flight, and their results are recorded
/// in candidate order. A page that fills drops the reads still in flight, so
/// it discards at most that many reads.
///
/// The candidate values map from the index's ordinals as the walk reaches
/// them, and the reads of their `content_revisions` rows run ahead of the
/// walk, with at most [`GREP_READS_IN_FLIGHT`] values in flight. The walk
/// takes the values in content order.
pub(super) async fn walk_page<S: ObjectStore>(
    plan: &QueryPlan<'_, S>,
    limit: usize,
) -> Result<GrepPage> {
    let from = plan.resume.as_ref().map(|resume| &resume.position().value);
    let values = CandidateValues::start(&plan.index, &plan.candidates, from).await?;
    let mut candidates = PageCandidates::new(values, |candidate| ValueFiles::load(plan, candidate));
    let mut matches = PageMatches::new(plan, limit);
    let mut reads = FuturesOrdered::new();
    let mut filling: Option<ValueGroup> = None;
    let mut no_more_candidates = false;
    loop {
        while !no_more_candidates && reads.len() < GREP_CONTENT_READS_IN_FLIGHT {
            let next = candidates.next_candidate(plan).await?;
            let finished = match (next, &mut filling) {
                (Some(candidate), Some(group)) if candidate.position.value == group.value => {
                    group.files.push(candidate);
                    None
                }
                (Some(candidate), _) => filling.replace(ValueGroup {
                    value: candidate.position.value,
                    files: vec![candidate],
                }),
                (None, _) => {
                    no_more_candidates = true;
                    filling.take()
                }
            };
            if let Some(group) = finished {
                reads.push_back(async move {
                    let content = group_content(plan, &group).await;
                    (group, content)
                });
            }
        }
        let Some((group, content)) = reads.next().await else {
            if candidates.budget_exhausted {
                matches.truncate_after_rejected_frontier(candidates.rejected_frontier.take());
            }
            break;
        };
        if matches.record(&group, content)? {
            break;
        }
    }
    Ok(GrepPage {
        next_cursor: matches.next_cursor(),
        matches: matches.matches,
    })
}

/// The examining side of a page walk: it reads the current state of the
/// plan's candidates in content order, and returns the ones whose content the
/// page reads.
struct PageCandidates<L, F: Future> {
    /// The walk's position in the plan's candidate values.
    values: CandidateValues,
    /// Whether `values` has returned its last value.
    values_exhausted: bool,
    /// Starts reading the files of a candidate value.
    load: L,
    /// The reads of the next values' files, in content order.
    loading: FuturesOrdered<F>,
    current_value: Option<ValueFiles>,
    /// Checked candidates, each with its file's state and whether the page
    /// serves the file at the head.
    resolved: VecDeque<(CandidatePosition, RevisionNo, CurrentFileState, Served)>,
    examined_candidates: usize,
    /// The values whose bytes the page reads, and the value of the last
    /// candidate that it returned.
    verified_values: usize,
    last_verified_value: Option<ContentValue>,
    rejected_frontier: Option<CandidatePosition>,
    budget_exhausted: bool,
}

impl<L, F> PageCandidates<L, F>
where
    L: Fn(CandidateValue) -> F,
    F: Future<Output = Result<ValueFiles>>,
{
    fn new(values: CandidateValues, load: L) -> Self {
        Self {
            values,
            values_exhausted: false,
            load,
            loading: FuturesOrdered::new(),
            current_value: None,
            resolved: VecDeque::new(),
            examined_candidates: 0,
            verified_values: 0,
            last_verified_value: None,
            rejected_frontier: None,
            budget_exhausted: false,
        }
    }

    /// Returns the next candidate file in content order, before any check.
    async fn next_unchecked<S: ObjectStore>(
        &mut self,
        plan: &QueryPlan<'_, S>,
    ) -> Result<Option<(CandidatePosition, RevisionNo)>> {
        loop {
            let files = match &mut self.current_value {
                Some(files) => files,
                None => {
                    let Some(files) = self.next_files(plan).await? else {
                        return Ok(None);
                    };
                    self.current_value.insert(files)
                }
            };
            match files.next(plan).await? {
                Some((inode_id, revision_no)) => {
                    let position = CandidatePosition {
                        value: files.value,
                        inode_id,
                    };
                    return Ok(Some((position, revision_no)));
                }
                None => self.current_value = None,
            }
        }
    }

    /// Returns the files of the next candidate value, and starts the reads
    /// of the values after it, so at most [`GREP_READS_IN_FLIGHT`] values'
    /// reads are in flight.
    async fn next_files<S: ObjectStore>(
        &mut self,
        plan: &QueryPlan<'_, S>,
    ) -> Result<Option<ValueFiles>> {
        while !self.values_exhausted && self.loading.len() < GREP_READS_IN_FLIGHT {
            match self.values.next(&plan.index, &plan.candidates).await? {
                Some(candidate) => self.loading.push_back((self.load)(candidate)),
                None => self.values_exhausted = true,
            }
        }
        self.loading.next().await.transpose()
    }

    /// Returns the next candidate whose content the page reads, or `None`
    /// once the candidates or one of the page's budgets run out.
    async fn next_candidate<S: ObjectStore>(
        &mut self,
        plan: &QueryPlan<'_, S>,
    ) -> Result<Option<GrepContentCandidate>> {
        loop {
            if self.resolved.is_empty() {
                let examinable =
                    MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE.saturating_sub(self.examined_candidates);
                let mut batch = Vec::new();
                while batch.len() < effective_batch_size(examinable) {
                    match self.next_unchecked(plan).await? {
                        Some(candidate) => batch.push(candidate),
                        None => break,
                    }
                }
                if batch.is_empty() {
                    return Ok(None);
                }
                if examinable == 0 {
                    self.budget_exhausted = true;
                    return Ok(None);
                }
                let states = candidate_states(plan, &batch).await?;
                self.resolved.extend(batch.into_iter().zip(states).map(
                    |((position, revision_no), (state, served))| {
                        (position, revision_no, state, served)
                    },
                ));
            }
            let Some((position, revision_no, state, served)) = self.resolved.pop_front() else {
                continue;
            };
            self.examined_candidates += 1;
            let CurrentFile::Visible {
                path,
                revision_no: Some(current_revision_no),
            } = state.file
            else {
                self.rejected_frontier = Some(position);
                continue;
            };
            if current_revision_no != revision_no
                || plan
                    .scope
                    .as_ref()
                    .is_some_and(|scope| !path_within_scope(&path, &scope.path))
                || served == Served::Withheld
            {
                self.rejected_frontier = Some(position);
                continue;
            }
            if self.last_verified_value != Some(position.value) {
                if self.verified_values == MAX_GREP_VERIFIED_VALUES_PER_PAGE {
                    self.budget_exhausted = true;
                    return Ok(None);
                }
                self.verified_values += 1;
                self.last_verified_value = Some(position.value);
            }
            return Ok(Some(GrepContentCandidate {
                position,
                revision_no,
                path,
                served,
            }));
        }
    }
}

/// The recording side of a page walk: the matches of the candidates whose
/// content the page read, and the position the next page resumes from.
struct PageMatches<'plan, 'reads, S> {
    plan: &'plan QueryPlan<'reads, S>,
    limit: usize,
    matches: Vec<GrepMatch>,
    progress: PageProgress,
}

impl<'plan, 'reads, S: ObjectStore> PageMatches<'plan, 'reads, S> {
    fn new(plan: &'plan QueryPlan<'reads, S>, limit: usize) -> Self {
        Self {
            plan,
            limit,
            matches: Vec::new(),
            progress: PageProgress::Scanning(plan.resume.clone()),
        }
    }

    /// Records the matches in one group's content for each of its files, and
    /// returns whether the page filled before the group's last match.
    fn record(&mut self, group: &ValueGroup, content: GroupContent) -> Result<bool> {
        let last = &group
            .files
            .last()
            .expect("a value group should have at least one file")
            .position;
        let content = match content {
            GroupContent::Oversized => {
                self.advance(GrepResume::AfterFile {
                    position: last.clone(),
                });
                return Ok(false);
            }
            GroupContent::Fetched(content) => content?,
        };
        if !is_indexable_text_content(&content) {
            self.advance(GrepResume::AfterFile {
                position: last.clone(),
            });
            return Ok(false);
        }
        let found = line_matches(&content, &self.plan.pattern);
        for file in &group.files {
            for line in &found {
                if matches!(
                    &self.plan.resume,
                    Some(GrepResume::InFile { position, after_offset })
                        if *position == file.position && line.byte_offset <= *after_offset
                ) {
                    continue;
                }
                if self.matches.len() == self.limit {
                    self.truncate();
                    return Ok(true);
                }
                self.advance(GrepResume::InFile {
                    position: file.position.clone(),
                    after_offset: line.byte_offset,
                });
                self.matches.push(GrepMatch {
                    path: file.path.clone(),
                    inode_id: file.position.inode_id,
                    revision_no: file.revision_no,
                    line_number: line.line_number,
                    byte_offset: line.byte_offset,
                    line: line.line.clone(),
                    line_truncated: line.line_truncated,
                });
            }
            self.advance(GrepResume::AfterFile {
                position: file.position.clone(),
            });
        }
        Ok(false)
    }

    fn advance(&mut self, position: GrepResume) {
        self.progress = PageProgress::Scanning(Some(position));
    }

    /// Stops the page on a budget exit, past every candidate it rejected.
    fn truncate_after_rejected_frontier(&mut self, rejected_frontier: Option<CandidatePosition>) {
        if let PageProgress::Scanning(position) = &mut self.progress {
            fold_rejected_frontier(position, rejected_frontier);
        }
        self.truncate();
    }

    fn truncate(&mut self) {
        let position = match &self.progress {
            PageProgress::Scanning(position) => position
                .clone()
                .expect("a truncated page should have scanned at least one candidate"),
            PageProgress::Truncated(position) => position.clone(),
        };
        self.progress = PageProgress::Truncated(position);
    }

    fn next_cursor(&self) -> Option<String> {
        let PageProgress::Truncated(resume) = &self.progress else {
            return None;
        };
        let position = resume.position();
        Some(
            encode_cursor(&GrepPageCursor {
                head_seq: self.plan.read_seq,
                index_captured_seq: self.plan.index_captured_seq,
                cut_seq: self.plan.cut_seq,
                last_content_id: position.value.content_id(),
                last_size_bytes: position.value.size_bytes,
                last_inode_id: position.inode_id,
                resume: resume.cursor_position(),
                fingerprint: self.plan.fingerprint,
            })
            .expect("a grep page cursor should encode"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(text: &str) -> regex::bytes::Regex {
        regex::bytes::Regex::new(text).expect("pattern")
    }

    #[test]
    fn line_matches_report_positions_once_per_line() {
        let content = b"alpha\nneedle one needle\nomega needle\n";
        let matches = line_matches(content, &pattern("needle"));
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].line_number, 2);
        assert_eq!(matches[0].byte_offset, 6);
        assert_eq!(matches[0].line, "needle one needle");
        assert_eq!(matches[1].line_number, 3);
        assert_eq!(matches[1].line, "omega needle");
    }

    #[test]
    fn rejected_frontier_folds_forward_only() {
        let position = |content: u128, inode: u128| CandidatePosition {
            value: ContentValue::new(
                &loonfs_types::ContentId::from_be_bytes(content.to_be_bytes()),
                10,
            ),
            inode_id: InodeId(inode),
        };
        let mut cursor = None;
        fold_rejected_frontier(&mut cursor, Some(position(1, 9)));
        assert_eq!(
            cursor,
            Some(GrepResume::AfterFile {
                position: position(1, 9)
            })
        );
        // A later value is ahead of an earlier value's larger inode.
        let ahead = GrepResume::InFile {
            position: position(2, 3),
            after_offset: 40,
        };
        cursor = Some(ahead.clone());
        fold_rejected_frontier(&mut cursor, Some(position(1, 9)));
        assert_eq!(cursor, Some(ahead));
    }

    #[test]
    fn scope_containment_stops_at_component_boundaries() {
        let path = |value: &str| AbsolutePath::parse(value).expect("valid path");
        let scope = path("/docs");
        assert!(path_within_scope(&scope, &scope), "the scope itself");
        assert!(path_within_scope(&path("/docs/a.txt"), &scope));
        assert!(path_within_scope(&path("/docs/deep/a.txt"), &scope));
        assert!(
            !path_within_scope(&path("/docsy/a.txt"), &scope),
            "a longer sibling name is not inside the scope"
        );
        assert!(!path_within_scope(&path("/other/a.txt"), &scope));
        assert!(
            path_within_scope(&path("/anything"), &AbsolutePath::root()),
            "the root scope holds everything"
        );
    }
}
