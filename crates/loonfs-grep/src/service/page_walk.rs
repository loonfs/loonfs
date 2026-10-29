//! One grep page: the walk over the plan's candidates in inode order, the
//! content reads, the matches, and the cursor of the next page.

use super::{
    QueryPlan, GREP_CONTENT_READS_IN_FLIGHT, GREP_LINE_CAP_BYTES,
    MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE, MAX_GREP_VERIFIED_FILES_PER_PAGE,
};
use crate::codec::{is_indexable_text_content, MAX_INDEX_GRAMS_FILE_BYTES};
use crate::reads::{effective_batch_size, NamespaceReadView};
use crate::Result;
use futures::stream::{FuturesOrdered, StreamExt};
use loonfs::{CurrentFile, CurrentFileState};
use loonfs_types::{
    encode_cursor, AbsolutePath, ErrorCode, GrepCursorPosition, GrepMatch, GrepPageCursor, InodeId,
    RevisionNo,
};
use std::collections::VecDeque;

/// One verified line match inside a file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LineMatch {
    line_number: u64,
    byte_offset: u64,
    line: String,
    line_truncated: bool,
}

/// Runs the pattern over content, one match per line, in offset order.
/// One forward pass: matches arrive in offset order, so the current line's
/// bounds and number advance monotonically — a file full of matches costs
/// one scan of its bytes, never a repeated scan per match.
fn line_matches(content: &[u8], pattern: &regex::bytes::Regex) -> Vec<LineMatch> {
    let mut matches = Vec::new();
    let mut line_start = 0usize;
    let mut line_number = 1u64;
    let mut emitted_line_start = usize::MAX;
    for found in pattern.find_iter(content) {
        // Advance the line window forward to the one holding this match.
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
    rejected_frontier: Option<InodeId>,
) {
    let Some(frontier) = rejected_frontier else {
        return;
    };
    let behind = resume_cursor.is_none_or(|cursor| match cursor {
        GrepResume::InFile { inode_id, .. } => inode_id <= frontier,
        GrepResume::AfterFile { inode_id } => inode_id < frontier,
    });
    if behind {
        *resume_cursor = Some(GrepResume::AfterFile { inode_id: frontier });
    }
}

/// Where a grep page resumes in candidate order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GrepResume {
    /// After the match at `after_offset` in this file.
    InFile {
        inode_id: InodeId,
        after_offset: u64,
    },
    /// After the whole file.
    AfterFile { inode_id: InodeId },
}

impl GrepResume {
    pub(super) fn from_cursor(cursor: &GrepPageCursor) -> Self {
        match cursor.resume {
            GrepCursorPosition::AfterMatch { byte_offset } => Self::InFile {
                inode_id: cursor.last_inode_id,
                after_offset: byte_offset,
            },
            GrepCursorPosition::AfterFile {} => Self::AfterFile {
                inode_id: cursor.last_inode_id,
            },
        }
    }

    /// Returns the position as the encoded cursor spells it: the inode, and
    /// where the next page resumes in that inode.
    fn cursor_position(self) -> (InodeId, GrepCursorPosition) {
        match self {
            Self::InFile {
                inode_id,
                after_offset,
            } => (
                inode_id,
                GrepCursorPosition::AfterMatch {
                    byte_offset: after_offset,
                },
            ),
            Self::AfterFile { inode_id } => (inode_id, GrepCursorPosition::AfterFile {}),
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
/// content read, carrying everything match emission needs so its content is
/// recorded without further metadata lookups.
struct GrepContentCandidate {
    inode_id: InodeId,
    revision_no: RevisionNo,
    path: AbsolutePath,
}

/// What one candidate's content fetch produced.
enum CandidateContent {
    /// A second read of the candidate's inode at the view's head disagrees
    /// with its derived path or revision. Treated as a rejection, never as a
    /// match.
    Superseded,
    /// The declared content size exceeds the index eligibility cap, so no
    /// read was issued: the file could never pass the post-read text check,
    /// and the walk counts it as fully scanned.
    Oversized,
    /// The content read's result, surfaced at this candidate's position in
    /// the ordered walk rather than where the fan-out produced it.
    Fetched(Result<Vec<u8>>),
}

/// Reads the candidate's inode again, then the bytes its revision publishes.
///
/// The second read proves the inode still has the candidate's path and
/// revision. It is by inode because a derived path may exceed the request
/// path limits. It also supplies the content reference, so the oversized
/// skip stays a decision on declared size, before any fetch.
async fn candidate_content(
    reads: &NamespaceReadView<'_>,
    candidate: &GrepContentCandidate,
) -> CandidateContent {
    let entry = match reads.resolve_inode(candidate.inode_id).await {
        Ok(entry) => entry,
        Err(error) if error.code() == ErrorCode::InodeNotFound => {
            return CandidateContent::Superseded
        }
        Err(error) => return CandidateContent::Fetched(Err(error)),
    };
    if entry.path != candidate.path || entry.revision_no() != Some(candidate.revision_no) {
        return CandidateContent::Superseded;
    }
    let Some(content_ref) = entry.content_ref() else {
        return CandidateContent::Superseded;
    };
    if content_ref.size_bytes > MAX_INDEX_GRAMS_FILE_BYTES {
        return CandidateContent::Oversized;
    }
    CandidateContent::Fetched(
        reads
            .read_file_revision_by_inode(
                candidate.inode_id,
                candidate.revision_no,
                MAX_INDEX_GRAMS_FILE_BYTES,
            )
            .await,
    )
}

/// One page of grep matches, and the cursor of the next page when the page
/// stopped before the last candidate.
pub(super) struct GrepPage {
    pub(super) matches: Vec<GrepMatch>,
    pub(super) next_cursor: Option<String>,
}

/// Walks the plan's candidates in inode order and records their matches until
/// the page fills or one of its budgets runs out.
///
/// The content reads run with at most [`GREP_CONTENT_READS_IN_FLIGHT`] in
/// flight, and their results are recorded in candidate order. A page that
/// fills drops the reads still in flight, so it discards at most that many
/// reads.
pub(super) async fn walk_page(plan: &QueryPlan<'_>, limit: usize) -> Result<GrepPage> {
    let mut candidates = PageCandidates::new(plan);
    let mut matches = PageMatches::new(plan, limit);
    let mut reads = FuturesOrdered::new();
    let mut no_more_candidates = false;
    loop {
        while !no_more_candidates && reads.len() < GREP_CONTENT_READS_IN_FLIGHT {
            match candidates.next_candidate(plan).await? {
                Some(candidate) => reads.push_back(async move {
                    let content = candidate_content(&plan.reads, &candidate).await;
                    (candidate, content)
                }),
                None => no_more_candidates = true,
            }
        }
        let Some((candidate, content)) = reads.next().await else {
            if candidates.budget_exhausted {
                matches.truncate_after_rejected_frontier(candidates.rejected_frontier);
            }
            break;
        };
        if matches.record(&candidate, content)? {
            break;
        }
    }
    Ok(GrepPage {
        next_cursor: matches.next_cursor(),
        matches: matches.matches,
    })
}

/// The examining side of a page walk: it reads the current state of the
/// plan's candidates in inode order, and returns the ones whose content the
/// page reads.
struct PageCandidates {
    ordered_candidates: Vec<InodeId>,
    next_candidate: usize,
    resolved: VecDeque<CurrentFileState>,
    examined_candidates: usize,
    verified_files: usize,
    rejected_frontier: Option<InodeId>,
    budget_exhausted: bool,
}

impl PageCandidates {
    fn new(plan: &QueryPlan<'_>) -> Self {
        let ordered_candidates = plan
            .candidates
            .inodes()
            .filter(|inode_id| match plan.resume {
                Some(GrepResume::InFile {
                    inode_id: last_inode,
                    ..
                }) => *inode_id >= last_inode,
                Some(GrepResume::AfterFile {
                    inode_id: last_inode,
                }) => *inode_id > last_inode,
                None => true,
            })
            .collect();
        Self {
            ordered_candidates,
            next_candidate: 0,
            resolved: VecDeque::new(),
            examined_candidates: 0,
            verified_files: 0,
            rejected_frontier: None,
            budget_exhausted: false,
        }
    }

    /// Returns the next candidate whose content the page reads, or `None`
    /// once the candidates or one of the page's budgets run out.
    async fn next_candidate(
        &mut self,
        plan: &QueryPlan<'_>,
    ) -> Result<Option<GrepContentCandidate>> {
        loop {
            if self.resolved.is_empty() {
                if self.next_candidate == self.ordered_candidates.len() {
                    return Ok(None);
                }
                let examinable =
                    MAX_GREP_EXAMINED_CANDIDATES_PER_PAGE.saturating_sub(self.examined_candidates);
                if examinable == 0 {
                    self.budget_exhausted = true;
                    return Ok(None);
                }
                let wanted = effective_batch_size(
                    examinable.min(self.ordered_candidates.len() - self.next_candidate),
                );
                let chunk =
                    &self.ordered_candidates[self.next_candidate..self.next_candidate + wanted];
                self.resolved
                    .extend(plan.reads.stat_current_files_by_inode(chunk).await?);
                self.next_candidate += wanted;
            }
            let Some(state) = self.resolved.pop_front() else {
                continue;
            };
            let inode_id = state.inode_id;
            self.examined_candidates += 1;
            let CurrentFile::Visible {
                path,
                revision_no: Some(revision_no),
            } = state.file
            else {
                self.rejected_frontier = Some(inode_id);
                continue;
            };
            if !plan.candidates.admits(inode_id, revision_no) {
                self.rejected_frontier = Some(inode_id);
                continue;
            }
            if plan
                .scope
                .as_ref()
                .is_some_and(|scope| !path_within_scope(&path, &scope.path))
            {
                self.rejected_frontier = Some(inode_id);
                continue;
            }
            if self.verified_files == MAX_GREP_VERIFIED_FILES_PER_PAGE {
                self.budget_exhausted = true;
                return Ok(None);
            }
            self.verified_files += 1;
            return Ok(Some(GrepContentCandidate {
                inode_id,
                revision_no,
                path,
            }));
        }
    }
}

/// The recording side of a page walk: the matches of the candidates whose
/// content the page read, and the position the next page resumes from.
struct PageMatches<'plan, 'reads> {
    plan: &'plan QueryPlan<'reads>,
    limit: usize,
    matches: Vec<GrepMatch>,
    progress: PageProgress,
}

impl<'plan, 'reads> PageMatches<'plan, 'reads> {
    fn new(plan: &'plan QueryPlan<'reads>, limit: usize) -> Self {
        Self {
            plan,
            limit,
            matches: Vec::new(),
            progress: PageProgress::Scanning(plan.resume),
        }
    }

    /// Records the matches in one candidate's content, and returns whether
    /// the page filled before the candidate's last match.
    fn record(
        &mut self,
        candidate: &GrepContentCandidate,
        content: CandidateContent,
    ) -> Result<bool> {
        let inode_id = candidate.inode_id;
        let content = match content {
            CandidateContent::Oversized | CandidateContent::Superseded => {
                self.advance(GrepResume::AfterFile { inode_id });
                return Ok(false);
            }
            CandidateContent::Fetched(content) => content?,
        };
        if !is_indexable_text_content(&content) {
            self.advance(GrepResume::AfterFile { inode_id });
            return Ok(false);
        }
        for found in line_matches(&content, &self.plan.pattern) {
            if matches!(
                self.plan.resume,
                Some(GrepResume::InFile { inode_id: last_inode, after_offset })
                    if inode_id == last_inode && found.byte_offset <= after_offset
            ) {
                continue;
            }
            if self.matches.len() == self.limit {
                self.truncate();
                return Ok(true);
            }
            self.advance(GrepResume::InFile {
                inode_id,
                after_offset: found.byte_offset,
            });
            self.matches.push(GrepMatch {
                path: candidate.path.clone(),
                inode_id,
                revision_no: candidate.revision_no,
                line_number: found.line_number,
                byte_offset: found.byte_offset,
                line: found.line,
                line_truncated: found.line_truncated,
            });
        }
        self.advance(GrepResume::AfterFile { inode_id });
        Ok(false)
    }

    fn advance(&mut self, position: GrepResume) {
        self.progress = PageProgress::Scanning(Some(position));
    }

    /// Stops the page on a budget exit, past every candidate it rejected.
    fn truncate_after_rejected_frontier(&mut self, rejected_frontier: Option<InodeId>) {
        if let PageProgress::Scanning(position) = &mut self.progress {
            fold_rejected_frontier(position, rejected_frontier);
        }
        self.truncate();
    }

    fn truncate(&mut self) {
        let position = match self.progress {
            PageProgress::Scanning(position) => {
                position.expect("a truncated page should have scanned at least one candidate")
            }
            PageProgress::Truncated(position) => position,
        };
        self.progress = PageProgress::Truncated(position);
    }

    fn next_cursor(&self) -> Option<String> {
        let PageProgress::Truncated(position) = self.progress else {
            return None;
        };
        let (last_inode_id, resume) = position.cursor_position();
        Some(
            encode_cursor(&GrepPageCursor {
                head_seq: self.plan.head_seq,
                last_inode_id,
                resume,
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
        let mut cursor = None;
        fold_rejected_frontier(&mut cursor, Some(InodeId(9)));
        assert_eq!(
            cursor,
            Some(GrepResume::AfterFile {
                inode_id: InodeId(9)
            })
        );
        let ahead = GrepResume::InFile {
            inode_id: InodeId(12),
            after_offset: 40,
        };
        cursor = Some(ahead);
        fold_rejected_frontier(&mut cursor, Some(InodeId(9)));
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
