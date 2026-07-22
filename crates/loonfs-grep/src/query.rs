//! Gram query planning: turns a pattern into the grams any match must
//! contain — the classic code-search transformation, conservatively.
//!
//! The plan is an AND of OR-sets over case-folded grams. Soundness is the
//! only obligation: every true match satisfies the plan, so dropping a
//! constraint (a complex subexpression, a wide alternation, a non-ASCII
//! case pair) costs candidates, never correctness — verification runs the
//! real pattern over every candidate. A pattern that yields no constraint
//! at all is unindexable and the caller decides between rejection and a
//! capped scan.
//!
//! The analysis walks the parsed pattern tracking runs of fixed bytes.
//! Case-insensitive patterns parse into character classes; a class whose
//! members fold to one ASCII byte extends the run (matching the index's
//! ASCII-folded tokenizer), anything wider breaks it. Zero-width
//! constructs (anchors, look-around the dialect permits) pass through;
//! optional or repeated subexpressions contribute their inner constraints
//! only when they must occur.

use crate::codec::{extract_grams, Gram, GRAM_LEN};
use regex_syntax::hir::{Class, Hir, HirKind, Look, Repetition};
use std::collections::BTreeSet;

/// AND of OR-sets: a candidate must contain, for every set, at least one
/// of the set's grams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GramQueryPlan {
    pub(crate) required: Vec<BTreeSet<Gram>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GramPlanOutcome {
    Indexable(GramQueryPlan),
    /// No construct requires any literal bytes; the index cannot narrow
    /// candidates.
    Unindexable,
}

/// AND-sets kept per plan; extra constraints are dropped (sound: fewer
/// constraints only widen the candidate set).
const MAX_REQUIRED_SETS: usize = 16;
/// Branches an alternation may contribute to one OR-set before the set is
/// dropped as unselective.
const MAX_OR_SET_GRAMS: usize = 64;

/// Plans a pattern. `Err` is a pattern-syntax problem; `Unindexable` is a
/// valid pattern with no required bytes.
pub(crate) fn plan_pattern(
    pattern: &str,
    case_insensitive: bool,
) -> Result<GramPlanOutcome, String> {
    let hir = regex_syntax::ParserBuilder::new()
        .utf8(false)
        .case_insensitive(case_insensitive)
        // Matches the executor's line-anchored compilation; the analysis
        // must see the same pattern the verifier runs.
        .multi_line(true)
        .build()
        .parse(pattern)
        .map_err(|error| error.to_string())?;
    let analysis = analyze(&hir);
    let mut required = analysis.sets;
    if let Some(run) = analysis.run {
        push_run_grams(&mut required, &run);
    }
    required.retain(|set| !set.is_empty() && set.len() <= MAX_OR_SET_GRAMS);
    required.sort();
    required.dedup();
    required.truncate(MAX_REQUIRED_SETS);
    if required.is_empty() {
        Ok(GramPlanOutcome::Unindexable)
    } else {
        Ok(GramPlanOutcome::Indexable(GramQueryPlan { required }))
    }
}

/// What one subexpression guarantees about any text it matches.
struct Analysis {
    /// Gram constraints every match of this subexpression satisfies.
    sets: Vec<BTreeSet<Gram>>,
    /// When the subexpression matches exactly one folded byte string, that
    /// string — so concatenation can join runs across children before
    /// extracting grams.
    run: Option<Vec<u8>>,
}

impl Analysis {
    fn unconstrained() -> Self {
        Self {
            sets: Vec::new(),
            run: None,
        }
    }

    fn empty_match() -> Self {
        Self {
            sets: Vec::new(),
            run: Some(Vec::new()),
        }
    }

    /// True when a match implies at least one gram.
    fn constrains(&self) -> bool {
        !self.sets.is_empty() || self.run.as_ref().is_some_and(|run| run.len() >= GRAM_LEN)
    }

    /// Every gram some match of this subexpression must contain.
    fn all_grams(&self) -> BTreeSet<Gram> {
        let mut grams: BTreeSet<Gram> = self.sets.iter().flatten().copied().collect();
        if let Some(run) = &self.run {
            grams.extend(extract_grams(run));
        }
        grams
    }
}

fn analyze(hir: &Hir) -> Analysis {
    match hir.kind() {
        HirKind::Empty => Analysis::empty_match(),
        HirKind::Literal(literal) => Analysis {
            sets: Vec::new(),
            run: Some(fold_bytes(&literal.0)),
        },
        HirKind::Class(class) => match single_folded_byte(class) {
            Some(byte) => Analysis {
                sets: Vec::new(),
                run: Some(vec![byte]),
            },
            None => Analysis::unconstrained(),
        },
        // Zero-width assertions match the empty string between bytes; they
        // neither add constraints nor break a byte run.
        HirKind::Look(look) => match look {
            Look::Start
            | Look::End
            | Look::StartLF
            | Look::EndLF
            | Look::StartCRLF
            | Look::EndCRLF
            | Look::WordAscii
            | Look::WordAsciiNegate
            | Look::WordUnicode
            | Look::WordUnicodeNegate
            | Look::WordStartAscii
            | Look::WordEndAscii
            | Look::WordStartUnicode
            | Look::WordEndUnicode
            | Look::WordStartHalfAscii
            | Look::WordEndHalfAscii
            | Look::WordStartHalfUnicode
            | Look::WordEndHalfUnicode => Analysis::empty_match(),
        },
        HirKind::Repetition(Repetition { min, max, sub, .. }) => {
            if *min == 0 {
                return Analysis::unconstrained();
            }
            let inner = analyze(sub);
            if *min == 1 && *max == Some(1) {
                return inner;
            }
            // The subexpression occurs at least once, so its constraints
            // hold; its bytes are not a fixed run at this position.
            let mut sets = inner.sets;
            if let Some(run) = inner.run {
                push_run_grams(&mut sets, &run);
            }
            Analysis { sets, run: None }
        }
        HirKind::Capture(capture) => analyze(&capture.sub),
        HirKind::Concat(children) => {
            let mut sets = Vec::new();
            let mut pending_run: Option<Vec<u8>> = Some(Vec::new());
            for child in children {
                let child_analysis = analyze(child);
                sets.extend(child_analysis.sets);
                match (pending_run.as_mut(), child_analysis.run) {
                    (Some(run), Some(child_run)) => run.extend_from_slice(&child_run),
                    (run_slot, child_run) => {
                        // The fixed run ends here (or the child restarts
                        // one): bank the accumulated bytes as grams.
                        if let Some(run) = run_slot.map(std::mem::take) {
                            push_run_grams(&mut sets, &run);
                        }
                        pending_run = child_run;
                    }
                }
            }
            match pending_run {
                // Every child was a fixed run: the whole concat is one.
                Some(run) if sets.is_empty() => Analysis {
                    sets,
                    run: Some(run),
                },
                Some(run) => {
                    push_run_grams(&mut sets, &run);
                    Analysis { sets, run: None }
                }
                None => Analysis { sets, run: None },
            }
        }
        HirKind::Alternation(branches) => {
            // Any match satisfies one branch, so one sound constraint is
            // "some gram from each branch's guarantees": the union over
            // branches, provided every branch guarantees something.
            let mut or_set = BTreeSet::new();
            for branch in branches {
                let branch_analysis = analyze(branch);
                if !branch_analysis.constrains() {
                    return Analysis::unconstrained();
                }
                or_set.extend(branch_analysis.all_grams());
            }
            Analysis {
                sets: vec![or_set],
                run: None,
            }
        }
    }
}

/// Adds each gram of a completed fixed-byte run as its own AND constraint.
fn push_run_grams(sets: &mut Vec<BTreeSet<Gram>>, run: &[u8]) {
    for gram in extract_grams(run) {
        sets.push(BTreeSet::from([gram]));
    }
}

fn fold_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().map(|byte| byte.to_ascii_lowercase()).collect()
}

/// The one ASCII-folded byte every member of the class becomes, if the
/// class is that narrow. `[Aa]` folds to `a`; anything wider (real
/// character choices, non-ASCII case pairs) returns `None`.
fn single_folded_byte(class: &Class) -> Option<u8> {
    let mut folded: Option<u8> = None;
    let mut fold_into = |byte: u8| -> bool {
        let byte = byte.to_ascii_lowercase();
        match folded {
            None => {
                folded = Some(byte);
                true
            }
            Some(existing) => existing == byte,
        }
    };
    match class {
        Class::Bytes(class) => {
            for range in class.ranges() {
                for byte in range.start()..=range.end() {
                    if !fold_into(byte) {
                        return None;
                    }
                }
            }
        }
        Class::Unicode(class) => {
            for range in class.ranges() {
                for character in range.start()..=range.end() {
                    let mut buffer = [0u8; 4];
                    let encoded = character.encode_utf8(&mut buffer).as_bytes();
                    if encoded.len() != 1 || !fold_into(encoded[0]) {
                        return None;
                    }
                }
            }
        }
    }
    folded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grams(plan: &GramPlanOutcome) -> Vec<Vec<String>> {
        let GramPlanOutcome::Indexable(plan) = plan else {
            unreachable!("expected an indexable plan");
        };
        plan.required
            .iter()
            .map(|set| set.iter().map(Gram::as_hex).collect())
            .collect()
    }

    fn gram(text: &[u8; 3]) -> String {
        Gram(*text).as_hex()
    }

    #[test]
    fn literals_require_their_grams() {
        let plan = plan_pattern("needle", false).expect("plan");
        let sets = grams(&plan);
        assert!(sets.contains(&vec![gram(b"nee")]));
        assert!(sets.contains(&vec![gram(b"dle")]));
    }

    #[test]
    fn case_insensitive_ascii_folds_into_the_same_grams() {
        let plan = plan_pattern("NeEdLe", true).expect("plan");
        let sets = grams(&plan);
        assert!(sets.contains(&vec![gram(b"nee")]));
    }

    #[test]
    fn concatenation_joins_runs_across_groups_and_anchors() {
        let plan = plan_pattern("^(ab)(cd)$", false).expect("plan");
        let sets = grams(&plan);
        assert!(sets.contains(&vec![gram(b"abc")]));
        assert!(sets.contains(&vec![gram(b"bcd")]));
    }

    #[test]
    fn alternation_requires_a_gram_from_some_branch() {
        let plan = plan_pattern("needle|haystack", false).expect("plan");
        let sets = grams(&plan);
        assert_eq!(sets.len(), 1);
        assert!(sets[0].contains(&gram(b"nee")));
        assert!(sets[0].contains(&gram(b"hay")));
    }

    #[test]
    fn optional_and_star_contribute_nothing() {
        assert_eq!(
            plan_pattern("(needle)?", false).expect("plan"),
            GramPlanOutcome::Unindexable
        );
        assert_eq!(
            plan_pattern(".*", false).expect("plan"),
            GramPlanOutcome::Unindexable
        );
        assert_eq!(
            plan_pattern("ab", false).expect("plan"),
            GramPlanOutcome::Unindexable,
            "two bytes cannot form a gram"
        );
    }

    #[test]
    fn plus_requires_its_inner_grams() {
        let plan = plan_pattern("(needle)+", false).expect("plan");
        let sets = grams(&plan);
        assert!(sets.contains(&vec![gram(b"nee")]));
    }

    #[test]
    fn unconstrained_alternation_branches_disable_the_set() {
        assert_eq!(
            plan_pattern("needle|n.", false).expect("plan"),
            GramPlanOutcome::Unindexable
        );
    }

    #[test]
    fn wildcards_break_runs_but_keep_both_sides() {
        let plan = plan_pattern("foo.+bar", false).expect("plan");
        let sets = grams(&plan);
        assert!(sets.contains(&vec![gram(b"foo")]));
        assert!(sets.contains(&vec![gram(b"bar")]));
    }

    #[test]
    fn non_ascii_case_pairs_stay_conservative() {
        // The folded index cannot answer for ß/SS pairs; the planner must
        // not require grams it will never find.
        let plan = plan_pattern("straße", true).expect("plan");
        match plan {
            GramPlanOutcome::Indexable(plan) => {
                for set in &plan.required {
                    for gram in set {
                        assert!(
                            gram.0.iter().all(|byte| byte.is_ascii()),
                            "non-ASCII grams must not be required under (?i)"
                        );
                    }
                }
            }
            GramPlanOutcome::Unindexable => {}
        }
    }

    #[test]
    fn invalid_patterns_error() {
        assert!(plan_pattern("(unclosed", false).is_err());
    }
}
