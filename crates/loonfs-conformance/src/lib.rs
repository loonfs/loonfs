//! Shared cases and server support for SDK conformance tests.
//!
//! Each client harness loads the same cases from `cases/`.

pub mod server;

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// One shared test case.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Case name and file stem.
    pub name: String,
    /// Behavior being tested.
    pub intent: String,
    /// Input values for this case.
    pub request: Value,
    /// Expected responses and behavior.
    pub expected: Value,
}

const EXPECTED_CASES: &[&str] = &[
    "changes",
    "commit_replay",
    "download",
    "end_to_end",
    "error_contract",
    "pagination",
    "proxy",
    "upload_abort",
    "upload_direct_put",
    "upload_multipart",
];

/// Failure to read or validate the test cases.
#[derive(Debug, Error)]
pub enum FixtureError {
    /// A fixture directory or file could not be read.
    #[error("failed to read `{path}`: {source}")]
    Read {
        /// Path that failed.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// A JSON fixture did not decode.
    #[error("failed to decode `{path}`: {source}")]
    Decode {
        /// Fixture path.
        path: PathBuf,
        /// JSON error.
        source: serde_json::Error,
    },
    /// A decoded fixture is invalid.
    #[error("invalid fixture `{path}`: {reason}")]
    Invalid {
        /// Fixture path or case directory.
        path: PathBuf,
        /// Failed rule.
        reason: String,
    },
}

/// Returns the checked-in case directory.
pub fn cases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("cases")
}

/// Loads and validates every case in name order.
pub fn load_cases() -> Result<Vec<Case>, FixtureError> {
    let directory = cases_dir();
    let mut paths = fs::read_dir(&directory)
        .map_err(|source| FixtureError::Read {
            path: directory.clone(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| FixtureError::Read {
                    path: directory.clone(),
                    source,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "json")
    });
    paths.sort();

    let mut cases = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = fs::read(&path).map_err(|source| FixtureError::Read {
            path: path.clone(),
            source,
        })?;
        let case: Case = serde_json::from_slice(&bytes).map_err(|source| FixtureError::Decode {
            path: path.clone(),
            source,
        })?;
        validate_case(&path, &case)?;
        cases.push(case);
    }
    cases.sort_by(|left, right| left.name.cmp(&right.name));
    validate_inventory(&directory, &cases)?;
    Ok(cases)
}

fn validate_case(path: &Path, case: &Case) -> Result<(), FixtureError> {
    let invalid = |reason: String| FixtureError::Invalid {
        path: path.to_path_buf(),
        reason,
    };
    let stem = path.file_stem().and_then(|stem| stem.to_str());
    if stem != Some(case.name.as_str()) {
        return Err(invalid(format!(
            "name `{}` must match the JSON file stem",
            case.name
        )));
    }
    if case.intent.trim().is_empty() {
        return Err(invalid("intent must not be empty".to_owned()));
    }
    if !case.request.is_object() {
        return Err(invalid("request must be a JSON object".to_owned()));
    }
    if !case.expected.is_object() {
        return Err(invalid("expected must be a JSON object".to_owned()));
    }
    Ok(())
}

fn validate_inventory(directory: &Path, cases: &[Case]) -> Result<(), FixtureError> {
    if cases.len() != EXPECTED_CASES.len() {
        return Err(FixtureError::Invalid {
            path: directory.to_path_buf(),
            reason: format!(
                "fixture corpus requires {} cases, found {}",
                EXPECTED_CASES.len(),
                cases.len()
            ),
        });
    }
    for (case, expected_name) in cases.iter().zip(EXPECTED_CASES) {
        if case.name != *expected_name {
            return Err(FixtureError::Invalid {
                path: directory.to_path_buf(),
                reason: format!("expected `{expected_name}`, found `{}`", case.name),
            });
        }
    }
    Ok(())
}

/// Invalid byte-pattern input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PatternError {
    /// The modulus is zero.
    #[error("byte pattern modulus must be greater than zero")]
    ZeroModulus,
}

/// Generates the byte pattern `offset % modulus`.
pub fn byte_pattern(length: usize, modulus: u8) -> Result<Vec<u8>, PatternError> {
    if modulus == 0 {
        return Err(PatternError::ZeroModulus);
    }
    Ok((0..length)
        .map(|offset| (offset % usize::from(modulus)) as u8)
        .collect())
}

/// Invalid pagination results.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PaginationInvariantError {
    /// The full walk returned an entry twice.
    #[error("full pagination walk repeated `{0}`")]
    Duplicate(String),
    /// The full walk did not match the expected entries.
    #[error("full pagination walk did not match the expected entries")]
    FullWalkMismatch,
    /// The saved cursor position is past the end.
    #[error("pagination resume offset {offset} exceeds {length} entries")]
    ResumeOffset {
        /// Saved position.
        offset: usize,
        /// Number of entries.
        length: usize,
    },
    /// The resumed walk did not match the remaining entries.
    #[error("resumed pagination walk did not match the expected suffix")]
    ResumeMismatch,
}

/// Checks a full page walk and a second walk resumed from a saved cursor.
pub fn validate_page_walk(
    expected: &[String],
    observed: &[String],
    resume_offset: usize,
    resumed: &[String],
) -> Result<(), PaginationInvariantError> {
    let mut seen = HashSet::with_capacity(observed.len());
    if let Some(duplicate) = observed.iter().find(|name| !seen.insert(name.as_str())) {
        return Err(PaginationInvariantError::Duplicate(duplicate.clone()));
    }
    if observed != expected {
        return Err(PaginationInvariantError::FullWalkMismatch);
    }
    let suffix = expected
        .get(resume_offset..)
        .ok_or(PaginationInvariantError::ResumeOffset {
            offset: resume_offset,
            length: expected.len(),
        })?;
    if resumed != suffix {
        return Err(PaginationInvariantError::ResumeMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_parses_and_is_complete() {
        let cases = load_cases().expect("load cases");
        assert_eq!(cases.len(), EXPECTED_CASES.len());
    }

    #[test]
    fn deterministic_pattern_has_the_documented_shape() {
        assert_eq!(
            byte_pattern(8, 3).expect("valid pattern"),
            [0, 1, 2, 0, 1, 2, 0, 1]
        );
        assert_eq!(byte_pattern(8, 0), Err(PatternError::ZeroModulus));
    }

    #[test]
    fn pagination_invariants_cover_completeness_uniqueness_and_resume() {
        let expected = ["a", "b", "c", "d"].map(str::to_owned);
        assert_eq!(
            validate_page_walk(&expected, &expected, 2, &expected[2..]),
            Ok(())
        );

        let repeated = ["a", "b", "b", "d"].map(str::to_owned);
        assert_eq!(
            validate_page_walk(&expected, &repeated, 2, &expected[2..]),
            Err(PaginationInvariantError::Duplicate("b".to_owned()))
        );
        assert_eq!(
            validate_page_walk(&expected, &expected, 2, &expected[1..]),
            Err(PaginationInvariantError::ResumeMismatch)
        );
    }
}
