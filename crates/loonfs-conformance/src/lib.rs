//! SDK conformance fixture loading and pure invariant checks.
//!
//! The integration harness is in `tests/reference.rs`. This library keeps
//! fixture parsing and checks that do not require a server available to unit
//! tests.

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Fixture format version accepted by this corpus.
pub const FIXTURE_VERSION: u32 = 1;

/// One harness branch in the version-one corpus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseFamily {
    /// Standard HTTP error responses.
    ErrorContract,
    /// Durable commit replay.
    CommitReplay,
    /// Direct whole-object upload.
    UploadDirectPut,
    /// Multipart upload and completion replay.
    UploadMultipart,
    /// Repeatable upload abort.
    UploadAbort,
    /// Granted direct download.
    Download,
    /// Cursor pagination and resumption.
    Pagination,
    /// Ordered change feed.
    Changes,
    /// Complete filesystem workflow.
    EndToEnd,
}

/// One language-neutral case document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Fixture format version.
    pub version: u32,
    /// Stable case identifier and file stem.
    pub name: String,
    /// Documented behavior and retry class, when applicable.
    pub intent: String,
    /// Harness branch that owns the sequence.
    pub family: CaseFamily,
    /// Ordered wire operations, for readers rather than dispatch.
    pub operations: Vec<String>,
    /// Family-specific request values.
    pub request: Value,
    /// Family-specific response fields and invariants.
    pub expected: Value,
}

const EXPECTED_CASES: &[(&str, CaseFamily)] = &[
    ("changes", CaseFamily::Changes),
    ("commit_replay", CaseFamily::CommitReplay),
    ("download", CaseFamily::Download),
    ("end_to_end", CaseFamily::EndToEnd),
    ("error_contract", CaseFamily::ErrorContract),
    ("pagination", CaseFamily::Pagination),
    ("upload_abort", CaseFamily::UploadAbort),
    ("upload_direct_put", CaseFamily::UploadDirectPut),
    ("upload_multipart", CaseFamily::UploadMultipart),
];

/// Failure to read or validate the corpus.
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
    /// A decoded fixture did not satisfy the corpus rules.
    #[error("invalid fixture `{path}`: {reason}")]
    Invalid {
        /// Fixture path or corpus directory.
        path: PathBuf,
        /// Failed rule.
        reason: String,
    },
}

/// Returns the checked-in fixture directory.
pub fn cases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("cases")
}

/// Loads and validates every version-one case in stable name order.
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
    if case.version != FIXTURE_VERSION {
        return Err(invalid(format!(
            "version must be {FIXTURE_VERSION}, found {}",
            case.version
        )));
    }
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
    if case.operations.is_empty() {
        return Err(invalid("operations must not be empty".to_owned()));
    }
    if case
        .operations
        .iter()
        .any(|operation| operation.trim().is_empty())
    {
        return Err(invalid("operation names must not be empty".to_owned()));
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
                "version one requires {} cases, found {}",
                EXPECTED_CASES.len(),
                cases.len()
            ),
        });
    }
    for (case, (expected_name, expected_family)) in cases.iter().zip(EXPECTED_CASES) {
        if case.name != *expected_name || case.family != *expected_family {
            return Err(FixtureError::Invalid {
                path: directory.to_path_buf(),
                reason: format!(
                    "expected `{expected_name}` with family {expected_family:?}, found `{}` with family {:?}",
                    case.name, case.family
                ),
            });
        }
    }
    Ok(())
}

/// Invalid deterministic byte-pattern input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PatternError {
    /// A zero modulus cannot define the pattern.
    #[error("byte pattern modulus must be greater than zero")]
    ZeroModulus,
    /// The declared length does not fit this process.
    #[error("byte pattern length does not fit usize")]
    LengthOverflow,
}

/// Expands the corpus byte pattern `offset % modulus`.
pub fn byte_pattern(length: u64, modulus: u8) -> Result<Vec<u8>, PatternError> {
    if modulus == 0 {
        return Err(PatternError::ZeroModulus);
    }
    let length = usize::try_from(length).map_err(|_| PatternError::LengthOverflow)?;
    Ok((0..length)
        .map(|offset| (offset % usize::from(modulus)) as u8)
        .collect())
}

/// Failure of the pagination completeness or resumption rules.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PaginationInvariantError {
    /// A full walk returned the same entry twice.
    #[error("full pagination walk repeated `{0}`")]
    Duplicate(String),
    /// The full walk differed from the expected ordered entries.
    #[error("full pagination walk did not match the expected entries")]
    FullWalkMismatch,
    /// The saved cursor position was outside the completed walk.
    #[error("pagination resume offset {offset} exceeds {length} entries")]
    ResumeOffset {
        /// Saved entry position.
        offset: usize,
        /// Full walk length.
        length: usize,
    },
    /// A resumed walk differed from the suffix after the saved cursor.
    #[error("resumed pagination walk did not match the expected suffix")]
    ResumeMismatch,
}

/// Checks a complete cursor walk and a second walk resumed from a saved cursor.
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
    fn version_one_inventory_parses_and_is_complete() {
        let cases = load_cases().expect("load version-one cases");
        assert_eq!(cases.len(), EXPECTED_CASES.len());
        assert!(cases.iter().all(|case| case.version == FIXTURE_VERSION));
        assert!(cases.iter().all(|case| !case.operations.is_empty()));
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
