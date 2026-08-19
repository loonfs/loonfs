//! Loads shared SDK test cases and checks behavior that does not need a server.
//!
//! The integration harness is in `tests/reference.rs`.

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Supported fixture format version.
pub const FIXTURE_VERSION: u32 = 1;

/// Function that runs a test case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseFamily {
    /// Standard API errors.
    ErrorContract,
    /// Repeated commit requests.
    CommitReplay,
    /// Direct PUT upload.
    UploadDirectPut,
    /// Multipart upload and repeated completion.
    UploadMultipart,
    /// Repeated upload abort.
    UploadAbort,
    /// Direct download.
    Download,
    /// Cursor pagination and resume.
    Pagination,
    /// Change feed order and identity.
    Changes,
    /// End-to-end filesystem workflow.
    EndToEnd,
}

/// One shared test case.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Fixture format version.
    pub version: u32,
    /// Case name and file stem.
    pub name: String,
    /// Behavior being tested.
    pub intent: String,
    /// Function that runs the case.
    pub family: CaseFamily,
    /// Human-readable calls in execution order.
    pub operations: Vec<String>,
    /// Input values for this case.
    pub request: Value,
    /// Expected responses and behavior.
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

/// Invalid byte-pattern input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PatternError {
    /// The modulus is zero.
    #[error("byte pattern modulus must be greater than zero")]
    ZeroModulus,
    /// The length does not fit in `usize`.
    #[error("byte pattern length does not fit usize")]
    LengthOverflow,
}

/// Generates the byte pattern `offset % modulus`.
pub fn byte_pattern(length: u64, modulus: u8) -> Result<Vec<u8>, PatternError> {
    if modulus == 0 {
        return Err(PatternError::ZeroModulus);
    }
    let length = usize::try_from(length).map_err(|_| PatternError::LengthOverflow)?;
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
