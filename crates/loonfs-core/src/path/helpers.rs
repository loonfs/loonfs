//! Path parsing helpers shared by the read and write paths.

use crate::error::{CoreError, Result};
use loonfs_api::{AbsolutePath, PathError};

/// Checks the mutation-path invariant without keeping the parsed path: the
/// path must parse as an absolute path and must not be the root.
///
/// Callers that only stage content before planning call this directly;
/// surfaces that go on to build an intent use [`parse_mutation_path`] and
/// keep the parsed path.
pub fn validate_path_for_mutation(absolute_path: &str) -> Result<()> {
    parse_mutation_path(absolute_path).map(|_| ())
}

pub(crate) fn parse_absolute_path_for_core(absolute_path: &str) -> Result<AbsolutePath> {
    AbsolutePath::parse(absolute_path).map_err(map_path_error_to_core)
}

/// Parses a raw string into the validated, normalized path a
/// [`PathMutationIntent`](crate::path::write::PathMutationIntent) carries:
/// absolute, normalized, and not the root.
///
/// This is the one named home for the invariant. Every surface that accepts
/// a raw mutation path parses through it exactly once; planning re-asserts
/// only the root guard ([`ensure_mutation_path`]) and never re-parses.
pub fn parse_mutation_path(absolute_path: &str) -> Result<AbsolutePath> {
    let path = parse_absolute_path_for_core(absolute_path)?;
    ensure_mutation_path(&path)?;
    Ok(path)
}

/// The root-mutation guard on an already-parsed path. Intents can be built
/// from parsed paths directly, so planning re-asserts this invariant.
pub(crate) fn ensure_mutation_path(path: &AbsolutePath) -> Result<()> {
    if path.is_root() {
        return Err(CoreError::RootMutationForbidden);
    }
    Ok(())
}

pub(crate) fn map_path_error_to_core(error: PathError) -> CoreError {
    CoreError::InvalidPath(error.invalid_path_input().to_owned())
}

pub(crate) fn final_component(absolute_path: &AbsolutePath) -> Result<String> {
    absolute_path
        .final_component()
        .map(|component| component.as_str().to_owned())
        .ok_or_else(|| CoreError::InvalidPath(absolute_path.as_str().to_owned()))
}
