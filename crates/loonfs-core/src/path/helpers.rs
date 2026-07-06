use crate::error::{CoreError, Result};
use loonfs_api::{AbsolutePath, PathError};

pub(crate) fn validate_path_for_mutation(absolute_path: &str) -> Result<()> {
    parse_mutation_path(absolute_path).map(|_| ())
}

pub(crate) fn parse_absolute_path_for_core(absolute_path: &str) -> Result<AbsolutePath> {
    AbsolutePath::parse(absolute_path).map_err(map_path_error_to_core)
}

pub(crate) fn parse_mutation_path(absolute_path: &str) -> Result<AbsolutePath> {
    let path = parse_absolute_path_for_core(absolute_path)?;
    if path.is_root() {
        return Err(CoreError::RootMutationForbidden);
    }
    Ok(path)
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
