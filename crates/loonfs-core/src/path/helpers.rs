use crate::error::CoreError;
use crate::metadata::{MetadataState, ResolvedVisiblePath, VisiblePathError};
use loonfs_api::{AbsolutePath, ChangeSeq, PathError};

pub(crate) fn lookup_path(
    metadata_state: &MetadataState,
    absolute_path: &AbsolutePath,
    name_policy: loonfs_api::NamePolicy,
    seq: ChangeSeq,
) -> Result<ResolvedVisiblePath, VisiblePathError> {
    metadata_state.resolve_visible_path(absolute_path, name_policy, seq)
}

pub(crate) fn validate_path_for_mutation(absolute_path: &str) -> Result<(), CoreError> {
    parse_mutation_path(absolute_path).map(|_| ())
}

pub(crate) fn parse_absolute_path_for_core(absolute_path: &str) -> Result<AbsolutePath, CoreError> {
    AbsolutePath::parse(absolute_path).map_err(map_path_error_to_core)
}

pub(crate) fn parse_mutation_path(absolute_path: &str) -> Result<AbsolutePath, CoreError> {
    let path = parse_absolute_path_for_core(absolute_path)?;
    if path.is_root() {
        return Err(CoreError::RootMutationForbidden);
    }
    Ok(path)
}

pub(crate) fn map_path_error_to_core(error: PathError) -> CoreError {
    CoreError::InvalidPath(error.invalid_path_input().to_owned())
}

pub(crate) fn final_component(absolute_path: &AbsolutePath) -> Result<String, CoreError> {
    absolute_path
        .final_component()
        .map(|component| component.as_str().to_owned())
        .ok_or_else(|| CoreError::InvalidPath(absolute_path.as_str().to_owned()))
}
