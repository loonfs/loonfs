//! Path-oriented reads over a loaded metadata view: resolution, listings,
//! revision history, and bulk current-state answers.

mod current_files;
mod listing;
mod materialized_view;

pub(crate) use current_files::{
    ensure_resolve_batch_within_cap, resolve_current_files, resolve_visible_inode,
};
pub use current_files::{CurrentFileState, MAX_CURRENT_FILES_PER_STAT};
#[cfg(test)]
pub(crate) use materialized_view::load_current_metadata_view;
pub(crate) use materialized_view::{
    ensure_within_read_limit, load_metadata_view, load_metadata_view_for_authorization,
    LoadedMetadataView, ReadLoadContext,
};
pub use materialized_view::{DirectDownloadByInodeTarget, DirectDownloadTarget, GrantedRange};
