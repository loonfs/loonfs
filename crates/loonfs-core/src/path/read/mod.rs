//! Path-oriented reads over a loaded metadata view: resolution, listings,
//! revision history, and bulk current-state answers.

mod current_files;
mod listing;
mod materialized_view;
mod tree_listing;

pub(crate) use current_files::{
    ensure_stat_batch_within_cap, resolve_visible_inode, stat_current_files_by_inode,
};
pub use current_files::{CurrentFile, CurrentFileState, MAX_CURRENT_FILES_PER_STAT};
#[cfg(test)]
pub(crate) use materialized_view::load_current_metadata_view;
pub(crate) use materialized_view::{
    ensure_within_read_limit, file_content_ref, load_metadata_view,
    load_metadata_view_for_authorization, LoadedMetadataView, ReadLoadContext,
};
pub use materialized_view::{
    DirectDownloadByInodeTarget, DirectDownloadTarget, FileBytes, FileRevisionsPage, GrantedRange,
};
