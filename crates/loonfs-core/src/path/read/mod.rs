//! Path-oriented reads over a loaded metadata view: resolution, listings,
//! revision history, and bulk current-state reads.

mod current_files;
mod file_reads;
mod listing;
mod loaded_view;
mod revisions;
mod trash;
mod tree_listing;

pub(crate) use current_files::{
    ensure_stat_batch_within_cap, resolve_visible_inode, stat_current_files_by_inode,
};
pub use current_files::{CurrentFile, CurrentFileState};
pub(crate) use file_reads::{ensure_within_read_limit, file_content_ref};
pub use file_reads::{DirectDownloadByInodeTarget, DirectDownloadTarget, FileBytes, GrantedRange};
pub(crate) use loaded_view::LoadedMetadataView;
pub use revisions::FileRevisionsPage;
