//! Path-oriented reads over a loaded metadata view: resolution, listings,
//! revision history, and content search.

mod grep;
mod listing;
mod materialized_view;

pub use materialized_view::LoadedMetadataView;
pub(crate) use materialized_view::{load_metadata_view, ReadLoadContext};

pub use grep::{
    DEFAULT_GREP_PAGE_LIMIT, MAX_GREP_PAGE_LIMIT, MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES,
};
