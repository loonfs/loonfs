//! Path-oriented reads over a loaded metadata view: resolution, listings,
//! revision history, and the narrow read view consumed by `loonfs-grep`.

mod listing;
mod materialized_view;

pub use materialized_view::LoadedMetadataView;
pub(crate) use materialized_view::{load_metadata_view, ReadLoadContext};
