//! Grep's entry in a namespace manifest: the index's lifecycle, its
//! in-progress reorganization, and its runs.

mod error;
mod grep_entry;
mod status;

pub use error::GrepEntryError;
pub use grep_entry::{grep_extension_name, GrepEntry, GREP_ENTRY_VERSION};
pub use status::{ChangeFeedResume, GrepIndexStatus, GrepPruning, GrepReorganizeState};
