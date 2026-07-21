//! The atomic grep root and its durable publication boundary.
//!
//! One root pairs the query-visible segment set with its
//! `built_through_seq` watermark, lifecycle, in-progress fold, and run
//! ordinal allocation. Publication replaces that whole set in one object
//! store compare-and-swap, so a reader never observes a watermark without
//! the segments that implement it.
//!
//! Segment objects are immutable derived data written before the root CAS.
//! A CAS loser therefore leaks only unreachable derived objects; it never
//! changes visible grep state. Grep-owned garbage collection will reclaim
//! those objects in a later change.

mod codec;
mod error;
mod state;
mod store;

pub use codec::{
    decode_grep_root, encode_grep_root, GrepRootEnvelope, GREP_ROOT_FORMAT_VERSION, GREP_ROOT_KIND,
};
pub use error::{GrepRootCodecError, GrepRootError, GrepRootStateError, Result};
pub use state::{
    GrepFoldState, GrepIndexState, GrepLifecycle, GrepRootState, GrepSegmentRef,
    GREP_INDEX_FORMAT_VERSION,
};
pub use store::{advance_grep_root, load_grep_root, seed_grep_root, LoadedGrepRoot};
