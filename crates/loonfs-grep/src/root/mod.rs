//! The atomic grep root pointer, immutable manifests, and publication boundary.
//!
//! One immutable manifest pairs the query-visible segment set with its
//! `(built_through_seq, next_event_index)` cursor, lifecycle, in-progress
//! reorganization, and run ordinal allocation. Publication writes that manifest
//! create-if-absent, then installs a tiny pointer in one object-store
//! compare-and-swap, so a reader never observes a cursor without the segments
//! that implement it.
//!
//! Segment and manifest objects are immutable derived data written before
//! the pointer CAS. A CAS loser therefore leaks only unreachable derived
//! objects; it never changes visible grep state. [`crate::GrepWorker`]
//! garbage collection reclaims those objects after its grace window.

mod codec;
mod error;
mod state;
mod store;

pub use codec::{
    decode_grep_manifest, decode_grep_root, encode_grep_manifest, encode_grep_root,
    GrepManifestEnvelope, GrepRootEnvelope, GREP_MANIFEST_FORMAT_VERSION, GREP_MANIFEST_KIND,
    GREP_ROOT_FORMAT_VERSION, GREP_ROOT_KIND,
};
pub use error::{GrepEnvelopeCodecError, GrepManifestStateError, GrepRootError};
pub(crate) use state::ChangeFeedResume;
pub use state::{
    GrepIndexState, GrepLifecycle, GrepManifestId, GrepManifestState, GrepReorganizeState,
    GrepRootPointer, GrepSegmentRef, GREP_INDEX_FORMAT_VERSION,
};
pub use store::{
    advance_grep_root, load_grep_manifest, load_grep_root, load_grep_root_pointer, seed_grep_root,
    LoadedGrepRoot, LoadedGrepRootPointer,
};
