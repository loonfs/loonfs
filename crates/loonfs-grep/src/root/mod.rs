//! The atomic grep root pointer, immutable manifests, and publication boundary.
//!
//! A manifest stores the visible segment set, change cursor, lifecycle state,
//! and run allocation. Publication writes an immutable manifest and then
//! updates the root pointer with compare-and-swap. Failed publications leave
//! only unreachable derived objects for grep garbage collection.

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
pub use state::{
    ChangeFeedResume, GrepIndexState, GrepIndexStatus, GrepManifestObjectId, GrepManifestState,
    GrepReorganizeState, GrepRootPointer, GrepSegmentRef,
};
pub use store::{
    advance_grep_root, load_grep_manifest, load_grep_root, load_grep_root_pointer, seed_grep_root,
    LoadedGrepRoot, LoadedGrepRootPointer,
};
