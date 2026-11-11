//! An installed entropy source determines the generated IDs of the whole process.
//!
//! This binary has one test, because the installed source is process-wide. If another test in
//! the same process drew an ID while this test had a seeded source installed, the draw would take
//! values out of the seeded stream, and the two sequences this test compares would differ.

use loonfs_types::{
    install_entropy, installed_entropy, CommitSeq, ContentId, InodeId, MetadataSegmentId,
    SeededEntropy,
};
use std::num::NonZeroU32;
use std::sync::Arc;

/// Draws a content ID, a segment ID, and an inode ID three times, and returns their spellings in
/// the order they were drawn.
fn draw_ids() -> Vec<String> {
    (0..3)
        .flat_map(|_| {
            [
                ContentId::generate().to_string(),
                MetadataSegmentId::generate().to_string(),
                InodeId::generate(CommitSeq(1), NonZeroU32::MIN).to_string(),
            ]
        })
        .collect()
}

#[test]
fn seeded_sources_repeat_generated_ids_until_the_previous_source_is_installed_again() {
    assert_eq!(format!("{:?}", installed_entropy()), "OsEntropy");

    let original = install_entropy(Arc::new(SeededEntropy::from_seed(7)));
    assert_eq!(format!("{original:?}"), "OsEntropy");
    let seeded = draw_ids();
    install_entropy(Arc::new(SeededEntropy::from_seed(7)));
    assert_eq!(draw_ids(), seeded, "the same seed should draw the same IDs");

    install_entropy(Arc::new(SeededEntropy::from_seed(8)));
    for (seeded_id, other_id) in seeded.iter().zip(draw_ids()) {
        assert_ne!(*seeded_id, other_id, "another seed should draw other IDs");
    }

    let replaced = install_entropy(Arc::clone(&original));
    assert!(
        format!("{replaced:?}").starts_with("SeededEntropy"),
        "{replaced:?}"
    );
    assert!(Arc::ptr_eq(&installed_entropy(), &original));
    assert_ne!(draw_ids(), seeded);
}
