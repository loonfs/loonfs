pub const INVARIANTS: &[&str] = &[
    "no_orphaned_live_entry",
    "visible_revision_points_to_durable_content",
    "stale_writer_cannot_publish",
    "subtree_tombstone_blocks_descendant_mutation",
    "restore_creates_new_revision_head",
];
