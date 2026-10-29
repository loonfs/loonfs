//! Runtime handles that the test targets of this crate and of the crates
//! above it build the same way.

use crate::{
    LoonFs, Maintenance, ReadOnly, SharedObjectStore, WallClock, Writable, WriterId,
    GC_DEFAULT_GRACE_WINDOW_MS,
};
use loonfs_types::SystemWallClock;
use std::sync::Arc;

/// A wall clock that reads the same time on every call.
#[derive(Debug)]
struct FixedWallClock {
    now_ms: u64,
}

impl WallClock for FixedWallClock {
    fn now_ms(&self) -> u64 {
        self.now_ms
    }
}

/// Builds a maintenance handle on `store` whose wall clock is one default
/// grace window ahead of the system clock, so a retention advance through it
/// can pass every commit made before the call.
pub async fn maintenance_past_the_grace_window(store: SharedObjectStore) -> Maintenance {
    let clock = FixedWallClock {
        now_ms: SystemWallClock.now_ms() + GC_DEFAULT_GRACE_WINDOW_MS,
    };
    let writer_id = test_writer_id("retention-maintenance");
    LoonFs::builder_with_store(store)
        .writer_id(writer_id.clone())
        .wall_clock(Arc::new(clock))
        .build()
        .await
        .expect("build the retention maintenance handle")
        .maintenance(writer_id)
}

/// Builds a writer on `store` as `writer_id`, with no minimum interval
/// between its publications.
pub async fn writer(store: SharedObjectStore, writer_id: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store)
        .writer_id(test_writer_id(writer_id))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer")
}

/// Builds a reader with nothing cached, so its first read loads the current
/// manifest.
pub async fn fresh_reader(store: SharedObjectStore) -> LoonFs<ReadOnly> {
    LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("build a fresh reader")
}

fn test_writer_id(value: &str) -> WriterId {
    WriterId::parse(value).expect("a test writer id should parse")
}
