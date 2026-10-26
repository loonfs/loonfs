//! The changes that the kernel's caches have to drop, which the engine sends
//! to the adapter once per feed page.

use crate::item::ItemId;
use std::ffi::OsString;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// The most invalidations that wait for one subscriber. A subscriber that
/// falls further behind loses invalidations, and the kernel's one-second
/// cache lifetime for clean items bounds how long the kernel can show stale
/// attributes after an invalidation is lost.
const MAX_QUEUED_INVALIDATIONS: usize = 4_096;

/// One change that the kernel's caches have to drop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invalidation {
    /// The kernel's cached lookup of `name` in `parent` is stale, including a
    /// cached absence of the name.
    Entry {
        /// The directory whose lookup is stale.
        parent: ItemId,
        /// The name that the kernel looked up.
        name: OsString,
    },
    /// The item's cached attributes are stale, and so are its cached pages
    /// in `pages`.
    Inode {
        /// The item whose attributes are stale.
        item: ItemId,
        /// The cached pages that are stale.
        pages: CachedPages,
    },
    /// `name` in `parent` no longer refers to `child`.
    Delete {
        /// The directory that had the name.
        parent: ItemId,
        /// The item that the name referred to.
        child: ItemId,
        /// The name that no longer refers to `child`.
        name: OsString,
    },
}

/// Which of an item's cached pages an invalidation drops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedPages {
    /// None of them, so only the attributes are stale.
    None,
    /// The pages of one byte range, such as the range that an append added.
    Range {
        /// The first byte of the range.
        offset: u64,
        /// The range's length.
        length: u64,
    },
}

/// The adapters that read a mount's invalidations.
#[derive(Debug, Clone, Default)]
pub(crate) struct Subscribers(Arc<Mutex<Vec<mpsc::Sender<Invalidation>>>>);

impl Subscribers {
    /// Adds a subscriber, which receives the invalidations sent after now.
    pub(crate) fn subscribe(&self) -> mpsc::Receiver<Invalidation> {
        let (sender, receiver) = mpsc::channel(MAX_QUEUED_INVALIDATIONS);
        self.0
            .lock()
            .expect("the subscriber lock should not be poisoned")
            .push(sender);
        receiver
    }

    /// Sends invalidations to every subscriber, drops subscribers that
    /// closed their receivers, and returns how many invalidations it dropped
    /// because a subscriber's queue was full.
    pub(crate) fn send(&self, invalidations: &[Invalidation]) -> u64 {
        let mut subscribers = self
            .0
            .lock()
            .expect("the subscriber lock should not be poisoned");
        let mut dropped = 0;
        subscribers.retain(|subscriber| {
            for invalidation in invalidations {
                match subscriber.try_send(invalidation.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => dropped += 1,
                    Err(mpsc::error::TrySendError::Closed(_)) => return false,
                }
            }
            true
        });
        dropped
    }
}
