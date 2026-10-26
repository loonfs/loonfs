//! The thread that sends the engine's invalidations to the kernel.

use fuser::{INodeNo, Notifier};
use loonfs_mount::{CachedPages, Invalidation};
use tokio::sync::mpsc;

/// Starts a thread that sends each invalidation from `invalidations` to the
/// kernel, until the engine stops and closes the channel.
pub(crate) fn start(
    mut invalidations: mpsc::Receiver<Invalidation>,
    notifier: Notifier,
) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("loonfs-fuse-invalidations".to_owned())
        .spawn(move || {
            while let Some(invalidation) = invalidations.blocking_recv() {
                // The kernel refuses an invalidation of what it doesn't
                // cache, and once the mount is released it refuses all of
                // them, so a failure needs nothing more.
                if let Err(error) = send(&notifier, &invalidation) {
                    tracing::debug!(%error, ?invalidation, "the kernel refused an invalidation");
                }
            }
        })
        .map(drop)
}

fn send(notifier: &Notifier, invalidation: &Invalidation) -> std::io::Result<()> {
    match invalidation {
        Invalidation::Entry { parent, name } => notifier.inval_entry(INodeNo(parent.0), name),
        Invalidation::Inode { item, pages } => {
            let (offset, length) = page_range(*pages);
            notifier.inval_inode(INodeNo(item.0), offset, length)
        }
        Invalidation::Delete {
            parent,
            child,
            name,
        } => notifier.delete(INodeNo(parent.0), INodeNo(child.0), name),
    }
}

/// Returns the offset and length of an inode invalidation. A negative offset
/// drops the attributes and no pages, and a length of zero drops every page
/// from the offset on.
fn page_range(pages: CachedPages) -> (i64, i64) {
    match pages {
        CachedPages::None => (-1, 0),
        CachedPages::Range { offset, length } => (
            i64::try_from(offset).unwrap_or(i64::MAX),
            i64::try_from(length).unwrap_or(0),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that an attribute invalidation drops no pages, and that an
    /// append's invalidation drops the pages of its range.
    #[test]
    fn inode_invalidations_drop_only_the_pages_they_list() {
        assert_eq!(page_range(CachedPages::None), (-1, 0));
        assert_eq!(
            page_range(CachedPages::Range {
                offset: 4096,
                length: 10
            }),
            (4096, 10)
        );
        assert_eq!(
            page_range(CachedPages::Range {
                offset: u64::MAX,
                length: u64::MAX
            }),
            (i64::MAX, 0)
        );
    }
}
