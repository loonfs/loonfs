//! The effects of a committed transaction on the in-memory tree, the
//! kernel's caches, and the content cache.

use super::ControlTask;
use crate::invalidation::Invalidation;
use crate::item::ItemId;
use crate::read_handle::PinnedRevision;
use crate::tree::ItemView;
use loonfs_types::{ContentId, ContentRef};

/// One effect of a committed transaction.
#[derive(Debug, Clone)]
pub(super) enum Effect {
    /// The Local tree shows the item this way.
    Show(ItemView),
    /// The item left the tree. The tree keeps it while the kernel or a
    /// descriptor uses it.
    Detach(ItemId),
    /// The kernel's caches have to drop something.
    Invalidate(Invalidation),
    /// An append extended the file's revision, so its descriptors follow it.
    Appended {
        item: ItemId,
        previous: ContentRef,
        current: PinnedRevision,
    },
    /// The item's revision stopped being current, because the item was
    /// replaced or deleted. Its descriptors keep reading that revision.
    Retired { item: ItemId, content_id: ContentId },
}

impl ControlTask {
    /// Applies the effects of a committed transaction: the tree changes
    /// first, under one write lock, so a reader sees the transaction whole.
    pub(super) fn apply_effects(&mut self, effects: Vec<Effect>) {
        {
            let mut tree = self.tree();
            for effect in &effects {
                match effect {
                    Effect::Show(view) => tree.show(view.clone()),
                    Effect::Detach(item) => tree.detach(*item),
                    Effect::Invalidate(_) | Effect::Appended { .. } | Effect::Retired { .. } => {}
                }
            }
        }
        for effect in effects {
            match effect {
                Effect::Invalidate(invalidation) => self.invalidations.push(invalidation),
                Effect::Appended {
                    item,
                    previous,
                    current,
                } => self.on_appended(item, &previous, current),
                Effect::Retired { item, content_id } => self.on_retired(item, &content_id),
                Effect::Show(_) | Effect::Detach(_) => {}
            }
        }
        self.sweep_tree();
    }
}
