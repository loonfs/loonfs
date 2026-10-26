//! What the control task sends once it has handled a message: replies to
//! the engine's callers, and batches of kernel invalidations.
//!
//! The task publishes its status before it sends any of them, so a caller
//! never sees a reply or an invalidation before the status that describes
//! the state behind it.

use crate::invalidation::Invalidation;
use tokio::sync::oneshot;

/// One reply or invalidation batch that waits to be sent.
enum Outgoing {
    Reply(Box<dyn FnOnce() + Send>),
    Invalidations(Vec<Invalidation>),
}

/// The replies and invalidations of the message being handled. It contains
/// at most what one message produces: the reads that one fetch satisfied, the
/// refresh calls that one poll ends, and the pages of one feed poll.
#[derive(Default)]
pub(super) struct Outbox(Vec<Outgoing>);

impl Outbox {
    /// Queues a reply to a caller.
    pub(super) fn reply<T: Send + 'static>(&mut self, sender: oneshot::Sender<T>, value: T) {
        self.0.push(Outgoing::Reply(Box::new(move || {
            // A caller that stopped waiting has no use for the reply.
            let _ = sender.send(value);
        })));
    }

    /// Queues a batch of invalidations.
    pub(super) fn invalidations(&mut self, batch: Vec<Invalidation>) {
        if !batch.is_empty() {
            self.0.push(Outgoing::Invalidations(batch));
        }
    }

    /// Sends everything queued, in order: each reply, and each invalidation
    /// batch through `send_batch`, which returns how many invalidations it
    /// dropped. Returns the total that it dropped.
    pub(super) fn deliver(&mut self, mut send_batch: impl FnMut(&[Invalidation]) -> u64) -> u64 {
        let mut dropped = 0;
        for outgoing in std::mem::take(&mut self.0) {
            match outgoing {
                Outgoing::Reply(reply) => reply(),
                Outgoing::Invalidations(batch) => dropped += send_batch(&batch),
            }
        }
        dropped
    }
}
