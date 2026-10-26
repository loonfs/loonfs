//! Fault hooks: dropped and held responses, the order of a commit's response and its echo in the
//! change feed, and failed content reads.

use crate::reply::{Reply, ReplyBody};
use crate::trace::{InjectedFault, Operation};
use loonfs_types::{ChangeSeq, NamespaceId};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::watch;

/// How a content read fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentReadFault {
    /// The client receives a transport failure instead of the response.
    TransportFailure,
    /// The response body ends with an error after this many bytes.
    InterruptedBody {
        /// The number of bytes the body returns before the error.
        after_bytes: u64,
    },
    /// The first byte of the response body is changed, so the bytes don't match the content's
    /// checksum. (A real server verifies a read of a whole file and ends its body with an error
    /// instead, which [`ContentReadFault::InterruptedBody`] models.)
    CorruptedByte,
}

/// The order in which the client sees a commit's response and the commit's echo in the change
/// feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EchoOrder {
    /// The commit is in the change feed at once, and its response waits until the test releases
    /// the [`Hold`].
    EchoFirst,
    /// The client receives the commit's response at once, and the change feed ends before the
    /// commit until the test releases the [`Hold`].
    ResponseFirst,
}

/// A fault that delays responses, or keeps a commit out of the change feed, until the test
/// releases it.
///
/// Dropping the hold releases it, so a test that fails early doesn't leave a client waiting.
#[derive(Debug)]
pub struct Hold {
    state: Arc<HoldState>,
}

impl Hold {
    pub(crate) fn new() -> (Self, Arc<HoldState>) {
        let (status, _) = watch::channel(HoldStatus {
            released: false,
            held_responses: 0,
        });
        let state = Arc::new(HoldState { status });
        (
            Self {
                state: state.clone(),
            },
            state,
        )
    }

    /// Releases every response that the hold keeps back, and ends the fault.
    pub fn release(self) {
        drop(self);
    }

    /// Returns how many responses the hold has kept back so far.
    pub fn held_responses(&self) -> usize {
        self.state.status.borrow().held_responses
    }

    /// Waits until the hold has kept back at least `count` responses.
    pub async fn wait_for_held_responses(&self, count: usize) {
        let mut status = self.state.status.subscribe();
        status
            .wait_for(|status| status.held_responses >= count)
            .await
            .expect("the hold keeps its sender while a test waits on it");
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.state.release();
    }
}

#[derive(Debug, Clone, Copy)]
struct HoldStatus {
    released: bool,
    held_responses: usize,
}

/// The shared state of one [`Hold`], which the server's responses wait on.
#[derive(Debug)]
pub(crate) struct HoldState {
    status: watch::Sender<HoldStatus>,
}

impl HoldState {
    fn release(&self) {
        self.status.send_modify(|status| status.released = true);
    }

    fn is_released(&self) -> bool {
        self.status.borrow().released
    }

    /// Counts one held response, and waits until the test releases the hold.
    pub(crate) async fn hold_response(&self) {
        let mut status = self.status.subscribe();
        self.status.send_modify(|status| status.held_responses += 1);
        status
            .wait_for(|status| status.released)
            .await
            .expect("the response keeps the hold's sender while it waits");
    }
}

/// How the service delivers a response.
pub(crate) enum Delivery {
    /// The client receives the response at once.
    Now,
    /// The client receives a transport failure instead of the response.
    Dropped,
    /// The response waits until the hold is released.
    Held(Arc<HoldState>),
}

/// A commit that the change feed doesn't show yet. The feed returns commits in order, so it
/// doesn't show any later commit of the namespace either.
#[derive(Debug)]
struct HiddenFeed {
    namespace_id: NamespaceId,
    last_visible_seq: ChangeSeq,
    hold: Arc<HoldState>,
}

/// A hold on content reads that starts after a number of them pass.
#[derive(Debug)]
struct ContentReadHold {
    /// The content reads that are still delivered at once.
    passing: usize,
    hold: Arc<HoldState>,
}

/// The fault hooks that a test has armed.
#[derive(Debug, Default)]
pub(crate) struct Faults {
    dropped_commit_responses: usize,
    /// The echo orders for the next commits, in the order they were armed.
    next_commit_echoes: VecDeque<(EchoOrder, Arc<HoldState>)>,
    feed_hold: Option<Arc<HoldState>>,
    hidden_feeds: Vec<HiddenFeed>,
    content_reads: VecDeque<ContentReadFault>,
    content_read_hold: Option<ContentReadHold>,
}

impl Faults {
    pub(crate) fn drop_next_commit_response(&mut self) {
        self.dropped_commit_responses += 1;
    }

    pub(crate) fn order_next_commit_echo(&mut self, order: EchoOrder) -> Hold {
        let (hold, state) = Hold::new();
        self.next_commit_echoes.push_back((order, state));
        hold
    }

    pub(crate) fn hold_feed_pages(&mut self) -> Hold {
        assert!(
            self.feed_hold
                .as_ref()
                .is_none_or(|hold| hold.is_released()),
            "a test should release the feed hold it has before it delays feed pages again"
        );
        let (hold, state) = Hold::new();
        self.feed_hold = Some(state);
        hold
    }

    pub(crate) fn fail_next_content_read(&mut self, fault: ContentReadFault) {
        self.content_reads.push_back(fault);
    }

    pub(crate) fn hold_content_reads_after(&mut self, passing: usize) -> Hold {
        assert!(
            self.content_read_hold
                .as_ref()
                .is_none_or(|held| held.hold.is_released()),
            "a test should release the content-read hold it has before it delays content reads again"
        );
        let (hold, state) = Hold::new();
        self.content_read_hold = Some(ContentReadHold {
            passing,
            hold: state,
        });
        hold
    }

    /// Returns the last sequence that the change feed of `namespace_id` shows, while a commit's
    /// echo is hidden.
    pub(crate) fn feed_horizon(&mut self, namespace_id: &NamespaceId) -> Option<ChangeSeq> {
        self.hidden_feeds
            .retain(|hidden| !hidden.hold.is_released());
        self.hidden_feeds
            .iter()
            .filter(|hidden| hidden.namespace_id == *namespace_id)
            .map(|hidden| hidden.last_visible_seq)
            .min()
    }

    /// Returns how the client receives `reply`, and changes the reply's body for a content read
    /// fault.
    pub(crate) fn deliver(
        &mut self,
        operation: Operation,
        reply: &mut Reply,
    ) -> (Delivery, Option<InjectedFault>) {
        if let Some(commit) = &reply.fresh_commit {
            return self.deliver_commit(commit.namespace_id.clone(), commit.committed_seq);
        }
        if operation == Operation::ListChanges {
            if let Some(hold) = self.feed_hold.as_ref().filter(|hold| !hold.is_released()) {
                return (
                    Delivery::Held(hold.clone()),
                    Some(InjectedFault::ResponseHeld),
                );
            }
            self.feed_hold = None;
        }
        if reply.byte_range.is_some() && reply.status.is_success() {
            if let Some(held) = self
                .content_read_hold
                .as_mut()
                .filter(|held| !held.hold.is_released())
            {
                if held.passing == 0 {
                    return (
                        Delivery::Held(held.hold.clone()),
                        Some(InjectedFault::ResponseHeld),
                    );
                }
                held.passing -= 1;
            }
            if let Some(fault) = self.content_reads.pop_front() {
                return (
                    apply_content_fault(fault, reply),
                    Some(InjectedFault::ContentRead(fault)),
                );
            }
        }
        (Delivery::Now, None)
    }

    fn deliver_commit(
        &mut self,
        namespace_id: NamespaceId,
        committed_seq: ChangeSeq,
    ) -> (Delivery, Option<InjectedFault>) {
        if let Some((order, hold)) = self.next_commit_echoes.pop_front() {
            return match order {
                EchoOrder::EchoFirst => (Delivery::Held(hold), Some(InjectedFault::ResponseHeld)),
                EchoOrder::ResponseFirst => {
                    let last_visible_seq = ChangeSeq(committed_seq.0 - 1);
                    self.hidden_feeds.push(HiddenFeed {
                        namespace_id,
                        last_visible_seq,
                        hold,
                    });
                    (Delivery::Now, Some(InjectedFault::HiddenFromFeed))
                }
            };
        }
        if self.dropped_commit_responses > 0 {
            self.dropped_commit_responses -= 1;
            return (Delivery::Dropped, Some(InjectedFault::ResponseDropped));
        }
        (Delivery::Now, None)
    }
}

fn apply_content_fault(fault: ContentReadFault, reply: &mut Reply) -> Delivery {
    match fault {
        ContentReadFault::TransportFailure => Delivery::Dropped,
        ContentReadFault::InterruptedBody { after_bytes } => {
            let ReplyBody::Full(bytes) = &reply.body else {
                return Delivery::Now;
            };
            let keep = usize::try_from(after_bytes)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            reply.body = ReplyBody::Interrupted(bytes.slice(..keep));
            Delivery::Now
        }
        ContentReadFault::CorruptedByte => {
            if let ReplyBody::Full(bytes) = &reply.body {
                let mut corrupted = bytes.to_vec();
                if let Some(first) = corrupted.first_mut() {
                    *first ^= 0xff;
                }
                reply.body = ReplyBody::Full(corrupted.into());
            }
            Delivery::Now
        }
    }
}
