//! [`SessionEpoch`], through which a writer session sends a commit's WAL put
//! and a tombstone's put.

use super::engine::WriterSessionStatus;
use crate::namespace::writer_epoch::AcquiredWriter;
use std::future::Future;
use std::sync::Mutex;

/// A writer session's acquired epoch, with the session status that a put
/// sent through this type releases (format spec, "Failed and unknown
/// outcomes").
///
/// The session releases its epoch immediately before it sends the put, and
/// takes the epoch back when it learns the put's outcome. A
/// put whose outcome stays unknown, or whose future is dropped or panics
/// while the put is in flight, leaves the session unacquired, so the
/// session's next publication claims a new epoch and writes a fence.
#[derive(Debug)]
pub(crate) struct SessionEpoch<'a> {
    session: &'a Mutex<WriterSessionStatus>,
    writer: AcquiredWriter,
}

impl<'a> SessionEpoch<'a> {
    /// Returns the epoch `writer`, which `session` has acquired.
    pub(crate) fn new(session: &'a Mutex<WriterSessionStatus>, writer: AcquiredWriter) -> Self {
        Self { session, writer }
    }

    /// Returns the writer and epoch that the session's puts are sent under.
    pub(crate) fn writer(&self) -> &AcquiredWriter {
        &self.writer
    }

    /// Sends `put` under the epoch and returns its result. The session takes
    /// the epoch back only when `known` says that the result shows the put's
    /// outcome.
    pub(crate) async fn send_put<T>(
        &self,
        put: impl Future<Output = T>,
        known: impl FnOnce(&T) -> bool,
    ) -> T {
        self.set(WriterSessionStatus::Unacquired);
        let result = put.await;
        if known(&result) {
            self.set(WriterSessionStatus::Acquired(self.writer.clone()));
        }
        result
    }

    fn set(&self, status: WriterSessionStatus) {
        *self
            .session
            .lock()
            .expect("writer session state lock should not be poisoned") = status;
    }
}
