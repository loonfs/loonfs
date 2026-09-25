//! Request deadlines and shared exchange activity.

use crate::transport::{transport_delay, IO_INACTIVITY_TIMEOUT};
use crate::{Body, TransportError};
use bytes::Bytes;
use futures::future::BoxFuture;
use http_body::{Body as HttpBody, Frame, SizeHint};
use loonfs_api::MonotonicTimer;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

#[derive(Debug)]
pub(crate) struct RequestActivity {
    timer: Arc<dyn MonotonicTimer>,
    last_activity_ms: AtomicU64,
}

impl RequestActivity {
    pub(crate) fn new(timer: Arc<dyn MonotonicTimer>) -> Self {
        Self {
            last_activity_ms: AtomicU64::new(timer.monotonic_now_ms()),
            timer,
        }
    }

    pub(crate) fn touch(&self) {
        self.last_activity_ms
            .fetch_max(self.timer.monotonic_now_ms(), Ordering::Relaxed);
    }

    pub(crate) fn wait_for_inactivity(self: Arc<Self>) -> impl Future<Output = ()> + Send {
        let mut timeout: Option<Pin<Box<Sleep>>> = None;
        std::future::poll_fn(move |context| loop {
            let elapsed = Duration::from_millis(
                self.timer
                    .monotonic_now_ms()
                    .saturating_sub(self.last_activity_ms.load(Ordering::Relaxed)),
            );
            let remaining = IO_INACTIVITY_TIMEOUT.saturating_sub(elapsed);
            if remaining.is_zero() {
                return Poll::Ready(());
            }
            let wait = timeout.get_or_insert_with(|| transport_delay(remaining));
            if wait.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            timeout = None;
        })
    }
}

pub(crate) struct TimedBody {
    body: Body,
    total_timeout: Option<Pin<Box<Sleep>>>,
    read_timeout: BoxFuture<'static, ()>,
    activity: Arc<RequestActivity>,
}

impl TimedBody {
    pub(crate) fn new(
        body: Body,
        total_timeout: Option<Pin<Box<Sleep>>>,
        read_timeout: BoxFuture<'static, ()>,
        activity: Arc<RequestActivity>,
    ) -> Self {
        Self {
            body,
            total_timeout,
            read_timeout,
            activity,
        }
    }
}

impl HttpBody for TimedBody {
    type Data = Bytes;
    type Error = TransportError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, TransportError>>> {
        let this = self.get_mut();
        if let Some(timeout) = &mut this.total_timeout {
            if timeout.as_mut().poll(context).is_ready() {
                return Poll::Ready(Some(Err(timeout_error())));
            }
        }
        if this.read_timeout.as_mut().poll(context).is_ready() {
            return Poll::Ready(Some(Err(timeout_error())));
        }
        let frame = Pin::new(&mut this.body).poll_frame(context);
        if matches!(&frame, Poll::Ready(Some(Ok(frame))) if frame.is_data()) {
            this.activity.touch();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

pub(crate) async fn wait_for_timeout(timeout: &mut Option<Pin<Box<Sleep>>>) {
    match timeout {
        Some(timeout) => timeout.await,
        None => std::future::pending().await,
    }
}

pub(crate) fn timeout_error() -> TransportError {
    TransportError::timeout(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "request timed out",
    ))
}
