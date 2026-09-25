//! Request deadlines and response read inactivity limits.

use crate::transport::{transport_delay, IO_INACTIVITY_TIMEOUT};
use crate::{Body, TransportError};
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::future::Future as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::Sleep;

pub(crate) struct TimedBody {
    body: Body,
    total_timeout: Option<Pin<Box<Sleep>>>,
    read_timeout: Option<Pin<Box<Sleep>>>,
}

impl TimedBody {
    pub(crate) fn new(body: Body, total_timeout: Option<Pin<Box<Sleep>>>) -> Self {
        Self {
            body,
            total_timeout,
            read_timeout: None,
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
        let timeout = this
            .read_timeout
            .get_or_insert_with(|| transport_delay(IO_INACTIVITY_TIMEOUT));
        if timeout.as_mut().poll(context).is_ready() {
            return Poll::Ready(Some(Err(timeout_error())));
        }
        let frame = Pin::new(&mut this.body).poll_frame(context);
        if frame.is_ready() {
            this.read_timeout = None;
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
