//! Streaming request and response bodies shared by client transports.

use crate::transport_body::RequestActivity;
use crate::TransportError;
use bytes::Bytes;
use futures::{Stream, StreamExt as _};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt as _, Full, StreamBody};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A streaming HTTP body whose frames contain `Bytes` and whose errors retain
/// their transport classification.
#[derive(Debug)]
pub struct Body {
    inner: UnsyncBoxBody<Bytes, TransportError>,
    buffered: Option<Bytes>,
    activity: Option<Arc<RequestActivity>>,
}

impl Body {
    /// Boxes a body without collecting its frames.
    pub fn new<B>(body: B) -> Self
    where
        B: HttpBody<Data = Bytes, Error = TransportError> + Send + 'static,
    {
        Self {
            inner: body.boxed_unsync(),
            buffered: None,
            activity: None,
        }
    }

    /// Produces no data frames.
    pub fn empty() -> Self {
        Self::from(Bytes::new())
    }

    /// Reads chunks only as the transport polls the body.
    pub fn from_stream<S, E>(stream: S) -> Self
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::new(StreamBody::new(stream.map(|chunk| {
            chunk.map(Frame::data).map_err(TransportError::body)
        })))
    }

    pub(crate) fn track_activity(mut self, activity: Arc<RequestActivity>) -> Self {
        self.activity = Some(activity);
        self
    }

    pub(crate) fn into_buffered(self) -> std::result::Result<Bytes, Self> {
        match self.buffered {
            Some(bytes) => Ok(bytes),
            None => Err(self),
        }
    }
}

impl From<Bytes> for Body {
    fn from(bytes: Bytes) -> Self {
        let mut body = Self::new(Full::new(bytes.clone()).map_err(|never| match never {}));
        // Reqwest can replay buffered bodies during redirects without collecting streams.
        body.buffered = Some(bytes);
        body
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = TransportError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, TransportError>>> {
        let this = self.get_mut();
        this.buffered = None;
        let frame = Pin::new(&mut this.inner).poll_frame(context);
        if matches!(&frame, Poll::Ready(Some(Ok(frame))) if frame.is_data()) {
            if let Some(activity) = &this.activity {
                activity.touch();
            }
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
