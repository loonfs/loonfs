//! Response frames that bound how far transport buffering can outlast admission.

use axum::body::{Body, Bytes};
use axum::response::Response;
use bytes::Buf;
use http_body::{Frame, SizeHint};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

// Small frames keep transport buffering past its threshold bounded after a request releases its slot.
const MAX_RESPONSE_FRAME_BYTES: usize = 64 * 1024;

pub(super) fn split_response(response: Response) -> Response {
    response.map(|body| {
        Body::new(ResponseFrames {
            body,
            pending: Bytes::new(),
        })
    })
}

struct ResponseFrames {
    body: Body,
    pending: Bytes,
}

impl http_body::Body for ResponseFrames {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.pending.is_empty() {
            let mut frame = match ready!(Pin::new(&mut this.body).poll_frame(cx)) {
                Some(Ok(frame)) => frame,
                result => return Poll::Ready(result),
            };
            let Some(data) = frame
                .data_mut()
                .filter(|data| data.len() > MAX_RESPONSE_FRAME_BYTES)
            else {
                return Poll::Ready(Some(Ok(frame)));
            };
            this.pending = std::mem::take(data);
        }

        let length = this.pending.len().min(MAX_RESPONSE_FRAME_BYTES);
        // Shared slices would let one small frame retain the original large allocation.
        let data = Bytes::copy_from_slice(&this.pending[..length]);
        this.pending.advance(length);
        if this.pending.is_empty() {
            this.pending = Bytes::new();
        }
        Poll::Ready(Some(Ok(Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_empty() && self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let inner = self.body.size_hint();
        let pending = self.pending.len() as u64;
        let mut hint = SizeHint::new();
        hint.set_lower(inner.lower().saturating_add(pending));
        if let Some(upper) = inner.upper().and_then(|upper| upper.checked_add(pending)) {
            hint.set_upper(upper);
        }
        hint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body::Body as _;
    use http_body_util::{BodyExt, StreamBody};
    use std::sync::Arc;

    #[tokio::test]
    async fn splitting_preserves_bytes_and_size_hints_without_retaining_large_allocations() {
        for length in [
            0,
            1,
            MAX_RESPONSE_FRAME_BYTES,
            MAX_RESPONSE_FRAME_BYTES * 2 + 17,
        ] {
            let owner: Arc<[u8]> = vec![b'x'; length].into();
            let allocation = Arc::downgrade(&owner);
            let pointer = owner.as_ptr();
            let mut body =
                split_response(Response::new(Body::from(Bytes::from_owner(owner)))).into_body();
            let mut frames = Vec::new();
            let mut remaining = length;
            assert_eq!(body.size_hint().exact(), Some(remaining as u64));
            while remaining > 0 {
                assert!(!body.is_end_stream());
                let data = body
                    .frame()
                    .await
                    .expect("frame")
                    .expect("data")
                    .into_data()
                    .expect("data frame");
                assert_eq!(data.len(), remaining.min(MAX_RESPONSE_FRAME_BYTES));
                assert!(data.iter().all(|byte| *byte == b'x'));
                if length <= MAX_RESPONSE_FRAME_BYTES {
                    assert_eq!(data.as_ptr(), pointer);
                }
                remaining -= data.len();
                frames.push(data);
                assert_eq!(body.size_hint().exact(), Some(remaining as u64));
            }
            assert!(body.is_end_stream());
            assert!(body.frame().await.is_none());
            if length > MAX_RESPONSE_FRAME_BYTES {
                assert!(allocation.upgrade().is_none());
            }
        }
    }

    #[tokio::test]
    async fn splitting_preserves_trailers_errors_and_unknown_size() {
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert("x-checksum", "verified".parse().expect("header"));
        let frames = futures::stream::iter([
            Ok(Frame::data(Bytes::from(vec![
                b'x';
                MAX_RESPONSE_FRAME_BYTES + 1
            ]))),
            Ok(Frame::trailers(trailers.clone())),
            Err(std::io::Error::other("body failed")),
        ]);
        let mut body =
            split_response(Response::new(Body::new(StreamBody::new(frames)))).into_body();
        assert_eq!(body.size_hint().lower(), 0);
        assert_eq!(body.size_hint().upper(), None);
        assert_eq!(
            body.frame()
                .await
                .expect("frame")
                .expect("data")
                .data_ref()
                .expect("data frame")
                .len(),
            MAX_RESPONSE_FRAME_BYTES
        );
        assert_eq!(body.size_hint().lower(), 1);
        assert_eq!(body.size_hint().upper(), None);
        assert_eq!(
            body.frame()
                .await
                .expect("frame")
                .expect("data")
                .data_ref()
                .expect("data frame")
                .len(),
            1
        );
        assert_eq!(
            body.frame()
                .await
                .expect("frame")
                .expect("trailers")
                .into_trailers()
                .expect("trailer frame"),
            trailers
        );
        assert_eq!(
            body.frame()
                .await
                .expect("frame")
                .expect_err("body error")
                .to_string(),
            "body failed"
        );
        assert!(body.frame().await.is_none());
    }
}
