//! Scripted services and request observations for client tests.

use crate::{Body, TransportError};
use bytes::Bytes;
use futures::future::BoxFuture;
use http::{Request, Response};
use http_body_util::BodyExt as _;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tower::Service;

pub(crate) enum Outcome {
    TransportFailure,
    Success(Vec<u8>),
    PartAccepted(String),
}

struct State {
    outcomes: VecDeque<Outcome>,
    sent: Vec<SentRequest>,
}

#[derive(Debug, Clone)]
pub(crate) struct SentRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body_chunks: Vec<usize>,
}

impl SentRequest {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub(crate) fn body_bytes(&self) -> usize {
        self.body_chunks.iter().sum()
    }

    pub(crate) fn largest_body_chunk(&self) -> usize {
        self.body_chunks.iter().copied().max().unwrap_or(0)
    }
}

#[derive(Clone)]
pub(crate) struct ScriptedTransport(Arc<Mutex<State>>);

impl ScriptedTransport {
    pub(crate) fn attempts(&self) -> usize {
        self.0
            .lock()
            .expect("script lock should not be poisoned")
            .sent
            .len()
    }

    pub(crate) fn sent(&self) -> Vec<SentRequest> {
        self.0
            .lock()
            .expect("script lock should not be poisoned")
            .sent
            .clone()
    }
}

pub(crate) fn failures(count: usize) -> ScriptedTransport {
    script(std::iter::repeat_with(|| Outcome::TransportFailure).take(count))
}

pub(crate) fn failure_then_success(body: Vec<u8>) -> ScriptedTransport {
    script([Outcome::TransportFailure, Outcome::Success(body)])
}

pub(crate) fn script(outcomes: impl IntoIterator<Item = Outcome>) -> ScriptedTransport {
    ScriptedTransport(Arc::new(Mutex::new(State {
        outcomes: outcomes.into_iter().collect(),
        sent: Vec::new(),
    })))
}

impl Service<Request<Body>> for ScriptedTransport {
    type Response = Response<Body>;
    type Error = TransportError;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let (outcome, index) = {
            let mut state = self.0.lock().expect("script lock should not be poisoned");
            let index = state.sent.len();
            state.sent.push(SentRequest {
                url: request.uri().to_string(),
                headers: request
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            value.to_str().expect("header should be text").to_owned(),
                        )
                    })
                    .collect(),
                body_chunks: Vec::new(),
            });
            (
                state
                    .outcomes
                    .pop_front()
                    .expect("script should cover every request"),
                index,
            )
        };
        let state = self.0.clone();
        Box::pin(async move {
            let mut body = request.into_body();
            while let Some(frame) = body.frame().await {
                let frame = frame?;
                if let Some(bytes) = frame.data_ref() {
                    state
                        .lock()
                        .expect("script lock should not be poisoned")
                        .sent[index]
                        .body_chunks
                        .push(bytes.len());
                }
            }
            match outcome {
                Outcome::TransportFailure => Err(TransportError::new(std::io::Error::other(
                    "injected transport failure",
                ))),
                Outcome::Success(bytes) => Ok(Response::new(Body::from(Bytes::from(bytes)))),
                Outcome::PartAccepted(etag) => Ok(Response::builder()
                    .header(http::header::ETAG, etag)
                    .body(Body::empty())
                    .expect("etag should be a valid header value")),
            }
        })
    }
}
