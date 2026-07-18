//! The HTTP transport: request sending, the bounded transient retry, and
//! response-to-error mapping.

use crate::{Client, ClientError};
use loonfs_api::{ApiError, ErrorCode};

/// Cap on attempts for transient-error retry: one initial try plus three
/// retries, sleeping with doubling backoff in between.
pub(crate) const MAX_TRANSIENT_ATTEMPTS: u32 = 4;
/// First transient-retry sleep; doubles per retry up to
/// [`MAX_TRANSIENT_RETRY_DELAY`].
pub(crate) const INITIAL_TRANSIENT_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_millis(250);
/// Ceiling for one transient-retry sleep.
pub(crate) const MAX_TRANSIENT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Socket read/write inactivity timeout applied to every request. A
/// connection that makes no progress for this long fails instead of hanging
/// the caller forever.
pub(crate) const IO_INACTIVITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// The retry policy for one failed attempt: `transport` is whether the
/// network layer itself reported the failure (classified before the error
/// is flattened by `map_error`), and served envelopes retry only on the
/// retryable-unavailability codes.
pub(crate) fn transient_failure(transport: bool, error: &ClientError) -> bool {
    transport
        || matches!(
            error,
            ClientError::Api { code, .. }
                if code == ErrorCode::ServerBusy.as_str()
                    || code == ErrorCode::CommitQueueFull.as_str()
                    || code == ErrorCode::ShuttingDown.as_str()
        )
}

/// Deterministic doubling, the same shape as the object-store transport
/// retry: workspace policy avoids ambient randomness, and a bounded
/// per-request retry does not need jitter.
fn transient_retry_backoff(attempt: u32) -> std::time::Duration {
    let doublings = attempt.saturating_sub(1).min(16);
    INITIAL_TRANSIENT_RETRY_DELAY
        .saturating_mul(1u32 << doublings)
        .min(MAX_TRANSIENT_RETRY_DELAY)
}

/// The blocking client waits an isolated OS timer between attempts; there
/// is no async runtime on this call path to time against.
#[allow(clippy::disallowed_methods)]
fn transient_retry_pause(backoff: std::time::Duration) {
    std::thread::sleep(backoff);
}

impl Client {
    pub(crate) fn request_json<Req, Resp>(
        &self,
        request: ureq::Request,
        body: Option<&Req>,
    ) -> Result<Resp, ClientError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let request = self.authenticated(request);
        let response = match body {
            Some(body) => {
                // Serialized once: every retry attempt resends identical
                // bytes under the same commit id.
                let bytes =
                    serde_json::to_vec(body).map_err(|err| ClientError::Json(err.to_string()))?;
                let request = request.set("content-type", "application/json");
                self.call_with_transient_retry(&request, Some(&bytes))?
            }
            None => self.call_with_transient_retry(&request, None)?,
        };
        serde_json::from_reader(response.into_reader())
            .map_err(|err| ClientError::Json(err.to_string()))
    }

    pub(crate) fn request_bytes(&self, url: &str) -> Result<Vec<u8>, ClientError> {
        let request = self.authenticated(self.agent.get(url));
        let response = self.call_with_transient_retry(&request, None)?;
        let mut reader = response.into_reader();
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut bytes)
            .map_err(|err| ClientError::Io(err.to_string()))?;
        Ok(bytes)
    }

    /// Sends one request, resending on quick-clearing transient failures —
    /// the retryable-unavailability codes (`server_busy`,
    /// `commit_queue_full`, `shutting_down`) and network-level transport
    /// errors — with doubling backoff, bounded by
    /// [`MAX_TRANSIENT_ATTEMPTS`]. Resending is safe for every request this
    /// client issues: mutation bodies carry their commit id (replayed,
    /// never double-committed) and staging repeats are recognized by the
    /// server, which is exactly what makes an ambiguous transport failure —
    /// the request may or may not have been served — safe to resend. Other
    /// unavailability codes are excluded on purpose — `maintenance_required`
    /// and `index_lagging` clear on a maintenance tick, not on a resend —
    /// and a served status with a non-envelope body is not retried: only
    /// failures the network layer itself reported count as transport.
    pub(crate) fn call_with_transient_retry(
        &self,
        request: &ureq::Request,
        body: Option<&[u8]>,
    ) -> Result<ureq::Response, ClientError> {
        let mut attempts = 0;
        loop {
            let outcome = match body {
                Some(bytes) => request.clone().send_bytes(bytes),
                None => request.clone().call(),
            };
            let error = match outcome {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };
            // Classified before `map_error` flattens it: only a true
            // network-level failure retries — a served status with a
            // non-envelope body (a load balancer's HTML 502) does not.
            let transport = matches!(&error, ureq::Error::Transport(_));
            let error = self.map_error(error);
            attempts += 1;
            let transient = transient_failure(transport, &error);
            if !self.transient_retry || !transient || attempts >= MAX_TRANSIENT_ATTEMPTS {
                return Err(error);
            }
            transient_retry_pause(transient_retry_backoff(attempts));
        }
    }

    pub(crate) fn authenticated(&self, request: ureq::Request) -> ureq::Request {
        match &self.auth_token {
            Some(token) => request.set("authorization", &format!("Bearer {token}")),
            None => request,
        }
    }

    pub(crate) fn map_error(&self, error: ureq::Error) -> ClientError {
        match error {
            ureq::Error::Status(status, response) => {
                let parsed = serde_json::from_reader::<_, ApiError>(response.into_reader());
                match parsed {
                    Ok(body) => ClientError::Api {
                        status,
                        code: body.code,
                        feature: body.feature,
                        message: body.message,
                        request_id: body.request_id,
                        details: body.details,
                    },
                    // A status with a non-envelope body is most commonly an
                    // intermediary answering for the server (a load
                    // balancer's HTML 502): keep the status — it is the only
                    // signal the response carried.
                    Err(err) => ClientError::Http(format!(
                        "http status {status} with a non-envelope body: {err}"
                    )),
                }
            }
            ureq::Error::Transport(err) => ClientError::Http(err.to_string()),
        }
    }
}
