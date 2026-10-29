//! TLS termination: the certificate/key load and the [`axum::serve`]
//! listener that wraps accepted TCP connections in a rustls session.

use crate::config::TlsServerConfig;
use crate::{Result, ServerError};
use loonfs_types::{MonotonicTimer as _, StdMonotonicTimer};
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream as RustlsStream;
use tokio_rustls::{Accept, TlsAcceptor};

/// Builds the rustls server configuration from the configured files.
///
/// The provider is named rather than taken from the process-wide default.
/// The server links two rustls crypto providers: `ring`, which the workspace
/// enables for this listener, and `aws-lc-rs`, which `object_store` and
/// `reqwest` use for their HTTP clients. With two providers linked, rustls
/// has no single default and would panic when asked for one, so the listener
/// names `ring`, and a bad configuration stays a returned error.
pub(super) fn server_config(config: &TlsServerConfig) -> Result<rustls::ServerConfig> {
    let certs = load_cert_chain(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut server_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| ServerError::TlsIdentity {
            path: config.key_path.clone(),
            cert_path: config.cert_path.clone(),
            reason: error.to_string(),
        })?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| ServerError::TlsIdentity {
            path: config.key_path.clone(),
            cert_path: config.cert_path.clone(),
            reason: error.to_string(),
        })?;
    // Offered in preference order. HTTP/2 first because axum serves it, and
    // `http/1.1` retained because a client that cannot speak h2 must still
    // reach the same routes.
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(server_config)
}

fn load_cert_chain(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_reader_iter(open(path)?)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| ServerError::TlsPem {
            path: path.to_owned(),
            reason: error.to_string(),
        })?;
    if certs.is_empty() {
        return Err(ServerError::TlsPem {
            path: path.to_owned(),
            reason: "no CERTIFICATE section found; expected the PEM chain, leaf first".to_owned(),
        });
    }
    Ok(certs)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_reader(open(path)?).map_err(|error| ServerError::TlsPem {
        path: path.to_owned(),
        reason: match error {
            pem::Error::NoItemsFound => {
                "no PRIVATE KEY section found; expected a PKCS#8, RSA, or EC private key".to_owned()
            }
            error => error.to_string(),
        },
    })
}

fn open(path: &str) -> Result<std::fs::File> {
    std::fs::File::open(Path::new(path)).map_err(|source| ServerError::TlsRead {
        path: path.to_owned(),
        source,
    })
}

/// A TCP listener that hands [`axum::serve`] TLS connections.
///
/// `accept` deliberately returns as soon as the TCP connection is accepted,
/// before the handshake runs. axum awaits `accept` in the loop that also
/// dispatches connections, so a handshake performed here would be a
/// head-of-line block: one client that connects and then stalls would keep
/// every other client waiting. The handshake instead runs inside the
/// connection's own task, on the first poll of the returned [`TlsIo`].
pub(super) struct TlsListener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
    timer: StdMonotonicTimer,
    /// When the listener last logged a failed accept, or `None` after a
    /// successful accept.
    last_logged_at_ms: Option<u64>,
    /// The failed accepts since the last logged one.
    suppressed: u64,
}

impl TlsListener {
    pub(super) fn new(tcp: TcpListener, config: rustls::ServerConfig) -> Self {
        Self {
            tcp,
            acceptor: TlsAcceptor::from(Arc::new(config)),
            timer: StdMonotonicTimer::default(),
            last_logged_at_ms: None,
            suppressed: 0,
        }
    }

    /// Mirrors what axum's own `Listener for TcpListener` does with a failed
    /// accept, because the trait makes each implementor responsible for it:
    /// a per-connection error is dropped, and anything else — `EMFILE` being
    /// the case worth naming — is logged and retried after a pause rather
    /// than ending the accept loop. The first failure logs, and then at most
    /// one line per [`ACCEPT_ERROR_LOG_INTERVAL_MS`], with the failures
    /// suppressed since the last line.
    async fn handle_accept_error(&mut self, error: io::Error) {
        if matches!(
            error.kind(),
            io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset
        ) {
            return;
        }
        let now_ms = self.timer.monotonic_now_ms();
        let since_ms = self
            .last_logged_at_ms
            .map(|logged_at_ms| now_ms.saturating_sub(logged_at_ms));
        if since_ms.is_some_and(|since_ms| since_ms < ACCEPT_ERROR_LOG_INTERVAL_MS) {
            self.suppressed = self.suppressed.saturating_add(1);
        } else {
            tracing::error!(
                error = %error,
                suppressed = std::mem::take(&mut self.suppressed),
                since_ms,
                "accept error"
            );
            self.last_logged_at_ms = Some(now_ms);
        }
        accept_retry_pause().await;
    }

    /// Logs the first successful accept after a logged accept error.
    fn accept_recovered(&mut self) {
        if self.last_logged_at_ms.take().is_some() {
            tracing::info!(
                suppressed = std::mem::take(&mut self.suppressed),
                "accepts succeed again after accept errors"
            );
        }
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsIo;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.tcp.accept().await {
                Ok((stream, addr)) => {
                    self.accept_recovered();
                    return (
                        TlsIo::Handshaking(Box::new(self.acceptor.accept(stream))),
                        addr,
                    );
                }
                Err(error) => self.handle_accept_error(error).await,
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "backs off an accept loop that would otherwise spin on a resource the process has run out of, and no protocol time depends on it"
)]
async fn accept_retry_pause() {
    tokio::time::sleep(ACCEPT_RETRY_PAUSE).await;
}

const ACCEPT_RETRY_PAUSE: Duration = Duration::from_secs(1);

/// Shortest time between two logged accept errors.
const ACCEPT_ERROR_LOG_INTERVAL_MS: u64 = 60_000;

/// One accepted connection, before or after its TLS handshake.
///
/// A handshake failure is this connection's failure and nothing else's: it
/// surfaces as an `io::Error` to the task serving this socket, which drops
/// it. The listener keeps accepting, so a plaintext client that reaches the
/// TLS port loses its own connection and no other.
pub(super) enum TlsIo {
    Handshaking(Box<Accept<TcpStream>>),
    Ready(Box<RustlsStream<TcpStream>>),
    /// The handshake failed. Held so a later poll reports the failure again
    /// instead of polling a future that has already completed.
    Failed,
}

impl TlsIo {
    /// Drives the handshake to completion, then yields the negotiated
    /// stream. Every read and write goes through here, so the handshake
    /// happens exactly once, on whichever comes first.
    fn poll_stream(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Pin<&mut RustlsStream<TcpStream>>>> {
        if let Self::Handshaking(accept) = self {
            match Pin::new(accept.as_mut()).poll(cx) {
                Poll::Ready(Ok(stream)) => *self = Self::Ready(Box::new(stream)),
                Poll::Ready(Err(error)) => {
                    *self = Self::Failed;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        match self {
            Self::Ready(stream) => Poll::Ready(Ok(Pin::new(stream.as_mut()))),
            // `Handshaking` cannot reach here: the block above either
            // replaced it or returned.
            Self::Handshaking(_) | Self::Failed => Poll::Ready(Err(handshake_failed())),
        }
    }
}

fn handshake_failed() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "tls handshake failed on this connection",
    )
}

impl AsyncRead for TlsIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        ready!(self.get_mut().poll_stream(cx))?.poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self.get_mut().poll_stream(cx))?.poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        ready!(self.get_mut().poll_stream(cx))?.poll_write_vectored(cx, bufs)
    }

    /// Constant for the wrapped stream in every state, so reporting it
    /// before the handshake finishes cannot contradict what the negotiated
    /// stream then does.
    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.get_mut().poll_stream(cx))?.poll_flush(cx)
    }

    /// A connection whose handshake never finished has no session to close,
    /// and completing one on the way out would make shutdown wait on a peer
    /// that has already stopped mattering. Dropping the socket is the whole
    /// close in that state.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TlsIo::Ready(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            TlsIo::Handshaking(_) | TlsIo::Failed => Poll::Ready(Ok(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{load_cert_chain, load_private_key};

    #[test]
    fn a_file_without_the_expected_section_names_what_was_expected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, b"").expect("write the empty file");
        let path = empty.to_str().expect("the temporary path should be UTF-8");
        assert_eq!(
            load_cert_chain(path)
                .expect_err("an empty chain file has no certificate")
                .to_string(),
            format!(
                "`{path}` is not valid PEM: no CERTIFICATE section found; expected the PEM chain, leaf first"
            )
        );
        assert_eq!(
            load_private_key(path)
                .expect_err("an empty key file has no key")
                .to_string(),
            format!(
                "`{path}` is not valid PEM: no PRIVATE KEY section found; expected a PKCS#8, RSA, or EC private key"
            )
        );
    }
}
