//! Connection admission before TCP accept and TLS handshakes.

use axum::serve::Listener;
use loonfs::metrics::{GaugeHandle, MetricsRecorder};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct ConnectionLimit<L> {
    listener: L,
    permits: Arc<Semaphore>,
    waiting: Arc<dyn GaugeHandle>,
}

impl<L> ConnectionLimit<L> {
    pub(super) fn new(listener: L, capacity: usize, metrics: &dyn MetricsRecorder) -> Self {
        Self {
            listener,
            permits: Arc::new(Semaphore::new(capacity)),
            waiting: metrics.register_gauge(
                "loonfs.server.connection_accept_waiting",
                "One while the accept loop waits for a connection permit, otherwise zero",
                &[],
            ),
        }
    }
}

impl<L: Listener> Listener for ConnectionLimit<L> {
    type Io = Connection<L::Io>;
    type Addr = L::Addr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let permit = match Arc::clone(&self.permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let _waiting = Waiting::new(Arc::clone(&self.waiting));
                Arc::clone(&self.permits)
                    .acquire_owned()
                    .await
                    .expect("connection semaphore should stay open")
            }
        };
        let (inner, address) = self.listener.accept().await;
        (
            Connection {
                inner,
                _permit: permit,
            },
            address,
        )
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

struct Waiting(Arc<dyn GaugeHandle>);

impl Waiting {
    fn new(gauge: Arc<dyn GaugeHandle>) -> Self {
        gauge.set(1);
        Self(gauge)
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.set(0);
    }
}

pub(super) struct Connection<I> {
    inner: I,
    _permit: OwnedSemaphorePermit,
}

impl<I: AsyncRead + Unpin> AsyncRead for Connection<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for Connection<I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests;
