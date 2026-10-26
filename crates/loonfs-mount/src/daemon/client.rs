//! The command side of the daemon's socket.

use super::error::DaemonError;
use super::protocol::{
    decode_response, encode_line, DaemonRequest, DaemonResponse, MountStep, MAX_RESPONSE_BYTES,
};
use crate::clock;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

/// How often a command tries to connect to a daemon that is starting.
const CONNECT_RETRY_INTERVAL_MS: u64 = 50;

/// One connection to the daemon, which sends one request.
#[derive(Debug)]
pub struct DaemonClient {
    socket: PathBuf,
    stream: UnixStream,
}

impl DaemonClient {
    /// Connects to the daemon that listens on `socket`, or returns `None`
    /// when no daemon listens there.
    pub async fn connect(socket: &Path) -> Result<Option<Self>, DaemonError> {
        match UnixStream::connect(socket).await {
            Ok(stream) => Ok(Some(Self {
                socket: socket.to_path_buf(),
                stream,
            })),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(DaemonError::io(socket, &error)),
        }
    }

    /// Connects to a daemon that is starting, trying until `budget_ms` has
    /// passed, and returns `None` when no daemon listens by then.
    pub async fn connect_within(
        socket: &Path,
        budget_ms: u64,
    ) -> Result<Option<Self>, DaemonError> {
        for _ in 0..budget_ms / CONNECT_RETRY_INTERVAL_MS {
            if let Some(client) = Self::connect(socket).await? {
                return Ok(Some(client));
            }
            clock::delay(CONNECT_RETRY_INTERVAL_MS).await;
        }
        Self::connect(socket).await
    }

    /// Sends `request`, passes each progress step to `progress`, and returns
    /// the final response.
    pub async fn request(
        self,
        request: &DaemonRequest,
        mut progress: impl FnMut(MountStep),
    ) -> Result<DaemonResponse, DaemonError> {
        let socket = self.socket;
        let io = |error: std::io::Error| DaemonError::io(&socket, &error);
        let (reader, mut writer) = self.stream.into_split();
        writer.write_all(&encode_line(request)).await.map_err(io)?;
        let limit = u64::try_from(MAX_RESPONSE_BYTES).expect("the response limit fits in a u64");
        let mut reader = BufReader::new(reader);
        loop {
            let mut line = Vec::new();
            (&mut reader)
                .take(limit)
                .read_until(b'\n', &mut line)
                .await
                .map_err(io)?;
            if line.is_empty() {
                return Err(DaemonError::protocol(
                    "the daemon closed the connection before it responded",
                ));
            }
            if line.last() != Some(&b'\n') {
                return Err(DaemonError::protocol(format!(
                    "a response is one line of at most {MAX_RESPONSE_BYTES} bytes"
                )));
            }
            match decode_response(&line) {
                Ok(DaemonResponse::Progress { step }) => progress(step),
                Ok(response) => return Ok(response),
                Err(error) => {
                    return Err(DaemonError::protocol(format!(
                        "the daemon's response didn't decode: {error}"
                    )));
                }
            }
        }
    }
}
