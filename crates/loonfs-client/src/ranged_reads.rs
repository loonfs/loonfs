//! Ranged reads of a file's content through the server.

use super::*;
use crate::transport::StreamedResponse;
use loonfs_types::options::{ReadFileOptions, ReadRange};

/// One byte range of a file, streamed through the server.
///
/// The server doesn't verify a range against the file's checksum, because the
/// checksum covers the whole file. A caller that needs verified bytes reads
/// the whole file instead. The body fails if the server sends more or fewer
/// than `length` bytes.
pub struct FileRangeStream {
    /// The offset in the file of the first byte of `body`.
    pub start_offset: u64,
    /// The number of bytes in `body`. A range that runs past the end of the
    /// file stops at its last byte, so this can be less than the length that
    /// the caller requested.
    pub length: u64,
    /// The size of the whole file.
    pub size_bytes: u64,
    /// The bytes of the range.
    pub body: PayloadStream,
}

impl std::fmt::Debug for FileRangeStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileRangeStream")
            .field("start_offset", &self.start_offset)
            .field("length", &self.length)
            .field("size_bytes", &self.size_bytes)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Streams one byte range of a file's current content through the server.
    ///
    /// See [`Self::read_file_range_stream_with_options`].
    pub async fn read_file_range_stream(
        &self,
        spec: &NamespacePath,
        range: ReadRange,
    ) -> Result<FileRangeStream> {
        self.read_file_range_stream_with_options(spec, range, &ReadFileOptions::default())
            .await
    }

    /// Streams one byte range of a file's content through the server.
    ///
    /// The range comes from the current revision, or from the revision that a
    /// snapshot captured when the options include a snapshot. The server
    /// doesn't verify the range. A range that starts at or past
    /// the end of the file returns `range_not_satisfiable`.
    pub async fn read_file_range_stream_with_options(
        &self,
        spec: &NamespacePath,
        range: ReadRange,
        options: &ReadFileOptions,
    ) -> Result<FileRangeStream> {
        self.read_range(&self.file_content_url(spec, None, options), range)
            .await
    }

    /// Streams one byte range of the current content of a visible file inode
    /// through the server, wherever the inode is bound.
    ///
    /// See [`Self::read_file_range_stream_with_options`].
    pub async fn read_file_range_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        range: ReadRange,
    ) -> Result<FileRangeStream> {
        self.read_file_range_stream_by_inode_with_options(
            namespace_id,
            inode_id,
            range,
            &ReadFileOptions::default(),
        )
        .await
    }

    /// Streams one byte range of a file's content through the server.
    ///
    /// The range comes from the current revision, or from the revision that a
    /// snapshot captured when the options include a snapshot. See
    /// [`Self::read_file_range_stream_with_options`].
    pub async fn read_file_range_stream_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        range: ReadRange,
        options: &ReadFileOptions,
    ) -> Result<FileRangeStream> {
        self.read_range(
            &self.inode_content_url(namespace_id, inode_id, options),
            range,
        )
        .await
    }

    async fn read_range(&self, url: &str, range: ReadRange) -> Result<FileRangeStream> {
        let request = self
            .get(url)
            .header(http::header::RANGE.as_str(), range.to_range_header());
        let response = self.call_for_streamed_response(&request).await?;
        let (start_offset, end_offset, size_bytes) =
            returned_range(&response, range).map_err(|message| {
                ClientError::Protocol(format!("ranged read of `{url}`: {message}"))
            })?;
        let length = end_offset - start_offset;
        Ok(FileRangeStream {
            start_offset,
            length,
            size_bytes,
            body: exact_length(response.body, length, url.to_owned()),
        })
    }
}

/// Checks a ranged read's response against the requested range, and returns
/// the offset of the first byte it contains, the offset after its last byte,
/// and the file's size.
fn returned_range(
    response: &StreamedResponse,
    requested: ReadRange,
) -> std::result::Result<(u64, u64, u64), String> {
    if response.status != http::StatusCode::PARTIAL_CONTENT {
        return Err(format!(
            "the server returned status {} instead of 206",
            response.status.as_u16()
        ));
    }
    let value = response
        .headers
        .get(http::header::CONTENT_RANGE)
        .ok_or("the response has no `Content-Range` header")?;
    let (start_offset, last_offset, size_bytes) = value
        .to_str()
        .ok()
        .and_then(parse_content_range)
        .ok_or_else(|| format!("the response's `Content-Range` {value:?} isn't one byte range"))?;
    let expected_end = requested.end_offset(size_bytes);
    if start_offset != requested.start_offset || expected_end != Some(last_offset + 1) {
        return Err(format!(
            "the server returned bytes {start_offset} to {last_offset} of {size_bytes}, which \
             isn't the range `{}` asked for",
            requested.to_range_header()
        ));
    }
    Ok((start_offset, last_offset + 1, size_bytes))
}

/// Parses a `Content-Range` header of the form `bytes start-last/size`.
fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let (range, size_bytes) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, last) = range.split_once('-')?;
    let (start, last, size_bytes) = (
        start.parse().ok()?,
        last.parse().ok()?,
        size_bytes.parse().ok()?,
    );
    (start <= last && last < size_bytes).then_some((start, last, size_bytes))
}

/// Fails `body` if it returns more or fewer than `length` bytes.
fn exact_length(body: PayloadStream, length: u64, url: String) -> PayloadStream {
    futures::stream::try_unfold(
        (body, length, url),
        |(mut body, remaining, url)| async move {
            match body.next().await {
                Some(Ok(chunk)) if chunk.len() as u64 > remaining => Err(std::io::Error::other(
                    format!("ranged read of `{url}` returned more bytes than its range"),
                )),
                Some(Ok(chunk)) => {
                    let remaining = remaining - chunk.len() as u64;
                    Ok(Some((chunk, (body, remaining, url))))
                }
                Some(Err(error)) => Err(error),
                None if remaining == 0 => Ok(None),
                None => Err(std::io::Error::other(format!(
                    "ranged read of `{url}` ended {remaining} bytes before the end of its range"
                ))),
            }
        },
    )
    .boxed()
}

#[cfg(test)]
mod tests;
