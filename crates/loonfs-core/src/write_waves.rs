//! Writes requests in bounded concurrent waves.

use futures::future::try_join_all;
use std::future::Future;
use std::num::NonZeroUsize;

/// Writes segment requests in bounded concurrent waves.
pub async fn write_segments_in_waves<Requests, Write, WriteFuture, Descriptor, Error>(
    requests: Requests,
    max_io: NonZeroUsize,
    mut write_segment: Write,
) -> std::result::Result<Vec<Descriptor>, Error>
where
    Requests: IntoIterator,
    Write: FnMut(Requests::Item) -> WriteFuture,
    WriteFuture: Future<Output = std::result::Result<Descriptor, Error>>,
{
    let mut descriptors = Vec::new();
    let mut pending = requests.into_iter();
    loop {
        let chunk = pending.by_ref().take(max_io.get()).collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        descriptors.extend(try_join_all(chunk.into_iter().map(&mut write_segment)).await?);
    }
    Ok(descriptors)
}
