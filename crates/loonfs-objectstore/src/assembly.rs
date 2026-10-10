//! Source validation and bounded part planning for immutable assemblies.

use crate::object_store::Result;
use crate::{
    AssemblySource, ByteRange, ImmutableWriteError, ObjectMetadata, ObjectStore, ObjectStoreError,
};
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use loonfs_types::{Checksum, ChecksumAlgorithm, StreamingChecksum};

pub(crate) const READ_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const R2_PART_BYTES: u64 = 64 * 1024 * 1024;
const MIN_PART_BYTES: u64 = 5 * 1024 * 1024;
const MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;

pub(crate) fn source_range(source: &AssemblySource, size_bytes: u64) -> Result<ByteRange> {
    let range = source.range.clone().unwrap_or(ByteRange {
        start_inclusive: 0,
        end_exclusive: size_bytes,
    });
    if range.start_inclusive > range.end_exclusive || range.end_exclusive > size_bytes {
        return Err(missing_source(&source.key));
    }
    Ok(range)
}

pub(crate) fn missing_source(key: &str) -> ObjectStoreError {
    ObjectStoreError::PreconditionFailed {
        object_key: key.to_owned(),
    }
}

pub(crate) fn check_expected(key: &str, expected: &Checksum, actual: &Checksum) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(ObjectStoreError::ChecksumMismatch {
            object_key: key.to_owned(),
        })
    }
}

pub(crate) fn combined_checksum<'a>(
    algorithm: ChecksumAlgorithm,
    sources: impl Iterator<Item = (&'a Checksum, u64)>,
    tail: &[Bytes],
) -> Option<Checksum> {
    let mut checksum = Checksum::compute(algorithm, &[]);
    for (next, length) in sources {
        checksum = checksum.crc_combine(next, length)?;
    }
    for piece in tail {
        checksum =
            checksum.crc_combine(&Checksum::compute(algorithm, piece), piece.len() as u64)?;
    }
    Some(checksum)
}

pub(crate) fn stream_sources<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    sources: &'a [AssemblySource],
    tail: Vec<Bytes>,
) -> BoxStream<'a, Result<Bytes>> {
    stream::iter(sources)
        .then(move |source| async move { source_stream(store, source).await })
        .try_flatten()
        .chain(stream::iter(tail).flat_map(|piece| {
            stream::iter(
                (0..piece.len())
                    .step_by(READ_BYTES as usize)
                    .map(move |start| {
                        Ok(piece.slice(start..(start + READ_BYTES as usize).min(piece.len())))
                    }),
            )
        }))
        .boxed()
}

async fn source_stream<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    source: &'a AssemblySource,
) -> Result<BoxStream<'a, Result<Bytes>>> {
    let metadata = store
        .head(&source.key)
        .await?
        .ok_or_else(|| missing_source(&source.key))?;
    let range = source_range(source, metadata.size_bytes)?;
    let checksum = StreamingChecksum::for_algorithm(source.checksum.algorithm);
    Ok(stream::try_unfold(
        (range.start_inclusive, Some(checksum)),
        move |(offset, checksum)| async move {
            let Some(mut checksum) = checksum else {
                return Ok(None);
            };
            if offset == range.end_exclusive {
                check_expected(&source.key, &source.checksum, &checksum.finish())?;
                return Ok(None);
            }
            let end = (offset + READ_BYTES).min(range.end_exclusive);
            let chunk = store
                .get(
                    &source.key,
                    Some(ByteRange {
                        start_inclusive: offset,
                        end_exclusive: end,
                    }),
                )
                .await?
                .ok_or_else(|| missing_source(&source.key))?;
            if chunk.len() as u64 != end - offset {
                return Err(missing_source(&source.key));
            }
            checksum.update(&chunk);
            let checksum = if end == range.end_exclusive {
                check_expected(&source.key, &source.checksum, &checksum.finish())?;
                None
            } else {
                Some(checksum)
            };
            Ok(Some((chunk, (end, checksum))))
        },
    )
    .boxed())
}

pub(crate) async fn read_sources<S: ObjectStore + ?Sized>(
    store: &S,
    sources: &[AssemblySource],
    tail: Vec<Bytes>,
    expected: &Checksum,
    key: &str,
) -> Result<Bytes> {
    let mut bytes = Vec::new();
    let mut body = stream_sources(store, sources, tail);
    while let Some(chunk) = body.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    check_expected(
        key,
        expected,
        &Checksum::compute(expected.algorithm, &bytes),
    )?;
    Ok(bytes.into())
}

pub(crate) async fn put_buffered<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    sources: &[AssemblySource],
    tail: Vec<Bytes>,
    expected: &Checksum,
) -> std::result::Result<ObjectMetadata, ImmutableWriteError> {
    let bytes = read_sources(store, sources, tail, expected, key)
        .await
        .map_err(|source| ImmutableWriteError::Transport {
            object_key: key.to_owned(),
            source,
        })?;
    store.put_immutable_verified(key, bytes).await
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AssemblyPart {
    pub range: ByteRange,
    pub source: Option<usize>,
}

pub(crate) fn plan_parts(
    lengths: &[u64],
    tail_length: u64,
    equal_parts: bool,
) -> Vec<AssemblyPart> {
    let mut parts = Vec::new();
    let source_length: u64 = lengths.iter().sum();
    let total = source_length + tail_length;
    let (mut offset, mut source, mut source_start) = (0, 0, 0);
    while offset < total {
        while source < lengths.len() && offset >= source_start + lengths[source] {
            source_start += lengths[source];
            source += 1;
        }
        let remaining = lengths
            .get(source)
            .map_or(0, |length| source_start + length - offset);
        let (length, copied) = if equal_parts {
            let length = R2_PART_BYTES.min(total - offset);
            (length, remaining >= length)
        } else if remaining >= MIN_PART_BYTES {
            let length = remaining.div_ceil(remaining.div_ceil(MAX_PART_BYTES));
            (length, true)
        } else {
            let length = if offset >= source_length {
                READ_BYTES
            } else {
                MIN_PART_BYTES
            };
            (length.min(total - offset), false)
        };
        parts.push(AssemblyPart {
            range: ByteRange {
                start_inclusive: offset,
                end_exclusive: offset + length,
            },
            source: copied.then_some(source),
        });
        offset += length;
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_fs_assembly_matches_buffered_sources_and_tail() {
        use crate::local_fs_store::LocalFsStore;

        let directory = tempfile::tempdir().expect("directory");
        let local = LocalFsStore::new(directory.path()).expect("store");
        let first = Bytes::from(vec![b'a'; READ_BYTES as usize + 7]);
        let second = Bytes::from_static(b"second");
        local
            .put_immutable_verified("source/first", first.clone())
            .await
            .expect("first");
        local
            .put_immutable_verified("source/second", second.clone())
            .await
            .expect("second");
        let sources = [
            AssemblySource {
                key: "source/first".to_owned(),
                range: Some(ByteRange {
                    start_inclusive: 1,
                    end_exclusive: first.len() as u64 - 1,
                }),
                checksum: Checksum::crc64nvme(&first[1..first.len() - 1]),
            },
            AssemblySource {
                key: "source/second".to_owned(),
                range: None,
                checksum: Checksum::crc64nvme(&second),
            },
        ];
        let tail = vec![Bytes::from_static(b"ta"), Bytes::from_static(b"il")];
        let expected = combined_checksum(
            ChecksumAlgorithm::Crc64nvme,
            sources
                .iter()
                .zip([first.len() as u64 - 2, second.len() as u64])
                .map(|(source, length)| (&source.checksum, length)),
            &tail,
        )
        .expect("checksum");
        local
            .assemble("streamed", &sources, tail.clone(), &expected)
            .await
            .expect("assembly");
        put_buffered(&local, "buffered", &sources, tail, &expected)
            .await
            .expect("buffered assembly");
        assert_eq!(
            local.get("streamed", None).await.expect("streamed"),
            local.get("buffered", None).await.expect("buffered")
        );
    }

    #[test]
    fn multipart_plans_pad_small_runs_and_copy_only_contained_r2_chunks() {
        let mib = 1024 * 1024;
        let aws = plan_parts(&[mib, 10 * mib], 1, false);
        assert_eq!(
            aws,
            vec![
                AssemblyPart {
                    range: ByteRange {
                        start_inclusive: 0,
                        end_exclusive: 5 * mib
                    },
                    source: None
                },
                AssemblyPart {
                    range: ByteRange {
                        start_inclusive: 5 * mib,
                        end_exclusive: 11 * mib
                    },
                    source: Some(1)
                },
                AssemblyPart {
                    range: ByteRange {
                        start_inclusive: 11 * mib,
                        end_exclusive: 11 * mib + 1
                    },
                    source: None
                },
            ]
        );
        let r2 = plan_parts(&[40 * mib, 90 * mib], 1, true);
        assert_eq!(
            r2,
            vec![
                AssemblyPart {
                    range: ByteRange {
                        start_inclusive: 0,
                        end_exclusive: 64 * mib
                    },
                    source: None
                },
                AssemblyPart {
                    range: ByteRange {
                        start_inclusive: 64 * mib,
                        end_exclusive: 128 * mib
                    },
                    source: Some(1)
                },
                AssemblyPart {
                    range: ByteRange {
                        start_inclusive: 128 * mib,
                        end_exclusive: 130 * mib + 1
                    },
                    source: None
                },
            ]
        );
        let large = plan_parts(&[12 * 1024 * mib], 0, false);
        assert_eq!(large.len(), 3);
        assert!(large.iter().all(|part| part.source == Some(0)
            && part.range.end_exclusive - part.range.start_inclusive == 4 * 1024 * mib));
    }
}
