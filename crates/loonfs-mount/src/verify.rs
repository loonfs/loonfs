//! Verification of a cache file against the checksum of the revision it
//! fills.

use crate::cache::VerifiedPrefix;
use crate::cache_file::CacheFile;
use loonfs_types::{Checksum, ChecksumDigest, ContentId, ContentRef};
use std::os::unix::fs::FileExt as _;
use std::sync::Arc;

/// The size of each read that verification makes.
const VERIFY_CHUNK_BYTES: usize = 1 << 20;

/// One verification of one cache file.
#[derive(Debug, Clone)]
pub(crate) struct VerifyJob {
    pub(crate) content_id: ContentId,
    pub(crate) file: Arc<CacheFile>,
    /// The revision whose bytes the file should contain.
    pub(crate) target: ContentRef,
    /// A prefix with a verified checksum, so verification reads only the
    /// bytes after the prefix.
    pub(crate) verified_prefix: Option<VerifiedPrefix>,
}

/// Whether a cache file matched its revision's checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verification {
    Matches,
    Mismatch,
}

/// Reads a cache file and compares its checksum with the revision's.
///
/// With a verified prefix, the checksum of the bytes after the prefix is
/// combined with the prefix's checksum, as the server does for an append, so
/// the prefix isn't read again.
pub(crate) fn verify(job: &VerifyJob) -> std::io::Result<Verification> {
    let algorithm = job.target.checksum.algorithm;
    let prefix = job.verified_prefix.as_ref().filter(|prefix| {
        prefix.checksum.algorithm == algorithm && prefix.size_bytes <= job.target.size_bytes
    });
    let start = prefix.map_or(0, |prefix| prefix.size_bytes);
    let mut digest = ChecksumDigest::new(algorithm);
    let mut buffer = vec![0; VERIFY_CHUNK_BYTES];
    let mut offset = start;
    while offset < job.target.size_bytes {
        let length = usize::try_from(job.target.size_bytes - offset)
            .map_or(VERIFY_CHUNK_BYTES, |remaining| {
                remaining.min(VERIFY_CHUNK_BYTES)
            });
        job.file
            .file()
            .read_exact_at(&mut buffer[..length], offset)?;
        digest.update(&buffer[..length]);
        offset += length as u64;
    }
    let suffix = digest.finish();
    let actual: Option<Checksum> = match prefix {
        Some(prefix) => prefix
            .checksum
            .crc_combine(&suffix, job.target.size_bytes - start),
        None => Some(suffix),
    };
    Ok(if actual.as_ref() == Some(&job.target.checksum) {
        Verification::Matches
    } else {
        Verification::Mismatch
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_types::{ChecksumAlgorithm, NamespaceId};

    fn job(bytes: &[u8], verified_prefix: Option<VerifiedPrefix>, target: &[u8]) -> VerifyJob {
        let file = tempfile::tempfile().expect("temporary file");
        file.write_all_at(bytes, 0).expect("write the cache file");
        let content_ref = ContentRef::blob_v1(
            NamespaceId::parse("demo").expect("namespace ID"),
            ContentId::generate(),
            target,
            ChecksumAlgorithm::Crc64nvme,
        );
        VerifyJob {
            content_id: content_ref.content_id.clone(),
            file: CacheFile::new(file, 0),
            target: content_ref,
            verified_prefix,
        }
    }

    /// Checks that a whole file and an appended file verify against their
    /// revision's checksum, and that a changed byte fails either way.
    #[test]
    fn a_file_verifies_whole_or_by_combining_the_prefix_and_the_appended_bytes() {
        let bytes = b"a log line\nanother log line\n";
        assert_eq!(
            verify(&job(bytes, None, bytes)).expect("verify"),
            Verification::Matches
        );
        let prefix = VerifiedPrefix {
            size_bytes: 11,
            checksum: Checksum::crc64nvme(&bytes[..11]),
        };
        assert_eq!(
            verify(&job(bytes, Some(prefix.clone()), bytes)).expect("verify"),
            Verification::Matches
        );
        let mut changed = bytes.to_vec();
        changed[20] = b'X';
        assert_eq!(
            verify(&job(&changed, Some(prefix), bytes)).expect("verify"),
            Verification::Mismatch
        );
        assert_eq!(
            verify(&job(&changed, None, bytes)).expect("verify"),
            Verification::Mismatch
        );
    }
}
