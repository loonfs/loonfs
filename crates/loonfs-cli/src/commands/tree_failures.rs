//! A complete failure report backed by a temporary file, never by tree-sized RAM.

use super::output::TreeTransferFailure;
use serde::ser::{SerializeSeq, Serializer};
use serde::Serialize;
use std::io::{self, BufRead, BufReader, Read, Write};
use tempfile::NamedTempFile;

#[derive(Debug, Default)]
pub(crate) struct TreeTransferFailures {
    spool: Option<NamedTempFile>,
    len: usize,
}

impl TreeTransferFailures {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn push(&mut self, failure: TreeTransferFailure) -> io::Result<()> {
        // Successful transfers need no temporary report at all. File writes
        // are unbuffered here, so readers never need to flush a hidden buffer.
        if self.spool.is_none() {
            self.spool = Some(NamedTempFile::new()?);
        }
        let file = self.spool.as_mut().expect("a report file was opened");
        serde_json::to_writer(file.as_file_mut(), &failure)?;
        file.write_all(b"\n")?;
        self.len += 1;
        Ok(())
    }

    pub(crate) fn iter(&self) -> io::Result<impl Iterator<Item = io::Result<TreeTransferFailure>>> {
        // Reopen, rather than clone the descriptor: every reader needs an
        // independent offset, including repeated human and JSON rendering.
        let reader: Box<dyn Read> = match &self.spool {
            Some(file) => Box::new(file.reopen()?),
            None => Box::new(io::empty()),
        };
        Ok(BufReader::new(reader)
            .lines()
            .map(|line| serde_json::from_str(&line?).map_err(io::Error::other)))
    }
}

impl Serialize for TreeTransferFailures {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let mut sequence = serializer.serialize_seq(Some(self.len))?;
        for failure in self.iter().map_err(S::Error::custom)? {
            sequence.serialize_element(&failure.map_err(S::Error::custom)?)?;
        }
        sequence.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CliError;

    #[test]
    fn reports_keep_every_failure_and_reopen_from_the_start() {
        let mut report = TreeTransferFailures::default();
        assert!(report.spool.is_none());
        for index in 0..10_000 {
            report
                .push(TreeTransferFailure {
                    path: format!("/file-{index}"),
                    error: CliError::invalid_request("bad\ninput"),
                })
                .expect("append failure");
        }
        assert_eq!(report.len(), 10_000);
        for _ in 0..2 {
            let mut count = 0;
            for failure in report.iter().expect("open reader") {
                let failure = failure.expect("read failure");
                assert_eq!(failure.path, format!("/file-{count}"));
                assert_eq!(failure.error.message, "bad\ninput");
                count += 1;
            }
            assert_eq!(count, 10_000);
        }
        let path = report.spool.as_ref().expect("spooled").path().to_owned();
        drop(report);
        assert!(!path.exists(), "the report is removed on drop");
    }

    #[test]
    fn both_renderers_stream_the_report_and_preserve_write_errors() {
        use crate::args::CommandKind;
        use crate::commands::{CommandData, CommandOutput};
        use crate::render::{write_success, OutputFormat};
        #[derive(Default)]
        struct CountWrites {
            total: usize,
            largest: usize,
        }
        impl Write for CountWrites {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.total += bytes.len();
                self.largest = self.largest.max(bytes.len());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        struct BrokenOutput;
        impl Write for BrokenOutput {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut failures = TreeTransferFailures::default();
        for index in 0..10_000 {
            failures
                .push(TreeTransferFailure {
                    path: format!("/file-{index}"),
                    error: CliError::invalid_request("failed"),
                })
                .expect("append");
        }
        let output = CommandOutput {
            kind: CommandKind::FilesystemPut,
            profile: None,
            mode: None,
            data: CommandData::TreeTransfer {
                source: "local".to_owned(),
                destination: "/remote".to_owned(),
                files: 3,
                directories: 1,
                head_drift: None,
                failures,
            },
        };
        assert!(output.data.reports_failures());
        for format in [OutputFormat::Json, OutputFormat::Human] {
            let mut count = CountWrites::default();
            write_success(&output, format, &mut count).expect("render");
            assert!(count.total > 100_000);
            assert!(
                count.largest < 1024,
                "renderer buffered {} bytes",
                count.largest
            );
            assert_eq!(
                write_success(&output, format, BrokenOutput)
                    .expect_err("closed output")
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        }
        let mut json = Vec::new();
        write_success(&output, OutputFormat::Json, &mut json).expect("json");
        let json: serde_json::Value = serde_json::from_slice(&json).expect("parse JSON");
        assert_eq!(
            json["data"]["failures"].as_array().expect("array").len(),
            10_000
        );
        assert_eq!(json["data"]["failures"][9999]["path"], "/file-9999");
    }

    #[test]
    fn report_write_failures_are_returned_without_counting_a_saved_error() {
        let spool = NamedTempFile::new().expect("spool");
        let (writer, path) = spool.into_parts();
        drop(writer);
        let reader = std::fs::File::open(&path).expect("read-only file");
        let mut report = TreeTransferFailures {
            spool: Some(NamedTempFile::from_parts(reader, path)),
            len: 0,
        };
        assert!(report
            .push(TreeTransferFailure {
                path: "/file".to_owned(),
                error: CliError::invalid_request("failed")
            })
            .is_err());
        assert_eq!(report.len(), 0);
    }

    #[test]
    fn a_damaged_report_is_an_error_instead_of_a_truncated_success() {
        let mut report = TreeTransferFailures::default();
        report
            .push(TreeTransferFailure {
                path: "/first".to_owned(),
                error: CliError::invalid_request("failed"),
            })
            .expect("append");
        report
            .spool
            .as_mut()
            .expect("spooled")
            .write_all(b"{broken\n")
            .expect("damage report");
        assert!(serde_json::to_writer(io::sink(), &report).is_err());
    }
}
