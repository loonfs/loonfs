//! Renders command outcomes as human-readable text or `--json`.

mod human;
mod human_maintenance;
mod json;
mod output;
mod summaries;

use crate::args::CommandKind;
use crate::commands::{CommandData, CommandFailure, CommandOutput, DoctorCheck, DoctorStatus};
use crate::config_file::ConfigSource;
use crate::error::CliError;
use loonfs_types::{
    AttributeValue, CheckpointOwner, GcResponse, GrepIndexStatus, StoreProbeCheck,
    StoreProbeCheckOutcome,
};
use serde::Serialize;
use std::io::{self, Write};

pub(crate) use json::{json_error, render_parse_error};
#[cfg(test)]
pub(crate) use output::write_success;
use output::{human_error, request_id_suffix};
pub(crate) use output::{
    more_entries_hint, render_error, render_success, write_jsonl_page, write_listing_drift_warning,
    write_path_entries_page, write_stderr_progress, write_stderr_warning, OutputFormat,
};
pub(crate) use summaries::{
    store_probe_run_outcome, store_probe_summary_line, StoreProbeRunOutcome,
};

#[cfg(test)]
mod tests;
