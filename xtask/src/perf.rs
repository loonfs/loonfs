use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

const PERF_SCHEMA_VERSION: u32 = 1;
const PERF_BENCHMARK: &str = "cli_roundtrip";
const DEFAULT_PERF_PAYLOAD_SIZES: &str = "4KiB,64KiB,1MiB";
const DEFAULT_PERF_JSON_OUT: &str = "target/loonfs-perf/perf.ndjson";
const PERF_CONCURRENCY: u32 = 1;

#[derive(Debug, Subcommand)]
pub(crate) enum PerfCommand {
    Remote(RemotePerfArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct RemotePerfArgs {
    #[command(flatten)]
    common: PerfCommonArgs,
    #[arg(long = "server-url")]
    server_url: String,
    #[arg(long = "auth-token")]
    auth_token: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PerfCommonArgs {
    #[arg(long, default_value_t = 30)]
    iterations: u32,
    #[arg(long = "payload-sizes", default_value = DEFAULT_PERF_PAYLOAD_SIZES)]
    payload_sizes: String,
    #[arg(long)]
    namespace: Option<String>,
    #[arg(long = "json-out", default_value = DEFAULT_PERF_JSON_OUT)]
    json_out: PathBuf,
    #[arg(long, default_value_t = 0)]
    seed: u64,
    #[arg(long)]
    keep_workspace: bool,
}

pub(crate) fn run_and_render(
    command: PerfCommand,
    runner: &dyn crate::LoonRunner,
) -> Result<String> {
    let report = match command {
        PerfCommand::Remote(args) => run_remote_perf_with_id(&args, runner, &perf_run_id())?,
    };
    Ok(render_report(&report))
}

#[derive(Debug, Clone, PartialEq)]
struct PerfReport {
    output: PathBuf,
    measured_events: usize,
    failures: usize,
    samples: Vec<PerfSample>,
}

#[derive(Debug, Clone, PartialEq)]
struct PerfSample {
    operation: &'static str,
    elapsed_ms: f64,
}

#[derive(Debug, Serialize)]
struct PerfEvent<'a> {
    schema_version: u32,
    run_id: &'a str,
    timestamp_ms: u128,
    git_sha: Option<&'a str>,
    benchmark: &'static str,
    mode: &'static str,
    namespace_class: &'static str,
    operation: &'static str,
    payload_bytes: Option<u64>,
    payload_class: Option<&'static str>,
    concurrency: u32,
    iteration: u32,
    elapsed_ms: f64,
    ok: bool,
    error_class: Option<&'a str>,
}

#[derive(Debug)]
struct PerfContext {
    run_id: String,
    git_sha: Option<String>,
    namespace_class: &'static str,
    output: PathBuf,
    measured_events: usize,
    samples: Vec<PerfSample>,
    failures: usize,
}

impl PerfContext {
    fn write_event(&mut self, input: PerfEventInput<'_>) -> Result<()> {
        if let Some(parent) = self.output.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create perf output directory {}", parent.display()))?;
        }
        let event = PerfEvent {
            schema_version: PERF_SCHEMA_VERSION,
            run_id: &self.run_id,
            timestamp_ms: now_ms(),
            git_sha: self.git_sha.as_deref(),
            benchmark: PERF_BENCHMARK,
            mode: "remote",
            namespace_class: self.namespace_class,
            operation: input.operation,
            payload_bytes: input.payload_bytes,
            payload_class: input.payload_bytes.map(payload_class),
            concurrency: PERF_CONCURRENCY,
            iteration: input.iteration,
            elapsed_ms: input.elapsed_ms,
            ok: input.ok,
            error_class: input.error_class,
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.output)
            .with_context(|| format!("open perf output {}", self.output.display()))?;
        serde_json::to_writer(&mut file, &event).context("encode perf event")?;
        file.write_all(b"\n").context("write perf event newline")?;
        self.measured_events += 1;
        if input.ok {
            self.samples.push(PerfSample {
                operation: input.operation,
                elapsed_ms: input.elapsed_ms,
            });
        } else {
            self.failures += 1;
        }
        Ok(())
    }
}

struct PerfEventInput<'a> {
    operation: &'static str,
    payload_bytes: Option<u64>,
    iteration: u32,
    elapsed_ms: f64,
    ok: bool,
    error_class: Option<&'a str>,
}

fn run_remote_perf_with_id(
    args: &RemotePerfArgs,
    runner: &dyn crate::LoonRunner,
    run_id: &str,
) -> Result<PerfReport> {
    let payload_sizes = parse_payload_sizes(&args.common.payload_sizes)?;
    let session = crate::SmokeSession::new()?;
    let add_result = crate::run_json_command(
        runner,
        &session,
        crate::profile_create_remote_args(&session, &args.server_url, args.auth_token.as_deref()),
    )?;
    let _ = crate::expect_success_kind(add_result, "profile_create")?;
    let namespace = perf_namespace(args.common.namespace.as_deref(), run_id);
    let namespace_class = namespace_class(args.common.namespace.as_deref());
    let config = PerfRunConfig {
        namespace_class,
        namespace,
        payload_sizes,
        iterations: args.common.iterations,
        json_out: args.common.json_out.clone(),
        seed: args.common.seed,
        keep_workspace: args.common.keep_workspace,
    };
    execute_perf_run(runner, &session, run_id, config)
}

#[derive(Debug)]
struct PerfRunConfig {
    namespace_class: &'static str,
    namespace: String,
    payload_sizes: Vec<usize>,
    iterations: u32,
    json_out: PathBuf,
    seed: u64,
    keep_workspace: bool,
}

fn execute_perf_run(
    runner: &dyn crate::LoonRunner,
    session: &crate::SmokeSession,
    run_id: &str,
    config: PerfRunConfig,
) -> Result<PerfReport> {
    let workspace = PerfWorkspace::new(run_id, config.keep_workspace)?;
    let mut context = PerfContext {
        run_id: run_id.to_owned(),
        git_sha: git_sha(),
        namespace_class: config.namespace_class,
        output: config.json_out.clone(),
        measured_events: 0,
        samples: Vec::new(),
        failures: 0,
    };

    measure_perf_operation(&mut context, crate::STEP_CREATE_NAMESPACE, None, 0, || {
        create_perf_namespace(runner, session, &config.namespace)
    })?;

    for &payload_size in &config.payload_sizes {
        for iteration in 0..config.iterations {
            execute_perf_iteration(
                runner,
                session,
                &workspace,
                &mut context,
                &config,
                payload_size,
                iteration,
            )?;
        }
    }

    Ok(PerfReport {
        output: context.output,
        measured_events: context.measured_events,
        failures: context.failures,
        samples: context.samples,
    })
}

fn execute_perf_iteration(
    runner: &dyn crate::LoonRunner,
    session: &crate::SmokeSession,
    workspace: &PerfWorkspace,
    context: &mut PerfContext,
    config: &PerfRunConfig,
    payload_size: usize,
    iteration: u32,
) -> Result<()> {
    let namespace = config.namespace.as_str();
    let run_id = context.run_id.clone();
    let payload = deterministic_payload(payload_size, config.seed, iteration);
    let payload_class = payload_class(payload_size as u64);
    let remote_root = format!("/xtask-perf/{run_id}/{payload_class}/{iteration}");
    let source_path = format!("{remote_root}/payload.bin");
    let copy_path = format!("{remote_root}/copy.bin");
    let moved_path = format!("{remote_root}/moved.bin");
    let upload_file = workspace
        .root
        .join(format!("{payload_class}-{iteration}-upload.bin"));
    let download_file = workspace
        .root
        .join(format!("{payload_class}-{iteration}-download.bin"));
    fs::write(&upload_file, &payload)
        .with_context(|| format!("write perf payload {}", upload_file.display()))?;

    measure_perf_operation(
        context,
        crate::STEP_PUT,
        Some(payload_size as u64),
        iteration,
        || {
            let envelope = expect_success_kind_classified(
                crate::run_json_command(
                    runner,
                    session,
                    crate::filesystem_put_args(session, namespace, &upload_file, &source_path),
                )?,
                "filesystem_put",
            )?;
            let data = crate::require_data_type(&envelope, "file_mutation")?;
            let expected_target = format!("{namespace}:{source_path}");
            let actual_target = crate::string_field(data, "target")?;
            if actual_target != expected_target {
                return Err(classified_error(
                    "unexpected_result",
                    format!("put target `{actual_target}` did not match expected target"),
                ));
            }
            Ok(Measured::ok(()))
        },
    )?;

    measure_perf_operation(
        context,
        crate::STEP_STAT,
        Some(payload_size as u64),
        iteration,
        || stat_file(runner, session, namespace, &source_path, payload.len()),
    )?;

    measure_perf_operation(
        context,
        crate::STEP_CAT,
        Some(payload_size as u64),
        iteration,
        || {
            let output = crate::run_stream_command(
                runner,
                session,
                crate::filesystem_cat_args(session, namespace, &source_path),
            )?;
            if output.stdout != payload {
                return Err(classified_error(
                    "bytes_mismatch",
                    format!("cat bytes for `{source_path}` did not match uploaded payload"),
                ));
            }
            Ok(Measured::ok(()))
        },
    )?;

    measure_perf_operation(
        context,
        crate::STEP_GET,
        Some(payload_size as u64),
        iteration,
        || {
            let envelope = expect_success_kind_classified(
                crate::run_json_command(
                    runner,
                    session,
                    crate::filesystem_get_args(session, namespace, &source_path, &download_file),
                )?,
                "filesystem_get",
            )?;
            let data = crate::require_data_type(&envelope, "file_transfer")?;
            let bytes_written = crate::u64_field(data, "bytes_written")?;
            if bytes_written != payload.len() as u64 {
                return Err(classified_error(
                    "unexpected_result",
                    format!(
                        "get wrote {bytes_written} bytes, expected {}",
                        payload.len()
                    ),
                ));
            }
            let downloaded = fs::read(&download_file)
                .with_context(|| format!("read {}", download_file.display()))?;
            if downloaded != payload {
                return Err(classified_error(
                    "bytes_mismatch",
                    format!("downloaded bytes for `{source_path}` did not match uploaded payload"),
                ));
            }
            Ok(Measured::ok(()))
        },
    )?;

    measure_perf_operation(
        context,
        crate::STEP_LS,
        Some(payload_size as u64),
        iteration,
        || {
            let envelope = expect_success_kind_classified(
                crate::run_json_command(
                    runner,
                    session,
                    crate::filesystem_ls_args(session, namespace, &remote_root),
                )?,
                "filesystem_ls",
            )?;
            let data = crate::require_data_type(&envelope, "path_entries")?;
            let entries = crate::array_field(data, "entries")?;
            if !entries.iter().any(|entry| {
                entry.get("absolute_path").and_then(Value::as_str) == Some(source_path.as_str())
            }) {
                return Err(classified_error(
                    "unexpected_result",
                    "ls did not include uploaded path",
                ));
            }
            Ok(Measured::ok(()))
        },
    )?;

    measure_perf_operation(
        context,
        crate::STEP_CP,
        Some(payload_size as u64),
        iteration,
        || {
            let envelope = expect_success_kind_classified(
                crate::run_json_command(
                    runner,
                    session,
                    crate::filesystem_cp_args(session, namespace, &source_path, &copy_path),
                )?,
                "filesystem_cp",
            )?;
            let data = crate::require_data_type(&envelope, "path_move")?;
            let expected_source = format!("{namespace}:{source_path}");
            let actual_source = crate::string_field(data, "from")?;
            if actual_source != expected_source {
                return Err(classified_error(
                    "unexpected_result",
                    format!("copy source `{actual_source}` did not match expected source"),
                ));
            }
            let expected_dest = format!("{namespace}:{copy_path}");
            let actual_dest = crate::string_field(data, "to")?;
            if actual_dest != expected_dest {
                return Err(classified_error(
                    "unexpected_result",
                    format!("copy destination `{actual_dest}` did not match expected destination"),
                ));
            }
            Ok(Measured::ok(()))
        },
    )?;

    measure_perf_operation(
        context,
        crate::STEP_MOVE,
        Some(payload_size as u64),
        iteration,
        || {
            let envelope = expect_success_kind_classified(
                crate::run_json_command(
                    runner,
                    session,
                    crate::filesystem_mv_args(session, namespace, &copy_path, &moved_path),
                )?,
                "filesystem_mv",
            )?;
            let data = crate::require_data_type(&envelope, "path_move")?;
            let expected_dest = format!("{namespace}:{moved_path}");
            let actual_dest = crate::string_field(data, "to")?;
            if actual_dest != expected_dest {
                return Err(classified_error(
                    "unexpected_result",
                    format!("move destination `{actual_dest}` did not match expected destination"),
                ));
            }
            Ok(Measured::ok(()))
        },
    )?;

    measure_perf_operation(
        context,
        crate::STEP_RM,
        Some(payload_size as u64),
        iteration,
        || {
            let envelope = expect_success_kind_classified(
                crate::run_json_command(
                    runner,
                    session,
                    crate::filesystem_rm_args(session, namespace, &moved_path),
                )?,
                "filesystem_rm",
            )?;
            let data = crate::require_data_type(&envelope, "file_mutation")?;
            let expected_target = format!("{namespace}:{moved_path}");
            let actual_target = crate::string_field(data, "target")?;
            if actual_target != expected_target {
                return Err(classified_error(
                    "unexpected_result",
                    format!("rm target `{actual_target}` did not match expected target"),
                ));
            }
            Ok(Measured::ok(()))
        },
    )?;

    Ok(())
}

fn render_report(report: &PerfReport) -> String {
    let mut lines = vec![
        "LoonFS perf run complete".to_owned(),
        "  mode: remote".to_owned(),
        format!("  output: {}", report.output.display()),
        format!("  operations: {}", report.measured_events),
        format!("  failures: {}", report.failures),
    ];
    for operation in [
        crate::STEP_PUT,
        crate::STEP_STAT,
        crate::STEP_CAT,
        crate::STEP_GET,
        crate::STEP_LS,
        crate::STEP_CP,
        crate::STEP_MOVE,
        crate::STEP_RM,
    ] {
        let values = report
            .samples
            .iter()
            .filter(|sample| sample.operation == operation)
            .map(|sample| sample.elapsed_ms)
            .collect::<Vec<_>>();
        if values.is_empty() {
            continue;
        }
        lines.push(format!(
            "  {operation} p50/p95: {:.2} ms / {:.2} ms",
            percentile(values.clone(), 0.50),
            percentile(values, 0.95)
        ));
    }
    lines.join("\n")
}

#[derive(Debug)]
struct Measured<T> {
    value: T,
    ok_error_class: Option<&'static str>,
}

impl<T> Measured<T> {
    fn ok(value: T) -> Self {
        Self {
            value,
            ok_error_class: None,
        }
    }

    fn ok_with_class(value: T, error_class: &'static str) -> Self {
        Self {
            value,
            ok_error_class: Some(error_class),
        }
    }
}

fn measure_perf_operation<T>(
    context: &mut PerfContext,
    operation: &'static str,
    payload_bytes: Option<u64>,
    iteration: u32,
    f: impl FnOnce() -> Result<Measured<T>>,
) -> Result<T> {
    let started = Instant::now();
    let result = f();
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok(measured) => {
            context.write_event(PerfEventInput {
                operation,
                payload_bytes,
                iteration,
                elapsed_ms,
                ok: true,
                error_class: measured.ok_error_class,
            })?;
            Ok(measured.value)
        }
        Err(error) => {
            let error_class = classify_error(&error);
            context.write_event(PerfEventInput {
                operation,
                payload_bytes,
                iteration,
                elapsed_ms,
                ok: false,
                error_class: Some(&error_class),
            })?;
            Err(error)
        }
    }
}

fn create_perf_namespace(
    runner: &dyn crate::LoonRunner,
    session: &crate::SmokeSession,
    namespace: &str,
) -> Result<Measured<()>> {
    match crate::run_json_command(
        runner,
        session,
        crate::namespace_create_args(session, namespace),
    )? {
        crate::JsonCommandResult::Success(envelope) => {
            let envelope = crate::expect_kind_format(envelope, "namespace_create")?;
            let data = crate::require_data_type(&envelope, "namespace_summary")?;
            let name = crate::namespace_field(data)?;
            if name != namespace {
                return Err(classified_error(
                    "unexpected_result",
                    "namespace create returned unexpected namespace",
                ));
            }
            Ok(Measured::ok(()))
        }
        crate::JsonCommandResult::Failure(envelope) => {
            let envelope = crate::expect_kind_format(envelope, "namespace_create")?;
            let error = envelope.error.ok_or_else(|| {
                classified_error("missing_error", "failure envelope missing error")
            })?;
            if error.code == "namespace_exists" {
                Ok(Measured::ok_with_class((), "already_exists"))
            } else {
                Err(classified_error(error.code, error.message))
            }
        }
    }
}

fn stat_file(
    runner: &dyn crate::LoonRunner,
    session: &crate::SmokeSession,
    namespace: &str,
    path: &str,
    expected_size: usize,
) -> Result<Measured<()>> {
    let envelope = expect_success_kind_classified(
        crate::run_json_command(
            runner,
            session,
            crate::filesystem_stat_args(session, namespace, path),
        )?,
        "filesystem_stat",
    )?;
    let data = crate::require_data_type(&envelope, "path_entry")?;
    let inode_kind = crate::string_field(data, "inode_kind")?;
    if inode_kind != "file" {
        return Err(classified_error(
            "unexpected_result",
            format!("expected file kind, got `{inode_kind}`"),
        ));
    }
    let size_bytes = crate::u64_field(data, "size_bytes")?;
    if size_bytes != expected_size as u64 {
        return Err(classified_error(
            "unexpected_result",
            format!("expected size {expected_size}, got {size_bytes}"),
        ));
    }
    Ok(Measured::ok(()))
}

fn expect_success_kind_classified(
    result: crate::JsonCommandResult,
    expected_kind: &str,
) -> Result<crate::JsonEnvelope> {
    match result {
        crate::JsonCommandResult::Success(envelope) => {
            crate::expect_kind_format(envelope, expected_kind)
        }
        crate::JsonCommandResult::Failure(envelope) => {
            let envelope = crate::expect_kind_format(envelope, expected_kind)?;
            let error = envelope.error.ok_or_else(|| {
                classified_error("missing_error", "failure envelope missing error")
            })?;
            Err(classified_error(error.code, error.message))
        }
    }
}

#[derive(Debug)]
struct ClassifiedError {
    class: String,
    message: String,
}

impl std::fmt::Display for ClassifiedError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.class, self.message)
    }
}

impl std::error::Error for ClassifiedError {}

fn classified_error(class: impl Into<String>, message: impl Into<String>) -> anyhow::Error {
    ClassifiedError {
        class: class.into(),
        message: message.into(),
    }
    .into()
}

fn classify_error(error: &anyhow::Error) -> String {
    error
        .downcast_ref::<ClassifiedError>()
        .map(|error| error.class.clone())
        .unwrap_or_else(|| "other_error".to_owned())
}

fn perf_run_id() -> String {
    let millis = now_ms();
    format!("{}-{millis}", std::process::id())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn git_sha() -> Option<String> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent()?;
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--short")
        .arg("HEAD")
        .current_dir(workspace_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if sha.is_empty() {
        None
    } else {
        Some(sha)
    }
}

fn perf_namespace(namespace: Option<&str>, run_id: &str) -> String {
    namespace
        .map(str::to_owned)
        .unwrap_or_else(|| format!("perf-{}", run_id_fragment(run_id)))
}

fn namespace_class(namespace: Option<&str>) -> &'static str {
    if namespace.is_some() {
        "provided"
    } else {
        "ephemeral"
    }
}

fn run_id_fragment(run_id: &str) -> String {
    let mut out = run_id
        .chars()
        .filter(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || *ch == '-')
        .take(48)
        .collect::<String>();
    if out.is_empty() {
        out.push('0');
    }
    out
}

fn deterministic_payload(size: usize, seed: u64, iteration: u32) -> Vec<u8> {
    let mut x = seed ^ ((iteration as u64) << 32) ^ (size as u64);
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push((x & 0xff) as u8);
    }
    out
}

fn parse_payload_sizes(input: &str) -> Result<Vec<usize>> {
    let mut sizes = Vec::new();
    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        sizes.push(parse_payload_size(part)?);
    }
    if sizes.is_empty() {
        bail!("payload sizes must include at least one size");
    }
    Ok(sizes)
}

fn parse_payload_size(input: &str) -> Result<usize> {
    let split_at = input
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, unit) = input.split_at(split_at);
    if number.is_empty() {
        bail!("invalid payload size `{input}`: missing number");
    }
    let value = number
        .parse::<usize>()
        .with_context(|| format!("parse payload size `{input}`"))?;
    if value == 0 {
        bail!("invalid payload size `{input}`: size must be greater than zero");
    }
    let multiplier = match unit {
        "" | "B" => 1usize,
        "KiB" => 1024,
        "MiB" => 1024 * 1024,
        "GiB" => 1024 * 1024 * 1024,
        _ => bail!("invalid payload size `{input}`: unsupported unit `{unit}`"),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow!("invalid payload size `{input}`: size overflow"))
}

fn payload_class(size: u64) -> &'static str {
    match size {
        0..=4096 => "tiny",
        4097..=65536 => "small",
        65537..=1048576 => "medium",
        1048577..=16777216 => "large",
        _ => "huge",
    }
}

fn percentile(mut values: Vec<f64>, quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let rank = ((values.len() as f64) * quantile).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    values[index]
}

#[derive(Debug)]
struct PerfWorkspace {
    _temp_dir: Option<tempfile::TempDir>,
    root: PathBuf,
}

impl PerfWorkspace {
    fn new(run_id: &str, keep_workspace: bool) -> Result<Self> {
        if keep_workspace {
            let root = PathBuf::from("target")
                .join("loonfs-perf")
                .join("workspaces")
                .join(run_id_fragment(run_id));
            fs::create_dir_all(&root)
                .with_context(|| format!("create perf workspace {}", root.display()))?;
            Ok(Self {
                _temp_dir: None,
                root,
            })
        } else {
            let temp_dir = tempdir().context("create tempdir for perf workspace")?;
            let root = temp_dir.path().to_path_buf();
            Ok(Self {
                _temp_dir: Some(temp_dir),
                root,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use tempfile::NamedTempFile;

    #[test]
    fn remote_perf_runs_expected_cli_sequence_and_writes_events() {
        let json_out = NamedTempFile::new().expect("temp ndjson");
        let args = RemotePerfArgs {
            common: PerfCommonArgs {
                iterations: 1,
                payload_sizes: "4KiB".to_owned(),
                namespace: Some("demo".to_owned()),
                json_out: json_out.path().to_path_buf(),
                seed: 1,
                keep_workspace: false,
            },
            server_url: "http://127.0.0.1:9400".to_owned(),
            auth_token: Some("dev-token".to_owned()),
        };
        let runner = RecordingRunner::new(perf_success_outputs("demo", "seed", 4096, 0));

        let report = run_remote_perf_with_id(&args, &runner, "seed").expect("remote perf");

        assert_eq!(report.measured_events, 9);
        let calls = runner.calls();
        assert_command_suffix(
            &calls[0],
            &[
                "--json",
                "--no-input",
                "profile",
                "create",
                "smoke",
                "--mode",
                "remote",
                "--server-url",
                "http://127.0.0.1:9400",
                "--auth-token",
                "dev-token",
            ],
        );
        assert!(calls.iter().any(|call| call.iter().any(|arg| arg == "cat")));

        let lines = fs::read_to_string(json_out.path()).expect("read ndjson");
        let events = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("event json"))
            .collect::<Vec<_>>();
        assert!(events
            .iter()
            .any(|event| event["operation"] == crate::STEP_PUT));
        assert!(events.iter().all(|event| event.get("namespace").is_none()));
        assert!(events.iter().all(|event| event.get("path").is_none()));
        assert!(events.iter().all(|event| event.get("payload").is_none()));
        assert_eq!(events[0]["namespace_class"], "provided");
    }

    #[test]
    fn payload_size_parser_and_payload_generation_are_stable() {
        assert_eq!(
            parse_payload_sizes("4KiB,64KiB,1MiB").expect("parse sizes"),
            vec![4096, 65536, 1048576]
        );
        assert!(parse_payload_sizes("0KiB").is_err());
        assert!(parse_payload_sizes("4KB").is_err());
        assert!(parse_payload_sizes("").is_err());

        let first = deterministic_payload(32, 7, 1);
        assert_eq!(first, deterministic_payload(32, 7, 1));
        assert_ne!(first, deterministic_payload(32, 8, 1));
        assert_ne!(first, deterministic_payload(32, 7, 2));
        assert_ne!(first, deterministic_payload(33, 7, 1));
    }

    #[test]
    fn perf_failure_records_failed_event() {
        let json_out = NamedTempFile::new().expect("temp ndjson");
        let args = RemotePerfArgs {
            common: PerfCommonArgs {
                iterations: 1,
                payload_sizes: "4KiB".to_owned(),
                namespace: Some("demo".to_owned()),
                json_out: json_out.path().to_path_buf(),
                seed: 1,
                keep_workspace: false,
            },
            server_url: "http://127.0.0.1:9400".to_owned(),
            auth_token: None,
        };
        let mut outputs = perf_success_outputs("demo", "seed", 4096, 0);
        outputs[2] = json_failure("filesystem_put", "durable_content", "missing content");
        let runner = RecordingRunner::new(outputs);

        let error = run_remote_perf_with_id(&args, &runner, "seed").expect_err("perf should fail");

        assert!(error.to_string().contains("durable_content"));
        let lines = fs::read_to_string(json_out.path()).expect("read ndjson");
        let events = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("event json"))
            .collect::<Vec<_>>();
        let put = events
            .iter()
            .find(|event| event["operation"] == crate::STEP_PUT)
            .expect("put event");
        assert_eq!(put["ok"], false);
        assert_eq!(put["error_class"], "durable_content");
    }

    fn perf_success_outputs(
        namespace: &str,
        run_id: &str,
        payload_size: usize,
        iteration: u32,
    ) -> Vec<crate::LoonOutput> {
        let mut outputs = Vec::new();
        outputs.push(json_success(
            "profile_create",
            json!({"type":"profile","name":"smoke","mode":"remote","active":true}),
        ));
        outputs.push(json_success(
            "namespace_create",
            json!({"type":"namespace_summary","name":namespace}),
        ));
        let payload_class = payload_class(payload_size as u64);
        let source_path = format!("/xtask-perf/{run_id}/{payload_class}/{iteration}/payload.bin");
        let copy_path = format!("/xtask-perf/{run_id}/{payload_class}/{iteration}/copy.bin");
        let moved_path = format!("/xtask-perf/{run_id}/{payload_class}/{iteration}/moved.bin");
        outputs.push(json_success(
            "filesystem_put",
            json!({"type":"file_mutation","target":format!("{namespace}:{source_path}"),"committed_seq":1}),
        ));
        outputs.push(json_success(
            "filesystem_stat",
            json!({"type":"path_entry","absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload_size,"authoritative_head_seq":1,"display_name":"payload.bin","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}),
        ));
        outputs.push(crate::LoonOutput {
            exit_code: Some(0),
            stdout: deterministic_payload(payload_size, 1, iteration),
            stderr: Vec::new(),
        });
        outputs.push(json_success(
            "filesystem_get",
            json!({"type":"file_transfer","target":format!("{namespace}:{source_path}"),"destination":"/tmp/downloaded.bin","bytes_written":payload_size}),
        ));
        outputs.push(json_success(
            "filesystem_ls",
            json!({"type":"path_entries","entries":[{"absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload_size,"authoritative_head_seq":1,"display_name":"payload.bin","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}]}),
        ));
        outputs.push(json_success(
            "filesystem_cp",
            json!({"type":"path_move","from":format!("{namespace}:{source_path}"),"to":format!("{namespace}:{copy_path}"),"committed_seq":2}),
        ));
        outputs.push(json_success(
            "filesystem_mv",
            json!({"type":"path_move","from":format!("{namespace}:{copy_path}"),"to":format!("{namespace}:{moved_path}"),"committed_seq":3}),
        ));
        outputs.push(json_success(
            "filesystem_rm",
            json!({"type":"file_mutation","target":format!("{namespace}:{moved_path}"),"committed_seq":4}),
        ));
        outputs
    }

    fn json_success(kind: &str, data: Value) -> crate::LoonOutput {
        let body = serde_json::to_vec(&json!({
            "kind": kind,
            "format_version": crate::FORMAT_VERSION,
            "profile": crate::PROFILE_NAME,
            "mode": "remote",
            "data": data
        }))
        .expect("encode success body");
        crate::LoonOutput {
            exit_code: Some(0),
            stdout: body,
            stderr: Vec::new(),
        }
    }

    fn json_failure(kind: &str, code: &str, message: &str) -> crate::LoonOutput {
        let body = serde_json::to_vec(&json!({
            "kind": kind,
            "format_version": crate::FORMAT_VERSION,
            "profile": crate::PROFILE_NAME,
            "mode": "remote",
            "error": {
                "code": code,
                "message": message
            }
        }))
        .expect("encode error body");
        crate::LoonOutput {
            exit_code: Some(1),
            stdout: Vec::new(),
            stderr: body,
        }
    }

    fn assert_command_suffix(actual: &[String], suffix: &[&str]) {
        let actual_suffix = &actual[actual.len() - suffix.len()..];
        let expected = suffix
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        assert_eq!(actual_suffix, expected.as_slice());
    }

    struct RecordingRunner {
        outputs: RefCell<VecDeque<crate::LoonOutput>>,
        calls: RefCell<Vec<Vec<String>>>,
        last_payload: RefCell<Option<Vec<u8>>>,
    }

    impl RecordingRunner {
        fn new(outputs: Vec<crate::LoonOutput>) -> Self {
            Self {
                outputs: RefCell::new(outputs.into()),
                calls: RefCell::new(Vec::new()),
                last_payload: RefCell::new(None),
            }
        }

        fn calls(&self) -> Vec<Vec<String>> {
            self.calls.borrow().clone()
        }
    }

    impl crate::LoonRunner for RecordingRunner {
        fn run(
            &self,
            _session: &crate::SmokeSession,
            args: &[String],
        ) -> Result<crate::LoonOutput> {
            self.calls.borrow_mut().push(args.to_vec());

            if args.iter().any(|arg| arg == "put") {
                let local_path = args
                    .get(args.len().saturating_sub(2))
                    .ok_or_else(|| anyhow!("missing put local path"))?;
                if let Ok(bytes) = fs::read(local_path) {
                    *self.last_payload.borrow_mut() = Some(bytes);
                }
            }

            if args.iter().any(|arg| arg == "get") {
                let destination = args
                    .last()
                    .ok_or_else(|| anyhow!("missing get destination"))?;
                let bytes = self
                    .last_payload
                    .borrow()
                    .clone()
                    .unwrap_or_else(|| b"xtask smoke payload demo seed\n".to_vec());
                fs::write(destination, bytes)
                    .with_context(|| format!("write fake download `{destination}`"))?;
            }

            self.outputs
                .borrow_mut()
                .pop_front()
                .ok_or_else(|| anyhow!("no fake output left for call {args:?}"))
        }
    }
}
