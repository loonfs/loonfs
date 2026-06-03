use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

const PROFILE_NAME: &str = "smoke";
const FORMAT_VERSION: u32 = 1;
const PERF_SCHEMA_VERSION: u32 = 1;
const PERF_BENCHMARK: &str = "cli_roundtrip";
const DEFAULT_PERF_PAYLOAD_SIZES: &str = "4KiB,64KiB,1MiB";
const DEFAULT_PERF_JSON_OUT: &str = "target/loonfs-perf/perf.ndjson";
const PERF_CONCURRENCY: u32 = 1;

const STEP_CREATE_NAMESPACE: &str = "create_namespace";
const STEP_LIST_NAMESPACES: &str = "list_namespaces";
const STEP_PUT: &str = "put";
const STEP_LS: &str = "ls";
const STEP_STAT: &str = "stat";
const STEP_CAT: &str = "cat";
const STEP_GET: &str = "get";
const STEP_CP: &str = "cp";
const STEP_MOVE: &str = "move";
const STEP_RM: &str = "rm";
const STEP_VERIFY_REMOVAL: &str = "verify_removal";

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    Smoke {
        #[command(subcommand)]
        command: SmokeCommand,
    },
    Perf {
        #[command(subcommand)]
        command: PerfCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SmokeCommand {
    Local(LocalSmokeArgs),
    Remote(RemoteSmokeArgs),
}

#[derive(Debug, Subcommand)]
enum PerfCommand {
    Local(LocalPerfArgs),
    Remote(RemotePerfArgs),
}

#[derive(Debug, Clone, Args)]
struct LocalSmokeArgs {
    #[arg(long = "server-config")]
    server_config: PathBuf,
    #[arg(long)]
    namespace: String,
}

#[derive(Debug, Clone, Args)]
struct RemoteSmokeArgs {
    #[arg(long = "server-url")]
    server_url: String,
    #[arg(long = "auth-token")]
    auth_token: Option<String>,
    #[arg(long)]
    namespace: String,
}

#[derive(Debug, Clone, Args)]
struct LocalPerfArgs {
    #[command(flatten)]
    common: PerfCommonArgs,
    #[arg(long = "server-config")]
    server_config: PathBuf,
}

#[derive(Debug, Clone, Args)]
struct RemotePerfArgs {
    #[command(flatten)]
    common: PerfCommonArgs,
    #[arg(long = "server-url")]
    server_url: String,
    #[arg(long = "auth-token")]
    auth_token: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PerfCommonArgs {
    #[arg(long, default_value_t = 5)]
    warmups: u32,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmokeMode {
    Local,
    Remote,
}

impl SmokeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }

    fn envelope_mode(self) -> &'static str {
        match self {
            Self::Local => "embedded",
            Self::Remote => "remote",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PerfMode {
    Local,
    Remote,
}

impl PerfMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SmokeReport {
    mode: SmokeMode,
    namespace: String,
    steps: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq)]
struct PerfReport {
    mode: PerfMode,
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

#[derive(Debug)]
struct LoonOutput {
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl LoonOutput {
    fn success(&self) -> bool {
        self.exit_code == Some(0)
    }
}

trait LoonRunner {
    fn run(&self, session: &SmokeSession, args: &[String]) -> Result<LoonOutput>;
}

struct CargoLoonRunner {
    cargo: String,
    workspace_root: PathBuf,
}

impl CargoLoonRunner {
    fn new() -> Result<Self> {
        let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .context("resolve workspace root from xtask manifest dir")?
            .to_path_buf();
        Ok(Self {
            cargo,
            workspace_root,
        })
    }
}

impl LoonRunner for CargoLoonRunner {
    fn run(&self, session: &SmokeSession, args: &[String]) -> Result<LoonOutput> {
        let output = Command::new(&self.cargo)
            .arg("run")
            .arg("--quiet")
            .arg("-p")
            .arg("loon-cli")
            .arg("--")
            .args(args)
            .env("HOME", &session.home_dir)
            .current_dir(&self.workspace_root)
            .output()
            .with_context(|| format!("run loon CLI with args `{}`", args.join(" ")))?;
        Ok(LoonOutput {
            exit_code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Debug)]
struct SmokeSession {
    _temp_dir: tempfile::TempDir,
    home_dir: PathBuf,
}

impl SmokeSession {
    fn new() -> Result<Self> {
        let temp_dir = tempdir().context("create tempdir for smoke session")?;
        let home_dir = temp_dir.path().join("home");
        fs::create_dir_all(&home_dir).context("create smoke home dir")?;
        Ok(Self {
            _temp_dir: temp_dir,
            home_dir,
        })
    }
}

#[derive(Debug, Deserialize)]
struct JsonEnvelope {
    kind: String,
    format_version: u32,
    profile: Option<String>,
    mode: Option<String>,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    error: Option<JsonError>,
}

#[derive(Debug, Deserialize)]
struct JsonError {
    code: String,
    message: String,
}

enum JsonCommandResult {
    Success(JsonEnvelope),
    Failure(JsonEnvelope),
}

#[derive(Debug, Serialize)]
struct PerfEvent<'a> {
    schema_version: u32,
    run_id: &'a str,
    timestamp_ms: u128,
    git_sha: Option<&'a str>,
    benchmark: &'static str,
    mode: &'static str,
    store_kind: Option<&'a str>,
    namespace_class: &'static str,
    operation: &'static str,
    payload_bytes: Option<u64>,
    payload_class: Option<&'static str>,
    concurrency: u32,
    iteration: u32,
    warmup: bool,
    elapsed_ms: f64,
    ok: bool,
    error_class: Option<&'a str>,
}

#[derive(Debug)]
struct PerfContext {
    run_id: String,
    git_sha: Option<String>,
    mode: PerfMode,
    store_kind: Option<String>,
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
            mode: self.mode.as_str(),
            store_kind: self.store_kind.as_deref(),
            namespace_class: self.namespace_class,
            operation: input.operation,
            payload_bytes: input.payload_bytes,
            payload_class: input.payload_bytes.map(payload_class),
            concurrency: PERF_CONCURRENCY,
            iteration: input.iteration,
            warmup: input.warmup,
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
        if !input.warmup {
            self.measured_events += 1;
        }
        if !input.warmup && input.ok {
            self.samples.push(PerfSample {
                operation: input.operation,
                elapsed_ms: input.elapsed_ms,
            });
        }
        if !input.ok {
            self.failures += 1;
        }
        Ok(())
    }
}

struct PerfEventInput<'a> {
    operation: &'static str,
    payload_bytes: Option<u64>,
    iteration: u32,
    warmup: bool,
    elapsed_ms: f64,
    ok: bool,
    error_class: Option<&'a str>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runner = CargoLoonRunner::new()?;
    let rendered = match cli.command {
        CommandKind::Smoke { command } => match command {
            SmokeCommand::Local(args) => render_report(&run_local_smoke_with(&args, &runner)?),
            SmokeCommand::Remote(args) => render_report(&run_remote_smoke_with(&args, &runner)?),
        },
        CommandKind::Perf { command } => match command {
            PerfCommand::Local(args) => render_perf_report(&run_local_perf_with(&args, &runner)?),
            PerfCommand::Remote(args) => render_perf_report(&run_remote_perf_with(&args, &runner)?),
        },
    };
    println!("{rendered}");
    Ok(())
}

fn run_local_smoke_with(args: &LocalSmokeArgs, runner: &dyn LoonRunner) -> Result<SmokeReport> {
    let smoke_id = smoke_id();
    run_local_smoke_with_id(args, runner, &smoke_id)
}

fn run_local_smoke_with_id(
    args: &LocalSmokeArgs,
    runner: &dyn LoonRunner,
    smoke_id: &str,
) -> Result<SmokeReport> {
    let session = SmokeSession::new()?;
    let add_result = run_json_command(
        runner,
        &session,
        profile_create_local_args(&session, &args.server_config)?,
    )?;
    let _ = expect_success_kind(add_result, "profile_create")?;

    let steps = execute_smoke_sequence(
        runner,
        &session,
        SmokeMode::Local,
        &args.namespace,
        smoke_id,
    )?;
    Ok(SmokeReport {
        mode: SmokeMode::Local,
        namespace: args.namespace.clone(),
        steps,
    })
}

fn run_remote_smoke_with(args: &RemoteSmokeArgs, runner: &dyn LoonRunner) -> Result<SmokeReport> {
    let smoke_id = smoke_id();
    run_remote_smoke_with_id(args, runner, &smoke_id)
}

fn run_remote_smoke_with_id(
    args: &RemoteSmokeArgs,
    runner: &dyn LoonRunner,
    smoke_id: &str,
) -> Result<SmokeReport> {
    let session = SmokeSession::new()?;
    let add_result = run_json_command(
        runner,
        &session,
        profile_create_remote_args(&session, &args.server_url, args.auth_token.as_deref()),
    )?;
    let _ = expect_success_kind(add_result, "profile_create")?;

    let steps = execute_smoke_sequence(
        runner,
        &session,
        SmokeMode::Remote,
        &args.namespace,
        smoke_id,
    )?;
    Ok(SmokeReport {
        mode: SmokeMode::Remote,
        namespace: args.namespace.clone(),
        steps,
    })
}

fn run_local_perf_with(args: &LocalPerfArgs, runner: &dyn LoonRunner) -> Result<PerfReport> {
    run_local_perf_with_id(args, runner, &perf_run_id())
}

fn run_local_perf_with_id(
    args: &LocalPerfArgs,
    runner: &dyn LoonRunner,
    run_id: &str,
) -> Result<PerfReport> {
    let payload_sizes = parse_payload_sizes(&args.common.payload_sizes)?;
    let session = SmokeSession::new()?;
    let add_result = run_json_command(
        runner,
        &session,
        profile_create_local_args(&session, &args.server_config)?,
    )?;
    let _ = expect_success_kind(add_result, "profile_create")?;
    let store_kind = load_store_kind(&args.server_config)?;
    let namespace = perf_namespace(args.common.namespace.as_deref(), run_id);
    let namespace_class = namespace_class(args.common.namespace.as_deref());
    let context = PerfRunConfig {
        mode: PerfMode::Local,
        store_kind: Some(store_kind),
        namespace_class,
        namespace,
        payload_sizes,
        warmups: args.common.warmups,
        iterations: args.common.iterations,
        json_out: args.common.json_out.clone(),
        seed: args.common.seed,
        keep_workspace: args.common.keep_workspace,
    };
    execute_perf_run(runner, &session, run_id, context)
}

fn run_remote_perf_with(args: &RemotePerfArgs, runner: &dyn LoonRunner) -> Result<PerfReport> {
    run_remote_perf_with_id(args, runner, &perf_run_id())
}

fn run_remote_perf_with_id(
    args: &RemotePerfArgs,
    runner: &dyn LoonRunner,
    run_id: &str,
) -> Result<PerfReport> {
    let payload_sizes = parse_payload_sizes(&args.common.payload_sizes)?;
    let session = SmokeSession::new()?;
    let add_result = run_json_command(
        runner,
        &session,
        profile_create_remote_args(&session, &args.server_url, args.auth_token.as_deref()),
    )?;
    let _ = expect_success_kind(add_result, "profile_create")?;
    let namespace = perf_namespace(args.common.namespace.as_deref(), run_id);
    let namespace_class = namespace_class(args.common.namespace.as_deref());
    let context = PerfRunConfig {
        mode: PerfMode::Remote,
        store_kind: None,
        namespace_class,
        namespace,
        payload_sizes,
        warmups: args.common.warmups,
        iterations: args.common.iterations,
        json_out: args.common.json_out.clone(),
        seed: args.common.seed,
        keep_workspace: args.common.keep_workspace,
    };
    execute_perf_run(runner, &session, run_id, context)
}

#[derive(Debug)]
struct PerfRunConfig {
    mode: PerfMode,
    store_kind: Option<String>,
    namespace_class: &'static str,
    namespace: String,
    payload_sizes: Vec<usize>,
    warmups: u32,
    iterations: u32,
    json_out: PathBuf,
    seed: u64,
    keep_workspace: bool,
}

fn execute_perf_run(
    runner: &dyn LoonRunner,
    session: &SmokeSession,
    run_id: &str,
    config: PerfRunConfig,
) -> Result<PerfReport> {
    let workspace = PerfWorkspace::new(run_id, config.keep_workspace)?;
    let mut context = PerfContext {
        run_id: run_id.to_owned(),
        git_sha: git_sha(),
        mode: config.mode,
        store_kind: config.store_kind,
        namespace_class: config.namespace_class,
        output: config.json_out.clone(),
        measured_events: 0,
        samples: Vec::new(),
        failures: 0,
    };

    measure_perf_operation(&mut context, STEP_CREATE_NAMESPACE, None, 0, false, || {
        create_perf_namespace(runner, session, &config.namespace)
    })?;

    for &payload_size in &config.payload_sizes {
        for warmup_iteration in 0..config.warmups {
            execute_perf_iteration(
                runner,
                session,
                &workspace,
                &mut context,
                &config.namespace,
                payload_size,
                config.seed,
                warmup_iteration,
                true,
            )?;
        }
        for iteration in 0..config.iterations {
            execute_perf_iteration(
                runner,
                session,
                &workspace,
                &mut context,
                &config.namespace,
                payload_size,
                config.seed,
                iteration,
                false,
            )?;
        }
    }

    Ok(PerfReport {
        mode: context.mode,
        output: context.output,
        measured_events: context.measured_events,
        failures: context.failures,
        samples: context.samples,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_perf_iteration(
    runner: &dyn LoonRunner,
    session: &SmokeSession,
    workspace: &PerfWorkspace,
    context: &mut PerfContext,
    namespace: &str,
    payload_size: usize,
    seed: u64,
    iteration: u32,
    warmup: bool,
) -> Result<()> {
    let run_id = context.run_id.clone();
    let payload = deterministic_payload(payload_size, seed, iteration);
    let payload_class = payload_class(payload_size as u64);
    let phase = if warmup { "warmup" } else { "measured" };
    let remote_root = format!("/xtask-perf/{run_id}/{payload_class}/{phase}-{iteration}");
    let source_path = format!("{remote_root}/payload.bin");
    let upload_file = workspace
        .root
        .join(format!("{payload_class}-{phase}-{iteration}-upload.bin"));
    let download_file = workspace
        .root
        .join(format!("{payload_class}-{phase}-{iteration}-download.bin"));
    fs::write(&upload_file, &payload)
        .with_context(|| format!("write perf payload {}", upload_file.display()))?;

    measure_perf_operation(
        context,
        STEP_PUT,
        Some(payload_size as u64),
        iteration,
        warmup,
        || {
            let envelope = expect_success_kind_classified(
                run_json_command(
                    runner,
                    session,
                    filesystem_put_args(session, namespace, &upload_file, &source_path),
                )?,
                "filesystem_put",
            )?;
            let data = require_data_type(&envelope, "file_mutation")?;
            let expected_target = format!("{namespace}:{source_path}");
            let actual_target = string_field(data, "target")?;
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
        STEP_STAT,
        Some(payload_size as u64),
        iteration,
        warmup,
        || stat_file(runner, session, namespace, &source_path, payload.len()),
    )?;

    measure_perf_operation(
        context,
        STEP_CAT,
        Some(payload_size as u64),
        iteration,
        warmup,
        || {
            let output = run_stream_command(
                runner,
                session,
                filesystem_cat_args(session, namespace, &source_path),
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
        STEP_GET,
        Some(payload_size as u64),
        iteration,
        warmup,
        || {
            let envelope = expect_success_kind_classified(
                run_json_command(
                    runner,
                    session,
                    filesystem_get_args(session, namespace, &source_path, &download_file),
                )?,
                "filesystem_get",
            )?;
            let data = require_data_type(&envelope, "file_transfer")?;
            let bytes_written = u64_field(data, "bytes_written")?;
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
        STEP_LS,
        Some(payload_size as u64),
        iteration,
        warmup,
        || {
            let envelope = expect_success_kind_classified(
                run_json_command(
                    runner,
                    session,
                    filesystem_ls_args(session, namespace, &remote_root),
                )?,
                "filesystem_ls",
            )?;
            let data = require_data_type(&envelope, "path_entries")?;
            let entries = array_field(data, "entries")?;
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

    Ok(())
}

fn execute_smoke_sequence(
    runner: &dyn LoonRunner,
    session: &SmokeSession,
    mode: SmokeMode,
    namespace: &str,
    smoke_id: &str,
) -> Result<Vec<&'static str>> {
    let expected_envelope_mode = mode.envelope_mode();
    let temp_dir = tempdir().context("create tempdir for smoke payloads")?;
    let smoke_root = format!("/xtask-smoke-{smoke_id}");
    let uploaded_path = format!("{smoke_root}/input.txt");
    let copied_path = format!("{smoke_root}/copy.txt");
    let moved_path = format!("{smoke_root}/moved.txt");
    let parent_path = smoke_root.clone();

    let upload_file = temp_dir.path().join("input.txt");
    let downloaded_file = temp_dir.path().join("downloaded.txt");
    let payload = format!("xtask smoke payload {namespace} {smoke_id}\n").into_bytes();
    fs::write(&upload_file, &payload)
        .with_context(|| format!("write smoke input file {}", upload_file.display()))?;

    let create_result =
        run_json_command(runner, session, namespace_create_args(session, namespace))?;
    match create_result {
        JsonCommandResult::Success(envelope) => {
            let envelope = expect_kind_format(envelope, "namespace_create")?;
            let data = require_data_type(&envelope, "namespace_summary")?;
            let name = namespace_field(data)?;
            if name != namespace {
                bail!("namespace create returned `{name}`, expected `{namespace}`");
            }
        }
        JsonCommandResult::Failure(envelope) => {
            let envelope = expect_kind_format(envelope, "namespace_create")?;
            let error = envelope
                .error
                .as_ref()
                .ok_or_else(|| anyhow!("namespace create failure envelope missing error"))?;
            if error.code != "namespace_exists" {
                bail!("create namespace `{namespace}`: {}", error.message);
            }
        }
    }

    let list_envelope = expect_success(
        run_json_command(runner, session, namespace_list_args(session))?,
        "namespace_list",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let list_data = require_data_type(&list_envelope, "namespace_list")?;
    let namespaces = array_field(list_data, "namespaces")?;
    if !namespaces
        .iter()
        .any(|value| namespace_field(value).ok() == Some(namespace))
    {
        bail!("namespace `{namespace}` was not returned by namespace list");
    }

    let put_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_put_args(session, namespace, &upload_file, &uploaded_path),
        )?,
        "filesystem_put",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let put_data = require_data_type(&put_envelope, "file_mutation")?;
    let expected_target = format!("{namespace}:{uploaded_path}");
    let put_target = string_field(put_data, "target")?;
    if put_target != expected_target {
        bail!("put target `{put_target}` did not match `{expected_target}`");
    }

    let ls_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_ls_args(session, namespace, &parent_path),
        )?,
        "filesystem_ls",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let ls_data = require_data_type(&ls_envelope, "path_entries")?;
    let ls_entries = array_field(ls_data, "entries")?;
    if !ls_entries.iter().any(|entry| {
        entry.get("absolute_path").and_then(Value::as_str) == Some(uploaded_path.as_str())
    }) {
        bail!("ls {parent_path} did not include `{uploaded_path}`");
    }

    let stat_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_stat_args(session, namespace, &uploaded_path),
        )?,
        "filesystem_stat",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let stat_data = require_data_type(&stat_envelope, "path_entry")?;
    let source_inode = id_field(stat_data, "inode_id")?;
    let inode_kind = string_field(stat_data, "inode_kind")?;
    if inode_kind != "file" {
        bail!("expected `{uploaded_path}` to be a file, got `{inode_kind}`");
    }
    let size_bytes = u64_field(stat_data, "size_bytes")?;
    if size_bytes != payload.len() as u64 {
        bail!(
            "expected `{uploaded_path}` size {}, got {size_bytes}",
            payload.len()
        );
    }

    let cat_output = run_stream_command(
        runner,
        session,
        filesystem_cat_args(session, namespace, &uploaded_path),
    )?;
    if cat_output.stdout != payload {
        bail!("cat bytes for `{uploaded_path}` did not match uploaded payload");
    }

    let get_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_get_args(session, namespace, &uploaded_path, &downloaded_file),
        )?,
        "filesystem_get",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let get_data = require_data_type(&get_envelope, "file_transfer")?;
    let bytes_written = u64_field(get_data, "bytes_written")?;
    if bytes_written != payload.len() as u64 {
        bail!(
            "expected get to write {} bytes, got {bytes_written}",
            payload.len()
        );
    }
    let downloaded = fs::read(&downloaded_file)
        .with_context(|| format!("read {}", downloaded_file.display()))?;
    if downloaded != payload {
        bail!("downloaded bytes for `{uploaded_path}` did not match uploaded payload");
    }

    let cp_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_cp_args(session, namespace, &uploaded_path, &copied_path),
        )?,
        "filesystem_cp",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let cp_data = require_data_type(&cp_envelope, "path_move")?;
    let cp_from = string_field(cp_data, "from")?;
    let cp_to = string_field(cp_data, "to")?;
    if cp_from != expected_target {
        bail!("copy source `{cp_from}` did not match `{expected_target}`");
    }
    let expected_copy_target = format!("{namespace}:{copied_path}");
    if cp_to != expected_copy_target {
        bail!("copy target `{cp_to}` did not match `{expected_copy_target}`");
    }

    let copy_stat_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_stat_args(session, namespace, &copied_path),
        )?,
        "filesystem_stat",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let copy_stat = require_data_type(&copy_stat_envelope, "path_entry")?;
    let copied_inode = id_field(copy_stat, "inode_id")?;
    if copied_inode == source_inode {
        bail!("copy `{copied_path}` reused source inode `{source_inode}`");
    }

    let mv_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_mv_args(session, namespace, &copied_path, &moved_path),
        )?,
        "filesystem_mv",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let mv_data = require_data_type(&mv_envelope, "path_move")?;
    let moved_to = string_field(mv_data, "to")?;
    let expected_moved_target = format!("{namespace}:{moved_path}");
    if moved_to != expected_moved_target {
        bail!("move target `{moved_to}` did not match `{expected_moved_target}`");
    }

    let rm_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_rm_args(session, namespace, &moved_path),
        )?,
        "filesystem_rm",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let rm_data = require_data_type(&rm_envelope, "file_mutation")?;
    let removed_target = string_field(rm_data, "target")?;
    if removed_target != expected_moved_target {
        bail!("rm target `{removed_target}` did not match `{expected_moved_target}`");
    }

    let final_ls_envelope = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_ls_args(session, namespace, &parent_path),
        )?,
        "filesystem_ls",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    let final_ls_data = require_data_type(&final_ls_envelope, "path_entries")?;
    let final_entries = array_field(final_ls_data, "entries")?;
    let source_present = final_entries.iter().any(|entry| {
        entry.get("absolute_path").and_then(Value::as_str) == Some(uploaded_path.as_str())
    });
    let moved_present = final_entries.iter().any(|entry| {
        entry.get("absolute_path").and_then(Value::as_str) == Some(moved_path.as_str())
    });
    if !source_present {
        bail!("final ls {parent_path} did not include original source `{uploaded_path}`");
    }
    if moved_present {
        bail!("final ls {parent_path} still included removed path `{moved_path}`");
    }

    let _ = expect_success(
        run_json_command(
            runner,
            session,
            filesystem_stat_args(session, namespace, &uploaded_path),
        )?,
        "filesystem_stat",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;

    let removed_stat = run_json_command(
        runner,
        session,
        filesystem_stat_args(session, namespace, &moved_path),
    )?;
    let removed_error = expect_failure(
        removed_stat,
        "filesystem_stat",
        Some(PROFILE_NAME),
        Some(expected_envelope_mode),
    )?;
    if removed_error.code != "path_not_found" {
        bail!(
            "expected stat on `{moved_path}` to fail with path_not_found, got {}",
            removed_error.code
        );
    }

    Ok(vec![
        STEP_CREATE_NAMESPACE,
        STEP_LIST_NAMESPACES,
        STEP_PUT,
        STEP_LS,
        STEP_STAT,
        STEP_CAT,
        STEP_GET,
        STEP_CP,
        STEP_MOVE,
        STEP_RM,
        STEP_VERIFY_REMOVAL,
    ])
}

fn render_report(report: &SmokeReport) -> String {
    format!(
        "mode={} namespace={} status=passed steps={}",
        report.mode.as_str(),
        report.namespace,
        report.steps.join(",")
    )
}

fn render_perf_report(report: &PerfReport) -> String {
    let mut lines = vec![
        "LoonFS perf run complete".to_owned(),
        format!("  mode: {}", report.mode.as_str()),
        format!("  output: {}", report.output.display()),
        format!("  operations: {}", report.measured_events),
        format!("  failures: {}", report.failures),
    ];
    for operation in [STEP_PUT, STEP_STAT, STEP_CAT, STEP_GET, STEP_LS] {
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
    warmup: bool,
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
                warmup,
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
                warmup,
                elapsed_ms,
                ok: false,
                error_class: Some(&error_class),
            })?;
            Err(error)
        }
    }
}

fn create_perf_namespace(
    runner: &dyn LoonRunner,
    session: &SmokeSession,
    namespace: &str,
) -> Result<Measured<()>> {
    match run_json_command(runner, session, namespace_create_args(session, namespace))? {
        JsonCommandResult::Success(envelope) => {
            let envelope = expect_kind_format(envelope, "namespace_create")?;
            let data = require_data_type(&envelope, "namespace_summary")?;
            let name = namespace_field(data)?;
            if name != namespace {
                return Err(classified_error(
                    "unexpected_result",
                    "namespace create returned unexpected namespace",
                ));
            }
            Ok(Measured::ok(()))
        }
        JsonCommandResult::Failure(envelope) => {
            let envelope = expect_kind_format(envelope, "namespace_create")?;
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
    runner: &dyn LoonRunner,
    session: &SmokeSession,
    namespace: &str,
    path: &str,
    expected_size: usize,
) -> Result<Measured<()>> {
    let envelope = expect_success_kind_classified(
        run_json_command(
            runner,
            session,
            filesystem_stat_args(session, namespace, path),
        )?,
        "filesystem_stat",
    )?;
    let data = require_data_type(&envelope, "path_entry")?;
    let inode_kind = string_field(data, "inode_kind")?;
    if inode_kind != "file" {
        return Err(classified_error(
            "unexpected_result",
            format!("expected file kind, got `{inode_kind}`"),
        ));
    }
    let size_bytes = u64_field(data, "size_bytes")?;
    if size_bytes != expected_size as u64 {
        return Err(classified_error(
            "unexpected_result",
            format!("expected size {expected_size}, got {size_bytes}"),
        ));
    }
    Ok(Measured::ok(()))
}

fn run_json_command(
    runner: &dyn LoonRunner,
    session: &SmokeSession,
    args: Vec<String>,
) -> Result<JsonCommandResult> {
    let output = runner.run(session, &args)?;
    if output.success() {
        Ok(JsonCommandResult::Success(parse_envelope(
            &output.stdout,
            "stdout",
            &args,
            &output,
        )?))
    } else {
        Ok(JsonCommandResult::Failure(parse_envelope(
            &output.stderr,
            "stderr",
            &args,
            &output,
        )?))
    }
}

fn run_stream_command(
    runner: &dyn LoonRunner,
    session: &SmokeSession,
    args: Vec<String>,
) -> Result<LoonOutput> {
    let output = runner.run(session, &args)?;
    if output.success() {
        Ok(output)
    } else {
        bail!(
            "command `{}` failed with exit code {:?}: {}",
            args.join(" "),
            output.exit_code,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
}

fn parse_envelope(
    bytes: &[u8],
    stream_name: &str,
    args: &[String],
    output: &LoonOutput,
) -> Result<JsonEnvelope> {
    serde_json::from_slice(bytes).with_context(|| {
        format!(
            "parse loon JSON envelope from {stream_name} for `{}` (exit code {:?}, stdout=`{}`, stderr=`{}`)",
            args.join(" "),
            output.exit_code,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )
    })
}

fn expect_success(
    result: JsonCommandResult,
    expected_kind: &str,
    expected_profile: Option<&str>,
    expected_mode: Option<&str>,
) -> Result<JsonEnvelope> {
    match result {
        JsonCommandResult::Success(envelope) => {
            expect_envelope(envelope, expected_kind, expected_profile, expected_mode)
        }
        JsonCommandResult::Failure(envelope) => {
            let envelope =
                expect_envelope(envelope, expected_kind, expected_profile, expected_mode)?;
            let error = envelope
                .error
                .ok_or_else(|| anyhow!("failure envelope missing error body"))?;
            bail!("command failed with {}: {}", error.code, error.message);
        }
    }
}

fn expect_success_kind(result: JsonCommandResult, expected_kind: &str) -> Result<JsonEnvelope> {
    match result {
        JsonCommandResult::Success(envelope) => expect_kind_format(envelope, expected_kind),
        JsonCommandResult::Failure(envelope) => {
            let envelope = expect_kind_format(envelope, expected_kind)?;
            let error = envelope
                .error
                .ok_or_else(|| anyhow!("failure envelope missing error body"))?;
            bail!("command failed with {}: {}", error.code, error.message);
        }
    }
}

fn expect_failure(
    result: JsonCommandResult,
    expected_kind: &str,
    expected_profile: Option<&str>,
    expected_mode: Option<&str>,
) -> Result<JsonError> {
    match result {
        JsonCommandResult::Success(envelope) => {
            let envelope =
                expect_envelope(envelope, expected_kind, expected_profile, expected_mode)?;
            bail!(
                "expected `{expected_kind}` to fail, but it succeeded with data type `{}`",
                data_type_name(&envelope.data)
            );
        }
        JsonCommandResult::Failure(envelope) => {
            let envelope =
                expect_envelope(envelope, expected_kind, expected_profile, expected_mode)?;
            envelope
                .error
                .ok_or_else(|| anyhow!("failure envelope missing error body"))
        }
    }
}

fn expect_success_kind_classified(
    result: JsonCommandResult,
    expected_kind: &str,
) -> Result<JsonEnvelope> {
    match result {
        JsonCommandResult::Success(envelope) => expect_kind_format(envelope, expected_kind),
        JsonCommandResult::Failure(envelope) => {
            let envelope = expect_kind_format(envelope, expected_kind)?;
            let error = envelope.error.ok_or_else(|| {
                classified_error("missing_error", "failure envelope missing error")
            })?;
            Err(classified_error(error.code, error.message))
        }
    }
}

fn expect_envelope(
    envelope: JsonEnvelope,
    expected_kind: &str,
    expected_profile: Option<&str>,
    expected_mode: Option<&str>,
) -> Result<JsonEnvelope> {
    let envelope = expect_kind_format(envelope, expected_kind)?;
    if envelope.profile.as_deref() != expected_profile {
        bail!(
            "expected profile {:?}, got {:?}",
            expected_profile,
            envelope.profile
        );
    }
    if envelope.mode.as_deref() != expected_mode {
        bail!("expected mode {:?}, got {:?}", expected_mode, envelope.mode);
    }
    Ok(envelope)
}

fn expect_kind_format(envelope: JsonEnvelope, expected_kind: &str) -> Result<JsonEnvelope> {
    if envelope.kind != expected_kind {
        bail!(
            "expected JSON kind `{expected_kind}`, got `{}`",
            envelope.kind
        );
    }
    if envelope.format_version != FORMAT_VERSION {
        bail!(
            "expected format_version {}, got {}",
            FORMAT_VERSION,
            envelope.format_version
        );
    }
    Ok(envelope)
}

fn require_data_type<'a>(envelope: &'a JsonEnvelope, expected_type: &str) -> Result<&'a Value> {
    let data = envelope
        .data
        .as_ref()
        .ok_or_else(|| anyhow!("success envelope missing data body"))?;
    let actual_type = data
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("success envelope data missing `type` field"))?;
    if actual_type != expected_type {
        bail!("expected data type `{expected_type}`, got `{actual_type}`");
    }
    Ok(data)
}

fn data_type_name(data: &Option<Value>) -> String {
    data.as_ref()
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("<missing>")
        .to_owned()
}

fn array_field<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| anyhow!("expected `{field}` to be an array"))
}

fn string_field<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("expected `{field}` to be a string"))
}

fn namespace_field(value: &Value) -> Result<&str> {
    value
        .get("namespace_id")
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("expected `namespace_id` to be a string"))
}

fn u64_field(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("expected `{field}` to be an unsigned integer"))
}

fn id_field(value: &Value, field: &str) -> Result<u64> {
    match value.get(field) {
        Some(Value::Number(number)) => number
            .as_u64()
            .ok_or_else(|| anyhow!("expected `{field}` to be an unsigned integer")),
        Some(Value::String(text)) => text
            .trim_start_matches("inode-")
            .parse::<u64>()
            .with_context(|| format!("parse `{field}` from `{text}`")),
        _ => Err(anyhow!(
            "expected `{field}` to be an inode id string or unsigned integer"
        )),
    }
}

fn base_args(_session: &SmokeSession, json: bool) -> Vec<String> {
    let mut args = Vec::new();
    if json {
        args.push("--json".to_owned());
    }
    args.push("--no-input".to_owned());
    args
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

#[derive(Debug, Deserialize)]
struct SmokeServerConfig {
    store: SmokeStoreConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum SmokeStoreConfig {
    LocalFs {
        root: String,
        key_prefix: Option<String>,
    },
    AwsS3 {
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        key_prefix: Option<String>,
        #[serde(default)]
        force_path_style: bool,
    },
    CloudflareR2 {
        bucket: String,
        account_id: String,
        endpoint_url: String,
        access_key_id: String,
        secret_access_key: String,
        key_prefix: Option<String>,
    },
}

impl SmokeStoreConfig {
    fn kind_str(&self) -> &'static str {
        match self {
            Self::LocalFs { .. } => "local-fs",
            Self::AwsS3 { .. } => "aws-s3",
            Self::CloudflareR2 { .. } => "cloudflare-r2",
        }
    }
}

fn load_store_kind(server_config: &Path) -> Result<String> {
    let config: SmokeServerConfig = toml::from_str(
        &fs::read_to_string(server_config)
            .with_context(|| format!("read perf server config {}", server_config.display()))?,
    )
    .with_context(|| format!("decode perf server config {}", server_config.display()))?;
    Ok(config.store.kind_str().to_owned())
}

fn profile_create_local_args(session: &SmokeSession, server_config: &Path) -> Result<Vec<String>> {
    let config: SmokeServerConfig =
        toml::from_str(&fs::read_to_string(server_config).with_context(|| {
            format!("read local smoke server config {}", server_config.display())
        })?)
        .with_context(|| {
            format!(
                "decode local smoke server config {}",
                server_config.display()
            )
        })?;
    let mut args = base_args(session, true);
    args.extend([
        "profile".to_owned(),
        "create".to_owned(),
        PROFILE_NAME.to_owned(),
        "--mode".to_owned(),
        "embedded".to_owned(),
    ]);
    match config.store {
        SmokeStoreConfig::LocalFs { root, key_prefix } => {
            args.extend([
                "--store-kind".to_owned(),
                "local-fs".to_owned(),
                "--root".to_owned(),
                root,
            ]);
            if let Some(key_prefix) = key_prefix {
                args.extend(["--key-prefix".to_owned(), key_prefix]);
            }
        }
        SmokeStoreConfig::AwsS3 {
            bucket,
            region,
            endpoint_url,
            access_key_id,
            secret_access_key,
            session_token,
            key_prefix,
            force_path_style,
        } => {
            args.extend([
                "--store-kind".to_owned(),
                "aws-s3".to_owned(),
                "--bucket".to_owned(),
                bucket,
                "--region".to_owned(),
                region,
                "--access-key-id".to_owned(),
                access_key_id,
                "--secret-access-key".to_owned(),
                secret_access_key,
            ]);
            if let Some(endpoint_url) = endpoint_url {
                args.extend(["--endpoint-url".to_owned(), endpoint_url]);
            }
            if let Some(session_token) = session_token {
                args.extend(["--session-token".to_owned(), session_token]);
            }
            if let Some(key_prefix) = key_prefix {
                args.extend(["--key-prefix".to_owned(), key_prefix]);
            }
            if force_path_style {
                args.push("--force-path-style".to_owned());
            }
        }
        SmokeStoreConfig::CloudflareR2 {
            bucket,
            account_id,
            endpoint_url,
            access_key_id,
            secret_access_key,
            key_prefix,
        } => {
            args.extend([
                "--store-kind".to_owned(),
                "cloudflare-r2".to_owned(),
                "--bucket".to_owned(),
                bucket,
                "--account-id".to_owned(),
                account_id,
                "--endpoint-url".to_owned(),
                endpoint_url,
                "--access-key-id".to_owned(),
                access_key_id,
                "--secret-access-key".to_owned(),
                secret_access_key,
            ]);
            if let Some(key_prefix) = key_prefix {
                args.extend(["--key-prefix".to_owned(), key_prefix]);
            }
        }
    }
    Ok(args)
}

fn profile_create_remote_args(
    session: &SmokeSession,
    server_url: &str,
    auth_token: Option<&str>,
) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "profile".to_owned(),
        "create".to_owned(),
        PROFILE_NAME.to_owned(),
        "--mode".to_owned(),
        "remote".to_owned(),
        "--server-url".to_owned(),
        server_url.to_owned(),
    ]);
    if let Some(auth_token) = auth_token {
        args.push("--auth-token".to_owned());
        args.push(auth_token.to_owned());
    }
    args
}

fn namespace_create_args(session: &SmokeSession, namespace: &str) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "namespace".to_owned(),
        "create".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        namespace.to_owned(),
    ]);
    args
}

fn namespace_list_args(session: &SmokeSession) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "namespace".to_owned(),
        "list".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
    ]);
    args
}

fn filesystem_put_args(
    session: &SmokeSession,
    namespace: &str,
    local_path: &Path,
    remote_path: &str,
) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "put".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        local_path.display().to_string(),
        remote_path.to_owned(),
    ]);
    args
}

fn filesystem_ls_args(session: &SmokeSession, namespace: &str, path: &str) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "ls".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        path.to_owned(),
    ]);
    args
}

fn filesystem_stat_args(session: &SmokeSession, namespace: &str, path: &str) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "stat".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        path.to_owned(),
    ]);
    args
}

fn filesystem_cat_args(session: &SmokeSession, namespace: &str, path: &str) -> Vec<String> {
    let mut args = base_args(session, false);
    args.extend([
        "cat".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        path.to_owned(),
    ]);
    args
}

fn filesystem_get_args(
    session: &SmokeSession,
    namespace: &str,
    remote_path: &str,
    destination: &Path,
) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "get".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        remote_path.to_owned(),
        destination.display().to_string(),
    ]);
    args
}

fn filesystem_cp_args(
    session: &SmokeSession,
    namespace: &str,
    source_path: &str,
    dest_path: &str,
) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "cp".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        source_path.to_owned(),
        dest_path.to_owned(),
    ]);
    args
}

fn filesystem_mv_args(
    session: &SmokeSession,
    namespace: &str,
    source_path: &str,
    dest_path: &str,
) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "mv".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        source_path.to_owned(),
        dest_path.to_owned(),
    ]);
    args
}

fn filesystem_rm_args(session: &SmokeSession, namespace: &str, path: &str) -> Vec<String> {
    let mut args = base_args(session, true);
    args.extend([
        "rm".to_owned(),
        "--profile".to_owned(),
        PROFILE_NAME.to_owned(),
        "--namespace".to_owned(),
        namespace.to_owned(),
        path.to_owned(),
    ]);
    args
}

fn smoke_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}-{millis}", std::process::id())
}

fn perf_run_id() -> String {
    smoke_id()
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
    use serde_json::json;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn smoke_report_renders_compact_summary() {
        let report = SmokeReport {
            mode: SmokeMode::Local,
            namespace: "demo".to_owned(),
            steps: vec![
                STEP_CREATE_NAMESPACE,
                STEP_LIST_NAMESPACES,
                STEP_VERIFY_REMOVAL,
            ],
        };
        assert_eq!(
            render_report(&report),
            "mode=local namespace=demo status=passed steps=create_namespace,list_namespaces,verify_removal"
        );
    }

    #[test]
    fn local_smoke_runs_expected_cli_sequence() {
        let server_config = write_local_server_config();
        let args = LocalSmokeArgs {
            server_config: server_config.path().to_path_buf(),
            namespace: "demo".to_owned(),
        };
        let runner = RecordingRunner::new(local_success_outputs("demo"));

        let report = run_local_smoke_with_id(&args, &runner, "seed").expect("local smoke");

        assert_eq!(report.mode, SmokeMode::Local);
        assert_eq!(
            report.steps,
            vec![
                STEP_CREATE_NAMESPACE,
                STEP_LIST_NAMESPACES,
                STEP_PUT,
                STEP_LS,
                STEP_STAT,
                STEP_CAT,
                STEP_GET,
                STEP_CP,
                STEP_MOVE,
                STEP_RM,
                STEP_VERIFY_REMOVAL,
            ]
        );
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
                "embedded",
                "--store-kind",
                "local-fs",
                "--root",
                "/tmp/xtask-loon-store",
            ],
        );
        assert_eq!(
            calls.last().unwrap().first().map(String::as_str),
            Some("--json")
        );
    }

    #[test]
    fn remote_smoke_runs_expected_cli_sequence() {
        let args = RemoteSmokeArgs {
            server_url: "http://127.0.0.1:9400".to_owned(),
            auth_token: Some("dev-token".to_owned()),
            namespace: "demo".to_owned(),
        };
        let runner = RecordingRunner::new(remote_success_outputs("demo"));

        let report = run_remote_smoke_with_id(&args, &runner, "seed").expect("remote smoke");

        assert_eq!(report.mode, SmokeMode::Remote);
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
        assert_eq!(report.steps.first(), Some(&STEP_CREATE_NAMESPACE));
    }

    #[test]
    fn local_smoke_propagates_failures() {
        let server_config = write_local_server_config();
        let args = LocalSmokeArgs {
            server_config: server_config.path().to_path_buf(),
            namespace: "demo".to_owned(),
        };
        let mut outputs = local_success_outputs("demo");
        outputs[4] = json_failure(
            "filesystem_ls",
            Some("smoke"),
            Some("embedded"),
            "client_error",
            "boom",
        );
        let runner = RecordingRunner::new(outputs);

        let error = run_local_smoke_with_id(&args, &runner, "seed").expect_err("smoke should fail");

        assert!(error.to_string().contains("boom"));
        let calls = runner.calls();
        assert!(calls.iter().any(|call| call.iter().any(|arg| arg == "ls")));
    }

    #[test]
    fn namespace_exists_is_non_fatal() {
        let args = RemoteSmokeArgs {
            server_url: "http://127.0.0.1:9400".to_owned(),
            auth_token: None,
            namespace: "demo".to_owned(),
        };
        let mut outputs = remote_success_outputs("demo");
        outputs[1] = json_failure(
            "namespace_create",
            Some("smoke"),
            Some("remote"),
            "namespace_exists",
            "namespace already exists",
        );
        let runner = RecordingRunner::new(outputs);

        let report = run_remote_smoke_with_id(&args, &runner, "seed").expect("remote smoke");

        assert_eq!(report.mode, SmokeMode::Remote);
        assert_eq!(report.steps[0], STEP_CREATE_NAMESPACE);
    }

    #[test]
    fn perf_local_runs_expected_cli_sequence_and_writes_events() {
        let server_config = write_local_server_config();
        let json_out = NamedTempFile::new().expect("temp ndjson");
        let args = LocalPerfArgs {
            common: PerfCommonArgs {
                warmups: 0,
                iterations: 1,
                payload_sizes: "4KiB".to_owned(),
                namespace: Some("demo".to_owned()),
                json_out: json_out.path().to_path_buf(),
                seed: 1,
                keep_workspace: false,
            },
            server_config: server_config.path().to_path_buf(),
        };
        let runner = RecordingRunner::new(perf_success_outputs(
            "demo", "local", "seed", 4096, 0, false,
        ));

        let report = run_local_perf_with_id(&args, &runner, "seed").expect("local perf");

        assert_eq!(report.mode, PerfMode::Local);
        assert!(report.measured_events > 0);
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
                "embedded",
                "--store-kind",
                "local-fs",
                "--root",
                "/tmp/xtask-loon-store",
            ],
        );
        assert!(calls.iter().any(|call| call.iter().any(|arg| arg == "put")));

        let lines = fs::read_to_string(json_out.path()).expect("read ndjson");
        let events = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("event json"))
            .collect::<Vec<_>>();
        assert!(events.iter().any(|event| event["operation"] == STEP_PUT));
        assert!(events.iter().all(|event| event.get("namespace").is_none()));
        assert!(events.iter().all(|event| event.get("path").is_none()));
        assert!(events.iter().all(|event| event.get("payload").is_none()));
        assert_eq!(events[0]["namespace_class"], "provided");
    }

    #[test]
    fn perf_remote_runs_expected_cli_sequence() {
        let json_out = NamedTempFile::new().expect("temp ndjson");
        let args = RemotePerfArgs {
            common: PerfCommonArgs {
                warmups: 0,
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
        let runner = RecordingRunner::new(perf_success_outputs(
            "demo", "remote", "seed", 4096, 0, false,
        ));

        let report = run_remote_perf_with_id(&args, &runner, "seed").expect("remote perf");

        assert_eq!(report.mode, PerfMode::Remote);
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
    }

    #[test]
    fn payload_size_parser_accepts_binary_units_and_rejects_bad_values() {
        assert_eq!(
            parse_payload_sizes("4KiB,64KiB,1MiB").expect("parse sizes"),
            vec![4096, 65536, 1048576]
        );
        assert!(parse_payload_sizes("0KiB").is_err());
        assert!(parse_payload_sizes("4KB").is_err());
        assert!(parse_payload_sizes("").is_err());
    }

    #[test]
    fn deterministic_payload_is_stable_and_input_sensitive() {
        let first = deterministic_payload(32, 7, 1);
        assert_eq!(first, deterministic_payload(32, 7, 1));
        assert_ne!(first, deterministic_payload(32, 8, 1));
        assert_ne!(first, deterministic_payload(32, 7, 2));
        assert_ne!(first, deterministic_payload(33, 7, 1));
    }

    #[test]
    fn perf_summary_reports_percentiles() {
        let report = PerfReport {
            mode: PerfMode::Local,
            output: PathBuf::from("target/loonfs-perf/test.ndjson"),
            measured_events: 3,
            failures: 0,
            samples: vec![
                PerfSample {
                    operation: STEP_PUT,
                    elapsed_ms: 10.0,
                },
                PerfSample {
                    operation: STEP_PUT,
                    elapsed_ms: 20.0,
                },
                PerfSample {
                    operation: STEP_PUT,
                    elapsed_ms: 30.0,
                },
            ],
        };

        let rendered = render_perf_report(&report);

        assert!(rendered.contains("operations: 3"));
        assert!(rendered.contains("put p50/p95: 20.00 ms / 30.00 ms"));
    }

    #[test]
    fn perf_failure_records_failed_event() {
        let server_config = write_local_server_config();
        let json_out = NamedTempFile::new().expect("temp ndjson");
        let args = LocalPerfArgs {
            common: PerfCommonArgs {
                warmups: 0,
                iterations: 1,
                payload_sizes: "4KiB".to_owned(),
                namespace: Some("demo".to_owned()),
                json_out: json_out.path().to_path_buf(),
                seed: 1,
                keep_workspace: false,
            },
            server_config: server_config.path().to_path_buf(),
        };
        let mut outputs = perf_success_outputs("demo", "local", "seed", 4096, 0, false);
        outputs[2] = json_failure(
            "filesystem_put",
            Some(PROFILE_NAME),
            Some("local"),
            "durable_content",
            "missing content",
        );
        let runner = RecordingRunner::new(outputs);

        let error = run_local_perf_with_id(&args, &runner, "seed").expect_err("perf should fail");

        assert!(error.to_string().contains("durable_content"));
        let lines = fs::read_to_string(json_out.path()).expect("read ndjson");
        let events = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("event json"))
            .collect::<Vec<_>>();
        let put = events
            .iter()
            .find(|event| event["operation"] == STEP_PUT)
            .expect("put event");
        assert_eq!(put["ok"], false);
        assert_eq!(put["error_class"], "durable_content");
    }

    fn assert_command_suffix(actual: &[String], suffix: &[&str]) {
        let actual_suffix = &actual[actual.len() - suffix.len()..];
        let expected = suffix
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        assert_eq!(actual_suffix, expected.as_slice());
    }

    fn write_local_server_config() -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("temp server config");
        writeln!(
            file,
            r#"
[store]
kind = "local-fs"
root = "/tmp/xtask-loon-store"
"#
        )
        .expect("write temp server config");
        file
    }

    fn local_success_outputs(namespace: &str) -> Vec<LoonOutput> {
        let mut outputs = Vec::new();
        outputs.push(json_success(
            "profile_create",
            Some(PROFILE_NAME),
            Some("local"),
            json!({"type":"profile","name":"smoke","mode":"embedded","active":true}),
        ));
        outputs.extend(sequence_outputs(namespace, "embedded"));
        outputs
    }

    fn remote_success_outputs(namespace: &str) -> Vec<LoonOutput> {
        let mut outputs = Vec::new();
        outputs.push(json_success(
            "profile_create",
            Some(PROFILE_NAME),
            Some("remote"),
            json!({"type":"profile","name":"smoke","mode":"remote","active":true}),
        ));
        outputs.extend(sequence_outputs(namespace, "remote"));
        outputs
    }

    fn perf_success_outputs(
        namespace: &str,
        mode: &str,
        run_id: &str,
        payload_size: usize,
        iteration: u32,
        warmup: bool,
    ) -> Vec<LoonOutput> {
        let mut outputs = Vec::new();
        outputs.push(json_success(
            "profile_create",
            Some(PROFILE_NAME),
            Some(mode),
            json!({"type":"profile","name":"smoke","mode":mode,"active":true}),
        ));
        outputs.push(json_success(
            "namespace_create",
            Some(PROFILE_NAME),
            Some(mode),
            json!({"type":"namespace_summary","name":namespace}),
        ));
        let payload_class = payload_class(payload_size as u64);
        let phase = if warmup { "warmup" } else { "measured" };
        let source_path =
            format!("/xtask-perf/{run_id}/{payload_class}/{phase}-{iteration}/payload.bin");
        outputs.push(json_success(
            "filesystem_put",
            Some(PROFILE_NAME),
            Some(mode),
            json!({"type":"file_mutation","target":format!("{namespace}:{source_path}"),"committed_seq":1}),
        ));
        outputs.push(json_success(
            "filesystem_stat",
            Some(PROFILE_NAME),
            Some(mode),
            json!({"type":"path_entry","absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload_size,"authoritative_head_seq":1,"display_name":"payload.bin","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}),
        ));
        outputs.push(LoonOutput {
            exit_code: Some(0),
            stdout: deterministic_payload(payload_size, 1, iteration),
            stderr: Vec::new(),
        });
        outputs.push(json_success(
            "filesystem_get",
            Some(PROFILE_NAME),
            Some(mode),
            json!({"type":"file_transfer","target":format!("{namespace}:{source_path}"),"destination":"/tmp/downloaded.bin","bytes_written":payload_size}),
        ));
        outputs.push(json_success(
            "filesystem_ls",
            Some(PROFILE_NAME),
            Some(mode),
            json!({"type":"path_entries","entries":[{"absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload_size,"authoritative_head_seq":1,"display_name":"payload.bin","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}]}),
        ));
        outputs
    }

    fn sequence_outputs(namespace: &str, mode: &str) -> Vec<LoonOutput> {
        let source_path = "/xtask-smoke-seed/input.txt";
        let copy_path = "/xtask-smoke-seed/copy.txt";
        let moved_path = "/xtask-smoke-seed/moved.txt";
        let payload = b"xtask smoke payload demo seed\n".to_vec();
        vec![
            json_success(
                "namespace_create",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"namespace_summary","name":namespace}),
            ),
            json_success(
                "namespace_list",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"namespace_list","namespaces":[{"name":namespace}]}),
            ),
            json_success(
                "filesystem_put",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"file_mutation","target":format!("{namespace}:{source_path}"),"committed_seq":1}),
            ),
            json_success(
                "filesystem_ls",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"path_entries","entries":[{"absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload.len(),"authoritative_head_seq":1,"display_name":"input.txt","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}]}),
            ),
            json_success(
                "filesystem_stat",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"path_entry","absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload.len(),"authoritative_head_seq":1,"display_name":"input.txt","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}),
            ),
            LoonOutput {
                exit_code: Some(0),
                stdout: payload.clone(),
                stderr: Vec::new(),
            },
            json_success(
                "filesystem_get",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"file_transfer","target":format!("{namespace}:{source_path}"),"destination":"/tmp/downloaded.txt","bytes_written":payload.len()}),
            ),
            json_success(
                "filesystem_cp",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"path_move","from":format!("{namespace}:{source_path}"),"to":format!("{namespace}:{copy_path}"),"committed_seq":2}),
            ),
            json_success(
                "filesystem_stat",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"path_entry","absolute_path":copy_path,"inode_id":"inode-2","inode_kind":"file","size_bytes":payload.len(),"authoritative_head_seq":2,"display_name":"copy.txt","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}),
            ),
            json_success(
                "filesystem_mv",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"path_move","from":format!("{namespace}:{copy_path}"),"to":format!("{namespace}:{moved_path}"),"committed_seq":3}),
            ),
            json_success(
                "filesystem_rm",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"file_mutation","target":format!("{namespace}:{moved_path}"),"committed_seq":4}),
            ),
            json_success(
                "filesystem_ls",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"path_entries","entries":[{"absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload.len(),"authoritative_head_seq":4,"display_name":"input.txt","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}]}),
            ),
            json_success(
                "filesystem_stat",
                Some(PROFILE_NAME),
                Some(mode),
                json!({"type":"path_entry","absolute_path":source_path,"inode_id":"inode-1","inode_kind":"file","size_bytes":payload.len(),"authoritative_head_seq":4,"display_name":"input.txt","namespace_id":namespace,"parent_inode_id":"inode-0","revision_no":1,"content_manifest_digest":"digest"}),
            ),
            json_failure(
                "filesystem_stat",
                Some(PROFILE_NAME),
                Some(mode),
                "path_not_found",
                "missing",
            ),
        ]
    }

    fn json_success(
        kind: &str,
        profile: Option<&str>,
        mode: Option<&str>,
        data: Value,
    ) -> LoonOutput {
        let body = serde_json::to_vec(&json!({
            "kind": kind,
            "format_version": FORMAT_VERSION,
            "profile": profile,
            "mode": mode,
            "data": data
        }))
        .expect("encode success body");
        LoonOutput {
            exit_code: Some(0),
            stdout: body,
            stderr: Vec::new(),
        }
    }

    fn json_failure(
        kind: &str,
        profile: Option<&str>,
        mode: Option<&str>,
        code: &str,
        message: &str,
    ) -> LoonOutput {
        let body = serde_json::to_vec(&json!({
            "kind": kind,
            "format_version": FORMAT_VERSION,
            "profile": profile,
            "mode": mode,
            "error": {
                "code": code,
                "message": message
            }
        }))
        .expect("encode error body");
        LoonOutput {
            exit_code: Some(1),
            stdout: Vec::new(),
            stderr: body,
        }
    }

    struct RecordingRunner {
        outputs: RefCell<VecDeque<LoonOutput>>,
        calls: RefCell<Vec<Vec<String>>>,
        last_payload: RefCell<Option<Vec<u8>>>,
    }

    impl RecordingRunner {
        fn new(outputs: Vec<LoonOutput>) -> Self {
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

    impl LoonRunner for RecordingRunner {
        fn run(&self, _session: &SmokeSession, args: &[String]) -> Result<LoonOutput> {
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

            let mut output = self
                .outputs
                .borrow_mut()
                .pop_front()
                .ok_or_else(|| anyhow!("no recorded output remaining"))?;
            if args.iter().any(|arg| arg == "cat") {
                if let Some(bytes) = self.last_payload.borrow().clone() {
                    output.stdout = bytes;
                }
            }
            Ok(output)
        }
    }
}
