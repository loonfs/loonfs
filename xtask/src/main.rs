use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use loon_api::InodeKind;
use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
use std::env;
use std::fs;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

const READY_TIMEOUT: Duration = Duration::from_secs(120);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const STEP_CREATE_NAMESPACE: &str = "create_namespace";
const STEP_LIST_NAMESPACES: &str = "list_namespaces";
const STEP_PUT: &str = "put";
const STEP_LS: &str = "ls";
const STEP_STAT: &str = "stat";
const STEP_GET: &str = "get";
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
    Smoke(SmokeArgs),
}

#[derive(Debug, Clone, Args)]
struct SmokeArgs {
    #[arg(long = "client-config")]
    client_config: String,
    #[arg(long = "server-config")]
    server_config: Option<String>,
    #[arg(long)]
    namespace: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmokeMode {
    External,
    Managed,
}

impl SmokeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::External => "external",
            Self::Managed => "managed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SmokeReport {
    mode: SmokeMode,
    namespace: String,
    steps: Vec<&'static str>,
}

trait SmokeExecutor {
    fn run(&self, client_config: ClientConfig, namespace: &str) -> Result<Vec<&'static str>>;
}

trait ManagedServerHandle {
    fn wait_until_ready(&mut self, server_url: &str, timeout: Duration) -> Result<()>;
}

trait ManagedServerLauncher {
    fn launch(&self, server_config_path: &Path) -> Result<Box<dyn ManagedServerHandle>>;
}

struct ClientSmokeExecutor;

impl SmokeExecutor for ClientSmokeExecutor {
    fn run(&self, client_config: ClientConfig, namespace: &str) -> Result<Vec<&'static str>> {
        let client = Client::new(client_config);
        execute_smoke_sequence(&client, namespace)
    }
}

struct CargoManagedServerLauncher;

impl ManagedServerLauncher for CargoManagedServerLauncher {
    fn launch(&self, server_config_path: &Path) -> Result<Box<dyn ManagedServerHandle>> {
        let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .context("resolve workspace root from xtask manifest dir")?;
        let child = Command::new(cargo)
            .arg("run")
            .arg("-p")
            .arg("loon-server")
            .arg("--bin")
            .arg("loond")
            .arg("--")
            .arg("--config")
            .arg(server_config_path)
            .current_dir(workspace_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .with_context(|| {
                format!(
                    "spawn managed loond with config {}",
                    server_config_path.display()
                )
            })?;
        Ok(Box::new(CargoManagedServer { child }))
    }
}

struct CargoManagedServer {
    child: Child,
}

impl ManagedServerHandle for CargoManagedServer {
    fn wait_until_ready(&mut self, server_url: &str, timeout: Duration) -> Result<()> {
        wait_for_server_ready(&mut self.child, server_url, timeout)
    }
}

impl Drop for CargoManagedServer {
    fn drop(&mut self) {
        terminate_child(&mut self.child);
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        CommandKind::Smoke(args) => {
            let report = run_smoke(&args)?;
            println!("{}", render_report(&report));
        }
    }
    Ok(())
}

fn run_smoke(args: &SmokeArgs) -> Result<SmokeReport> {
    run_smoke_with(args, &ClientSmokeExecutor, &CargoManagedServerLauncher)
}

fn run_smoke_with(
    args: &SmokeArgs,
    executor: &dyn SmokeExecutor,
    launcher: &dyn ManagedServerLauncher,
) -> Result<SmokeReport> {
    let client_config = ClientConfig::load(&args.client_config)
        .map_err(|err| anyhow!("load client config {}: {err}", args.client_config))?;
    let mode = match &args.server_config {
        Some(server_config_path) => {
            let mut server = launcher.launch(Path::new(server_config_path))?;
            server.wait_until_ready(&client_config.server_url, READY_TIMEOUT)?;
            let steps = executor.run(client_config, &args.namespace)?;
            return Ok(SmokeReport {
                mode: SmokeMode::Managed,
                namespace: args.namespace.clone(),
                steps,
            });
        }
        None => SmokeMode::External,
    };

    let steps = executor.run(client_config, &args.namespace)?;
    Ok(SmokeReport {
        mode,
        namespace: args.namespace.clone(),
        steps,
    })
}

fn execute_smoke_sequence(client: &Client, namespace: &str) -> Result<Vec<&'static str>> {
    accept_namespace_exists(client.create_namespace(namespace), namespace)?;

    let namespaces = client
        .list_namespaces()
        .with_context(|| format!("list namespaces for smoke namespace `{namespace}`"))?;
    if !namespaces
        .iter()
        .any(|summary| summary.name.as_str() == namespace)
    {
        bail!("namespace `{namespace}` was not returned by list namespaces");
    }

    let temp_dir = tempdir().context("create tempdir for smoke inputs")?;
    let smoke_id = smoke_id();
    let smoke_root = format!("/xtask-smoke-{smoke_id}");
    let parent_path = NamespacePath::parse(&format!("{namespace}:{smoke_root}"))
        .context("build smoke parent path")?;
    let uploaded_path = NamespacePath::parse(&format!("{namespace}:{smoke_root}/input.txt"))
        .context("build smoke upload path")?;
    let moved_path = NamespacePath::parse(&format!("{namespace}:{smoke_root}/renamed.txt"))
        .context("build smoke moved path")?;

    let upload_file = temp_dir.path().join("input.txt");
    let downloaded_file = temp_dir.path().join("downloaded.txt");
    let payload = format!("xtask smoke payload {namespace} {smoke_id}\n").into_bytes();
    fs::write(&upload_file, &payload)
        .with_context(|| format!("write smoke input file {}", upload_file.display()))?;

    client
        .put_from_path(&upload_file, &uploaded_path)
        .with_context(|| format!("put {}", uploaded_path.absolute_path))?;

    let listed_entries = client
        .list_path(&parent_path)
        .with_context(|| format!("list {}", parent_path.absolute_path))?;
    if !listed_entries
        .iter()
        .any(|entry| entry.absolute_path == uploaded_path.absolute_path)
    {
        bail!(
            "list {} did not include uploaded file {}",
            parent_path.absolute_path,
            uploaded_path.absolute_path
        );
    }

    let stat_entry = client
        .stat_path(&uploaded_path)
        .with_context(|| format!("stat {}", uploaded_path.absolute_path))?;
    if stat_entry.inode_kind != InodeKind::File {
        bail!(
            "expected {} to be a file, got {:?}",
            uploaded_path.absolute_path,
            stat_entry.inode_kind
        );
    }
    if stat_entry.size_bytes != Some(payload.len() as u64) {
        bail!(
            "expected {} size {}, got {:?}",
            uploaded_path.absolute_path,
            payload.len(),
            stat_entry.size_bytes
        );
    }

    client
        .get_to_path(&uploaded_path, &downloaded_file)
        .with_context(|| format!("get {}", uploaded_path.absolute_path))?;
    let downloaded = fs::read(&downloaded_file)
        .with_context(|| format!("read {}", downloaded_file.display()))?;
    if downloaded != payload {
        bail!(
            "downloaded bytes for {} did not match uploaded payload",
            uploaded_path.absolute_path
        );
    }

    client
        .move_path(&uploaded_path, &moved_path)
        .with_context(|| {
            format!(
                "move {} to {}",
                uploaded_path.absolute_path, moved_path.absolute_path
            )
        })?;

    client
        .delete_path(&moved_path)
        .with_context(|| format!("rm {}", moved_path.absolute_path))?;

    let final_entries = client
        .list_path(&parent_path)
        .with_context(|| format!("list {} after delete", parent_path.absolute_path))?;
    if final_entries
        .iter()
        .any(|entry| entry.absolute_path == uploaded_path.absolute_path)
    {
        bail!(
            "removed file {} still appeared in final list {}",
            uploaded_path.absolute_path,
            parent_path.absolute_path
        );
    }
    if final_entries
        .iter()
        .any(|entry| entry.absolute_path == moved_path.absolute_path)
    {
        bail!(
            "removed file {} still appeared in final list {}",
            moved_path.absolute_path,
            parent_path.absolute_path
        );
    }
    expect_path_not_found(client.stat_path(&moved_path), &moved_path)?;

    Ok(vec![
        STEP_CREATE_NAMESPACE,
        STEP_LIST_NAMESPACES,
        STEP_PUT,
        STEP_LS,
        STEP_STAT,
        STEP_GET,
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

fn accept_namespace_exists<T>(
    result: std::result::Result<T, ClientError>,
    namespace: &str,
) -> Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(ClientError::Api { code, .. }) if code == "namespace_exists" => Ok(()),
        Err(err) => Err(anyhow!("create namespace `{namespace}`: {err}")),
    }
}

fn expect_path_not_found(
    result: std::result::Result<loon_api::AuthoritativePathEntry, ClientError>,
    path: &NamespacePath,
) -> Result<()> {
    match result {
        Ok(entry) => bail!(
            "expected {} to be absent after delete, but stat returned inode {}",
            path.absolute_path,
            entry.inode_id
        ),
        Err(ClientError::Api { code, .. }) if code == "path_not_found" => Ok(()),
        Err(err) => Err(anyhow!(
            "expected stat {} to return path_not_found after delete: {err}",
            path.absolute_path
        )),
    }
}

fn smoke_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}-{millis}", std::process::id())
}

fn wait_for_server_ready(child: &mut Child, server_url: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let healthz_url = format!("{}/healthz", server_url.trim_end_matches('/'));
    loop {
        match ureq::get(&healthz_url)
            .timeout(Duration::from_millis(250))
            .call()
        {
            Ok(response) if response.status() == 200 => return Ok(()),
            Ok(_) | Err(_) => {}
        }

        if let Some(status) = child
            .try_wait()
            .context("poll managed server child process")?
        {
            return Err(early_exit_error(status, &healthz_url));
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for managed server readiness at {healthz_url}");
        }
        thread::sleep(READY_POLL_INTERVAL);
    }
}

fn early_exit_error(status: ExitStatus, healthz_url: &str) -> anyhow::Error {
    anyhow!(
        "managed server exited before readiness check completed (status: {status}) while waiting for {healthz_url}"
    )
}

fn terminate_child(child: &mut Child) {
    match child.try_wait() {
        Ok(Some(_)) => return,
        Ok(None) => {}
        Err(_) => return,
    }

    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_server::{app, ServerConfig, StoreConfig};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    #[test]
    fn smoke_report_renders_compact_summary() {
        let report = SmokeReport {
            mode: SmokeMode::Managed,
            namespace: "demo".to_owned(),
            steps: vec![
                STEP_CREATE_NAMESPACE,
                STEP_LIST_NAMESPACES,
                STEP_VERIFY_REMOVAL,
            ],
        };
        assert_eq!(
            render_report(&report),
            "mode=managed namespace=demo status=passed steps=create_namespace,list_namespaces,verify_removal"
        );
    }

    #[test]
    fn external_smoke_path_uses_executor_without_launching_server() {
        let config_path = write_client_config("http://127.0.0.1:9400", Some("dev-token"));
        let args = SmokeArgs {
            client_config: config_path.display().to_string(),
            server_config: None,
            namespace: "demo".to_owned(),
        };
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = RecordingExecutor::new(events.clone(), false);
        let launcher = RecordingLauncher::new(events.clone(), None, None);

        let report = run_smoke_with(&args, &executor, &launcher).expect("run smoke");

        assert_eq!(
            report.steps,
            vec![
                STEP_CREATE_NAMESPACE,
                STEP_LIST_NAMESPACES,
                STEP_VERIFY_REMOVAL
            ]
        );
        assert_eq!(report.mode, SmokeMode::External);
        assert_eq!(
            events.lock().expect("events").as_slice(),
            ["execute:demo:http://127.0.0.1:9400"]
        );
    }

    #[test]
    fn managed_smoke_path_launches_waits_executes_and_stops() {
        let config_path = write_client_config("http://127.0.0.1:9400", Some("dev-token"));
        let server_path = temp_file_path("server.toml");
        fs::write(&server_path, "bind = \"127.0.0.1:9400\"\n").expect("write server config");
        let args = SmokeArgs {
            client_config: config_path.display().to_string(),
            server_config: Some(server_path.display().to_string()),
            namespace: "demo".to_owned(),
        };
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = RecordingExecutor::new(events.clone(), false);
        let launcher = RecordingLauncher::new(events.clone(), None, None);

        let report = run_smoke_with(&args, &executor, &launcher).expect("run smoke");

        assert_eq!(report.mode, SmokeMode::Managed);
        assert_eq!(
            events.lock().expect("events").as_slice(),
            [
                format!("launch:{}", server_path.display()),
                "wait:http://127.0.0.1:9400".to_owned(),
                "execute:demo:http://127.0.0.1:9400".to_owned(),
                "stop".to_owned(),
            ]
        );
    }

    #[test]
    fn managed_smoke_path_surfaces_readiness_timeout() {
        let config_path = write_client_config("http://127.0.0.1:9400", Some("dev-token"));
        let server_path = temp_file_path("server-timeout.toml");
        fs::write(&server_path, "bind = \"127.0.0.1:9400\"\n").expect("write server config");
        let args = SmokeArgs {
            client_config: config_path.display().to_string(),
            server_config: Some(server_path.display().to_string()),
            namespace: "demo".to_owned(),
        };
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = RecordingExecutor::new(events.clone(), false);
        let launcher = RecordingLauncher::new(
            events.clone(),
            Some("timed out waiting for managed server readiness at http://127.0.0.1:9400/healthz"),
            None,
        );

        let error = run_smoke_with(&args, &executor, &launcher).expect_err("timeout");

        assert!(error
            .to_string()
            .contains("timed out waiting for managed server readiness"));
        assert_eq!(
            events.lock().expect("events").as_slice(),
            [
                format!("launch:{}", server_path.display()),
                "wait:http://127.0.0.1:9400".to_owned(),
                "stop".to_owned(),
            ]
        );
    }

    #[test]
    fn managed_smoke_path_surfaces_child_failure() {
        let config_path = write_client_config("http://127.0.0.1:9400", Some("dev-token"));
        let server_path = temp_file_path("server-failure.toml");
        fs::write(&server_path, "bind = \"127.0.0.1:9400\"\n").expect("write server config");
        let args = SmokeArgs {
            client_config: config_path.display().to_string(),
            server_config: Some(server_path.display().to_string()),
            namespace: "demo".to_owned(),
        };
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let executor = RecordingExecutor::new(events.clone(), false);
        let launcher = RecordingLauncher::new(
            events.clone(),
            Some("managed server exited before readiness check completed (status: 1) while waiting for http://127.0.0.1:9400/healthz"),
            None,
        );

        let error = run_smoke_with(&args, &executor, &launcher).expect_err("child failure");

        assert!(error
            .to_string()
            .contains("managed server exited before readiness check completed"));
        assert_eq!(
            events.lock().expect("events").as_slice(),
            [
                format!("launch:{}", server_path.display()),
                "wait:http://127.0.0.1:9400".to_owned(),
                "stop".to_owned(),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_smoke_sequence_round_trips_against_real_server() {
        let temp_dir = tempdir().expect("tempdir");
        let config = ServerConfig {
            bind: "127.0.0.1:0".to_owned(),
            auth_token: Some("test-token".to_owned()),
            writer_id: "loond-test".to_owned(),
            writer_version: "loond-test/0.1.0".to_owned(),
            lease_duration_ms: 60_000,
            store: StoreConfig::LocalFs {
                root: temp_dir.path().join("store").display().to_string(),
                key_prefix: Some("xtask-smoke-test".to_owned()),
            },
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let router = app(config).expect("build app");
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve app");
        });

        let client_config = write_client_config(&format!("http://{}", addr), Some("test-token"));
        let args = SmokeArgs {
            client_config: client_config.display().to_string(),
            server_config: None,
            namespace: "xtask-demo".to_owned(),
        };

        let report = tokio::task::spawn_blocking(move || {
            run_smoke_with(&args, &ClientSmokeExecutor, &PanicLauncher)
        })
        .await
        .expect("join smoke task")
        .expect("run smoke");

        assert_eq!(report.mode, SmokeMode::External);
        assert_eq!(
            report.steps,
            vec![
                STEP_CREATE_NAMESPACE,
                STEP_LIST_NAMESPACES,
                STEP_PUT,
                STEP_LS,
                STEP_STAT,
                STEP_GET,
                STEP_MOVE,
                STEP_RM,
                STEP_VERIFY_REMOVAL,
            ]
        );

        server.abort();
    }

    #[test]
    fn wait_for_server_ready_times_out_cleanly() {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 5")
            .spawn()
            .expect("spawn sleep");

        let error = wait_for_server_ready(
            &mut child,
            "http://127.0.0.1:65530",
            Duration::from_millis(300),
        )
        .expect_err("timeout");

        assert!(error
            .to_string()
            .contains("timed out waiting for managed server readiness"));
        terminate_child(&mut child);
    }

    #[test]
    fn wait_for_server_ready_detects_early_child_exit() {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 7")
            .spawn()
            .expect("spawn failing shell");

        let error =
            wait_for_server_ready(&mut child, "http://127.0.0.1:65531", Duration::from_secs(2))
                .expect_err("early exit");

        assert!(error
            .to_string()
            .contains("managed server exited before readiness check completed"));
    }

    fn write_client_config(server_url: &str, auth_token: Option<&str>) -> PathBuf {
        let path = temp_file_path("client.toml");
        let mut body = format!("server_url = \"{server_url}\"\n");
        match auth_token {
            Some(token) => body.push_str(&format!("auth_token = \"{token}\"\n")),
            None => body.push_str("auth_token = \"\"\n"),
        }
        fs::write(&path, body).expect("write client config");
        path
    }

    fn temp_file_path(name: &str) -> PathBuf {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join(name);
        let _ = dir.keep();
        path
    }

    struct RecordingExecutor {
        events: Arc<Mutex<Vec<String>>>,
        fail: bool,
    }

    impl RecordingExecutor {
        fn new(events: Arc<Mutex<Vec<String>>>, fail: bool) -> Self {
            Self { events, fail }
        }
    }

    impl SmokeExecutor for RecordingExecutor {
        fn run(&self, client_config: ClientConfig, namespace: &str) -> Result<Vec<&'static str>> {
            self.events
                .lock()
                .expect("events")
                .push(format!("execute:{namespace}:{}", client_config.server_url));
            if self.fail {
                bail!("executor failure");
            }
            Ok(vec![
                STEP_CREATE_NAMESPACE,
                STEP_LIST_NAMESPACES,
                STEP_VERIFY_REMOVAL,
            ])
        }
    }

    struct RecordingLauncher {
        events: Arc<Mutex<Vec<String>>>,
        ready_error: Option<String>,
        launch_error: Option<String>,
    }

    impl RecordingLauncher {
        fn new(
            events: Arc<Mutex<Vec<String>>>,
            ready_error: Option<&str>,
            launch_error: Option<&str>,
        ) -> Self {
            Self {
                events,
                ready_error: ready_error.map(ToOwned::to_owned),
                launch_error: launch_error.map(ToOwned::to_owned),
            }
        }
    }

    impl ManagedServerLauncher for RecordingLauncher {
        fn launch(&self, server_config_path: &Path) -> Result<Box<dyn ManagedServerHandle>> {
            self.events
                .lock()
                .expect("events")
                .push(format!("launch:{}", server_config_path.display()));
            if let Some(error) = &self.launch_error {
                bail!("{error}");
            }
            Ok(Box::new(RecordingHandle {
                events: self.events.clone(),
                ready_error: self.ready_error.clone(),
            }))
        }
    }

    struct RecordingHandle {
        events: Arc<Mutex<Vec<String>>>,
        ready_error: Option<String>,
    }

    impl ManagedServerHandle for RecordingHandle {
        fn wait_until_ready(&mut self, server_url: &str, _timeout: Duration) -> Result<()> {
            self.events
                .lock()
                .expect("events")
                .push(format!("wait:{server_url}"));
            if let Some(error) = &self.ready_error {
                bail!("{error}");
            }
            Ok(())
        }
    }

    impl Drop for RecordingHandle {
        fn drop(&mut self) {
            self.events.lock().expect("events").push("stop".to_owned());
        }
    }

    struct PanicLauncher;

    impl ManagedServerLauncher for PanicLauncher {
        fn launch(&self, _server_config_path: &Path) -> Result<Box<dyn ManagedServerHandle>> {
            panic!("launcher should not be used for external smoke")
        }
    }
}
