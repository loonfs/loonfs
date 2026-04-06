use crate::error::CliError;
use loon_client::{Client, ClientConfig};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};

const START_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedLocalState {
    pub profile_name: String,
    pub pid: u32,
    pub server_url: String,
    pub server_config_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalServerStatusKind {
    Running,
    Stale,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalServerStatus {
    pub profile_name: String,
    pub status: LocalServerStatusKind,
    pub server_url: String,
    pub server_config_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
}

pub fn server_url_from_bind(bind: &str) -> Result<String, CliError> {
    let addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|err: std::net::AddrParseError| CliError::invalid_config(err.to_string()))?;
    let host = if addr.ip().is_unspecified() {
        "127.0.0.1".to_owned()
    } else {
        addr.ip().to_string()
    };
    Ok(format!("http://{host}:{}", addr.port()))
}

pub fn runtime_path(config_path: &Path, profile_name: &str) -> Result<PathBuf, CliError> {
    let parent = config_path.parent().ok_or_else(|| {
        CliError::invalid_config(format!(
            "config path has no parent directory: {}",
            config_path.display()
        ))
    })?;
    Ok(parent
        .join("runtime")
        .join(format!("{}.toml", runtime_file_slug(profile_name))))
}

pub fn inspect_local_server(
    config_path: &Path,
    profile_name: &str,
    server_url: &str,
    server_config_path: &Path,
) -> Result<LocalServerStatus, CliError> {
    let runtime_path = runtime_path(config_path, profile_name)?;
    let Some(state) = load_runtime_state(&runtime_path)? else {
        return Ok(LocalServerStatus {
            profile_name: profile_name.to_owned(),
            status: LocalServerStatusKind::Stopped,
            server_url: server_url.to_owned(),
            server_config_path: server_config_path.display().to_string(),
            pid: None,
        });
    };

    if state.profile_name != profile_name
        || state.server_url != server_url
        || state.server_config_path != server_config_path.display().to_string()
    {
        return Ok(LocalServerStatus {
            profile_name: profile_name.to_owned(),
            status: LocalServerStatusKind::Stale,
            server_url: server_url.to_owned(),
            server_config_path: server_config_path.display().to_string(),
            pid: Some(state.pid),
        });
    }

    if process_exists(state.pid) && healthz(server_url) {
        Ok(LocalServerStatus {
            profile_name: state.profile_name,
            status: LocalServerStatusKind::Running,
            server_url: state.server_url,
            server_config_path: state.server_config_path,
            pid: Some(state.pid),
        })
    } else {
        Ok(LocalServerStatus {
            profile_name: state.profile_name,
            status: LocalServerStatusKind::Stale,
            server_url: state.server_url,
            server_config_path: state.server_config_path,
            pid: Some(state.pid),
        })
    }
}

pub fn start_local_server(
    config_path: &Path,
    profile_name: &str,
    server_url: &str,
    server_config_path: &Path,
) -> Result<LocalServerStatus, CliError> {
    let runtime_path = runtime_path(config_path, profile_name)?;
    let existing = inspect_local_server(config_path, profile_name, server_url, server_config_path)?;
    if existing.status == LocalServerStatusKind::Running {
        return Err(CliError::local_server_already_running(profile_name));
    }
    if existing.status == LocalServerStatusKind::Stale {
        clear_runtime_state(&runtime_path)?;
    }

    let loond = loond_binary_path()?;
    let mut child = Command::new(loond)
        .arg("--config")
        .arg(server_config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| CliError::client_error(format!("failed to start loond: {err}")))?;

    let pid = child.id();
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if healthz(server_url) {
            let state = ManagedLocalState {
                profile_name: profile_name.to_owned(),
                pid,
                server_url: server_url.to_owned(),
                server_config_path: server_config_path.display().to_string(),
            };
            save_runtime_state(&runtime_path, &state)?;
            return Ok(LocalServerStatus {
                profile_name: profile_name.to_owned(),
                status: LocalServerStatusKind::Running,
                server_url: server_url.to_owned(),
                server_config_path: server_config_path.display().to_string(),
                pid: Some(pid),
            });
        }

        if let Some(status) = child
            .try_wait()
            .map_err(|err| CliError::client_error(format!("failed to wait on loond: {err}")))?
        {
            return Err(CliError::client_error(format!(
                "loond exited before becoming ready with status {status}"
            )));
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            return Err(CliError::client_error(format!(
                "loond did not become ready within {}s",
                START_TIMEOUT.as_secs()
            )));
        }

        thread::sleep(Duration::from_millis(100));
    }
}

pub fn stop_local_server(
    config_path: &Path,
    profile_name: &str,
    server_url: &str,
    server_config_path: &Path,
) -> Result<LocalServerStatus, CliError> {
    let runtime_path = runtime_path(config_path, profile_name)?;
    let status = inspect_local_server(config_path, profile_name, server_url, server_config_path)?;
    match status.status {
        LocalServerStatusKind::Stopped => Ok(status),
        LocalServerStatusKind::Stale => {
            clear_runtime_state(&runtime_path)?;
            Ok(LocalServerStatus {
                profile_name: profile_name.to_owned(),
                status: LocalServerStatusKind::Stopped,
                server_url: server_url.to_owned(),
                server_config_path: server_config_path.display().to_string(),
                pid: None,
            })
        }
        LocalServerStatusKind::Running => {
            if let Some(pid) = status.pid {
                kill_process(pid)?;
                let deadline = Instant::now() + STOP_TIMEOUT;
                while Instant::now() < deadline {
                    if !process_exists(pid) {
                        clear_runtime_state(&runtime_path)?;
                        return Ok(LocalServerStatus {
                            profile_name: profile_name.to_owned(),
                            status: LocalServerStatusKind::Stopped,
                            server_url: server_url.to_owned(),
                            server_config_path: server_config_path.display().to_string(),
                            pid: None,
                        });
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                return Err(CliError::client_error(format!(
                    "timed out stopping loond process {pid}"
                )));
            }
            clear_runtime_state(&runtime_path)?;
            Ok(LocalServerStatus {
                profile_name: profile_name.to_owned(),
                status: LocalServerStatusKind::Stopped,
                server_url: server_url.to_owned(),
                server_config_path: server_config_path.display().to_string(),
                pid: None,
            })
        }
    }
}

fn save_runtime_state(path: &Path, state: &ManagedLocalState) -> Result<(), CliError> {
    let parent = path.parent().ok_or_else(|| {
        CliError::invalid_config(format!(
            "runtime path has no parent directory: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        CliError::client_error(format!("failed to create runtime directory: {err}"))
    })?;
    let contents = toml::to_string_pretty(state)
        .map_err(|err| CliError::client_error(format!("failed to encode runtime state: {err}")))?;
    write_owner_only(path, contents.as_bytes())
}

fn load_runtime_state(path: &Path) -> Result<Option<ManagedLocalState>, CliError> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(path)
        .map_err(|err| CliError::client_error(format!("failed to read runtime state: {err}")))?;
    let state = toml::from_str::<ManagedLocalState>(&contents)
        .map_err(|err| CliError::client_error(format!("failed to decode runtime state: {err}")))?;
    Ok(Some(state))
}

fn clear_runtime_state(path: &Path) -> Result<(), CliError> {
    if path.exists() {
        fs::remove_file(path).map_err(|err| {
            CliError::client_error(format!("failed to remove runtime state: {err}"))
        })?;
    }
    Ok(())
}

fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .map_err(|err| {
                CliError::client_error(format!("failed to create runtime file: {err}"))
            })?
    };
    #[cfg(not(unix))]
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|err| CliError::client_error(format!("failed to create runtime file: {err}")))?;

    file.write_all(bytes)
        .map_err(|err| CliError::client_error(format!("failed to write runtime state: {err}")))?;
    file.sync_all()
        .map_err(|err| CliError::client_error(format!("failed to flush runtime state: {err}")))?;
    Ok(())
}

fn runtime_file_slug(name: &str) -> String {
    let mut slug = String::new();
    for byte in name.as_bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                slug.push(*byte as char)
            }
            other => slug.push_str(&format!("_{other:02x}")),
        }
    }
    if slug.is_empty() {
        "_".to_owned()
    } else {
        slug
    }
}

fn loond_binary_path() -> Result<PathBuf, CliError> {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_loond") {
        return Ok(PathBuf::from(path));
    }

    let current_exe = std::env::current_exe().map_err(|err| {
        CliError::client_error(format!("failed to locate current executable: {err}"))
    })?;
    let sibling = current_exe.with_file_name(if cfg!(windows) { "loond.exe" } else { "loond" });
    if sibling.exists() {
        return Ok(sibling);
    }

    Ok(PathBuf::from("loond"))
}

fn healthz(server_url: &str) -> bool {
    Client::new(ClientConfig {
        server_url: server_url.to_owned(),
        auth_token: None,
    })
    .healthz()
    .is_ok()
}

fn process_exists(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes();
    system.process(Pid::from_u32(pid)).is_some()
}

fn kill_process(pid: u32) -> Result<(), CliError> {
    let mut system = System::new();
    system.refresh_processes();
    let Some(process) = system.process(Pid::from_u32(pid)) else {
        return Ok(());
    };
    if process.kill() {
        Ok(())
    } else {
        Err(CliError::client_error(format!(
            "failed to stop loond process {pid}"
        )))
    }
}
