//! Finding, loading, locking, and persisting the CLI config file.

use crate::config::{absolute_env_path, non_empty_env, CliConfig, CONFIG_PATH_ENV, CONFIG_VERSION};
use crate::error::{CliError, Result};
use loonfs_types::config::validate_non_empty;
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const CONFIG_FILE_NAME: &str = "config.toml";
/// Directory under `$XDG_CONFIG_HOME`.
const XDG_CONFIG_SUBDIR: &str = "loonfs";

/// Recovery instructions appended to config-load errors.
///
/// Strict decoding is safe only when users can select or create another
/// config file, including when the default file is unreadable.
const CONFIG_REMEDY: &str = "fix that file, or run against a different one with `--config <path>` \
     or `LOONFS_CONFIG=<path>`; `loonfs config path` prints the file in use, and \
     `loonfs init --config <path>` writes a fresh one";

/// How [`resolve_config_location`] chose the file, so `config path` reports
/// both "which file" and "why that one". The serialized spellings are the
/// `--json` surface: `flag`, `env`, `xdg`, `default`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigSource {
    /// The `--config` flag.
    Flag,
    /// The `LOONFS_CONFIG` environment variable.
    Env,
    /// `$XDG_CONFIG_HOME/loonfs/config.toml`.
    Xdg,
    /// `$HOME/.config/loonfs/config.toml`.
    Default,
}

/// The one config file this invocation reads and writes.
pub(crate) struct ConfigLocation {
    pub path: PathBuf,
    pub source: ConfigSource,
}

impl ConfigLocation {
    fn at(path: PathBuf, source: ConfigSource) -> Self {
        Self { path, source }
    }
}

/// Resolves the config path in this order: `--config`,
/// `LOONFS_CONFIG`, then the default location.
///
/// An explicit path is selected even when the file does not exist, allowing
/// commands such as `loonfs init --config <path>` to create a replacement
/// for an invalid default config.
pub(crate) fn resolve_config_location(flag: Option<&Path>) -> Result<ConfigLocation> {
    if let Some(path) = flag {
        return Ok(ConfigLocation::at(path.to_path_buf(), ConfigSource::Flag));
    }
    if let Some(path) = non_empty_env(CONFIG_PATH_ENV).map(PathBuf::from) {
        return Ok(ConfigLocation::at(path, ConfigSource::Env));
    }
    default_config_location()
}

/// Selects `$XDG_CONFIG_HOME/loonfs/config.toml` when the variable is
/// absolute, otherwise `$HOME/.config/loonfs/config.toml`. File existence
/// never changes which location is selected.
fn default_config_location() -> Result<ConfigLocation> {
    let (directory, source) = match absolute_env_path("XDG_CONFIG_HOME") {
        Some(directory) => (directory, ConfigSource::Xdg),
        None => {
            let home = absolute_env_path("HOME").ok_or_else(|| {
                CliError::invalid_config(format!(
                    "unable to determine the home directory; name the config file with \
                     `--config <path>` or `{CONFIG_PATH_ENV}=<path>`"
                ))
            })?;
            (home.join(".config"), ConfigSource::Default)
        }
    };
    Ok(ConfigLocation::at(
        directory.join(XDG_CONFIG_SUBDIR).join(CONFIG_FILE_NAME),
        source,
    ))
}

pub(crate) fn validate_profile_name(name: &str) -> Result<()> {
    Ok(validate_non_empty("profile name", name)?)
}

pub(crate) fn profile_field(name: &str, field: &str) -> String {
    format!("{name}.{field}")
}

pub(crate) fn load_config(path: &Path) -> Result<CliConfig> {
    let source = load_config_source(path)?;
    decode_config_source(path, &source.text)
}

/// Config result used by repair commands.
///
/// When strict decoding fails, repair commands receive the raw TOML table so
/// they can edit the invalid file.
pub(crate) enum ConfigLoad {
    Valid(CliConfig),
    Degraded {
        table: toml::Table,
        /// The strict-decode failure, reported alongside the degraded output.
        error: Box<CliError>,
    },
}

pub(crate) fn load_config_for_repair(path: &Path) -> Result<ConfigLoad> {
    let source = load_config_source(path)?;
    match decode_config_source(path, &source.text) {
        Ok(config) => Ok(ConfigLoad::Valid(config)),
        Err(error) => Ok(ConfigLoad::Degraded {
            table: source.table,
            error: Box::new(error),
        }),
    }
}

/// A config file that got as far as being TOML of this version.
struct ConfigDocument {
    /// The file's own text, which the strict decode reads so its errors can
    /// point at a line and column in it.
    text: String,
    /// The same file as a loose table, for the version probe and for the
    /// repair commands.
    table: toml::Table,
}

/// Stage one of loading: bytes to text and a loose TOML table, plus the
/// version probe. Unknown fields and per-profile shape problems survive
/// this stage; unreadable files, TOML syntax errors, and other config
/// versions do not.
fn load_config_source(path: &Path) -> Result<ConfigDocument> {
    let bytes = fs::read(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            CliError::invalid_config(format!(
                "config file does not exist: {}; run `loonfs init` to create it",
                path.display()
            ))
        } else {
            unusable_config(format!("failed to read config {}: {err}", path.display()))
        }
    })?;
    let text = String::from_utf8(bytes).map_err(|err| {
        unusable_config(format!("failed to decode config {}: {err}", path.display()))
    })?;
    let table: toml::Table = toml::from_str(&text).map_err(|err| {
        unusable_config(format!("failed to decode config {}: {err}", path.display()))
    })?;
    // Version before shape: a config written for another version should say
    // so directly, not surface as unknown-field noise from the strict decode.
    match table.get("config_version") {
        None => {
            return Err(unusable_config(format!(
                "config {} is missing `config_version`",
                path.display()
            )));
        }
        Some(value) => match value.as_integer() {
            Some(version) if version == i64::from(CONFIG_VERSION) => {}
            Some(version) => {
                return Err(unusable_config(format!(
                    "config {} declares `config_version = {version}`; this build supports \
                     `{CONFIG_VERSION}`",
                    path.display()
                )));
            }
            None => {
                return Err(unusable_config(format!(
                    "config {}: `config_version` must be an integer",
                    path.display()
                )));
            }
        },
    }
    Ok(ConfigDocument { text, table })
}

/// Stage two of loading: the strict typed decode (`deny_unknown_fields`)
/// plus semantic validation.
///
/// The decode reads the file's own text rather than the table stage one
/// already built. That costs a second parse of a small file and buys the
/// line, column, and quoted source line a TOML error includes only when the parser
/// has located the error in the text.
fn decode_config_source(path: &Path, text: &str) -> Result<CliConfig> {
    let config: CliConfig = toml::from_str(text).map_err(|err| {
        unusable_config(format!("failed to decode config {}: {err}", path.display()))
    })?;
    // Validation messages are field-rooted (`<profile>.store.<field>`), so
    // the file they belong to is named here rather than in every check.
    config.validate().map_err(|error| {
        unusable_config(format!(
            "invalid config {}: {}",
            path.display(),
            error.message
        ))
    })?;
    Ok(config)
}

/// Marks a config file this build will not read, so the failure includes the
/// way past it. Every load failure but "no file yet" goes through here.
fn unusable_config(problem: String) -> CliError {
    CliError::invalid_config(format!("{problem}\n{CONFIG_REMEDY}"))
}

pub(crate) fn load_config_if_exists(path: &Path) -> Result<Option<CliConfig>> {
    // A broken symlink is a file the user put there, not an absent config.
    match fs::symlink_metadata(path) {
        Ok(_) => load_config(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(unusable_config(format!(
            "failed to read config metadata {}: {error}",
            path.display()
        ))),
    }
}

pub(crate) fn load_or_default_config(path: &Path) -> Result<CliConfig> {
    Ok(load_config_if_exists(path)?.unwrap_or_default())
}

fn write_config_locked(path: &Path, config: &CliConfig) -> Result<()> {
    // The caller already has the config lock, which is not reentrant.
    config.validate()?;
    let contents = toml::to_string_pretty(config)
        .map_err(|err| CliError::invalid_config(format!("failed to encode config: {err}")))?;
    persist_config_contents(path, &contents)
}

/// Persists a loose config table as-is (atomic, owner-only), for the repair
/// commands when the file no longer strict-decodes: round-tripping through
/// [`CliConfig`] would drop the content the user still needs to fix.
pub(crate) fn save_config_table(path: &Path, table: &toml::Table) -> Result<()> {
    let _lock = lock_config(path)?;
    let contents = toml::to_string_pretty(table)
        .map_err(|err| CliError::invalid_config(format!("failed to encode config: {err}")))?;
    persist_config_contents(path, &contents)
}

/// Applies one edit to the latest config under the exclusive lock, so
/// concurrent writers cannot lose each other's updates. The closure's value
/// comes back to the caller once the file is durably replaced.
pub(crate) fn mutate_config<T>(
    path: &Path,
    mutate: impl FnOnce(&mut CliConfig) -> Result<T>,
) -> Result<T> {
    let _lock = lock_config(path)?;
    let mut config = load_or_default_config(path)?;
    let value = mutate(&mut config)?;
    write_config_locked(path, &config)?;
    Ok(value)
}

fn lock_config(path: &Path) -> Result<File> {
    let parent = path.parent().ok_or_else(|| {
        CliError::invalid_config(format!(
            "config path has no parent directory: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        CliError::invalid_config(format!("failed to create config directory: {err}"))
    })?;
    let lock_path = path.with_extension("lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|err| {
            CliError::invalid_config(format!(
                "failed to open the lock file `{}`: {err}",
                lock_path.display()
            ))
        })?;
    fs4::fs_std::FileExt::lock_exclusive(&file).map_err(|err| {
        CliError::invalid_config(format!("failed to lock `{}`: {err}", lock_path.display()))
    })?;
    Ok(file)
}

fn persist_config_contents(path: &Path, contents: &str) -> Result<()> {
    // The caller's config lock already proved that this parent directory exists.
    let parent = path
        .parent()
        .expect("a locked config path should have a parent directory");
    let mut temp_file = tempfile::Builder::new()
        .prefix(".loonfs-config-")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|err| {
            CliError::invalid_config(format!(
                "failed to create temporary config file in {}: {err}",
                parent.display()
            ))
        })?;
    temp_file
        .write_all(contents.as_bytes())
        .map_err(|err| CliError::invalid_config(format!("failed to write config: {err}")))?;
    temp_file
        .flush()
        .map_err(|err| CliError::invalid_config(format!("failed to flush config: {err}")))?;
    temp_file
        .as_file()
        .sync_all()
        .map_err(|err| CliError::invalid_config(format!("failed to sync config: {err}")))?;
    temp_file.persist(path).map_err(|err| {
        CliError::invalid_config(format!(
            "failed to persist config {}: {}",
            path.display(),
            err.error
        ))
    })?;
    // Rename durability is best-effort because some filesystems cannot sync
    // directories, and the rename itself has already succeeded.
    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}
