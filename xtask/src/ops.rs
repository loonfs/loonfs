use anyhow::Result;

pub fn run(args: impl IntoIterator<Item = String>) -> Result<String> {
    loon_ops::run_args(args)
}

#[cfg(test)]
mod tests {
    use super::run;
    use loon_client::state_db::SqliteStateDb;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn forwards_bootstrap_namespace_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-bootstrap");
        let config_path = write_local_fs_config(&temp_dir);

        let rendered = run([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops bootstrap");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-bootstrap-namespace/ops_bootstrap_namespace.txt"
            )
        );
    }

    #[test]
    fn forwards_show_client_state_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-show-client");
        let config_path = write_local_fs_config(&temp_dir);
        let db_path = temp_dir.join("client.sqlite3");
        let _db = SqliteStateDb::open(&db_path).expect("open client db");

        let rendered = run([
            "show-client-state".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops show-client-state");

        assert_eq!(
            rendered,
            include_str!("../../tests/snapshots/ops-show-client-state/ops_show_client_state.txt")
        );
    }

    #[test]
    fn forwards_import_remote_observations_to_loon_ops() {
        let temp_dir = unique_temp_dir("xtask-ops-import");
        let config_path = write_local_fs_config(&temp_dir);

        run([
            "bootstrap-namespace".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("bootstrap namespace");

        let rendered = run([
            "import-remote-observations".to_owned(),
            "--config".to_owned(),
            config_path.display().to_string(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("run xtask ops import-remote-observations");

        assert_eq!(
            rendered,
            include_str!(
                "../../tests/snapshots/ops-import-remote-observations/ops_import_remote_observations.txt"
            )
        );
    }

    fn write_local_fs_config(temp_dir: &Path) -> PathBuf {
        let config_path = temp_dir.join("loondb-demo.toml");
        fs::write(
            &config_path,
            format!(
                r#"[object_store]
kind = "local-fs"
root = "{}"
key_prefix = "tenant-a"

[client]
state_db_path = "{}"
mirror_root = "{}"

[server]
writer_id = "writer-a"
writer_version = "xtask-ops-test"
lease_duration_ms = 60000

[ops]
now_ms = 1000
"#,
                temp_dir.join("store").display(),
                temp_dir.join("client.sqlite3").display(),
                temp_dir.join("mirror").display(),
            ),
        )
        .expect("write config");
        fs::create_dir_all(temp_dir.join("mirror")).expect("create mirror root");
        config_path
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loondb-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
