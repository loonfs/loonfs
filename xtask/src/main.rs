use anyhow::{bail, Result};
use loon_testkit::fixtures::fixture_path;
use loon_testkit::render::render_case;
use loon_testkit::scenario::Scenario;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("render-case") => {
            let path = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing scenario path"))?;
            let resolved_path = resolve_scenario_path(&path);
            let scenario = Scenario::load(&resolved_path)?;
            println!("path={}", resolved_path.display());
            println!("{}", render_case(&scenario));
            Ok(())
        }
        Some("replay-seed") => {
            println!("TODO: wire deterministic replay");
            Ok(())
        }
        Some("minimize-case") => {
            println!("TODO: wire scenario minimization");
            Ok(())
        }
        Some(other) => bail!("unknown xtask command: {other}"),
        None => {
            println!("xtask commands: render-case | replay-seed | minimize-case");
            Ok(())
        }
    }
}

fn resolve_scenario_path(path_arg: &str) -> PathBuf {
    let requested = PathBuf::from(path_arg);
    if requested.is_file() {
        return requested;
    }

    if looks_like_fixture_key(&requested) {
        return fixture_path(path_arg);
    }

    requested
}

fn looks_like_fixture_key(path: &Path) -> bool {
    path.extension().is_some()
        && !path.is_absolute()
        && path.components().all(|component| matches!(component, std::path::Component::Normal(_)))
}
