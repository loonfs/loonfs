use anyhow::{bail, Result};
use loon_testkit::fixtures::fixture_path;
use loon_testkit::minimize::minimize_replay_scenario;
use loon_testkit::render::{render_case, render_yaml};
use loon_testkit::replay::run_replay_scenario;
use loon_testkit::scenario::Scenario;
use loon_testkit::seed::Seed;
use loon_testkit::snapshots::{fixture_key_from_path, write_snapshot, SnapshotKind};
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("render-case") => {
            let render_args = parse_render_case_args(args)?;
            let resolved_path = resolve_scenario_path(&render_args.scenario_path);
            let scenario = Scenario::load(&resolved_path)?;
            let rendered = render_case(&scenario);
            println!("path={}", resolved_path.display());
            println!("{rendered}");
            maybe_write_snapshot(
                render_args.snapshot,
                &resolved_path,
                SnapshotKind::RenderCase,
                None,
                &format!(
                    "command=render-case\nfixture={}\n{}",
                    fixture_key_from_path(&resolved_path)?,
                    rendered
                ),
            )?;
            Ok(())
        }
        Some("replay-seed") => {
            let replay_args = parse_replay_seed_args(args)?;
            let resolved_path = resolve_scenario_path(&replay_args.scenario_path);
            let scenario = Scenario::load(&resolved_path)?;
            let report = run_replay_scenario(&scenario, replay_args.seed)?;

            println!("path={}", resolved_path.display());
            println!("harness=replay");
            println!("mode={}", report.harness_kind.as_str());
            println!("scenario={}", report.scenario_name);
            println!("seed={:?}", report.effective_seed.map(|seed| seed.0));
            if report.observed_invariants.is_empty() {
                println!("invariants=<none>");
            } else {
                println!("invariants={}", report.observed_invariants.join(","));
            }
            println!("{}", report.rendered_trace);
            let snapshot_contents = format!(
                "command=replay-seed\nfixture={}\nmode={}\nscenario={}\nseed={:?}\ninvariants={}\n{}",
                fixture_key_from_path(&resolved_path)?,
                report.harness_kind.as_str(),
                report.scenario_name,
                report.effective_seed.map(|seed| seed.0),
                if report.observed_invariants.is_empty() {
                    "<none>".to_owned()
                } else {
                    report.observed_invariants.join(",")
                },
                report.rendered_trace
            );
            maybe_write_snapshot(
                replay_args.snapshot,
                &resolved_path,
                SnapshotKind::ReplaySeed,
                report.effective_seed.map(|seed| seed.0),
                &snapshot_contents,
            )?;
            Ok(())
        }
        Some("minimize-case") => {
            let minimize_args = parse_minimize_case_args(args)?;
            let resolved_path = resolve_scenario_path(&minimize_args.scenario_path);
            let scenario = Scenario::load(&resolved_path)?;
            let report = minimize_replay_scenario(&scenario, minimize_args.seed)?;
            let minimized_yaml = render_yaml(&report.scenario)?;

            println!("path={}", resolved_path.display());
            println!("harness=minimize");
            println!("mode={}", report.harness_kind.as_str());
            println!("scenario={}", report.scenario_name);
            println!("seed={:?}", report.effective_seed.map(|seed| seed.0));
            println!("original_wal_objects={}", report.original_wal_objects);
            println!("minimized_wal_objects={}", report.minimized_wal_objects);
            println!(
                "original_failure={}",
                report.original_failure.lines().next().unwrap_or("<none>")
            );
            println!(
                "minimized_failure={}",
                report.minimized_failure.lines().next().unwrap_or("<none>")
            );

            if let Some(write_path) = minimize_args.write_path {
                std::fs::write(&write_path, &minimized_yaml)?;
                println!("minimized_path={}", write_path.display());
            } else {
                println!("[minimized]");
                print!("{minimized_yaml}");
            }
            let snapshot_contents = format!(
                "command=minimize-case\nfixture={}\nmode={}\nscenario={}\nseed={:?}\noriginal_wal_objects={}\nminimized_wal_objects={}\noriginal_failure={}\nminimized_failure={}\n[minimized]\n{}",
                fixture_key_from_path(&resolved_path)?,
                report.harness_kind.as_str(),
                report.scenario_name,
                report.effective_seed.map(|seed| seed.0),
                report.original_wal_objects,
                report.minimized_wal_objects,
                report.original_failure.lines().next().unwrap_or("<none>"),
                report.minimized_failure.lines().next().unwrap_or("<none>"),
                minimized_yaml
            );
            maybe_write_snapshot(
                minimize_args.snapshot,
                &resolved_path,
                SnapshotKind::MinimizeCase,
                report.effective_seed.map(|seed| seed.0),
                &snapshot_contents,
            )?;
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
        && path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
}

struct ReplaySeedArgs {
    scenario_path: String,
    seed: Option<Seed>,
    snapshot: bool,
}

fn parse_replay_seed_args(mut args: impl Iterator<Item = String>) -> Result<ReplaySeedArgs> {
    let first = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: replay-seed [replay] <scenario> [--seed <u64>] [--snapshot]"))?;

    let scenario_path = if first == "replay" {
        args.next().ok_or_else(|| {
            anyhow::anyhow!("usage: replay-seed [replay] <scenario> [--seed <u64>] [--snapshot]")
        })?
    } else {
        first
    };

    let mut seed = None;
    let mut snapshot = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seed" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for --seed in replay-seed"))?;
                let parsed = value
                    .parse::<u64>()
                    .map_err(|err| anyhow::anyhow!("invalid --seed value `{value}`: {err}"))?;
                seed = Some(Seed(parsed));
            }
            "--snapshot" => snapshot = true,
            other => bail!("unexpected replay-seed argument: {other}"),
        }
    }

    Ok(ReplaySeedArgs {
        scenario_path,
        seed,
        snapshot,
    })
}

struct RenderCaseArgs {
    scenario_path: String,
    snapshot: bool,
}

fn parse_render_case_args(mut args: impl Iterator<Item = String>) -> Result<RenderCaseArgs> {
    let scenario_path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: render-case <scenario> [--snapshot]"))?;
    let mut snapshot = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--snapshot" => snapshot = true,
            other => bail!("unexpected render-case argument: {other}"),
        }
    }

    Ok(RenderCaseArgs {
        scenario_path,
        snapshot,
    })
}

struct MinimizeCaseArgs {
    scenario_path: String,
    seed: Option<Seed>,
    snapshot: bool,
    write_path: Option<PathBuf>,
}

fn parse_minimize_case_args(mut args: impl Iterator<Item = String>) -> Result<MinimizeCaseArgs> {
    let first = args.next().ok_or_else(|| {
        anyhow::anyhow!(
            "usage: minimize-case [replay] <scenario> [--seed <u64>] [--snapshot] [--write <path>]"
        )
    })?;

    let scenario_path = if first == "replay" {
        args.next().ok_or_else(|| {
            anyhow::anyhow!(
                "usage: minimize-case [replay] <scenario> [--seed <u64>] [--snapshot] [--write <path>]"
            )
        })?
    } else {
        first
    };

    let mut seed = None;
    let mut snapshot = false;
    let mut write_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--seed" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for --seed in minimize-case"))?;
                let parsed = value
                    .parse::<u64>()
                    .map_err(|err| anyhow::anyhow!("invalid --seed value `{value}`: {err}"))?;
                seed = Some(Seed(parsed));
            }
            "--snapshot" => snapshot = true,
            "--write" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for --write in minimize-case"))?;
                write_path = Some(PathBuf::from(value));
            }
            other => bail!("unexpected minimize-case argument: {other}"),
        }
    }

    Ok(MinimizeCaseArgs {
        scenario_path,
        seed,
        snapshot,
        write_path,
    })
}

fn maybe_write_snapshot(
    enabled: bool,
    scenario_path: &Path,
    kind: SnapshotKind,
    seed_suffix: Option<u64>,
    contents: &str,
) -> Result<()> {
    if !enabled {
        return Ok(());
    }

    let fixture_key = fixture_key_from_path(scenario_path)?;
    let snapshot_path = write_snapshot(&fixture_key, kind, seed_suffix, contents)?;
    println!("snapshot_path={}", snapshot_path.display());
    Ok(())
}
