use anyhow::{bail, Result};
use loon_testkit::fixtures::fixture_path;
use loon_testkit::render::render_case;
use loon_testkit::replay::run_replay_scenario;
use loon_testkit::scenario::Scenario;
use loon_testkit::seed::Seed;
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
        && path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
}

struct ReplaySeedArgs {
    scenario_path: String,
    seed: Option<Seed>,
}

fn parse_replay_seed_args(mut args: impl Iterator<Item = String>) -> Result<ReplaySeedArgs> {
    let first = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: replay-seed [replay] <scenario> [--seed <u64>]"))?;

    let scenario_path = if first == "replay" {
        args.next().ok_or_else(|| {
            anyhow::anyhow!("usage: replay-seed [replay] <scenario> [--seed <u64>]")
        })?
    } else {
        first
    };

    let mut seed = None;
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
            other => bail!("unexpected replay-seed argument: {other}"),
        }
    }

    Ok(ReplaySeedArgs {
        scenario_path,
        seed,
    })
}
