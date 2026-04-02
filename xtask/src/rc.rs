use anyhow::{anyhow, bail, Result};
use loon_client::ops::{run_command, OpsCommand};
use loon_types::NamespaceId;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Debug, Clone, PartialEq, Eq)]
struct RcLocalArgs {
    config_path: PathBuf,
    namespace_id: NamespaceId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RcLocalStep {
    FmtCheck,
    ClippyWorkspace,
    WorkspaceTests,
    ObjectStoreConformanceLocalFs,
    OpsSmoke,
}

impl RcLocalStep {
    fn as_str(self) -> &'static str {
        match self {
            Self::FmtCheck => "fmt-check",
            Self::ClippyWorkspace => "clippy-workspace",
            Self::WorkspaceTests => "workspace-tests",
            Self::ObjectStoreConformanceLocalFs => "objectstore-conformance-localfs",
            Self::OpsSmoke => "ops-smoke",
        }
    }

    fn cargo_args(self) -> Option<&'static [&'static str]> {
        match self {
            Self::FmtCheck => Some(&["fmt", "--all", "--check"]),
            Self::ClippyWorkspace => Some(&[
                "clippy",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ]),
            Self::WorkspaceTests => Some(&["test", "--workspace"]),
            Self::ObjectStoreConformanceLocalFs => {
                Some(&["test", "-p", "loon-objectstore", "--test", "conformance"])
            }
            Self::OpsSmoke => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RcLocalReport {
    namespace_id: NamespaceId,
    completed_steps: Vec<RcLocalStep>,
    smoke_output: String,
}

trait RcLocalRunner {
    fn run_cargo_step(&mut self, step: RcLocalStep, args: &[&str]) -> Result<()>;
    fn run_ops_smoke(&mut self, config_path: &Path, namespace_id: &NamespaceId) -> Result<String>;
}

struct ProcessRunner;

impl RcLocalRunner for ProcessRunner {
    fn run_cargo_step(&mut self, step: RcLocalStep, args: &[&str]) -> Result<()> {
        let status = Command::new("cargo")
            .args(args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|error| anyhow!("rc-local step {} failed to start: {error}", step.as_str()))?;
        if status.success() {
            Ok(())
        } else {
            bail!(
                "rc-local step {} failed with exit status {}",
                step.as_str(),
                status
            )
        }
    }

    fn run_ops_smoke(&mut self, config_path: &Path, namespace_id: &NamespaceId) -> Result<String> {
        run_command(
            OpsCommand::Smoke {
                config_path: config_path.to_path_buf(),
                namespace_id: namespace_id.clone(),
            },
            &crate::transport::make_transport,
            &crate::transport::open_store,
        )
    }
}

pub fn run(args: impl IntoIterator<Item = String>) -> Result<String> {
    let parsed = parse_args(args)?;
    let mut runner = ProcessRunner;
    run_with_runner(parsed, &mut runner)
}

fn run_with_runner<R: RcLocalRunner>(args: RcLocalArgs, runner: &mut R) -> Result<String> {
    let mut completed_steps = Vec::new();
    for step in [
        RcLocalStep::FmtCheck,
        RcLocalStep::ClippyWorkspace,
        RcLocalStep::WorkspaceTests,
        RcLocalStep::ObjectStoreConformanceLocalFs,
    ] {
        let cargo_args = step
            .cargo_args()
            .expect("cargo-backed rc-local steps must expose cargo args");
        runner.run_cargo_step(step, cargo_args)?;
        completed_steps.push(step);
    }

    let smoke_output = runner.run_ops_smoke(&args.config_path, &args.namespace_id)?;
    completed_steps.push(RcLocalStep::OpsSmoke);

    Ok(render_report(&RcLocalReport {
        namespace_id: args.namespace_id,
        completed_steps,
        smoke_output,
    }))
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<RcLocalArgs> {
    let mut config_path = None;
    let mut namespace_id = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("usage: rc-local --config <path> --namespace <id>"))?;
                if config_path.replace(PathBuf::from(value)).is_some() {
                    bail!("duplicate --config");
                }
            }
            "--namespace" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("usage: rc-local --config <path> --namespace <id>"))?;
                if namespace_id.replace(NamespaceId::from(value)).is_some() {
                    bail!("duplicate --namespace");
                }
            }
            other => bail!("unknown rc-local argument: {other}"),
        }
    }

    Ok(RcLocalArgs {
        config_path: config_path
            .ok_or_else(|| anyhow!("usage: rc-local --config <path> --namespace <id>"))?,
        namespace_id: namespace_id
            .ok_or_else(|| anyhow!("usage: rc-local --config <path> --namespace <id>"))?,
    })
}

fn render_report(report: &RcLocalReport) -> String {
    let steps = report
        .completed_steps
        .iter()
        .map(|step| step.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "command=rc-local\nnamespace={}\nstatus=passed\nsteps_run={}\nsteps={}\n[smoke]\n{}",
        report.namespace_id.as_str(),
        report.completed_steps.len(),
        steps,
        report.smoke_output
    )
}

#[cfg(test)]
mod tests {
    use super::{parse_args, run_with_runner, RcLocalArgs, RcLocalRunner, RcLocalStep};
    use loon_types::NamespaceId;
    use std::path::{Path, PathBuf};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Invocation {
        Cargo {
            step: &'static str,
            args: Vec<String>,
        },
        OpsSmoke {
            config_path: PathBuf,
            namespace_id: NamespaceId,
        },
    }

    struct FakeRunner {
        invocations: Vec<Invocation>,
        fail_step: Option<RcLocalStep>,
        smoke_output: String,
    }

    impl FakeRunner {
        fn success() -> Self {
            Self {
                invocations: Vec::new(),
                fail_step: None,
                smoke_output: include_str!("../../tests/snapshots/ops-smoke/ops_smoke.txt")
                    .to_owned(),
            }
        }
    }

    impl RcLocalRunner for FakeRunner {
        fn run_cargo_step(&mut self, step: RcLocalStep, args: &[&str]) -> anyhow::Result<()> {
            self.invocations.push(Invocation::Cargo {
                step: step.as_str(),
                args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            });
            if self.fail_step == Some(step) {
                anyhow::bail!("rc-local step {} failed with exit status 1", step.as_str());
            }
            Ok(())
        }

        fn run_ops_smoke(
            &mut self,
            config_path: &Path,
            namespace_id: &NamespaceId,
        ) -> anyhow::Result<String> {
            self.invocations.push(Invocation::OpsSmoke {
                config_path: config_path.to_path_buf(),
                namespace_id: namespace_id.clone(),
            });
            Ok(self.smoke_output.clone())
        }
    }

    #[test]
    fn parses_rc_local_args() {
        let parsed = parse_args([
            "--config".to_owned(),
            "loondb-demo.toml".to_owned(),
            "--namespace".to_owned(),
            "demo".to_owned(),
        ])
        .expect("parse rc-local args");

        assert_eq!(
            parsed,
            RcLocalArgs {
                config_path: PathBuf::from("loondb-demo.toml"),
                namespace_id: NamespaceId::from("demo"),
            }
        );
    }

    #[test]
    fn runs_exact_steps_in_order() {
        let mut runner = FakeRunner::success();
        let rendered = run_with_runner(
            RcLocalArgs {
                config_path: PathBuf::from("loondb-demo.toml"),
                namespace_id: NamespaceId::from("demo"),
            },
            &mut runner,
        )
        .expect("run rc-local");

        assert_eq!(
            runner.invocations,
            vec![
                Invocation::Cargo {
                    step: "fmt-check",
                    args: vec!["fmt", "--all", "--check"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                },
                Invocation::Cargo {
                    step: "clippy-workspace",
                    args: vec![
                        "clippy",
                        "--workspace",
                        "--all-targets",
                        "--",
                        "-D",
                        "warnings",
                    ]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                },
                Invocation::Cargo {
                    step: "workspace-tests",
                    args: vec!["test", "--workspace"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                },
                Invocation::Cargo {
                    step: "objectstore-conformance-localfs",
                    args: vec!["test", "-p", "loon-objectstore", "--test", "conformance"]
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                },
                Invocation::OpsSmoke {
                    config_path: PathBuf::from("loondb-demo.toml"),
                    namespace_id: NamespaceId::from("demo"),
                },
            ]
        );

        assert_eq!(
            rendered,
            include_str!("../../tests/snapshots/rc-local/rc_local_success.txt")
        );
    }

    #[test]
    fn fails_fast_and_names_failed_step() {
        let mut runner = FakeRunner {
            fail_step: Some(RcLocalStep::ClippyWorkspace),
            ..FakeRunner::success()
        };

        let error = run_with_runner(
            RcLocalArgs {
                config_path: PathBuf::from("loondb-demo.toml"),
                namespace_id: NamespaceId::from("demo"),
            },
            &mut runner,
        )
        .expect_err("clippy step should fail");

        assert!(
            error
                .to_string()
                .contains("rc-local step clippy-workspace failed"),
            "unexpected error: {error}"
        );
        assert_eq!(runner.invocations.len(), 2);
    }

    #[test]
    fn invokes_existing_ops_smoke_path() {
        let mut runner = FakeRunner::success();
        let _ = run_with_runner(
            RcLocalArgs {
                config_path: PathBuf::from("configs/loondb-demo.local-fs.example.toml"),
                namespace_id: NamespaceId::from("demo"),
            },
            &mut runner,
        )
        .expect("run rc-local");

        assert_eq!(
            runner.invocations.last(),
            Some(&Invocation::OpsSmoke {
                config_path: PathBuf::from("configs/loondb-demo.local-fs.example.toml"),
                namespace_id: NamespaceId::from("demo"),
            })
        );
    }
}
