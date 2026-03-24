use anyhow::{Context, Result};
use clap::Args;
use clap_mangen::Man;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManpagesCommand {
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone, Args)]
pub struct ManpagesArgs {
    output_dir: PathBuf,
}

impl ManpagesArgs {
    pub(crate) fn into_command(self) -> ManpagesCommand {
        ManpagesCommand {
            output_dir: self.output_dir,
        }
    }
}

pub(crate) fn render_manpages(command: clap::Command, request: ManpagesCommand) -> Result<String> {
    fs::create_dir_all(&request.output_dir)
        .with_context(|| format!("create manpage output dir {}", request.output_dir.display()))?;
    let mut written = Vec::new();
    write_command_manpages(command, "loon", &request.output_dir, &mut written)?;
    Ok(format!(
        "generated_manpages={}\noutput_dir={}\n",
        written.len(),
        request.output_dir.display()
    ))
}

fn write_command_manpages(
    command: clap::Command,
    full_name: &str,
    output_dir: &Path,
    written: &mut Vec<PathBuf>,
) -> Result<()> {
    let manpage_path = output_dir.join(format!("{full_name}.1"));
    let mut file = File::create(&manpage_path)
        .with_context(|| format!("create manpage {}", manpage_path.display()))?;
    Man::new(command.clone())
        .render(&mut file)
        .with_context(|| format!("render manpage {}", manpage_path.display()))?;
    written.push(manpage_path);

    for subcommand in command.get_subcommands() {
        write_command_manpages(
            subcommand.clone(),
            &format!("{full_name}-{}", subcommand.get_name()),
            output_dir,
            written,
        )?;
    }

    Ok(())
}
