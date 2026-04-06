use crate::error::CliError;
use std::io::{self, Write};

pub fn prompt_line(label: &str) -> Result<String, CliError> {
    eprint!("{label}: ");
    io::stderr().flush().map_err(CliError::io)?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).map_err(CliError::io)?;
    let value = line.trim().to_owned();
    if value.is_empty() {
        return Err(CliError::invalid_input(format!("{label} is required")));
    }
    Ok(value)
}

pub fn prompt_line_default(label: &str, default: &str) -> Result<String, CliError> {
    eprint!("{label} [{default}]: ");
    io::stderr().flush().map_err(CliError::io)?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).map_err(CliError::io)?;
    let value = line.trim();
    if value.is_empty() {
        Ok(default.to_owned())
    } else {
        Ok(value.to_owned())
    }
}

pub fn prompt_optional(label: &str, current: Option<&str>) -> Result<Option<String>, CliError> {
    match current {
        Some(val) => eprint!("{label} [{val}]: "),
        None => eprint!("{label} (optional): "),
    }
    io::stderr().flush().map_err(CliError::io)?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).map_err(CliError::io)?;
    let value = line.trim();
    if value.is_empty() {
        Ok(current.map(ToOwned::to_owned))
    } else {
        Ok(Some(value.to_owned()))
    }
}

pub fn prompt_choice(label: &str, options: &[&str]) -> Result<String, CliError> {
    for (i, option) in options.iter().enumerate() {
        eprintln!("  {}. {option}", i + 1);
    }
    eprint!("{label}: ");
    io::stderr().flush().map_err(CliError::io)?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).map_err(CliError::io)?;
    let value = line.trim();
    if let Ok(index) = value.parse::<usize>() {
        if index >= 1 && index <= options.len() {
            return Ok(options[index - 1].to_owned());
        }
    }
    if options.contains(&value) {
        return Ok(value.to_owned());
    }
    Err(CliError::invalid_input(format!(
        "invalid choice: `{value}` (expected 1-{} or one of: {})",
        options.len(),
        options.join(", ")
    )))
}

pub fn prompt_confirm(label: &str) -> Result<bool, CliError> {
    eprint!("{label} [y/N]: ");
    io::stderr().flush().map_err(CliError::io)?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).map_err(CliError::io)?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "Yes" | "YES"))
}
