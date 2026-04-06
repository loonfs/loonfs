use crate::error::CliError;
use dialoguer::{Confirm, Input, Select};

pub fn prompt_line(label: &str) -> Result<String, CliError> {
    Input::new()
        .with_prompt(label)
        .interact_text()
        .map_err(|err| CliError::io(std::io::Error::other(err)))
}

pub fn prompt_line_default(label: &str, default: &str) -> Result<String, CliError> {
    Input::new()
        .with_prompt(label)
        .default(default.to_owned())
        .interact_text()
        .map_err(|err| CliError::io(std::io::Error::other(err)))
}

pub fn prompt_optional(label: &str, current: Option<&str>) -> Result<Option<String>, CliError> {
    let prompt = match current {
        Some(val) => format!("{label} (current: {val}, enter to keep, empty to clear)"),
        None => format!("{label} (optional, enter to skip)"),
    };
    let value: String = Input::new()
        .with_prompt(prompt)
        .default(current.unwrap_or("").to_owned())
        .allow_empty(true)
        .interact_text()
        .map_err(|err| CliError::io(std::io::Error::other(err)))?;
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub fn prompt_choice(label: &str, options: &[&str]) -> Result<String, CliError> {
    prompt_choice_default(label, options, 0)
}

pub fn prompt_choice_default(
    label: &str,
    options: &[&str],
    default: usize,
) -> Result<String, CliError> {
    let selection = Select::new()
        .with_prompt(label)
        .items(options)
        .default(default)
        .interact()
        .map_err(|err| CliError::io(std::io::Error::other(err)))?;
    Ok(options[selection].to_owned())
}

pub fn prompt_confirm(label: &str) -> Result<bool, CliError> {
    Confirm::new()
        .with_prompt(label)
        .default(false)
        .interact()
        .map_err(|err| CliError::io(std::io::Error::other(err)))
}
