use crate::error::CliError;
use dialoguer::{Confirm, FuzzySelect, Input, Password, Select};

pub(crate) fn prompt_line(label: &str) -> Result<String, CliError> {
    Input::new()
        .with_prompt(label)
        .interact_text()
        .map_err(|err| CliError::io(std::io::Error::other(err)))
}

pub(crate) fn prompt_line_default(label: &str, default: &str) -> Result<String, CliError> {
    Input::new()
        .with_prompt(label)
        .default(default.to_owned())
        .interact_text()
        .map_err(|err| CliError::io(std::io::Error::other(err)))
}

/// Prompts for a new secret with hidden input; empty input re-prompts.
pub(crate) fn prompt_secret(label: &str) -> Result<String, CliError> {
    Password::new()
        .with_prompt(format!("{label} (hidden)"))
        .interact()
        .map_err(|err| CliError::io(std::io::Error::other(err)))
}

pub(crate) fn prompt_secret_keep_current(label: &str, current: &str) -> Result<String, CliError> {
    let value = Password::new()
        .with_prompt(format!("{label} (hidden, enter to keep current)"))
        .allow_empty_password(true)
        .interact()
        .map_err(|err| CliError::io(std::io::Error::other(err)))?;
    if value.is_empty() {
        Ok(current.to_owned())
    } else {
        Ok(value)
    }
}

/// Prompts for an optional secret with hidden input. Empty input keeps the
/// current value (or stays unset); the current value is never displayed.
pub(crate) fn prompt_secret_optional(
    label: &str,
    current: Option<&str>,
) -> Result<Option<String>, CliError> {
    let prompt = match current {
        Some(_) => format!("{label} (hidden, enter to keep current)"),
        None => format!("{label} (optional, hidden, enter to skip)"),
    };
    let value = Password::new()
        .with_prompt(prompt)
        .allow_empty_password(true)
        .interact()
        .map_err(|err| CliError::io(std::io::Error::other(err)))?;
    if value.is_empty() {
        Ok(current.map(ToOwned::to_owned))
    } else {
        Ok(Some(value))
    }
}

pub(crate) fn prompt_optional(
    label: &str,
    current: Option<&str>,
) -> Result<Option<String>, CliError> {
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

pub(crate) fn prompt_choice(label: &str, options: &[&str]) -> Result<String, CliError> {
    prompt_choice_default(label, options, 0)
}

pub(crate) fn prompt_choice_default(
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

pub(crate) fn prompt_fuzzy_choice(
    label: &str,
    options: &[&str],
    default: usize,
) -> Result<String, CliError> {
    let mut items: Vec<String> = options.iter().map(|s| s.to_string()).collect();
    items.push("(other)".to_string());
    let selection = FuzzySelect::new()
        .with_prompt(label)
        .items(&items)
        .default(default)
        .interact()
        .map_err(|err| CliError::io(std::io::Error::other(err)))?;
    if selection == options.len() {
        prompt_line(label)
    } else {
        Ok(options[selection].to_owned())
    }
}

pub(crate) fn prompt_confirm(label: &str) -> Result<bool, CliError> {
    Confirm::new()
        .with_prompt(label)
        .default(false)
        .interact()
        .map_err(|err| CliError::io(std::io::Error::other(err)))
}
