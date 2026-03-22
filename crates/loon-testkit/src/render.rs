use std::fmt::Write as _;

use crate::scenario::Scenario;
use serde::Serialize;

pub fn render_summary(s: &Scenario) -> String {
    format!(
        "scenario={} kind={} schema_version={} seed={:?} actions={} faults={} expect_keys={}",
        s.name,
        s.scenario_kind.as_str(),
        s.schema_version,
        s.seed,
        s.actions.len(),
        s.faults.len(),
        s.expect.len()
    )
}

pub fn render_trace<T>(scenario: &Scenario, trace_lines: &[T]) -> String
where
    T: AsRef<str>,
{
    let mut rendered = String::new();
    let _ = writeln!(&mut rendered, "{}", render_summary(scenario));

    for (index, line) in trace_lines.iter().enumerate() {
        let _ = writeln!(&mut rendered, "  {}. {}", index + 1, line.as_ref());
    }

    rendered
}

pub fn render_case(scenario: &Scenario) -> String {
    let mut rendered = String::new();
    let _ = writeln!(&mut rendered, "{}", render_summary(scenario));
    render_mapping_section(&mut rendered, "initial", &scenario.initial);
    render_sequence_section(&mut rendered, "actions", &scenario.actions);
    render_sequence_section(&mut rendered, "faults", &scenario.faults);
    if let Some(explore) = &scenario.explore {
        render_mapping_section(&mut rendered, "explore", explore);
    }
    render_mapping_section(&mut rendered, "expect", &scenario.expect);
    rendered
}

pub fn render_yaml<T>(value: &T) -> Result<String, serde_yaml::Error>
where
    T: Serialize,
{
    serde_yaml::to_string(value)
}

fn render_mapping_section<T>(rendered: &mut String, label: &str, value: &T)
where
    T: Serialize,
{
    let _ = writeln!(rendered);
    let _ = writeln!(rendered, "[{label}]");
    write_yaml_block(rendered, value);
}

fn render_sequence_section<T>(rendered: &mut String, label: &str, values: &[T])
where
    T: Serialize,
{
    let _ = writeln!(rendered);
    let _ = writeln!(rendered, "[{label}]");

    if values.is_empty() {
        let _ = writeln!(rendered, "  <empty>");
        return;
    }

    for (index, value) in values.iter().enumerate() {
        let _ = writeln!(rendered, "  {}.", index + 1);
        write_yaml_block(rendered, value);
    }
}

fn write_yaml_block<T>(rendered: &mut String, value: &T)
where
    T: Serialize,
{
    match serde_yaml::to_string(value) {
        Ok(yaml) => {
            for line in yaml.lines() {
                let _ = writeln!(rendered, "    {}", line);
            }
        }
        Err(error) => {
            let _ = writeln!(rendered, "    <render error: {error}>");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{render_case, render_summary};
    use crate::scenario::Scenario;
    use serde_yaml::from_str;

    #[test]
    fn render_case_emits_sectioned_yaml() {
        let scenario: Scenario = from_str(
            r#"
schema_version: 1
scenario_kind: client
name: demo_case
seed: 42
initial:
  namespace_id: ns-1
actions:
  - step: one
  - step: two
expect:
  invariants:
    - example
"#,
        )
        .expect("parse scenario");

        let rendered = render_case(&scenario);

        assert!(rendered.starts_with(&render_summary(&scenario)));
        assert!(rendered.contains("[initial]"));
        assert!(rendered.contains("[actions]"));
        assert!(rendered.contains("[faults]"));
        assert!(rendered.contains("[expect]"));
        assert!(rendered.contains("1."));
        assert!(rendered.contains("step: one"));
        assert!(rendered.contains("<empty>"));
    }
}
