use std::fmt::Write as _;

use crate::scenario::Scenario;

pub fn render_summary(s: &Scenario) -> String {
    format!(
        "scenario={} seed={:?} actions={} faults={} expect_keys={}",
        s.name,
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
