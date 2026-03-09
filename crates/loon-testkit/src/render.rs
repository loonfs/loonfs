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
