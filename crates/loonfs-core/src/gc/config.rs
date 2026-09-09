//! GC configuration.

use crate::error::{CoreError, Result};
use crate::limits::{GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS};
use serde::{Deserialize, Serialize};

/// Per-call limits for namespace collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    pub grace_window_ms: u64,
    /// Maximum candidates inspected after roots have been loaded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<u64>,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_window_ms: GC_DEFAULT_GRACE_WINDOW_MS,
            max_steps: None,
        }
    }
}

impl GcConfig {
    pub(super) fn validate(&self) -> Result<()> {
        // The minimum grace window is derived from the publication budgets
        // and provider deadlines in `limits`. Below it, a publish still in
        // flight could have written objects that already look old enough to
        // delete, so the configuration is rejected outright.
        if self.grace_window_ms < GC_MIN_GRACE_WINDOW_MS {
            return Err(CoreError::InvalidGcConfig(format!(
                "grace_window_ms {} is below the derived safety minimum {}",
                self.grace_window_ms, GC_MIN_GRACE_WINDOW_MS
            )));
        }
        if self.max_steps == Some(0) {
            return Err(CoreError::InvalidGcConfig(
                "max_steps must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::GcConfig;

    #[test]
    fn an_unknown_budget_name_cannot_become_an_unlimited_run() {
        let config = GcConfig {
            max_steps: Some(1),
            ..GcConfig::default()
        };
        let mut encoded = serde_json::to_value(&config).expect("encode config");
        assert_eq!(
            serde_json::from_value::<GcConfig>(encoded.clone()).expect("decode config"),
            config
        );
        let fields = encoded.as_object_mut().expect("config object");
        let budget = fields.remove("max_steps").expect("budget");
        fields.insert("max_objects".to_owned(), budget);
        assert!(serde_json::from_value::<GcConfig>(encoded).is_err());
    }
}
