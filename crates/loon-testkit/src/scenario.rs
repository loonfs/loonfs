use anyhow::{bail, Result};
use loon_sim::faults::{FaultPlan, InjectedClientFault};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

pub const CURRENT_SCENARIO_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioKind {
    Client,
    Model,
    Native,
    Sim,
}

impl ScenarioKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Model => "model",
            Self::Native => "native",
            Self::Sim => "sim",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "client" => Ok(Self::Client),
            "model" => Ok(Self::Model),
            "native" => Ok(Self::Native),
            "sim" => Ok(Self::Sim),
            other => bail!("unknown scenario kind `{other}`"),
        }
    }

    pub fn from_path(path: &Path) -> Option<Self> {
        let components: Vec<_> = path.components().collect();
        components
            .iter()
            .position(|component| component.as_os_str() == "scenarios")
            .and_then(|index| components.get(index + 1))
            .and_then(|component| component.as_os_str().to_str())
            .and_then(|value| Self::parse(value).ok())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scenario {
    pub schema_version: u32,
    pub scenario_kind: ScenarioKind,
    pub name: String,
    pub seed: Option<u64>,
    #[serde(default)]
    pub initial: BTreeMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub actions: Vec<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default)]
    pub faults: Vec<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default)]
    pub explore: Option<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default)]
    pub expect: BTreeMap<String, serde_yaml::Value>,
}

impl Scenario {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)?;
        let scenario: Self = serde_yaml::from_str(&text)?;
        scenario.validate(path)?;
        Ok(scenario)
    }

    pub fn decode_initial<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        decode_fragment(&self.initial)
    }

    pub fn decode_actions<T>(&self) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        self.actions.iter().map(decode_fragment).collect()
    }

    pub fn decode_faults<T>(&self) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        self.faults.iter().map(decode_fragment).collect()
    }

    pub fn decode_expect<T>(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        decode_fragment(&self.expect)
    }

    pub fn decode_explore<T>(&self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.explore.as_ref().map(decode_fragment).transpose()
    }

    pub fn decode_fault_plan(&self) -> Result<FaultPlan> {
        let faults: Vec<InjectedClientFault> = self.decode_faults()?;
        Ok(FaultPlan::new(faults))
    }

    fn validate(&self, path: &Path) -> Result<()> {
        if self.schema_version != CURRENT_SCENARIO_SCHEMA_VERSION {
            bail!(
                "unsupported scenario schema version {} for {} (expected {})",
                self.schema_version,
                path.display(),
                CURRENT_SCENARIO_SCHEMA_VERSION
            );
        }

        if let Some(expected_kind) = ScenarioKind::from_path(path) {
            if self.scenario_kind != expected_kind {
                bail!(
                    "scenario kind mismatch for {}: file says `{}`, path says `{}`",
                    path.display(),
                    self.scenario_kind.as_str(),
                    expected_kind.as_str()
                );
            }
        }

        Ok(())
    }
}

fn decode_fragment<T, S>(value: &S) -> Result<T>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let value = serde_yaml::to_value(value)?;
    Ok(serde_yaml::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::{Scenario, ScenarioKind, CURRENT_SCENARIO_SCHEMA_VERSION};

    #[test]
    fn scenario_load_rejects_missing_metadata() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("scenario-load-rejects-missing-metadata.yaml");
        std::fs::write(
            &path,
            r#"
name: missing_metadata
initial: {}
expect: {}
"#,
        )
        .expect("write missing metadata scenario");

        let error = Scenario::load(&path).expect_err("missing metadata should fail");
        assert!(
            error.to_string().contains("missing field `schema_version`"),
            "unexpected error: {error}"
        );

        std::fs::remove_file(&path).expect("remove temp scenario");
    }

    #[test]
    fn scenario_load_rejects_kind_path_mismatch() {
        let temp_root = std::env::temp_dir().join("scenario-kind-mismatch");
        let scenario_dir = temp_root.join("tests/scenarios/client");
        std::fs::create_dir_all(&scenario_dir).expect("create temp scenario dir");
        let path = scenario_dir.join("kind-mismatch.yaml");
        std::fs::write(
            &path,
            r#"
schema_version: 1
scenario_kind: native
name: kind_mismatch
initial: {}
expect: {}
"#,
        )
        .expect("write kind mismatch scenario");

        let error = Scenario::load(&path).expect_err("kind mismatch should fail");
        assert!(
            error.to_string().contains("scenario kind mismatch"),
            "unexpected error: {error}"
        );

        std::fs::remove_file(&path).expect("remove temp scenario");
        std::fs::remove_dir_all(&temp_root).expect("remove temp scenario root");
    }

    #[test]
    fn scenario_kind_from_path_matches_fixture_family() {
        let path = Path::new("/tmp/work/tests/scenarios/native/example.yaml");
        assert_eq!(ScenarioKind::from_path(path), Some(ScenarioKind::Native));
        assert_eq!(CURRENT_SCENARIO_SCHEMA_VERSION, 1);
    }

    use std::path::Path;
}
