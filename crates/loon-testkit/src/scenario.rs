use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub seed: Option<u64>,
    #[serde(default)]
    pub initial: BTreeMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub actions: Vec<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default)]
    pub faults: Vec<BTreeMap<String, serde_yaml::Value>>,
    #[serde(default)]
    pub expect: BTreeMap<String, serde_yaml::Value>,
}

impl Scenario {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&text)?)
    }
}
