use anyhow::Result;
use serde::de::DeserializeOwned;
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
}

fn decode_fragment<T, S>(value: &S) -> Result<T>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let value = serde_yaml::to_value(value)?;
    Ok(serde_yaml::from_value(value)?)
}
