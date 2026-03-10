use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use crate::consts::DEFAULT_PEP_VERSION;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectConfig {
    pub pep_version: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub sample_table: Option<String>,
    pub subsample_table: Option<SubsampleTable>,
    pub sample_table_index: Option<String>,
    pub subsample_table_index: Option<String>,
    pub sample_modifiers: Option<SampleModifiers>,
    pub project_modifiers: Option<ProjectModifiers>,
    pub raw: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SubsampleTable {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SubsampleTableIndex {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SampleModifiers {
    pub remove: Option<Vec<String>>,
    pub append: Option<HashMap<String, String>>,
    pub duplicate: Option<HashMap<String, String>>,
    pub imply: Option<Vec<ImplyRule>>,
    pub derive: Option<DeriveRule>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImplyRule {
    #[serde(rename = "if")]
    pub if_condition: HashMap<String, ImplyCondition>,
    #[serde(rename = "then")]
    pub then_action: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ImplyCondition {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeriveRule {
    pub attributes: Vec<String>,
    pub sources: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectModifiers {
    pub import: Option<Vec<String>>,
    pub amend: Option<HashMap<String, AmendVariant>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AmendVariant {
    pub sample_table: Option<String>,
    pub subsample_table: Option<SubsampleTable>,
    pub sample_table_index: Option<String>,
    pub subsample_table_index: Option<SubsampleTableIndex>,
    pub sample_modifiers: Option<SampleModifiers>,
    pub project_modifiers: Option<ProjectModifiers>,
}

impl ProjectConfig {
    ///
    /// Apply an amendment to the project configuration
    /// given an amend variant
    ///
    pub fn with_amendment(mut self, amendment: AmendVariant) -> Self {
        if let Some(val) = amendment.sample_table {
            self.sample_table = Some(val);
        }
        if let Some(val) = amendment.subsample_table {
            self.subsample_table = Some(val);
        }
        if let Some(val) = amendment.sample_table_index {
            self.sample_table_index = Some(val);
        }
        if let Some(val) = amendment.subsample_table_index {
            self.subsample_table_index = Some(match val {
                SubsampleTableIndex::Single(s) => s,
                SubsampleTableIndex::Multiple(v) => v.into_iter().next().unwrap_or_default(),
            });
        }
        if let Some(val) = amendment.sample_modifiers {
            self.sample_modifiers = Some(val);
        }
        if let Some(val) = amendment.project_modifiers {
            self.project_modifiers = Some(val);
        }

        self
    }

    ///
    /// Merge the current project configuration with another one. This
    /// is useful for import project modifiers
    ///
    pub fn with_merge(mut self, other: ProjectConfig) -> Self {
        // the `pep_version` is a required field, so we always take the value from `other`.
        self.pep_version = other.pep_version;

        // for all optional fields, we only overwrite if `other` has a value.
        if other.sample_table.is_some() {
            self.sample_table = other.sample_table;
        }
        if other.subsample_table.is_some() {
            self.subsample_table = other.subsample_table;
        }
        if other.sample_table_index.is_some() {
            self.sample_table_index = other.sample_table_index;
        }
        if other.subsample_table_index.is_some() {
            self.subsample_table_index = other.subsample_table_index;
        }
        if other.sample_modifiers.is_some() {
            self.sample_modifiers = other.sample_modifiers;
        }
        if other.project_modifiers.is_some() {
            self.project_modifiers = other.project_modifiers;
        }

        self
    }
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            pep_version: String::from(DEFAULT_PEP_VERSION),
            name: None,
            description: None,
            sample_table: None,
            subsample_table: None,
            sample_table_index: None,
            subsample_table_index: None,
            sample_modifiers: None,
            project_modifiers: None,
            raw: None,
        }
    }
}

impl ProjectConfig {
    pub(crate) fn get_raw_config(
        &self,
        sample_table: Option<&str>,
        subsample_table: Option<Vec<&str>>,
    ) -> Option<Value> {
        if let Some(raw) = &self.raw {
            let mut config = raw.clone();
            config["name"] = Value::String(self.name.clone().unwrap_or_default());
            config["description"] = Value::String(self.description.clone().unwrap_or_default());
            config["pep_version"] = Value::String(self.pep_version.clone());

            if let Some(val) = sample_table {
                config["sample_table"] = Value::String(val.to_string());
            }
            if let Some(val) = subsample_table {
                config["subsample_table"] = serde_json::json!(val);
            }

            return Some(config);
        }
        None
    }

    ///
    /// Save config as yaml
    ///
    pub fn save_yaml<P: AsRef<Path>>(
        &self,
        path: P,
        sample_table: Option<&str>,
        subsample_table: Option<Vec<&str>>,
    ) -> Result<()> {
        let new_config = self.get_raw_config(sample_table, subsample_table);
        if let Some(config) = new_config {
            let file = std::fs::File::create(path.as_ref())?;
            serde_yaml::to_writer(file, &config)?;
        }
        Ok(())
    }
}

pub fn config_to_value(config: &ProjectConfig) -> Result<Value> {
    Ok(serde_json::to_value(&config.raw)?)
}
