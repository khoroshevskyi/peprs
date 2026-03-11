use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use crate::consts::DEFAULT_PEP_VERSION;

///
/// Top-level PEP project configuration parsed from a YAML file.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectConfig {
    pub pep_version: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub sample_table: Option<String>,
    pub subsample_table: Option<SubsampleTable>,
    pub sample_table_index: Option<String>,
    pub subsample_table_index: Option<Vec<String>>,
    pub sample_modifiers: Option<SampleModifiers>,
    pub project_modifiers: Option<ProjectModifiers>,
    pub raw: Option<Value>,
}

///
/// Path(s) to subsample table(s) — either a single path or multiple.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SubsampleTable {
    Single(String),
    Multiple(Vec<String>),
}

///
/// Index column name(s) for subsample tables — single or multiple.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SubsampleTableIndex {
    Single(String),
    Multiple(Vec<String>),
}

///
/// Sample-level modifiers: remove, append, duplicate, imply, derive.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SampleModifiers {
    pub remove: Option<Vec<String>>,
    pub append: Option<HashMap<String, String>>,
    pub duplicate: Option<HashMap<String, String>>,
    pub imply: Option<Vec<ImplyRule>>,
    pub derive: Option<DeriveRule>,
}

///
/// A conditional rule: if a column matches a value, set other columns.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImplyRule {
    #[serde(rename = "if")]
    pub if_condition: HashMap<String, ImplyCondition>,
    #[serde(rename = "then")]
    pub then_action: HashMap<String, String>,
}

///
/// Condition value(s) for an imply rule — single value or list.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ImplyCondition {
    Single(String),
    Multiple(Vec<String>),
}

///
/// Rule for deriving new column values from template strings.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeriveRule {
    pub attributes: Vec<String>,
    pub sources: HashMap<String, String>,
}

///
/// Project-level modifiers: imports and amendments.
///
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectModifiers {
    pub import: Option<Vec<String>>,
    pub amend: Option<HashMap<String, AmendVariant>>,
}

///
/// A named amendment variant that can override config sections.
///
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
    /// given an amend variant.
    ///
    /// # Arguments
    ///
    /// * `amendment` - The amendment variant to apply.
    ///
    /// # Returns
    ///
    /// The modified `ProjectConfig` with amendment fields overridden.
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
                SubsampleTableIndex::Single(s) => vec![s],
                SubsampleTableIndex::Multiple(v) => v,
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
    /// Merge the current project configuration with another one.
    /// Useful for import project modifiers.
    ///
    /// # Arguments
    ///
    /// * `other` - The config to merge in. Non-`None` fields override `self`.
    ///
    /// # Returns
    ///
    /// The merged `ProjectConfig`.
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
    pub fn get_raw_config(
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
    /// Save the config as a YAML file.
    ///
    /// # Arguments
    ///
    /// * `path` - Destination file path.
    /// * `sample_table` - Optional sample table filename to embed in config.
    /// * `subsample_table` - Optional subsample table filenames to embed in config.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if serialization/IO fails.
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

///
/// Converts the raw config to a JSON [`Value`].
///
/// # Arguments
///
/// * `config` - The project config to convert.
///
/// # Returns
///
/// The raw config as a JSON `Value`, or an error if serialization fails.
///
pub fn config_to_value(config: &ProjectConfig) -> Result<Value> {
    Ok(serde_json::to_value(&config.raw)?)
}
