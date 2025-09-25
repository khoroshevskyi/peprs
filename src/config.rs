use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub pep_version: String,
    pub sample_table: Option<String>,
    pub subsample_table: Option<SubsampleTable>,
    pub sample_table_index: Option<String>,
    pub subsample_table_index: Option<SubsampleTableIndex>,
    pub sample_modifiers: Option<SampleModifiers>,
    pub project_modifiers: Option<ProjectModifiers>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubsampleTable {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubsampleTableIndex {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SampleModifiers {
    pub remove: Option<Vec<String>>,
    pub append: Option<HashMap<String, String>>,
    pub duplicate: Option<HashMap<String, String>>,
    pub imply: Option<Vec<ImplyRule>>,
    pub derive: Option<DeriveRule>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImplyRule {
    #[serde(rename = "if")]
    pub if_condition: HashMap<String, ImplyCondition>,
    #[serde(rename = "then")]
    pub then_action: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ImplyCondition {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeriveRule {
    pub attributes: Vec<String>,
    pub sources: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectModifiers {
    pub import: Option<Vec<String>>,
    pub amend: Option<HashMap<String, AmendVariant>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AmendVariant {
    pub sample_table: Option<String>,
    pub subsample_table: Option<SubsampleTable>,
    pub sample_table_index: Option<String>,
    pub subsample_table_index: Option<SubsampleTableIndex>,
    pub sample_modifiers: Option<SampleModifiers>,
    pub project_modifiers: Option<ProjectModifiers>,
}
