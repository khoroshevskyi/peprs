use std::path::Path;

use serde_json::Value;

use crate::error::EidoError;

pub struct EidoSchema {
    pub raw: Value,
    pub description: Option<String>,
    pub imports: Vec<String>,
    pub sample_schema: Option<Value>,
    pub project_schema: Option<Value>,
    pub tangible: Vec<String>,
    pub files: Vec<String>,
    pub required_sample_attrs: Vec<String>,
}

impl EidoSchema {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, EidoError> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;

        let value: Value = match path.extension().and_then(|e| e.to_str()) {
            Some("json") => serde_json::from_str(&content)?,
            _ => serde_yaml::from_str(&content)?,
        };

        Self::from_value(value)
    }

    pub fn from_value(value: Value) -> Result<Self, EidoError> {
        let description = value
            .get("description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let imports = value
            .get("imports")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let sample_schema = value.pointer("/properties/samples/items").cloned();
        let project_schema = value.pointer("/properties/config").cloned();

        let tangible = sample_schema
            .as_ref()
            .and_then(|s| s.get("tangible"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let files = sample_schema
            .as_ref()
            .and_then(|s| s.get("files"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let required_sample_attrs = sample_schema
            .as_ref()
            .and_then(|s| s.get("required"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            raw: value,
            description,
            imports,
            sample_schema,
            project_schema,
            tangible,
            files,
            required_sample_attrs,
        })
    }
}