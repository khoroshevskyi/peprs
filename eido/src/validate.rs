use std::collections::HashSet;
use std::fmt;
use std::path::Path;

use peprs_core::project::Project;
use polars_jsonschema_bridge::serialise::dataframe_to_json_schema;

use crate::error::EidoError;
use crate::schema::EidoSchema;

#[derive(Debug)]
pub enum ValidationError {
    MissingRequiredColumn {
        column: String,
    },
    TypeMismatch {
        column: String,
        expected: String,
        actual: String,
    },
    MissingFile {
        column: String,
        sample_index: usize,
        path: String,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRequiredColumn { column } => {
                write!(f, "Missing required column: '{}'", column)
            }
            Self::TypeMismatch {
                column,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "Type mismatch for column '{}': expected '{}', got '{}'",
                    column, expected, actual
                )
            }
            Self::MissingFile {
                column,
                sample_index,
                path,
            } => {
                write!(
                    f,
                    "Required file not found for column '{}' in sample {}: '{}'",
                    column, sample_index, path
                )
            }
        }
    }
}

pub struct ValidationReport {
    pub errors: Vec<ValidationError>,
}

impl ValidationReport {
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_valid() {
            write!(f, "Validation passed.")
        } else {
            writeln!(f, "Validation failed with {} error(s):", self.errors.len())?;
            for error in &self.errors {
                writeln!(f, "  - {}", error)?;
            }
            Ok(())
        }
    }
}

pub fn validate_project(
    project: &Project,
    schema: &EidoSchema,
) -> Result<ValidationReport, EidoError> {
    let mut errors = Vec::new();

    let df_columns: HashSet<&str> = project
        .samples
        .get_column_names()
        .into_iter()
        .map(|c| c.as_str())
        .collect();

    // 1. Check required columns exist in the DataFrame
    for required_col in &schema.required_sample_attrs {
        if !df_columns.contains(required_col.as_str()) {
            errors.push(ValidationError::MissingRequiredColumn {
                column: required_col.clone(),
            });
        }
    }

    // 2. Type compatibility check using polars-jsonschema-bridge
    //
    // Convert the DataFrame schema to a JSON Schema representation,
    // then compare property types against the eido schema's expected types.
    if let Some(sample_schema) = &schema.sample_schema {
        let df_json_schema =
            dataframe_to_json_schema(&project.samples, &Default::default())?;

        if let Some(expected_props) = sample_schema.get("properties").and_then(|p| p.as_object()) {
            let actual_props = df_json_schema
                .get("properties")
                .and_then(|p| p.as_object());

            for (prop_name, prop_schema) in expected_props {
                if !df_columns.contains(prop_name.as_str()) {
                    continue;
                }

                let expected_type = prop_schema
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("string");

                if let Some(actual_prop) = actual_props.and_then(|p| p.get(prop_name)) {
                    let actual_type = actual_prop
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("unknown");

                    if !json_types_compatible(expected_type, actual_type) {
                        errors.push(ValidationError::TypeMismatch {
                            column: prop_name.clone(),
                            expected: expected_type.to_string(),
                            actual: actual_type.to_string(),
                        });
                    }
                }
            }
        }
    }

    // 3. Check tangible (required) files exist
    for tangible_attr in &schema.tangible {
        if let Ok(col) = project.samples.column(tangible_attr.as_str())
            && let Ok(str_col) = col.str()
        {
            for (idx, value) in str_col.iter().enumerate() {
                if let Some(file_path) = value
                    && !Path::new(file_path).exists()
                {
                    errors.push(ValidationError::MissingFile {
                        column: tangible_attr.clone(),
                        sample_index: idx,
                        path: file_path.to_string(),
                    });
                }
            }
        }
    }

    Ok(ValidationReport { errors })
}

/// Check if two JSON Schema types are compatible.
///
/// CSV-sourced DataFrames often have all-string columns, so we treat
/// a string actual type as compatible with any expected type. Additionally,
/// integer and number are treated as interchangeable (integer is a subset
/// of number in JSON Schema).
fn json_types_compatible(expected: &str, actual: &str) -> bool {
    if expected == actual {
        return true;
    }

    // A string column in the DataFrame is compatible with any expected type
    // because CSV data is often read as strings even for numeric fields.
    if actual == "string" {
        return true;
    }

    // integer is a subset of number in JSON Schema
    if expected == "number" && actual == "integer" {
        return true;
    }

    if expected == "integer" && actual == "number" {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use peprs_core::project::Project;
    use serde_json::json;

    #[test]
    fn test_valid_basic_pep() {
        let proj = Project::from_config("../example-peps/example_basic/project_config.yaml")
            .build()
            .unwrap();

        let schema_value = json!({
            "description": "Test schema for basic PEP",
            "properties": {
                "samples": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sample_name": { "type": "string" },
                            "protocol": { "type": "string" },
                            "file": { "type": "string" }
                        },
                        "required": ["sample_name", "protocol", "file"]
                    }
                }
            },
            "required": ["samples"]
        });

        let schema = EidoSchema::from_value(schema_value).unwrap();
        let report = validate_project(&proj, &schema).unwrap();
        assert!(report.is_valid(), "Expected valid, got: {}", report);
    }

    #[test]
    fn test_missing_required_column() {
        let proj = Project::from_config("../example-peps/example_basic/project_config.yaml")
            .build()
            .unwrap();

        let schema_value = json!({
            "properties": {
                "samples": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sample_name": { "type": "string" },
                            "genome": { "type": "string" }
                        },
                        "required": ["sample_name", "genome"]
                    }
                }
            }
        });

        let schema = EidoSchema::from_value(schema_value).unwrap();
        let report = validate_project(&proj, &schema).unwrap();
        assert!(!report.is_valid());
        assert_eq!(report.errors.len(), 1);
        match &report.errors[0] {
            ValidationError::MissingRequiredColumn { column } => {
                assert_eq!(column, "genome");
            }
            other => panic!("Expected MissingRequiredColumn, got: {:?}", other),
        }
    }

    #[test]
    fn test_json_types_compatible() {
        assert!(json_types_compatible("string", "string"));
        assert!(json_types_compatible("integer", "integer"));
        assert!(json_types_compatible("number", "integer"));
        assert!(json_types_compatible("integer", "number"));
        assert!(json_types_compatible("integer", "string")); // CSV leniency
        assert!(!json_types_compatible("string", "integer"));
    }
}