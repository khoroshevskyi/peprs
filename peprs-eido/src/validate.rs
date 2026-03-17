use std::path::Path;

use peprs_core::project::Project;
use peprs_core::utils::any_value_to_json;
use polars_jsonschema_bridge::schema_to_polars_fields;
use serde_json::Value;
use tracing::warn;

use crate::error::{EidoError, MissingFile, Result, ValidationError};
use crate::schema::EidoSchema;

/// Validate samples against the schema using a structural pre-check (polars-jsonschema-bridge)
/// followed by per-sample JSON Schema validation.
pub fn validate_samples(project: &Project, schema: &EidoSchema) -> Result<()> {
    let mut errors = Vec::new();

    // Validate against imported schemas first
    for import in &schema.imports {
        if let Err(EidoError::Validation(import_errors)) = validate_samples(project, import) {
            errors.extend(import_errors);
        }
    }

    let Some(sample_schema) = &schema.sample_schema else {
        return if errors.is_empty() {
            Ok(())
        } else {
            Err(EidoError::Validation(errors))
        };
    };

    // Strategy B: Structural pre-check via polars-jsonschema-bridge
    if let Err(structural_errors) = structural_precheck(project, sample_schema) {
        errors.extend(structural_errors);
    }

    // Strategy A: Per-sample JSON Schema validation
    let validator = jsonschema::validator_for(sample_schema).map_err(|e| {
        EidoError::SchemaCompile(format!("Failed to compile sample schema: {e}"))
    })?;

    // Use Project.to_json_string() to bulk-convert samples to JSON
    let json_str = project
        .to_json_string()
        .map_err(|e| EidoError::Project(e))?;
    let samples_json: Vec<Value> = serde_json::from_str(&json_str)?;

    let sample_index = &project.sample_table_index;

    for sample_value in &samples_json {
        let sample_name = sample_value
            .get(sample_index)
            .and_then(|v| v.as_str())
            .unwrap_or("<unknown>")
            .to_string();

        for error in validator.iter_errors(sample_value) {
            errors.push(ValidationError {
                path: error.instance_path.to_string(),
                message: error.to_string(),
                sample_name: Some(sample_name.clone()),
            });
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(EidoError::Validation(errors))
    }
}

/// Validate project-level config against the schema.
pub fn validate_project(project: &Project, schema: &EidoSchema) -> Result<()> {
    let mut errors = Vec::new();

    // Validate against imported schemas first
    for import in &schema.imports {
        if let Err(EidoError::Validation(import_errors)) = validate_project(project, import) {
            errors.extend(import_errors);
        }
    }

    let Some(project_schema) = &schema.project_schema else {
        return if errors.is_empty() {
            Ok(())
        } else {
            Err(EidoError::Validation(errors))
        };
    };

    // Use ProjectConfig.raw directly — already a serde_json::Value
    let config_value = match &project.config {
        Some(cfg) => match &cfg.raw {
            Some(raw) => raw.clone(),
            None => Value::Object(serde_json::Map::new()),
        },
        None => Value::Object(serde_json::Map::new()),
    };

    let validator = jsonschema::validator_for(project_schema).map_err(|e| {
        EidoError::SchemaCompile(format!("Failed to compile project schema: {e}"))
    })?;

    for error in validator.iter_errors(&config_value) {
        errors.push(ValidationError {
            path: error.instance_path.to_string(),
            message: error.to_string(),
            sample_name: None,
        });
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(EidoError::Validation(errors))
    }
}

/// Validate that tangible file attributes point to existing files.
pub fn validate_input_files(project: &Project, schema: &EidoSchema) -> Result<()> {
    if schema.tangible.is_empty() {
        return Ok(());
    }

    let mut missing = Vec::new();
    let sample_index = &project.sample_table_index;

    // Use iter_samples() + any_value_to_json for per-sample file path checking
    for sample in project.iter_samples() {
        let sample_name = sample
            .get(sample_index)
            .map(|v| any_value_to_json(v.clone()))
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| "<unknown>".to_string());

        for attr in &schema.tangible {
            let Some(value) = sample.get(attr) else {
                missing.push(MissingFile {
                    sample_name: sample_name.clone(),
                    attribute: attr.clone(),
                    path: "<attribute not found>".to_string(),
                });
                continue;
            };

            let path_str = any_value_to_json(value.clone());
            let Some(path_str) = path_str.as_str() else {
                continue;
            };

            if path_str.is_empty() || path_str == "null" {
                missing.push(MissingFile {
                    sample_name: sample_name.clone(),
                    attribute: attr.clone(),
                    path: "<empty>".to_string(),
                });
                continue;
            }

            if !Path::new(path_str).exists() {
                missing.push(MissingFile {
                    sample_name: sample_name.clone(),
                    attribute: attr.clone(),
                    path: path_str.to_string(),
                });
            }
        }
    }

    // Check optional files — just warn, don't error
    for sample in project.iter_samples() {
        let sample_name = sample
            .get(sample_index)
            .map(|v| any_value_to_json(v.clone()))
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| "<unknown>".to_string());

        for attr in &schema.files {
            // Skip if also in tangible (already checked)
            if schema.tangible.contains(attr) {
                continue;
            }
            if let Some(value) = sample.get(attr) {
                let path_str = any_value_to_json(value.clone());
                if let Some(p) = path_str.as_str() {
                    if !p.is_empty() && p != "null" && !Path::new(p).exists() {
                        warn!(
                            sample = sample_name,
                            attribute = attr,
                            path = p,
                            "Optional file attribute points to non-existent file"
                        );
                    }
                }
            }
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(EidoError::MissingFiles(missing))
    }
}

/// Structural pre-check: compare DataFrame schema against JSON Schema using polars-jsonschema-bridge.
fn structural_precheck(
    project: &Project,
    sample_schema: &Value,
) -> std::result::Result<(), Vec<ValidationError>> {
    let Some(properties) = sample_schema.get("properties") else {
        return Ok(());
    };

    // Build a minimal JSON Schema object to pass to schema_to_polars_fields
    let schema_for_bridge = serde_json::json!({
        "type": "object",
        "properties": unwrap_any_of_properties(properties),
    });

    let expected_fields = match schema_to_polars_fields(
        &schema_for_bridge,
        polars_jsonschema_bridge::SchemaFormat::JsonSchema,
        false,
    ) {
        Ok(fields) => fields,
        Err(e) => {
            // If the bridge can't parse it, skip structural check — per-sample validation
            // will still catch issues.
            warn!(error = %e, "polars-jsonschema-bridge could not parse schema, skipping structural pre-check");
            return Ok(());
        }
    };

    let df_schema = project.samples.schema();
    let mut errors = Vec::new();

    // Check required columns exist
    if let Some(required) = sample_schema.get("required").and_then(|r| r.as_array()) {
        for req in required {
            if let Some(col_name) = req.as_str() {
                if df_schema.get(col_name).is_none() {
                    errors.push(ValidationError {
                        path: format!("/properties/{col_name}"),
                        message: format!("Required column '{col_name}' is missing from sample table"),
                        sample_name: None,
                    });
                }
            }
        }
    }

    // Check type compatibility for columns that exist in both
    // expected_fields is Vec<(field_name: String, dtype_string: String)>
    for (field_name, expected_dtype_str) in &expected_fields {
        if let Some(df_dtype) = df_schema.get(field_name.as_str()) {
            if !dtype_str_compatible(df_dtype, expected_dtype_str) {
                errors.push(ValidationError {
                    path: format!("/properties/{field_name}"),
                    message: format!(
                        "Column '{field_name}' has type {:?} but schema expects {expected_dtype_str}",
                        df_dtype,
                    ),
                    sample_name: None,
                });
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Unwrap anyOf wrappers we added during preprocessing so the bridge sees plain types.
fn unwrap_any_of_properties(properties: &Value) -> Value {
    let Some(obj) = properties.as_object() else {
        return properties.clone();
    };

    let mut result = serde_json::Map::new();
    for (key, value) in obj {
        if let Some(any_of) = value.get("anyOf").and_then(|a| a.as_array()) {
            // Take the first variant (the scalar one)
            if let Some(first) = any_of.first() {
                result.insert(key.clone(), first.clone());
                continue;
            }
        }
        result.insert(key.clone(), value.clone());
    }
    Value::Object(result)
}

/// Check if a Polars DataType is compatible with an expected type string from polars-jsonschema-bridge.
/// The bridge returns strings like "Int64", "Float64", "String", "Boolean", etc.
/// We're lenient: e.g., any integer type is compatible with "Int64", String is always compatible.
fn dtype_str_compatible(actual: &polars::prelude::DataType, expected_str: &str) -> bool {
    use polars::prelude::DataType;

    let actual_str = format!("{actual:?}");
    if actual_str == expected_str {
        return true;
    }

    // String is always compatible (CSV data is often all strings)
    if matches!(actual, DataType::String) || expected_str == "String" {
        return true;
    }

    let is_actual_int = matches!(
        actual,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    );
    let is_actual_float = matches!(actual, DataType::Float32 | DataType::Float64);
    let is_expected_int = matches!(
        expected_str,
        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64"
    );
    let is_expected_float = matches!(expected_str, "Float32" | "Float64");

    // All numeric types are compatible with each other
    if (is_actual_int || is_actual_float) && (is_expected_int || is_expected_float) {
        return true;
    }

    false
}
