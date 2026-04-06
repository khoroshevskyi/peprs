use std::path::Path;

use serde_json::Value;

use crate::error::{EidoError, Result};

const SAMPLES_KEY: &str = "samples";
const TANGIBLE_KEY: &str = "tangible";
const FILES_KEY: &str = "files";
const IMPORTS_KEY: &str = "imports";

/// A loaded and preprocessed eido schema, ready for validation.
#[derive(Debug, Clone)]
pub struct EidoSchema {
    pub raw: Value,
    /// Schema for validating project-level config attributes.
    pub project_schema: Option<Value>,
    /// Schema for validating individual sample records.
    pub sample_schema: Option<Value>,
    /// Attributes that must point to files that exist on disk.
    pub tangible: Vec<String>,
    /// Attributes that may point to files (optional existence).
    pub files: Vec<String>,
    /// Imported schemas (validated before this one).
    pub imports: Vec<EidoSchema>,
}

/// Load an eido schema from a file path or URL (YAML or JSON).
pub fn load_schema(path: &str) -> Result<EidoSchema> {
    if is_url(path) {
        load_schema_from_url(path)
    } else {
        let path = Path::new(path);
        load_schema_from_path(path)
    }
}

/// Load an eido schema from a `serde_json::Value`.
pub fn load_schema_from_value(value: Value) -> Result<EidoSchema> {
    build_schema(value, None)
}

fn load_schema_from_path(path: &Path) -> Result<EidoSchema> {
    let content = std::fs::read_to_string(path)?;
    let value: Value = if path.extension().is_some_and(|ext| ext == "json") {
        serde_json::from_str(&content)?
    } else {
        // Assume YAML for .yaml, .yml, or anything else
        serde_yaml::from_str(&content)?
    };
    build_schema(value, path.parent())
}

fn is_url(path: &str) -> bool {
    path.starts_with("http://") || path.starts_with("https://")
}

#[cfg(feature = "native")]
fn fetch_url_content(url: &str) -> Result<String> {
    let mut response = ureq::get(url)
        .call()
        .map_err(|e| EidoError::SchemaLoad(format!("Failed to fetch schema from {url}: {e}")))?;
    let body = response
        .body_mut()
        .read_to_string()
        .map_err(|e| EidoError::SchemaLoad(format!("Failed to read response from {url}: {e}")))?;
    Ok(body)
}

#[cfg(not(feature = "native"))]
fn fetch_url_content(url: &str) -> Result<String> {
    Err(EidoError::SchemaLoad(format!(
        "URL schema loading requires the 'native' feature: {url}"
    )))
}

fn load_schema_from_url(url: &str) -> Result<EidoSchema> {
    let content = fetch_url_content(url)?;
    // Parse as YAML (superset of JSON, handles both)
    let value: Value = serde_yaml::from_str(&content)?;
    build_schema(value, None)
}

fn build_schema(raw: Value, base_dir: Option<&Path>) -> Result<EidoSchema> {
    // Resolve imports
    let imports = resolve_imports(&raw, base_dir)?;

    // Extract tangible and files lists
    let tangible = extract_string_array(&raw, TANGIBLE_KEY);
    let files = extract_string_array(&raw, FILES_KEY);

    // Extract and preprocess project and sample schemas
    let (project_schema, sample_schema) = extract_schemas(&raw);

    Ok(EidoSchema {
        raw,
        project_schema,
        sample_schema,
        tangible,
        files,
        imports,
    })
}

/// Recursively resolve imported schemas.
fn resolve_imports(schema: &Value, base_dir: Option<&Path>) -> Result<Vec<EidoSchema>> {
    let Some(imports) = schema.get(IMPORTS_KEY) else {
        return Ok(Vec::new());
    };

    let import_list = imports.as_array().ok_or_else(|| {
        EidoError::SchemaLoad("'imports' must be an array of schema paths".to_string())
    })?;

    let mut resolved = Vec::new();
    for import_val in import_list {
        let import_path_str = import_val.as_str().ok_or_else(|| {
            EidoError::SchemaLoad(format!("import entry must be a string, got: {import_val}"))
        })?;

        if is_url(import_path_str) {
            resolved.push(load_schema_from_url(import_path_str)?);
        } else {
            let import_path = if let Some(base) = base_dir {
                base.join(import_path_str)
            } else {
                import_path_str.into()
            };
            resolved.push(load_schema_from_path(&import_path)?);
        }
    }

    Ok(resolved)
}

/// Extract project and sample schemas from the raw eido schema.
///
/// The eido convention:
/// - `properties.samples.items` → sample schema
/// - Everything else under `properties` (minus `samples`) → project schema
fn extract_schemas(raw: &Value) -> (Option<Value>, Option<Value>) {
    let properties = match raw.get("properties") {
        Some(p) if p.is_object() => p,
        _ => return (None, None),
    };

    // Sample schema: properties.samples.items
    let sample_schema = properties
        .get(SAMPLES_KEY)
        .and_then(|s| s.get("items"))
        .cloned()
        .map(|mut schema| {
            preprocess_multi_value(&mut schema);
            wrap_as_object_schema(schema)
        });

    // Project schema: top-level schema minus the samples property
    let project_schema = {
        let mut proj = raw.clone();
        // Strip "samples" from required, since it's handled by sample validation
        if let Some(req) = proj.get_mut("required") {
            if let Some(arr) = req.as_array_mut() {
                arr.retain(|v| v.as_str() != Some(SAMPLES_KEY));
            }
        }
        if let Some(props) = proj.get_mut("properties") {
            if let Some(obj) = props.as_object_mut() {
                obj.remove(SAMPLES_KEY);
                if !obj.is_empty() { Some(proj) } else { None }
            } else {
                None
            }
        } else {
            None
        }
    };

    (project_schema, sample_schema)
}

/// Wrap a sample items schema as a standalone object schema for validation.
fn wrap_as_object_schema(items_schema: Value) -> Value {
    // If items_schema already has "type": "object", return as-is
    if items_schema.get("type").and_then(|t| t.as_str()) == Some("object") {
        return items_schema;
    }

    // Otherwise wrap it
    let mut obj = serde_json::Map::new();
    obj.insert("type".to_string(), Value::String("object".to_string()));

    if let Some(props) = items_schema.get("properties") {
        obj.insert("properties".to_string(), props.clone());
    }
    if let Some(req) = items_schema.get("required") {
        obj.insert("required".to_string(), req.clone());
    }

    Value::Object(obj)
}

/// Preprocess sample properties for automatic multi-value support.
///
/// For every scalar type (string, number, integer, boolean), wrap it in
/// an `anyOf` that also accepts an array of that type. This accommodates
/// PEP's subsample_table feature where a single attribute can become a list.
///
/// Non-required properties also accept null, since Polars stores missing
/// values as null and validation should not reject those.
fn preprocess_multi_value(schema: &mut Value) {
    let required: Vec<String> = schema
        .get("required")
        .and_then(|r| r.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let Some(properties) = schema.get_mut("properties") else {
        return;
    };
    let Some(props_obj) = properties.as_object_mut() else {
        return;
    };

    for (key, prop_value) in props_obj.iter_mut() {
        let is_required = required.contains(key);
        wrap_scalar_as_any_of(prop_value, is_required);
    }
}

/// If `prop` has a scalar `"type"`, replace it with `anyOf [original, array<original>]`.
/// Non-required properties also accept null.
fn wrap_scalar_as_any_of(prop: &mut Value, is_required: bool) {
    let type_str = match prop.get("type").and_then(|t| t.as_str()) {
        Some(t) => t.to_string(),
        None => return,
    };

    let scalar_types = ["string", "number", "integer", "boolean"];
    if !scalar_types.contains(&type_str.as_str()) {
        return;
    }

    // Already wrapped
    if prop.get("anyOf").is_some() {
        return;
    }

    let original = prop.clone();
    let array_variant = serde_json::json!({
        "type": "array",
        "items": { "type": type_str }
    });

    let mut variants = vec![original, array_variant];
    if !is_required {
        variants.push(serde_json::json!({ "type": "null" }));
    }

    *prop = serde_json::json!({
        "anyOf": variants
    });
}

fn extract_string_array(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_value_wrapping() {
        let mut schema = serde_json::json!({
            "properties": {
                "name": { "type": "string" },
                "age": { "type": "integer" },
                "scores": { "type": "array", "items": { "type": "number" } }
            },
            "required": ["name"]
        });

        preprocess_multi_value(&mut schema);
        let props = schema["properties"].as_object().unwrap();

        // string and integer should be wrapped in anyOf
        assert!(props["name"].get("anyOf").is_some());
        assert!(props["age"].get("anyOf").is_some());
        // array should not be wrapped
        assert!(props["scores"].get("anyOf").is_none());
        assert_eq!(props["scores"]["type"], "array");

        // required "name" should NOT have null variant (2 variants: scalar + array)
        let name_variants = props["name"]["anyOf"].as_array().unwrap();
        assert_eq!(name_variants.len(), 2);

        // non-required "age" SHOULD have null variant (3 variants: scalar + array + null)
        let age_variants = props["age"]["anyOf"].as_array().unwrap();
        assert_eq!(age_variants.len(), 3);
        assert_eq!(age_variants[2]["type"], "null");
    }

    #[test]
    fn test_extract_schemas() {
        let raw = serde_json::json!({
            "description": "test schema",
            "properties": {
                "samples": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sample_name": { "type": "string" },
                            "protocol": { "type": "string" }
                        },
                        "required": ["sample_name"]
                    }
                },
                "description": { "type": "string" }
            }
        });

        let (project_schema, sample_schema) = extract_schemas(&raw);

        // Sample schema should exist and contain sample_name
        let sample = sample_schema.unwrap();
        assert!(sample["properties"]["sample_name"].is_object());
        // sample_name type should be wrapped in anyOf (multi-value)
        assert!(sample["properties"]["sample_name"].get("anyOf").is_some());
        assert_eq!(sample["required"][0], "sample_name");

        // Project schema should exist and NOT contain samples
        let project = project_schema.unwrap();
        assert!(project["properties"].get("samples").is_none());
        assert!(project["properties"]["description"].is_object());
    }

    #[test]
    fn test_extract_tangible_and_files() {
        let raw = serde_json::json!({
            "tangible": ["read1", "genome_file"],
            "files": ["read1", "read2"]
        });
        assert_eq!(
            extract_string_array(&raw, "tangible"),
            vec!["read1", "genome_file"]
        );
        assert_eq!(extract_string_array(&raw, "files"), vec!["read1", "read2"]);
    }

    #[test]
    fn test_load_from_value() {
        let raw = serde_json::json!({
            "properties": {
                "samples": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sample_name": { "type": "string" }
                        },
                        "required": ["sample_name"]
                    }
                }
            },
            "tangible": ["file_path"]
        });

        let schema = load_schema_from_value(raw).unwrap();
        assert!(schema.sample_schema.is_some());
        assert_eq!(schema.tangible, vec!["file_path"]);
        assert!(schema.imports.is_empty());
    }
}
