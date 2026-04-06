use std::io::Cursor;

use peprs_core::config::ProjectConfig;
use peprs_core::project::Project;
use peprs_eido::error::EidoError;
use peprs_eido::schema::load_schema_from_value;
use peprs_eido::validate::{validate_project, validate_samples};
use polars::io::SerReader;
use polars::prelude::JsonReader;
use serde::Serialize;
use serde_json::Value;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct WasmProject {
    inner: Project,
}

#[wasm_bindgen]
impl WasmProject {
    #[wasm_bindgen(constructor)]
    pub fn new(json_str: &str) -> Result<WasmProject, JsError> {
        let raw_value: Value =
            serde_json::from_str(json_str).map_err(|e| JsError::new(&format!("Invalid JSON: {e}")))?;

        // 1. Config
        let config_value = raw_value
            .get("config")
            .ok_or_else(|| JsError::new("Missing 'config' key"))?;
        let mut config: ProjectConfig = serde_json::from_value(config_value.clone())
            .map_err(|e| JsError::new(&format!("Invalid config: {e}")))?;
        config.raw = Some(config_value.clone());

        // 2. Samples — support both PEPHub format (sample_list) and dict format (samples)
        let samples_obj = raw_value
            .get("sample_list")
            .or_else(|| raw_value.get("samples"))
            .ok_or_else(|| JsError::new("Missing 'sample_list' or 'samples' key"))?;
        let samples_bytes = samples_obj.to_string();
        let samples_df = JsonReader::new(Cursor::new(samples_bytes.as_bytes()))
            .finish()
            .map_err(|e| JsError::new(&format!("Failed to parse samples: {e}")))?;

        // 3. Subsamples (optional) — support both formats
        let subsamples = match raw_value
            .get("subsample_list")
            .or_else(|| raw_value.get("subsamples"))
        {
            Some(Value::Array(subs_list)) => {
                let mut dfs = Vec::new();
                for sub_item in subs_list {
                    let sub_bytes = sub_item.to_string();
                    let sub_df = JsonReader::new(Cursor::new(sub_bytes.as_bytes()))
                        .finish()
                        .map_err(|e| JsError::new(&format!("Failed to parse subsample: {e}")))?;
                    dfs.push(sub_df);
                }
                Some(dfs)
            }
            Some(Value::Null) | None => None,
            _ => return Err(JsError::new("Invalid subsamples format")),
        };

        // 4. Build
        let inner = Project::from_memory(config, samples_df, subsamples)
            .build()
            .map_err(|e| JsError::new(&format!("Failed to build project: {e}")))?;

        Ok(WasmProject { inner })
    }

    pub fn get_name(&self) -> Option<String> {
        self.inner.get_name()
    }

    pub fn get_description(&self) -> Option<String> {
        self.inner.get_description()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn to_json(&self) -> Result<String, JsError> {
        self.inner
            .to_json_string()
            .map_err(|e| JsError::new(&format!("JSON serialization failed: {e}")))
    }

    pub fn to_csv(&self) -> Result<String, JsError> {
        self.inner
            .to_csv_string()
            .map_err(|e| JsError::new(&format!("CSV serialization failed: {e}")))
    }
}

#[derive(Serialize)]
struct ValidationResult {
    valid: bool,
    errors: Vec<ValidationErrorEntry>,
}

#[derive(Serialize)]
struct ValidationErrorEntry {
    path: String,
    message: String,
    sample_name: Option<String>,
}

#[wasm_bindgen]
pub fn validate(project: &WasmProject, schema_json: &str) -> Result<JsValue, JsError> {
    let schema_value: Value = serde_json::from_str(schema_json)
        .map_err(|e| JsError::new(&format!("Invalid schema JSON: {e}")))?;

    let schema = load_schema_from_value(schema_value)
        .map_err(|e| JsError::new(&format!("Failed to load schema: {e}")))?;

    let mut all_errors = Vec::new();

    // Validate project config
    if let Err(EidoError::Validation(errs)) = validate_project(&project.inner, &schema) {
        all_errors.extend(errs);
    }

    // Validate samples
    if let Err(EidoError::Validation(errs)) = validate_samples(&project.inner, &schema) {
        all_errors.extend(errs);
    }

    // Skip validate_input_files — no filesystem in WASM

    let result = if all_errors.is_empty() {
        ValidationResult {
            valid: true,
            errors: vec![],
        }
    } else {
        ValidationResult {
            valid: false,
            errors: all_errors
                .into_iter()
                .map(|e| ValidationErrorEntry {
                    path: e.path,
                    message: e.message,
                    sample_name: e.sample_name,
                })
                .collect(),
        }
    };

    serde_wasm_bindgen::to_value(&result).map_err(|e| JsError::new(&format!("Serialization error: {e}")))
}
