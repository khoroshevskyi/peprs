use pyo3::prelude::*;
use serde_json::Value;

use peprs_core::utils::any_value_to_json;
use peprs_eido::schema::{load_schema, load_schema_from_value, EidoSchema};

use crate::eido::error::eido_error_to_pyerr;
use crate::project::PyProject;

/// Resolve a schema argument: accepts a file path (str) or a pre-loaded dict.
fn resolve_schema(schema: &Bound<'_, PyAny>) -> PyResult<EidoSchema> {
    // Try string (file path) first
    if let Ok(path) = schema.extract::<String>() {
        return load_schema(&path).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to load schema: {e}"))
        });
    }

    // Try dict (pre-loaded schema)
    let value: Value = pythonize::depythonize(schema).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid schema dict: {e}"))
    })?;

    load_schema_from_value(value).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Failed to parse schema: {e}"))
    })
}

/// Validate a PEP project against an eido schema (both config and samples).
#[pyfunction]
#[pyo3(signature = (project, schema))]
pub fn validate_project(
    py: Python<'_>,
    project: &PyProject,
    schema: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let schema = resolve_schema(schema)?;
    peprs_eido::validate_with_schema(&project.inner, &schema)
        .map_err(|e| eido_error_to_pyerr(py, e))
}

/// Validate a single sample by name against an eido schema.
#[pyfunction]
#[pyo3(signature = (project, sample_name, schema))]
pub fn validate_sample(
    py: Python<'_>,
    project: &PyProject,
    sample_name: &str,
    schema: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let schema = resolve_schema(schema)?;

    // Get the sample and convert to JSON Value
    let sample = project
        .inner
        .get_sample(sample_name)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Sample '{sample_name}' not found in sample table"
            ))
        })?;

    let json_map: serde_json::Map<String, Value> = sample
        .iter()
        .map(|(k, v)| (k.clone(), any_value_to_json(v.clone())))
        .collect();
    let sample_json = Value::Object(json_map);

    peprs_eido::validate_single_sample(&sample_json, &schema, sample_name)
        .map_err(|e| eido_error_to_pyerr(py, e))
}

/// Validate only the project-level config against an eido schema.
#[pyfunction]
#[pyo3(signature = (project, schema))]
pub fn validate_config(
    py: Python<'_>,
    project: &PyProject,
    schema: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let schema = resolve_schema(schema)?;
    peprs_eido::validate::validate_project(&project.inner, &schema)
        .map_err(|e| eido_error_to_pyerr(py, e))
}

/// Validate that tangible file attributes point to existing files.
#[pyfunction]
#[pyo3(signature = (project, schema))]
pub fn validate_input_files(
    py: Python<'_>,
    project: &PyProject,
    schema: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let schema = resolve_schema(schema)?;
    peprs_eido::validate::validate_input_files(&project.inner, &schema)
        .map_err(|e| eido_error_to_pyerr(py, e))
}
