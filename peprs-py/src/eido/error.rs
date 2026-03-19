use std::collections::BTreeMap;

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use peprs_eido::error::{EidoError, MissingFile, ValidationError};

pyo3::create_exception!(peprs.eido, EidoValidationError, PyException);
pyo3::create_exception!(peprs.eido, PathAttrNotFoundError, PyException);

/// Convert an `EidoError` into the appropriate Python exception.
pub fn eido_error_to_pyerr(py: Python<'_>, err: EidoError) -> PyErr {
    match err {
        EidoError::Validation(errors) => {
            let grouped = build_grouped_errors(py, &errors);
            let msg = format_grouped_summary(&errors);
            let py_err = EidoValidationError::new_err(msg);
            let val = py_err.value(py);
            let _ = val.setattr("errors_by_type", grouped);
            py_err
        }
        EidoError::MissingFiles(files) => {
            let msg = format_missing_files(&files);
            let py_err = PathAttrNotFoundError::new_err(msg);
            let val = py_err.value(py);
            let file_dicts: Vec<_> = files
                .iter()
                .map(|f| {
                    let d = PyDict::new(py);
                    let _ = d.set_item("sample_name", &f.sample_name);
                    let _ = d.set_item("attribute", &f.attribute);
                    let _ = d.set_item("path", &f.path);
                    d
                })
                .collect();
            let _ = val.setattr("missing_files", &file_dicts);
            py_err
        }
        other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
    }
}

fn classify_error(error: &ValidationError) -> &'static str {
    if error.message.starts_with("type mismatch") {
        "type_mismatch"
    } else if error.message.ends_with("is a required property") {
        "missing_required"
    } else if error.message.contains("is missing from sample table") {
        "missing_column"
    } else {
        "validation"
    }
}

/// Group key: (path, message). Value: list of sample names.
struct GroupedEntry {
    path: String,
    message: String,
    sample_names: Vec<String>,
}

fn build_grouped_errors<'py>(
    py: Python<'py>,
    errors: &[ValidationError],
) -> Bound<'py, PyDict> {
    // category -> [(path, message)] -> sample_names
    let mut categories: BTreeMap<&str, Vec<GroupedEntry>> = BTreeMap::new();

    for error in errors {
        let category = classify_error(error);
        let entries = categories.entry(category).or_default();

        // Find existing entry with same (path, message)
        if let Some(entry) = entries
            .iter_mut()
            .find(|e| e.path == error.path && e.message == error.message)
        {
            if let Some(name) = &error.sample_name {
                entry.sample_names.push(name.clone());
            }
        } else {
            entries.push(GroupedEntry {
                path: error.path.clone(),
                message: error.message.clone(),
                sample_names: error.sample_name.iter().cloned().collect(),
            });
        }
    }

    let result = PyDict::new(py);
    for (category, entries) in &categories {
        let py_entries: Vec<_> = entries
            .iter()
            .map(|entry| {
                let d = PyDict::new(py);
                let _ = d.set_item("path", &entry.path);
                let _ = d.set_item("message", &entry.message);
                if !entry.sample_names.is_empty() {
                    let names = PyList::new(py, &entry.sample_names).unwrap();
                    let _ = d.set_item("sample_names", names);
                }
                d
            })
            .collect();
        let _ = result.set_item(*category, &py_entries);
    }
    result
}

fn format_grouped_summary(errors: &[ValidationError]) -> String {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for error in errors {
        *counts.entry(classify_error(error)).or_default() += 1;
    }
    let parts: Vec<String> = counts
        .iter()
        .map(|(cat, n)| format!("{n} {cat}"))
        .collect();
    format!("Validation failed: {}", parts.join(", "))
}

fn format_missing_files(files: &[MissingFile]) -> String {
    let mut msg = format!("Missing {} required file(s):\n", files.len());
    for f in files {
        msg.push_str(&format!("  - {f}\n"));
    }
    msg
}
