use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use peprs_eido::error::{EidoError, MissingFile, ValidationError};

pyo3::create_exception!(peprs.eido, EidoValidationError, PyException);
pyo3::create_exception!(peprs.eido, PathAttrNotFoundError, PyException);

/// Convert an `EidoError` into the appropriate Python exception.
pub fn eido_error_to_pyerr(py: Python<'_>, err: EidoError) -> PyErr {
    match err {
        EidoError::Validation(errors) => {
            let msg = format_validation_errors(&errors);
            let py_err = EidoValidationError::new_err(msg);
            let val = py_err.value(py);
            let dict = PyDict::new(py);
            let error_dicts: Vec<_> = errors
                .iter()
                .map(|e| {
                    let d = PyDict::new(py);
                    let _ = d.set_item("path", &e.path);
                    let _ = d.set_item("message", &e.message);
                    let _ = d.set_item("sample_name", &e.sample_name);
                    d
                })
                .collect();
            let _ = dict.set_item("errors", &error_dicts);
            let _ = val.setattr("errors_by_type", dict);
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

fn format_validation_errors(errors: &[ValidationError]) -> String {
    let mut msg = format!("Validation failed with {} error(s):\n", errors.len());
    for e in errors {
        msg.push_str(&format!("  - {e}\n"));
    }
    msg
}

fn format_missing_files(files: &[MissingFile]) -> String {
    let mut msg = format!("Missing {} required file(s):\n", files.len());
    for f in files {
        msg.push_str(&format!("  - {f}\n"));
    }
    msg
}
