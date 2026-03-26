pub mod eido;
pub mod error;
pub mod project;
pub mod samples;
pub mod utils;

use pyo3::prelude::*;

use project::PyProject;
use samples::PySample;

/// CLI entry point callable from Python.
#[pyfunction]
fn _cli_main(py: Python<'_>) {
    let sys = py.import("sys").expect("failed to import sys");
    let argv: Vec<String> = sys
        .getattr("argv")
        .expect("failed to get sys.argv")
        .extract()
        .expect("failed to extract sys.argv");
    peprs_cli::run_with_args(argv);
}

/// A Python module implemented in Rust.
#[pymodule]
fn peprs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProject>()?;
    m.add_class::<PySample>()?;
    m.add_function(wrap_pyfunction!(_cli_main, m)?)?;
    eido::register_eido_module(m)?;
    Ok(())
}
