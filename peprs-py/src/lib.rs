pub mod error;
pub mod project;
pub mod samples;
pub mod utils;

use pyo3::prelude::*;

use project::PyProject;

/// A Python module implemented in Rust.
#[pymodule]
fn peprs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyProject>()?;
    Ok(())
}
