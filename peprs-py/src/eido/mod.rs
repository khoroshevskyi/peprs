pub mod error;
pub mod functions;

use pyo3::prelude::*;
use pyo3::types::PyModule;

use error::{EidoValidationError, PathAttrNotFoundError};
use functions::{validate_config, validate_input_files, validate_project, validate_sample};

/// Register the `peprs.eido` submodule.
pub fn register_eido_module(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = parent.py();
    let eido = PyModule::new(py, "eido")?;

    eido.add_function(wrap_pyfunction!(validate_project, &eido)?)?;
    eido.add_function(wrap_pyfunction!(validate_sample, &eido)?)?;
    eido.add_function(wrap_pyfunction!(validate_config, &eido)?)?;
    eido.add_function(wrap_pyfunction!(validate_input_files, &eido)?)?;
    eido.add("EidoValidationError", py.get_type::<EidoValidationError>())?;
    eido.add(
        "PathAttrNotFoundError",
        py.get_type::<PathAttrNotFoundError>(),
    )?;

    // Register in sys.modules so `from peprs.eido import ...` works
    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?;
    modules.set_item("peprs.eido", &eido)?;

    parent.add_submodule(&eido)?;
    Ok(())
}
