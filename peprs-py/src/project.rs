use pyo3::prelude::*;

use peprs_core::project::Project;

use crate::error::PeprsCoreError;

#[pyclass(name = "Project")]
pub struct PyProject {
    pub inner: Project
}

#[pymethods]
impl PyProject {
    #[new]
    pub fn py_new(path: String) -> Result<Self, PeprsCoreError> {
        // if yaml file, assume config
        if path.ends_with(".yaml") || path.ends_with(".yml") {
            let inner = Project::from_config(&path)?;
            Ok(PyProject { inner })
        } else if path.ends_with(".csv") {
            let inner = Project::from_csv(&path)?;
            Ok(PyProject { inner })
        } else {
            Err(PeprsCoreError::from(peprs_core::error::Error::InvalidFormat("Input file must be csv or yaml".to_string())))
        }
    }

    #[getter]
    pub fn get_pep_version(&self) -> PyResult<&str> {
        Ok(self.inner.get_pep_version())
    }
}