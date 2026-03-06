use std::collections::HashMap;

use peprs_core::sample::Sample;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::project::PyProject;
use crate::utils::anyvalue_to_pyobject;

#[pyclass(name = "SamplesIter")]
pub struct PySamplesIter {
    pub project: Py<PyProject>,
    pub index: usize,
}

#[pymethods]
impl PySamplesIter {
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        // borrow the project from the Py<PyProject> handle to ensure it's not dropped
        // while the iterator is alive.
        let project = self.project.borrow(py);

        if self.index >= project.inner.samples.height() {
            return Ok(None);
        }

        // create the sample for the current row
        let sample_result = Sample::from_dataframe_row(&project.inner.samples, self.index);
        self.index += 1;

        match sample_result {
            Ok(sample) => {
                let map: HashMap<String, PyObject> = sample
                    .iter()
                    .map(|(k, v)| (k.clone(), anyvalue_to_pyobject(py, v)))
                    .collect();
                match map.into_pyobject(py) {
                    Ok(py_dict) => Ok(Some(py_dict.unbind().into())),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn __len__(&self, py: Python) -> usize {
        let project = self.project.borrow(py);
        project.inner.samples.height()
    }

    fn __repr__(&self, py: Python) -> String {
        let project = self.project.borrow(py);
        format!(
            "SamplesIter(samples={}, index={})",
            project.inner.samples.height(),
            self.index
        )
    }
}
