use std::collections::HashMap;

use peprs_core::sample::Sample;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::project::PyProject;
use crate::utils::anyvalue_to_pyobject;

///
/// Python-exposed iterator over project samples.
///
/// Yields each sample as a Python dict of column-name to value pairs.
///
#[pyclass(name = "SamplesIter")]
pub struct PySamplesIter {
    pub project: Py<PyProject>,
    pub index: usize,
}

#[pymethods]
impl PySamplesIter {
    ///
    /// Returns the iterator itself (Python `__iter__` protocol).
    ///
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    ///
    /// Yields the next sample as a Python dict, or `None` when exhausted.
    ///
    /// # Arguments
    ///
    /// * `py` - The Python GIL token.
    ///
    /// # Returns
    ///
    /// `Some(dict)` for the next sample, or `None` at end.
    ///
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

    ///
    /// Get a sample by index (supports negative indexing).
    ///
    /// # Arguments
    ///
    /// * `py` - The Python GIL token.
    /// * `index` - Zero-based index; negative values count from the end.
    ///
    /// # Returns
    ///
    /// A Python dict for the sample at the given index.
    ///
    fn __getitem__(&self, py: Python, index: isize) -> PyResult<PyObject> {
        let project = self.project.borrow(py);
        let len = project.inner.samples.height() as isize;
        let idx = if index < 0 { index + len } else { index };
        if idx < 0 || idx >= len {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "sample index out of range",
            ));
        }
        let sample = Sample::from_dataframe_row(&project.inner.samples, idx as usize)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let map: HashMap<String, PyObject> = sample
            .iter()
            .map(|(k, v)| (k.clone(), anyvalue_to_pyobject(py, v)))
            .collect();
        Ok(map.into_pyobject(py)?.unbind().into())
    }

    ///
    /// Returns the number of samples.
    ///
    /// # Arguments
    ///
    /// * `py` - The Python GIL token.
    ///
    /// # Returns
    ///
    /// The total sample count.
    ///
    fn __len__(&self, py: Python) -> usize {
        let project = self.project.borrow(py);
        project.inner.samples.height()
    }

    ///
    /// Returns a string representation of the iterator state.
    ///
    /// # Arguments
    ///
    /// * `py` - The Python GIL token.
    ///
    /// # Returns
    ///
    /// A string like `SamplesIter(samples=N, index=M)`.
    ///
    fn __repr__(&self, py: Python) -> String {
        let project = self.project.borrow(py);
        format!(
            "SamplesIter(samples={}, index={})",
            project.inner.samples.height(),
            self.index
        )
    }
}
