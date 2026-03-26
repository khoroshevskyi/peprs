use std::collections::HashMap;

use peprs_core::sample::Sample;
use pyo3::exceptions::{PyAttributeError, PyKeyError, PyRuntimeError};
use pyo3::prelude::*;

use crate::project::PyProject;
use crate::utils::anyvalue_to_pyobject;

/// Convert a peprs-core Sample into a PySample.
pub fn sample_to_pysample(py: Python, sample: &Sample) -> PySample {
    let inner: HashMap<String, PyObject> = sample
        .iter()
        .map(|(k, v)| (k.clone(), anyvalue_to_pyobject(py, v)))
        .collect();
    PySample { inner }
}

///
/// A single sample with both attribute and dict-style access.
///
#[pyclass(name = "Sample")]
pub struct PySample {
    inner: HashMap<String, PyObject>,
}

#[pymethods]
impl PySample {
    fn __getitem__(&self, py: Python, key: &str) -> PyResult<PyObject> {
        self.inner
            .get(key)
            .map(|v| v.clone_ref(py))
            .ok_or_else(|| PyKeyError::new_err(key.to_string()))
    }

    fn __getattr__(&self, py: Python, name: &str) -> PyResult<PyObject> {
        self.inner
            .get(name)
            .map(|v| v.clone_ref(py))
            .ok_or_else(|| {
                PyAttributeError::new_err(format!("'Sample' object has no attribute '{}'", name))
            })
    }

    fn __contains__(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        let items: Vec<String> = self
            .inner
            .iter()
            .map(|(k, v)| {
                let v_repr = v
                    .bind(py)
                    .repr()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|_| "?".to_string());
                format!("{}={}", k, v_repr)
            })
            .collect();
        Ok(format!("Sample({})", items.join(", ")))
    }

    fn keys(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }

    fn values(&self, py: Python) -> Vec<PyObject> {
        self.inner.values().map(|v| v.clone_ref(py)).collect()
    }

    fn items(&self, py: Python) -> Vec<(String, PyObject)> {
        self.inner
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect()
    }

    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python, key: &str, default: Option<PyObject>) -> PyObject {
        self.inner
            .get(key)
            .map(|v| v.clone_ref(py))
            .unwrap_or_else(|| default.unwrap_or_else(|| py.None()))
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let map: HashMap<String, PyObject> = self
            .inner
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect();
        Ok(map.into_pyobject(py)?.unbind().into())
    }
}

///
/// Python-exposed iterator over project samples.
///
/// Yields each sample as a `Sample` object.
///
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

    fn __next__(&mut self, py: Python) -> PyResult<Option<PySample>> {
        let project = self.project.borrow(py);

        if self.index >= project.inner.samples.height() {
            return Ok(None);
        }

        let sample_result = Sample::from_dataframe_row(&project.inner.samples, self.index);
        self.index += 1;

        match sample_result {
            Ok(sample) => Ok(Some(sample_to_pysample(py, &sample))),
            Err(e) => Err(PyRuntimeError::new_err(e.to_string())),
        }
    }

    fn __getitem__(&self, py: Python, index: isize) -> PyResult<PySample> {
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
        Ok(sample_to_pysample(py, &sample))
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
