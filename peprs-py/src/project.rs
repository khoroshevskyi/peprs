use std::collections::HashMap;

use peprs_core::consts::DEFAULT_SAMPLE_TABLE_INDEX;
use peprs_core::project::Project;
use pephub_client::api::Api;
use polars::io::SerReader;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_polars::PyDataFrame;
use polars::prelude::{CsvReader, LazyFrame};
use std::io::Cursor;
use pythonize::pythonize;

use crate::error::PeprsCoreError;
use crate::samples::PySamplesIter;

#[pyclass(name = "Project")]
pub struct PyProject {
    pub inner: Project,
}

#[pymethods]
impl PyProject {
    #[new]
    pub fn py_new(path: String) -> Result<Self, PeprsCoreError> {
        // if yaml file, assume config
        if path.ends_with(".yaml") || path.ends_with(".yml") {
            let inner = Project::from_config(&path).build()?;
            Ok(PyProject { inner })
        } else if path.ends_with(".csv") {
            let inner = Project::from_csv(&path)?.build()?;
            Ok(PyProject { inner })
        } else {
            Err(PeprsCoreError::from(
                peprs_core::error::Error::InvalidFormat(
                    "Input file must be csv or yaml".to_string(),
                ),
            ))
        }
    }

    #[classmethod]
    #[pyo3(signature = (df, sample_table_index=None))]
    pub fn from_polars(
        _cls: &Bound<'_, PyType>,
        df: PyDataFrame,
        sample_table_index: Option<String>,
    ) -> Result<Self, PeprsCoreError> {
        let sample_table_index =
            sample_table_index.unwrap_or(DEFAULT_SAMPLE_TABLE_INDEX.to_string());
        let inner = Project::from_dataframe(df.into())
            .with_sample_table_index(sample_table_index)
            .build()?;

        Ok(PyProject { inner })
    }

    #[classmethod]
    #[pyo3(signature = (registry))]
    pub fn from_pephub(
        _cls: &Bound<'_, PyType>,
        registry: String,
    ) -> Result<Self, PeprsCoreError> {
        let pephub = Api::new().unwrap();
        let cfg = pephub.get_config(&registry).unwrap();
        let samples_csv_bytes = pephub.get_samples(&registry).unwrap();

        let cursor = Cursor::new(samples_csv_bytes);
        let df = CsvReader::new(cursor)
            // .infer_schema(Some(100))
            .has_header(true)
            .finish()?;

        let inner = Project::from_memory(cfg, df).build()?;

        Ok(PyProject { inner })
    }

    #[pyo3(signature = (raw=false))]
    pub fn to_dict(&self, raw: Option<bool>) -> PyResult<HashMap<String, PyObject>> {
        let raw = raw.unwrap_or(false);

        Python::with_gil(|py| {
            let mut project_dict: HashMap<String, PyObject> = HashMap::new();
            let cfg_object = pythonize(py, &self.inner.config.clone().unwrap_or_default())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            project_dict.insert("project".to_string(), cfg_object.unbind());

            match raw {
                true => {
                    let samples: Result<Vec<Bound<'_, PyAny>>, PyErr> = self
                        .inner
                        .iter_samples_raw()
                        .map(|s| {
                            pythonize(py, &s.into_map()).map_err(|e| {
                                PyRuntimeError::new_err(format!(
                                    "Failed to convert sample to Python object: {}",
                                    e
                                ))
                            })
                        })
                        .collect();
                    let samples = samples?;
                    let samples_list = samples.into_pyobject(py)?;
                    project_dict.insert("samples".to_string(), samples_list.unbind());
                    Ok(project_dict)
                }
                false => {
                    let samples: Result<Vec<Bound<'_, PyAny>>, PyErr> = self
                        .inner
                        .iter_samples()
                        .map(|s| {
                            pythonize(py, &s.into_map()).map_err(|e| {
                                PyRuntimeError::new_err(format!(
                                    "Failed to convert sample to Python object: {}",
                                    e
                                ))
                            })
                        })
                        .collect();
                    let samples = samples?;
                    let samples_list = samples.into_pyobject(py)?;
                    project_dict.insert("samples".to_string(), samples_list.unbind());
                    Ok(project_dict)
                }
            }
        })
    }

    #[getter]
    pub fn get_pep_version(&self) -> PyResult<&str> {
        Ok(self.inner.get_pep_version())
    }

    #[getter]
    fn samples(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PySamplesIter>> {
        Py::new(
            py,
            PySamplesIter {
                project: slf,
                index: 0,
            },
        )
    }

    pub fn get_sample(&self, name: &str) -> PyResult<HashMap<String, String>> {
        match self.inner.get_sample(name) {
            Ok(sample) => match sample {
                Some(s) => Ok(s.into_map()),
                None => Err(PyValueError::new_err(format!(
                    "Sample name: '{}' not found in sample table",
                    name
                ))),
            },
            Err(err) => Err(PyRuntimeError::new_err(err.to_string())),
        }
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner.samples)
    }
}
