use std::collections::HashMap;

use pephub_client::api::Api;
use peprs_core::consts::DEFAULT_SAMPLE_TABLE_INDEX;
use peprs_core::project::Project;
use polars::io::SerReader;
use polars::prelude::*;
use polars::prelude::{CsvReader, LazyFrame};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_polars::PyDataFrame;
use pythonize::pythonize;
use std::io::Cursor;
use serde_json::Value;

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
    pub fn from_pephub(_cls: &Bound<'_, PyType>, registry: String) -> Result<Self, PeprsCoreError> {
        let pephub = Api::new().unwrap();
        let cfg = pephub.get_config(&registry).unwrap();
        let samples_csv_bytes = pephub.get_samples(&registry).unwrap();

        let csv_reader_options = CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(1000));

        let cursor = Cursor::new(samples_csv_bytes);
        let df = CsvReader::new(cursor)
            .with_options(csv_reader_options)
            .finish();

        match df {
            Ok(df) => {
                let inner = Project::from_memory(cfg, df).build()?;
                Ok(PyProject { inner })
            }
            Err(err) => Err(PeprsCoreError::from(
                peprs_core::error::Error::InvalidFormat(format!("Error reading CSV: {}", err)),
            )),
        }
    }

    #[pyo3(signature = (raw=false))]
    pub fn to_dict(&self, raw: Option<bool>) -> PyResult<HashMap<String, PyObject>> {
        let raw = raw.unwrap_or(false);

        Python::with_gil(|py| {
            let mut project_dict: HashMap<String, PyObject> = HashMap::new();

            // Extract full yaml file, to have all information from config
            let cfg_object: Option<Value> = match &self.inner.config {
                Some(config) => config.raw.clone(),
                None => None,
            };
            let cfg_py_object = pythonize(py, &cfg_object.unwrap_or_default())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            project_dict.insert("project".to_string(), cfg_py_object.unbind());

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

    #[pyo3(signature = (raw=false))]
    pub fn to_polars(&self, raw: Option<bool>) -> PyResult<PyDataFrame> {
        let raw = raw.unwrap_or(false);
        match raw {
            true => Ok(PyDataFrame(self.inner.samples_raw.clone())),
            false => Ok(PyDataFrame(self.inner.samples.clone())),
        }
    }

    #[pyo3(signature = (raw=false))]
    pub fn to_pandas(&self, py: Python<'_>, raw: Option<bool>) -> PyResult<Py<PyAny>> {
        // to_pandas method doesn't exist in rust, we need first convert to Python polars object,
        // and then using Python method convert it to Pandas
        self.to_polars(raw)?
          .into_pyobject(py)?
          .call_method0("to_pandas")
          .map(|b| b.unbind())
        }

    #[getter]
    pub fn get_pep_version(&self) -> PyResult<&str> {
        Ok(self.inner.get_pep_version())
    }

    #[getter]
    pub fn get_config(&self) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| {
            match &self.inner.config {
                Some(config) => {
                    let value = config.raw.clone().unwrap_or_default();
                    let obj = pythonize(py, &value)
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    Ok(obj.into())
                }
                None => Ok(py.None()),
            }
        })
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
