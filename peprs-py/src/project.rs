use std::collections::HashMap;
use std::path::PathBuf;

use pephub_client::api::Api;
use peprs_core::consts::DEFAULT_SAMPLE_TABLE_INDEX;
use peprs_core::config::ProjectConfig;
use peprs_core::project::Project;
use polars::io::SerReader;
use polars::prelude::*;
use polars::prelude::{CsvReader, LazyFrame};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use pyo3_polars::PyDataFrame;
use pythonize::{depythonize, pythonize};
use serde_json::Value;
use std::io::Cursor;

use crate::error::PeprsCoreError;
use crate::samples::PySamplesIter;
use crate::utils::anyvalue_to_pyobject;

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
    pub fn from_dict(
        _cls: &Bound<'_, PyType>,
        pep_dictionary: &Bound<'_, PyDict>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        // 1. Config
        let config_obj = pep_dictionary
            .get_item("config")?
            .ok_or_else(|| PyValueError::new_err("Missing 'config' key"))?;
        let config_value: Value = depythonize(&config_obj)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut config: ProjectConfig = serde_json::from_value(config_value.clone())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        config.raw = Some(config_value);

        // 2. Samples
        let samples_obj = pep_dictionary
            .get_item("samples")?
            .ok_or_else(|| PyValueError::new_err("Missing 'samples' key"))?;
        let pl = py.import("polars")?;
        let py_df = pl.call_method1("DataFrame", (samples_obj,))?;
        let samples_df: DataFrame = py_df.extract::<PyDataFrame>()?.0;

        // 3. Subsamples
        let subsamples = match pep_dictionary.get_item("subsamples")? {
            Some(subs_list) => {
                let mut dfs = Vec::new();
                for sub_item in subs_list.try_iter()? {
                    let sub_item = sub_item?;
                    let py_sub_df = pl.call_method1("DataFrame", (&sub_item,))?;
                    dfs.push(py_sub_df.extract::<PyDataFrame>()?.0);
                }
                Some(dfs)
            }
            None => None,
        };

        // 4. Build
        let inner = Project::finalize_project_creation(config, samples_df, subsamples)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
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

    #[pyo3(signature = (raw=false, by_sample=true))]
    pub fn to_dict(
        &self,
        py: Python<'_>,
        raw: Option<bool>,
        by_sample: Option<bool>,
    ) -> PyResult<HashMap<String, PyObject>> {
        let raw = raw.unwrap_or(false);
        let by_sample = by_sample.unwrap_or(true);

        let mut project_dict: HashMap<String, PyObject> = HashMap::new();

        if raw == true {
            // --- config ---
            let cfg_object: Option<Value> = match &self.inner.config {
                Some(config) => config.raw.clone(),
                None => None,
            };
            let cfg_py_object = pythonize(py, &cfg_object.unwrap_or_default())
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            project_dict.insert("config".to_string(), cfg_py_object.unbind());

            // --- samples (via Python polars .to_dict) ---
            let py_df = PyDataFrame(self.inner.samples_raw.clone());
            let py_df_bound = py_df.into_pyobject(py)?;
            if by_sample == true {
                let samples_dict = py_df_bound.call_method("to_dicts", (), None)?;
                project_dict.insert("samples".to_string(), samples_dict.unbind());
            } else {
                let kwargs = PyDict::new(py);
                kwargs.set_item("as_series", false)?;
                let samples_dict = py_df_bound.call_method("to_dict", (), Some(&kwargs))?;
                project_dict.insert("samples".to_string(), samples_dict.unbind());
            }

            // --- subsamples ---
            if let Some(ref sub_dfs) = self.inner.subsamples {
                let py_list = pyo3::types::PyList::empty(py);
                for sub_df in sub_dfs {
                    let py_sub_df = PyDataFrame(sub_df.clone());
                    let py_sub_df_bound = py_sub_df.into_pyobject(py)?;
                    if by_sample == true {
                        let sub_dict = py_sub_df_bound.call_method("to_dicts", (), None)?;
                        py_list.append(sub_dict)?;
                    } else {
                        let kwargs = PyDict::new(py);
                        kwargs.set_item("as_series", false)?;
                        let sub_dict = py_sub_df_bound.call_method("to_dict", (), Some(&kwargs))?;
                        py_list.append(sub_dict)?;
                    }
                }
                project_dict.insert("subsamples".to_string(), py_list.into_any().unbind());
            }

            Ok(project_dict)
        } else {
            // --- processed samples samples (via Python polars .to_dict) ---
            let py_df = PyDataFrame(self.inner.samples.clone());
            let py_df_bound = py_df.into_pyobject(py)?;

            if by_sample == true {
                let samples_dict = py_df_bound.call_method("to_dicts", (), None)?;
                project_dict.insert("samples".to_string(), samples_dict.unbind());
            } else {
                let kwargs = PyDict::new(py);
                kwargs.set_item("as_series", false)?;
                let samples_dict = py_df_bound.call_method("to_dict", (), Some(&kwargs))?;
                project_dict.insert("samples".to_string(), samples_dict.unbind());
            }

            Ok(project_dict)
        }
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

    pub fn write_yaml(&mut self, path: PathBuf) -> PyResult<()> {
        self.inner
            .write_yaml(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    pub fn write_json(&mut self, path: PathBuf) -> PyResult<()> {
        self.inner
            .write_json(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    pub fn write_csv(&mut self, path: PathBuf) -> PyResult<()> {
        self.inner
            .write_csv(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    pub fn print_yaml(&self) -> PyResult<()> {
        self.inner
            .print_yaml()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    pub fn print_json(&self) -> PyResult<()> {
        self.inner
            .print_json()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    pub fn print_csv(&self) -> PyResult<()> {
        self.inner
            .print_csv()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    #[getter]
    pub fn get_pep_version(&self) -> PyResult<&str> {
        Ok(self.inner.get_pep_version())
    }

    #[getter]
    pub fn get_config(&self) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| match &self.inner.config {
            Some(config) => {
                let value = config.raw.clone().unwrap_or_default();
                let obj =
                    pythonize(py, &value).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(obj.into())
            }
            None => Ok(py.None()),
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

    pub fn get_sample(&self, py: Python<'_>, name: &str) -> PyResult<HashMap<String, PyObject>> {
        match self.inner.get_sample(name) {
            Ok(sample) => match sample {
                Some(s) => {
                    let map = s
                        .iter()
                        .map(|(k, v)| (k.clone(), anyvalue_to_pyobject(py, v)))
                        .collect();
                    Ok(map)
                }
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
