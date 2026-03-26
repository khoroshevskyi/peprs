use std::collections::HashMap;
use std::path::PathBuf;

use pephub_client::api::Api;
use peprs_core::config::ProjectConfig;
use peprs_core::consts::DEFAULT_SAMPLE_TABLE_INDEX;
use peprs_core::project::Project;
use polars::io::SerReader;
use polars::prelude::JsonReader;
use polars::prelude::*;
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

///
/// Python-exposed PEP project, wrapping a [`Project`] from peprs-core.
///
#[pyclass(name = "Project")]
pub struct PyProject {
    pub inner: Project,
}

#[pymethods]
impl PyProject {
    ///
    /// Create a new Project from a YAML config or CSV file path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to a `.yaml`/`.yml` config or `.csv` sample table.
    ///
    /// # Returns
    ///
    /// A new `PyProject`, or an error if the file format is unsupported.
    ///
    #[new]
    #[pyo3(signature = (path, amendments=None, sample_table_index=None, subsample_table_index=None))]
    pub fn py_new(
        path: String,
        amendments: Option<Vec<String>>,
        sample_table_index: Option<String>,
        subsample_table_index: Option<Vec<String>>,
    ) -> Result<Self, PeprsCoreError> {
        let mut builder = if path.ends_with(".yaml") || path.ends_with(".yml") {
            Project::from_config(&path)
        } else if path.ends_with(".csv") {
            Project::from_csv(&path)?
        } else {
            return Err(PeprsCoreError::from(
                peprs_core::error::Error::InvalidFormat(
                    "Input file must be csv or yaml".to_string(),
                ),
            ));
        };

        if let Some(ref amendments) = amendments {
            builder = builder.with_amendments(amendments);
        }
        if let Some(sample_table_index) = sample_table_index {
            builder = builder.with_sample_table_index(sample_table_index);
        }
        if let Some(ref subsample_table_index) = subsample_table_index {
            builder = builder.with_subsample_table_index(subsample_table_index);
        }

        let inner = builder.build()?;
        Ok(PyProject { inner })
    }

    ///
    /// Create a Project from a Polars DataFrame.
    ///
    /// # Arguments
    ///
    /// * `df` - A Polars DataFrame with sample data.
    /// * `sample_table_index` - Optional column name for the sample index (default: `"sample_name"`).
    ///
    /// # Returns
    ///
    /// A new `PyProject`.
    ///
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

    ///
    /// Create a Project from a Pandas DataFrame.
    ///
    /// # Arguments
    ///
    /// * `df` - A Pandas DataFrame with sample data.
    /// * `sample_table_index` - Optional column name for the sample index (default: `"sample_name"`).
    ///
    /// # Returns
    ///
    /// A new `PyProject`.
    ///
    #[classmethod]
    #[pyo3(signature = (df, sample_table_index=None))]
    pub fn from_pandas(
        _cls: &Bound<'_, PyType>,
        df: &Bound<'_, PyAny>,
        sample_table_index: Option<String>,
        py: Python<'_>,
    ) -> Result<Self, PeprsCoreError> {
        let pl = py
            .import("polars")
            .map_err(|e| peprs_core::error::Error::InvalidFormat(e.to_string()))?;
        let polars_df: DataFrame = pl
            .call_method1("from_pandas", (df,))
            .map_err(|e| peprs_core::error::Error::InvalidFormat(e.to_string()))?
            .extract::<PyDataFrame>()
            .map_err(|e| peprs_core::error::Error::InvalidFormat(e.to_string()))?
            .0;

        let sample_table_index =
            sample_table_index.unwrap_or(DEFAULT_SAMPLE_TABLE_INDEX.to_string());
        let inner = Project::from_dataframe(polars_df)
            .with_sample_table_index(sample_table_index)
            .build()?;

        Ok(PyProject { inner })
    }

    ///
    /// Create a Project from a Python dict with `config`, `samples`, and optional `subsamples` keys.
    ///
    /// # Arguments
    ///
    /// * `pep_dictionary` - A Python dict with keys `"config"`, `"samples"`, and optionally `"subsamples"`.
    /// * `py` - The Python GIL token.
    ///
    /// # Returns
    ///
    /// A new `PyProject`.
    ///
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
        let config_value: Value =
            depythonize(&config_obj).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
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
        let inner = Project::from_memory(config, samples_df, subsamples)
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyProject { inner })
    }

    ///
    /// Create a Project from a PepHub registry path.
    ///
    /// # Arguments
    ///
    /// * `registry` - The PepHub registry path (e.g. `"namespace/name:tag"`).
    ///
    /// # Returns
    ///
    /// A new `PyProject` loaded from PepHub.
    ///
    #[classmethod]
    #[pyo3(signature = (registry))]
    pub fn from_pephub(_cls: &Bound<'_, PyType>, registry: String) -> Result<Self, PeprsCoreError> {
        let pephub = Api::new().map_err(|e| {
            peprs_core::error::Error::Processing(format!("Failed to create PepHub client: {}", e))
        })?;

        // Fetch the full project JSON (config + samples + subsamples)
        let raw = pephub.get_raw(registry.as_str()).map_err(|e| {
            peprs_core::error::Error::Processing(format!("Failed to fetch from PepHub: {}", e))
        })?;

        let raw_value: Value =
            serde_json::from_str(&raw).map_err(peprs_core::error::Error::Json)?;

        // 1. Config
        let config_value = raw_value
            .get("config")
            .ok_or_else(|| peprs_core::error::Error::invalid_format("Missing 'config' key"))?;
        let mut config: ProjectConfig =
            serde_json::from_value(config_value.clone()).map_err(peprs_core::error::Error::Json)?;
        config.raw = Some(config_value.clone());

        // 2. Samples
        let samples_obj = raw_value
            .get("sample_list")
            .ok_or_else(|| peprs_core::error::Error::invalid_format("Missing 'sample_list' key"))?;
        let samples_bytes = samples_obj.to_string();
        let samples_df = JsonReader::new(Cursor::new(samples_bytes.as_bytes()))
            .finish()
            .map_err(peprs_core::error::Error::Polars)?;

        // 3. Subsamples
        let subsamples = match raw_value.get("subsample_list") {
            Some(Value::Array(subs_list)) => {
                let mut dfs = Vec::new();
                for sub_item in subs_list {
                    let sub_bytes = sub_item.to_string();
                    let sub_df = JsonReader::new(Cursor::new(sub_bytes.as_bytes()))
                        .finish()
                        .map_err(peprs_core::error::Error::Polars)?;
                    dfs.push(sub_df);
                }
                Some(dfs)
            }
            Some(Value::Null) | None => None,
            _ => {
                return Err(
                    peprs_core::error::Error::invalid_format("Invalid 'subsamples' format").into(),
                )
            }
        };

        // Build the project
        let inner = Project::from_memory(config, samples_df, subsamples).build()?;
        Ok(PyProject { inner })
    }

    ///
    /// Convert the project to a Python dict.
    ///
    /// # Arguments
    ///
    /// * `raw` - If `true`, include raw config/samples/subsamples; otherwise processed samples only.
    /// * `by_sample` - If `true`, samples are a list of row-dicts; if `false`, a column-dict.
    ///
    /// # Returns
    ///
    /// A Python dict with `"config"`, `"samples"`, and optionally `"subsamples"` keys.
    ///
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

    ///
    /// Return the samples as a Polars DataFrame.
    ///
    /// # Arguments
    ///
    /// * `raw` - If `true`, return raw (unprocessed) samples; otherwise processed.
    ///
    /// # Returns
    ///
    /// A Polars `DataFrame`.
    ///
    #[pyo3(signature = (raw=false))]
    pub fn to_polars(&self, raw: Option<bool>) -> PyResult<PyDataFrame> {
        let raw = raw.unwrap_or(false);
        match raw {
            true => Ok(PyDataFrame(self.inner.samples_raw.clone())),
            false => Ok(PyDataFrame(self.inner.samples.clone())),
        }
    }

    ///
    /// Return the samples as a Pandas DataFrame.
    ///
    /// # Arguments
    ///
    /// * `raw` - If `true`, return raw (unprocessed) samples; otherwise processed.
    ///
    /// # Returns
    ///
    /// A Pandas `DataFrame`.
    ///
    #[pyo3(signature = (raw=false))]
    pub fn to_pandas(&self, py: Python<'_>, raw: Option<bool>) -> PyResult<Py<PyAny>> {
        // to_pandas method doesn't exist in rust, we need first convert to Python polars object,
        // and then using Python method convert it to Pandas
        self.to_polars(raw)?
            .into_pyobject(py)?
            .call_method0("to_pandas")
            .map(|b| b.unbind())
    }

    ///
    /// Write processed samples to a YAML file.
    ///
    /// # Arguments
    ///
    /// * `path` - Destination file path.
    ///
    pub fn write_yaml(&mut self, path: PathBuf) -> PyResult<()> {
        self.inner
            .write_yaml(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    ///
    /// Write processed samples to a JSON file.
    ///
    /// # Arguments
    ///
    /// * `path` - Destination file path.
    ///
    pub fn write_json(&mut self, path: PathBuf) -> PyResult<()> {
        self.inner
            .write_json(path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    ///
    /// Write processed samples to a CSV file.
    ///
    /// Falls back to Pandas export if the Polars CSV writer fails (e.g. list columns).
    ///
    /// # Arguments
    ///
    /// * `path` - Destination file path.
    /// * `py` - The Python GIL token.
    ///
    pub fn write_csv(&mut self, path: PathBuf, py: Python<'_>) -> PyResult<()> {
        match self.inner.write_csv(path.clone()) {
            Ok(()) => Ok(()),
            Err(_) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("index", false)?;
                let path_str = path.to_string_lossy().to_string();
                self.to_polars(Some(false))?
                    .into_pyobject(py)?
                    .call_method0("to_pandas")?
                    .call_method("to_csv", (path_str,), Some(&kwargs))?;
                Ok(())
            }
        }
    }

    ///
    /// Write the raw project (config, samples, subsamples) to disk.
    ///
    /// # Arguments
    ///
    /// * `path` - Destination path (folder or zip file).
    /// * `zipped` - If `true`, write as a zip archive; otherwise as a folder.
    ///
    #[pyo3(signature = (path, zipped=false))]
    pub fn write_raw(&mut self, path: PathBuf, zipped: bool) -> PyResult<()> {
        self.inner
            .write_raw(path, Some(zipped))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    ///
    /// Return processed samples as a YAML string.
    ///
    pub fn to_yaml_string(&self) -> PyResult<String> {
        self.inner
            .to_yaml_string()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    ///
    /// Return processed samples as a JSON string.
    ///
    pub fn to_json_string(&self) -> PyResult<String> {
        self.inner
            .to_json_string()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    ///
    /// Return processed samples as a CSV string.
    ///
    /// Falls back to Pandas export if the Polars CSV writer fails.
    ///
    /// # Arguments
    ///
    /// * `py` - The Python GIL token.
    ///
    pub fn to_csv_string(&self, py: Python<'_>) -> PyResult<String> {
        match self.inner.to_csv_string() {
            Ok(csv) => Ok(csv),
            Err(_) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("index", false)?;
                let csv_string = self
                    .to_polars(Some(false))?
                    .into_pyobject(py)?
                    .call_method0("to_pandas")?
                    .call_method("to_csv", (py.None(),), Some(&kwargs))?
                    .extract::<String>()?;
                Ok(csv_string)
            }
        }
    }

    ///
    /// Get the PEP version string.
    ///
    #[getter]
    pub fn get_pep_version(&self) -> PyResult<&str> {
        Ok(self.inner.get_pep_version())
    }

    ///
    /// Get the project description, or `None` if not set.
    ///
    #[getter]
    pub fn get_description(&self) -> PyResult<Option<String>> {
        Ok(self.inner.get_description())
    }

    ///
    /// Get the project name, or `None` if not set.
    ///
    #[getter]
    pub fn get_name(&self) -> PyResult<Option<String>> {
        Ok(self.inner.get_name())
    }

    ///
    /// Set the project description.
    ///
    /// # Arguments
    ///
    /// * `description` - The new description, or `None` to clear it.
    ///
    #[setter]
    pub fn set_description(&mut self, description: Option<String>) {
        self.inner.set_description(description);
    }

    ///
    /// Set the project name.
    ///
    /// # Arguments
    ///
    /// * `name` - The new name, or `None` to clear it.
    ///
    #[setter]
    pub fn set_name(&mut self, name: Option<String>) {
        self.inner.set_name(name);
    }

    ///
    /// Get the raw project config as a Python dict.
    ///
    /// # Returns
    ///
    /// A Python dict of the raw config, or `None` if no config exists.
    ///
    #[getter]
    pub fn get_config(&self) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| match &self.inner.config {
            Some(config) => {
                let value = config.get_raw_config(None, None);
                let obj =
                    pythonize(py, &value).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(obj.into())
            }
            None => Ok(py.None()),
        })
    }

    ///
    /// Get a [`PySamplesIter`] over the project's processed samples.
    ///
    /// # Returns
    ///
    /// An iterator yielding each sample as a Python dict.
    ///
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

    ///
    /// Look up a single sample by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The sample name to look up.
    ///
    /// # Returns
    ///
    /// A Python dict of column-name to value pairs for the matching sample.
    ///
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

    ///
    /// Returns a boolean value if project sample table is the same.
    /// This function DOES NOT check if configs are the same
    ///
    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner.samples)
    }

    ///
    /// Return the number of samples in the PEP.
    ///
    fn __len__(&self) -> PyResult<usize> {
        Ok(self.inner.len())
    }

    ///
    /// Return the number of samples in the PEP.
    ///
    fn len(&self) -> PyResult<usize> {
        Ok(self.inner.len())
    }
}
