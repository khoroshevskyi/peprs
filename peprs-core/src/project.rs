use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use polars::prelude::*;
use serde_json;
use serde_yaml;
use serde_yaml::Value as YValue;

use crate::config::{ImplyCondition, ProjectConfig};
use crate::consts::{self, DEFAULT_SAMPLE_TABLE_INDEX};
use crate::error::Error;
use crate::sample::{Sample, SamplesIter};
use crate::utils::build_derive_template_expr;
#[cfg(feature = "wdl")]
use crate::wdl::WdlInputParsingOptions;
#[cfg(feature = "wdl")]
use crate::wdl::get_inputs_from_wdl;
#[cfg(feature = "wdl")]
use serde_json::Value;

#[cfg(feature = "wdl")]
fn any_value_to_json(any_value: AnyValue) -> Value {
    match any_value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => Value::Bool(b),
        AnyValue::String(s) => Value::String(s.to_string()),
        AnyValue::Float32(f) => Value::from(f),
        AnyValue::Float64(f) => Value::from(f),
        AnyValue::Int8(i) => Value::from(i),
        AnyValue::Int16(i) => Value::from(i),
        AnyValue::Int32(i) => Value::from(i),
        AnyValue::Int64(i) => Value::from(i),
        AnyValue::UInt8(u) => Value::from(u),
        AnyValue::UInt16(u) => Value::from(u),
        AnyValue::UInt32(u) => Value::from(u),
        AnyValue::UInt64(u) => Value::from(u),
        av => Value::String(av.to_string()),
    }
}

// Define the possible sources for a project
#[allow(clippy::large_enum_variant)]
enum ProjectSource {
    Path(PathBuf),
    DataFrame(DataFrame),
    InMemory {
        config: ProjectConfig,
        samples: DataFrame,
    },
}

pub struct ProjectBuilder {
    source: ProjectSource,
    amendments: Option<Vec<String>>,
    sample_table_index: Option<String>,
}

pub struct Project {
    pub config: Option<ProjectConfig>,
    pub samples: DataFrame,
    pub samples_raw: DataFrame,
    subsamples: Option<Vec<DataFrame>>,
    pub sample_table_index: String,
}

impl ProjectBuilder {
    ///
    /// Specify a list of amendments to activate when building the project.
    ///
    pub fn with_amendments(mut self, amendments: &[String]) -> Self {
        self.amendments = Some(amendments.to_vec());
        self
    }

    ///
    /// Specify a custom sample table index column name.
    ///
    pub fn with_sample_table_index(mut self, index: String) -> Self {
        self.sample_table_index = Some(index);
        self
    }

    /// Construct the `Project` using the specified configuration.
    ///
    /// This is the final step that will perform file I/O and parsing.
    pub fn build(self) -> Result<Project, Error> {
        match self.source {
            ProjectSource::Path(path) => {
                let config = Project::load_project_config(&path, self.amendments.as_deref())?;
                let config_dir = path.parent().unwrap_or_else(|| Path::new("."));

                // honor the priority for sample_table_index:
                // 1. Value from builder (highest)
                // 2. Value from config file
                // 3. Default
                let final_index = self
                    .sample_table_index
                    .or(config.sample_table_index.clone())
                    .unwrap_or_else(|| DEFAULT_SAMPLE_TABLE_INDEX.to_string());

                let mut final_config = config;
                final_config.sample_table_index = Some(final_index);

                Project::new_from_parsed_config(final_config, config_dir)
            }
            ProjectSource::DataFrame(df) => {
                let index = self
                    .sample_table_index
                    .unwrap_or_else(|| DEFAULT_SAMPLE_TABLE_INDEX.to_string());

                Ok(Project {
                    config: None,
                    samples: df.clone(),
                    samples_raw: df,
                    subsamples: None,
                    sample_table_index: index,
                })
            }
            ProjectSource::InMemory {
                mut config,
                samples,
            } => {
                // honor the sample_table_index from the builder, if provided
                if let Some(idx) = self.sample_table_index {
                    config.sample_table_index = Some(idx);
                }
                // call the shared logic
                Project::finalize_project_creation(config, samples)
            }
        }
    }
}

impl Project {
    /// Create a project from a CSV file.
    /// This will load the CSV into a DataFrame and return a builder.
    pub fn from_csv<P: AsRef<Path>>(path: P) -> Result<ProjectBuilder, Error> {
        let df = LazyCsvReader::new(PlPath::new(path.as_ref().to_str().unwrap()))
            .with_has_header(true)
            .with_infer_schema_length(Some(10_000))
            .finish()?
            .with_column(col(DEFAULT_SAMPLE_TABLE_INDEX).cast(DataType::String))
            .collect()?;

        Ok(ProjectBuilder {
            source: ProjectSource::DataFrame(df),
            amendments: None,
            sample_table_index: None,
        })
    }

    ///
    /// Create a project from a YAML configuration file.
    /// This returns a builder that will process the file upon `.build()`.
    pub fn from_config<P: AsRef<Path>>(path: P) -> ProjectBuilder {
        ProjectBuilder {
            source: ProjectSource::Path(path.as_ref().to_path_buf()),
            amendments: None,
            sample_table_index: None,
        }
    }

    ///
    /// Create a project from an in-memory Polars DataFrame.
    pub fn from_dataframe(df: DataFrame) -> ProjectBuilder {
        ProjectBuilder {
            source: ProjectSource::DataFrame(df),
            amendments: None,
            sample_table_index: None,
        }
    }

    pub fn from_memory(config: ProjectConfig, samples: DataFrame) -> ProjectBuilder {
        ProjectBuilder {
            source: ProjectSource::InMemory { config, samples },
            amendments: None,
            sample_table_index: None,
        }
    }

    ///
    /// Get the pep version in the config if it exists
    /// otherwise return the default version
    ///
    pub fn get_pep_version(&self) -> &str {
        self.config
            .as_ref()
            .map_or(consts::DEFAULT_PEP_VERSION, |cfg| &cfg.pep_version)
    }

    ///
    /// Get the number of samples in the project
    ///
    pub fn len(&self) -> usize {
        self.samples.height()
    }

    ///
    /// Check if the project contains no samples
    ///
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    ///
    /// Retrieve a sample by its sample name.
    ///
    pub fn get_sample<'a>(&'a self, name: &str) -> PolarsResult<Option<Sample<'a>>> {
        let mask = self
            .samples
            .column(&self.sample_table_index)?
            .as_series()
            .ok_or_else(|| {
                PolarsError::ColumnNotFound(
                    format!(
                        "Sample table index column '{}' not found",
                        self.sample_table_index
                    )
                    .into(),
                )
            })?
            .equal(name)?;

        let idx: Vec<usize> = mask
            .into_iter()
            .enumerate()
            .filter_map(|(i, v)| (v == Some(true)).then_some(i))
            .collect();

        match idx.len() {
            0 => Ok(None), // No samples found
            1 => Ok(Some(Sample::from_dataframe_row(
                &self.samples,
                idx.first().unwrap().clone(),
            )?)),
            _ => {
                println!("More then one sample found. Returning first sample");
                // Ok(Some(found_samples_count))
                Ok(None)
            }
        }
    }

    ///
    /// The main entry point for loading the project configuration
    ///
    pub fn load_project_config(
        path: impl AsRef<Path>,
        amendments: Option<&[String]>,
    ) -> Result<ProjectConfig, Error> {
        let path = path.as_ref();
        let config_file = File::open(path)?;
        let reader = BufReader::new(config_file);
        let raw_config: YValue = serde_yaml::from_reader(reader)?;
        let mut config: ProjectConfig = serde_yaml::from_value(raw_config.clone())?;
        config.raw = match serde_json::to_value(raw_config) {
            Ok(raw) => Some(raw),
            Err(_) => None,
        };

        // start the recursive parsing process, passing the parent dir for path resolution
        let parent_dir = path.parent().unwrap_or_else(|| Path::new(""));

        Self::parse_and_apply_project_modifiers(config, parent_dir, amendments)
    }

    ///
    /// Recursive helper function that consumes and returns a config
    /// after applying and potential project modifiers
    ///
    fn parse_and_apply_project_modifiers(
        mut config: ProjectConfig,
        base_path: &Path,
        amendments_to_activate: Option<&[String]>,
    ) -> Result<ProjectConfig, Error> {
        // take the modifiers out, leaving None in their place to avoid re-processing.
        if let Some(modifiers) = config.project_modifiers.take() {
            // handle imports first (they are the base)
            if let Some(import_paths) = modifiers.import {
                for import_path_str in import_paths {
                    // resolve the path relative to the current config's directory
                    let import_path = base_path.join(import_path_str);

                    // recursively load and parse the imported config
                    let imported_config =
                        Self::load_project_config(&import_path, amendments_to_activate)?;
                    config = config.with_merge(imported_config);
                }
            }

            // check if there amendments in the actual config file, and then
            // check if the user passed amendments to activate
            if let (Some(defined_amendments), Some(active_amendments)) =
                (modifiers.amend, amendments_to_activate)
            {
                // iterate through the NAMES of the amendments the user wants to activate
                for name_to_activate in active_amendments {
                    if let Some(amendment_variant) = defined_amendments.get(name_to_activate) {
                        // if found, apply it
                        config = config.with_amendment(amendment_variant.clone());
                    } else {
                        // if not found, return an error
                        return Err(Error::AmendmentNotFound(name_to_activate.to_string()));
                    }
                }
            }
        }

        Ok(config)
    }

    ///
    /// Create new Project object after parsing the project config. This
    /// is an internal function to enable moer abstact wrappers
    ///
    fn new_from_parsed_config<P>(config: ProjectConfig, config_dir: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let samples_df_raw = match &config.sample_table {
            Some(sample_table) => {
                let sample_table_path = config_dir.as_ref().join(sample_table);
                LazyCsvReader::new(PlPath::new(sample_table_path.to_str().unwrap()))
                    .with_has_header(true)
                    .with_infer_schema_length(Some(10_000))
                    .finish()?
                    .collect()?
            }
            None => DataFrame::empty(),
        };

        Self::finalize_project_creation(config, samples_df_raw)
    }

    ///
    /// Finally parse and create the project. This takes a parsed project configuration,
    /// and a raw sample table (as a [`DataFrame`]) and then applies all sample modifiers
    /// TODO: add here subsamples handling
    fn finalize_project_creation(
        config: ProjectConfig,
        samples_df_raw: DataFrame,
    ) -> Result<Self, Error> {
        let sample_table_index = config
            .sample_table_index
            .as_deref()
            .unwrap_or(DEFAULT_SAMPLE_TABLE_INDEX);

        let mut samples_lf = Some(samples_df_raw.clone().lazy());
        let subsamples = match &config.subsample_table {
            Some(_subsample_table) => None,
            None => None,
        };

        // apply modifiers if they exist and if there is a sample table
        #[allow(clippy::collapsible_if)]
        if let Some(modifiers) = &config.sample_modifiers {
            // make sure they passed a sample table at all
            if let Some(lf) = samples_lf.take() {
                let mut new_lf = lf;

                // REMOVE
                if let Some(cols_to_remove) = &modifiers.remove {
                    new_lf = new_lf.drop(cols(cols_to_remove));
                }

                // DUPLICATE
                if let Some(duplicate_map) = &modifiers.duplicate {
                    for (old_attribute_name, new_attribute_name) in duplicate_map {
                        new_lf =
                            new_lf.with_column(col(old_attribute_name).alias(new_attribute_name));
                    }
                }

                // APPEND
                if let Some(append_map) = &modifiers.append {
                    for (new_col_name, value) in append_map {
                        new_lf = new_lf.with_column(lit(value.to_string()).alias(new_col_name))
                    }
                }

                // IMPLY
                if let Some(imply_rules) = &modifiers.imply {
                    for rule in imply_rules {
                        let schema = new_lf.collect_schema()?;

                        let mut condition: Option<Expr> = None;
                        for (attr_name, imply_condition) in &rule.if_condition {
                            let attr_cond = match imply_condition {
                                ImplyCondition::Single(val) => col(attr_name).eq(lit(val.clone())),
                                ImplyCondition::Multiple(vals) => {
                                    vals.iter().fold(lit(false), |acc, v| {
                                        acc.or(col(attr_name).eq(lit(v.clone())))
                                    })
                                }
                            };
                            condition = Some(match condition.take() {
                                None => attr_cond,
                                Some(existing) => existing.and(attr_cond),
                            });
                        }

                        if let Some(cond_expr) = condition {
                            for (attr_name, value) in &rule.then_action {
                                // Preserve existing values for non-matching rows;
                                // use null for columns that don't yet exist.
                                let else_expr = if schema.contains(attr_name.as_str()) {
                                    col(attr_name)
                                } else {
                                    lit(NULL)
                                };
                                new_lf = new_lf.with_column(
                                    when(cond_expr.clone())
                                        .then(lit(value.clone()))
                                        .otherwise(else_expr)
                                        .alias(attr_name),
                                );
                            }
                        }
                    }
                }

                // DERIVE
                if let Some(derive_rule) = &modifiers.derive {
                    for col_to_derive in &derive_rule.attributes {
                        // start with the original column as the final "else" case
                        let mut final_expr = col(col_to_derive);

                        // chain a when-then for each source template
                        for (key, template) in &derive_rule.sources {
                            let template_expr = build_derive_template_expr(template)?;

                            final_expr = when(col(col_to_derive).eq(lit(key.clone())))
                                .then(template_expr)
                                .otherwise(final_expr);
                        }

                        // apply the chained expression to the DataFrame
                        new_lf = new_lf.with_column(final_expr.alias(col_to_derive));
                    }
                }

                // TODO: collapse samples if duplicated samples
                // In polars it seems impossible, and hard to achieve.
                // Maybe we can do it using function .get_sample() ? so it will be done only this way

                // after all potential modifications, re-assign
                samples_lf = Some(new_lf)
            }
        }

        // finally, collect the lazy frame
        let samples = match samples_lf {
            Some(lf) => Some(lf.collect()?),
            None => None,
        };

        Ok(Self {
            sample_table_index: sample_table_index.to_owned(),
            config: Some(config),
            samples: samples.unwrap_or(DataFrame::empty()),
            samples_raw: samples_df_raw,
            subsamples,
        })
    }

    ///
    /// Iterate over the samples in the project
    ///
    pub fn iter_samples(&'_ self) -> SamplesIter<'_> {
        SamplesIter::new(&self.samples)
    }

    ///
    /// Iterate over the raw, unprocessed samples in the project
    ///
    pub fn iter_samples_raw(&'_ self) -> SamplesIter<'_> {
        SamplesIter::new(&self.samples_raw)
    }

    #[cfg(feature = "wdl")]
    pub fn to_mapped_wdl_input(&self, options: WdlInputParsingOptions) -> Result<String, Error> {
        use serde_json::{Map, Value};

        // Get the wdl file input schema -- this function is in a helper module
        // and i basically ripped it out of the sprocket repository for now
        // since it works so well
        let input_schema = get_inputs_from_wdl(options)
            .map_err(|e| Error::Processing(format!("WDL parsing error: {}", e)))?;

        // Grab all the columns in our sample table
        let pep_columns: std::collections::HashSet<&str> = self
            .samples
            .get_column_names()
            .into_iter()
            .map(|c| c.as_str())
            .collect();

        // Verify that the PEP has all the necessary attributes
        for (key, value) in &input_schema {
            // Assume key is "workflow_name.input_name", we just want "input_name"
            let wdl_input_name = key.split('.').last().unwrap_or(key);

            if let Some(s) = value.as_str() {
                if s.contains("<REQUIRED>") && !pep_columns.contains(wdl_input_name) {
                    return Err(Error::ProjectMissingAttribute(wdl_input_name.to_string()));
                }
            }
        }

        let mut populated_samples: Vec<Value> = Vec::with_capacity(self.len());

        for sample in self.iter_samples() {
            let mut sample_map = Map::new();
            for (key, _) in &input_schema {
                let wdl_input_name = key.split('.').last().unwrap_or(key);

                // If the PEP has this column, get the value and add it to the JSON
                if let Some(any_value) = sample.get(wdl_input_name) {
                    let json_value = any_value_to_json(any_value.clone());
                    sample_map.insert(wdl_input_name.to_string(), json_value);
                }
            }
            populated_samples.push(Value::Object(sample_map));
        }

        // Return the populated samples as a JSON Value array
        Ok(
            serde_json::to_string_pretty(&Value::Array(populated_samples)).map_err(|e| {
                crate::error::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
            })?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;
    use rstest::*;

    #[fixture]
    fn basic_csv() -> &'static str {
        "../example-peps/example_basic/sample_table.csv"
    }

    #[fixture]
    fn basic_pep() -> &'static str {
        "../example-peps/example_basic/project_config.yaml"
    }

    #[fixture]
    fn new_st_index() -> &'static str {
        "../example-peps/example_new_st_index/project_config.yaml"
    }

    #[fixture]
    fn remove_pep() -> &'static str {
        "../example-peps/example_remove/project_config.yaml"
    }

    #[fixture]
    fn duplicate_pep() -> &'static str {
        "../example-peps/example_duplicate/project_config.yaml"
    }

    #[fixture]
    fn append_pep() -> &'static str {
        "../example-peps/example_append/project_config.yaml"
    }

    #[fixture]
    fn imply_pep() -> &'static str {
        "../example-peps/example_imply/project_config.yaml"
    }

    #[fixture]
    fn derive_pep() -> &'static str {
        "../example-peps/example_derive/project_config.yaml"
    }

    #[fixture]
    fn import_pep() -> &'static str {
        "../example-peps/example_imports/project_config.yaml"
    }

    #[fixture]
    fn amendments1_pep() -> &'static str {
        "../example-peps/example_amendments1/project_config.yaml"
    }

    #[rstest]
    fn pep_from_csv(basic_csv: &'static str) {
        let proj = Project::from_csv(basic_csv);
        assert_eq!(proj.is_ok(), true);
    }

    #[rstest]
    #[case("../example-peps/example_basic/project_config.yaml")]
    #[case("../example-peps/example_new_st_index/project_config.yaml")]
    #[case("../example-peps/example_remove/project_config.yaml")]
    #[case("../example-peps/example_duplicate/project_config.yaml")]
    #[case("../example-peps/example_append/project_config.yaml")]
    #[case("../example-peps/example_imply/project_config.yaml")]
    #[case("../example-peps/example_derive/project_config.yaml")]
    #[case("../example-peps/example_imports/project_config.yaml")]
    #[case("../example-peps/example_amendments1/project_config.yaml")]
    fn instantiate_pep(#[case] cfg_path: &'static str) {
        let proj = Project::from_config(cfg_path).build();
        let proj = proj.unwrap();
        println!("{:?}", proj.samples);
        // assert_eq!(proj.is_ok(), true);
    }

    #[rstest]
    fn test_new_st_index(new_st_index: &'static str) {
        let proj = Project::from_config(new_st_index).build();
        assert_eq!(proj.is_ok(), true);

        let proj = proj.unwrap();
        assert_eq!(proj.sample_table_index, "id");

        let sample1 = proj.get_sample("frog_1");
        assert_eq!(sample1.is_ok(), true);

        let sample1 = sample1.unwrap();
        assert_eq!(sample1.is_some(), true);
    }

    #[rstest]
    fn remove_pep_project(remove_pep: &'static str) {
        let proj = Project::from_config(remove_pep).build();
        assert_eq!(proj.is_ok(), true);

        let samples = proj.unwrap().samples;
        let cols = samples.get_column_names();
        assert_eq!(cols, &["sample_name", "organism"])
    }

    #[rstest]
    fn duplicate_pep_project(duplicate_pep: &'static str) {
        let proj = Project::from_config(duplicate_pep).build();
        assert_eq!(proj.is_ok(), true);

        let samples = proj.unwrap().samples;
        let cols = samples.get_column_names();
        assert_eq!(cols, &["sample_name", "organism", "time", "animal"])
    }

    #[rstest]
    fn append_pep_project(append_pep: &'static str) {
        let proj = Project::from_config(append_pep).build();
        assert_eq!(proj.is_ok(), true);

        let samples = proj.unwrap().samples;
        let cols = samples.get_column_names();
        assert_eq!(cols, &["sample_name", "organism", "time", "read_type"])
    }

    #[rstest]
    fn imply_pep_project(imply_pep: &'static str) {
        let proj = Project::from_config(imply_pep).build();
        // let proj = proj.unwrap();
        assert_eq!(proj.is_ok(), true);

        println!("{:?}", proj.unwrap().samples);
    }

    #[rstest]
    fn derive_pep_project(derive_pep: &'static str) {
        let proj = Project::from_config(derive_pep).build();
        assert_eq!(proj.is_ok(), true);

        let correct_vals = vec![
            format!(
                "{}/data/lab/project/pig_0h.fastq",
                std::env::var("HOME").unwrap()
            ),
            format!(
                "{}/data/lab/project/pig_1h.fastq",
                std::env::var("HOME").unwrap()
            ),
            "/path/from/collaborator/weirdNamingScheme_id_003.fastq".to_string(),
            "/path/from/collaborator/weirdNamingScheme_id_004.fastq".to_string(),
        ];
        let proj = proj.unwrap();
        let protocol_values = proj
            .samples
            .column("file_path")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(protocol_values, correct_vals);
    }

    #[rstest]
    fn import_pep_project(import_pep: &'static str) {
        let proj = Project::from_config(import_pep).build();
        assert_eq!(proj.is_ok(), true);
        assert_eq!(
            proj.unwrap().samples.get_column_names_str(),
            vec!["sample_name", "protocol", "file", "imported_attr"]
        );
    }

    #[rstest]
    fn import_amendments1_pep(amendments1_pep: &'static str) {
        let proj = Project::from_config(amendments1_pep)
            .with_amendments(&["newLib".to_string()])
            .build();

        assert_eq!(proj.is_ok(), true);
        let correct_vals = vec!["ABCD", "ABCD", "ABCD", "ABCD"];
        let proj = proj.unwrap();
        let protocol_values = proj
            .samples
            .column("protocol")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(protocol_values, correct_vals);

        // do it again, but without an amendment
        let proj = Project::from_config(amendments1_pep).build();

        assert_eq!(proj.is_ok(), true);

        let correct_vals = vec!["RRBS", "RRBS", "RRBS", "RRBS"];
        let proj = proj.unwrap();
        let protocol_values = proj
            .samples
            .column("protocol")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(protocol_values, correct_vals);
    }

    #[rstest]
    fn get_sample_from_pep(basic_pep: &'static str) {
        let proj = Project::from_config(basic_pep).build();
        assert_eq!(proj.is_ok(), true);

        let proj = proj.unwrap();
        let sample = proj.get_sample("frog_1");

        assert_eq!(sample.is_ok(), true);

        let sample = sample.unwrap();
        assert_eq!(sample.is_some(), true);

        let sample = sample.unwrap();
        assert_eq!(sample.get("file").is_some(), true);
        assert_eq!(
            sample.get("file").unwrap().str_value(),
            "data/frog1_data.txt"
        );
    }

    #[rstest]
    fn iterate_samples(basic_pep: &'static str) {
        let proj = Project::from_config(basic_pep).build();
        assert_eq!(proj.is_ok(), true);

        let proj = proj.unwrap();
        let samples = proj.iter_samples().collect::<Vec<Sample<'_>>>();

        assert_eq!(samples.len(), 2);
    }

    #[rstest]
    fn iterate_samples_get_values(basic_pep: &'static str) {
        let proj = Project::from_config(basic_pep).build();
        assert_eq!(proj.is_ok(), true);

        let proj = proj.unwrap();
        let samples = proj
            .iter_samples()
            .filter_map(|s| s.get("file").map(|av| av.str_value().to_string()))
            .collect::<Vec<String>>();

        assert_eq!(samples.len(), 2);
        assert_eq!(samples, &["data/frog1_data.txt", "data/frog2_data.txt"]);
    }
}
