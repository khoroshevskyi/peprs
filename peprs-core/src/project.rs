use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use polars::prelude::*;
use serde_yaml;

use crate::config::{ImplyCondition, ProjectConfig};
use crate::consts::{self, DEFAULT_SAMPLE_TABLE_INDEX};
use crate::error::Error;
use crate::sample::{Sample, SamplesIter};
use crate::utils::build_derive_template_expr;

pub struct ProjectBuilder {
    path: PathBuf,
    amendments: Option<Vec<String>>,
}

pub struct Project {
    pub config: Option<ProjectConfig>,
    pub samples: DataFrame,
    pub subsamples: Option<Vec<DataFrame>>,
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

    /// Construct the `Project` using the specified configuration.
    ///
    /// This is the final step that will perform file I/O and parsing.
    pub fn build(self) -> Result<Project, Error> {
        let config = Project::load_project_config(&self.path, self.amendments.as_deref())?;
        let config_dir = self.path.parent().unwrap_or_else(|| Path::new("."));
        Project::new_from_parsed_config(config, config_dir)
    }
}



impl Project {
    ///
    /// Create a new PEP project struct from a simple csv alone (no modifiers)
    ///
    pub fn from_csv<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let config = None;
        let subsamples = None;
        let samples = LazyCsvReader::new(PlPath::new(path.as_ref().to_str().unwrap()))
            .with_has_header(true)
            .finish()?
            .collect()?;

        Ok(Self {
            sample_table_index: DEFAULT_SAMPLE_TABLE_INDEX.to_string(),
            config,
            samples,
            subsamples,
        })
    }

    ///
    /// Create a new PEP project struct from a project configuration file
    /// that is a physical file on disk.
    ///
    pub fn from_config<P>(path: P) -> ProjectBuilder
    where
        P: AsRef<Path> + Clone,
    {
        // let config = Self::load_project_config(path.clone(), amendments)?;
        // let config_dir = path.as_ref().parent().unwrap_or(Path::new("."));
        // Project::new_from_parsed_config(config, config_dir)
        ProjectBuilder {
            path: path.as_ref().to_path_buf(),
            amendments: None
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

        // find the index of the first `true` value in our mask.
        // we can iterate through the mask and find the position of the first `Some(true)`.
        if let Some(row_index) = mask.iter().position(|val| val == Some(true)) {
            Ok(Some(Sample::from_dataframe_row(&self.samples, row_index)?))
        } else {
            // if no `true` values were in the mask, the sample was not found.
            Ok(None)
        }
    }

    ///
    /// The main entry point for loading the project configuration
    ///
    pub fn load_project_config(path: impl AsRef<Path>, amendments: Option<&[String]>) -> Result<ProjectConfig, Error> {
        let path = path.as_ref();
        let config_file = File::open(path)?;
        let reader = BufReader::new(config_file);
        let config: ProjectConfig = serde_yaml::from_reader(reader)?;

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
        amendments_to_activate: Option<&[String]>
    ) -> Result<ProjectConfig, Error> {
        // take the modifiers out, leaving None in their place to avoid re-processing.
        if let Some(modifiers) = config.project_modifiers.take() {
            // handle imports first (they are the base)
            if let Some(import_paths) = modifiers.import {
                for import_path_str in import_paths {
                    // resolve the path relative to the current config's directory
                    let import_path = base_path.join(import_path_str);

                    // recursively load and parse the imported config
                    let imported_config = Self::load_project_config(&import_path, amendments_to_activate)?;
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

        // Return the fully processed config
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
        let sample_table_index = config
            .sample_table_index
            .as_deref()
            .unwrap_or(DEFAULT_SAMPLE_TABLE_INDEX);

        // read in the sample table if it exists
        // if the user has specified a sample table, read it in
        // assuming its in the same directory as the project config file.
        let mut samples_lf = match &config.sample_table {
            Some(sample_table) => {
                let sample_table_path = config_dir.as_ref().join(sample_table);
                Some(
                    LazyCsvReader::new(PlPath::new(sample_table_path.to_str().unwrap()))
                        .with_has_header(true)
                        .with_infer_schema_length(Some(10_000))
                        .finish()?, // TODO: merge duplicate sample names
                )
            }
            None => None,
        };

        let subsamples = match &config.subsample_table {
            // TODO: implement subsample table logic
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
                if let Some(imply_rules) = &modifiers.imply {}

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
            subsamples,
        })
    }

    ///
    /// Iterate over the samples in the project
    ///
    pub fn iter_samples(&'_ self) -> SamplesIter<'_> {
        SamplesIter::new(&self.samples)
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
    fn basic_pep_project(basic_pep: &'static str) {
        let proj = Project::from_config(basic_pep).build();
        assert_eq!(proj.is_ok(), true);
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
        let proj = Project::from_config(amendments1_pep)
            .build();

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
