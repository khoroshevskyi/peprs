use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use polars::prelude::*;
use serde_yaml;

use crate::config::ProjectConfig;
use crate::error::Error;

pub struct Project {
    pub config: Option<ProjectConfig>,
    pub samples: Option<LazyFrame>,
    pub subsamples: Option<Vec<LazyFrame>>,
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
        let samples = Some(
            LazyCsvReader::new(PlPath::new(path.as_ref().to_str().unwrap()))
                .with_has_header(true)
                .finish()?, // TODO
                            // handle duplicate rows, with the same `sample_name`
                            // other attributes becoming lists
        );

        Ok(Self {
            config,
            samples,
            subsamples,
        })
    }

    ///
    /// Create a new PEP project struct from a project configuration file
    ///
    pub fn from_config<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        // open configuration file and deserialize from yaml to struct
        let config_file = File::open(&path)?;
        let reader = BufReader::new(config_file);
        let config: ProjectConfig = serde_yaml::from_reader(reader)?;

        // exrtract out the directory of the config file
        let config_dir = path.as_ref().parent().unwrap_or(Path::new("."));

        // read in the sample table if it exists
        // if the user has specified a sample table, read it in
        // assuming its in the same directory as the project config file.
        let mut samples_lf = match &config.sample_table {
            Some(sample_table) => {
                let sample_table_path = config_dir.join(sample_table);
                Some(
                    LazyCsvReader::new(PlPath::new(sample_table_path.to_str().unwrap()))
                        .with_has_header(true)
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

                // REMOVE modifier
                if let Some(cols_to_remove) = &modifiers.remove {
                    new_lf = new_lf.drop(cols(cols_to_remove));
                }

                // after all potential modifications, re-assign
                samples_lf = Some(new_lf)
            }
        }

        Ok(Self {
            config: Some(config),
            samples: samples_lf,
            subsamples,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;
    use rstest::*;

    #[fixture]
    fn basic_csv() -> &'static str {
        "tests/example-peps/example_basic/sample_table.csv"
    }

    #[fixture]
    fn basic_pep() -> &'static str {
        "tests/example-peps/example_basic/project_config.yaml"
    }

    #[fixture]
    fn remove_pep() -> &'static str {
        "tests/example-peps/example_remove/project_config.yaml"
    }

    #[rstest]
    fn pep_from_csv(basic_csv: &'static str) {
        let proj = Project::from_csv(basic_csv);
        assert_eq!(proj.is_ok(), true);
    }

    #[rstest]
    fn basic_pep_project(basic_pep: &'static str) {
        let proj = Project::from_config(basic_pep);
        assert_eq!(proj.is_ok(), true);
    }

    #[rstest]
    fn remove_pep_project(remove_pep: &'static str) {
        let proj = Project::from_config(remove_pep);
        assert_eq!(proj.is_ok(), true);

        let samples = proj.unwrap().samples.unwrap().collect().unwrap();
        let cols = samples.get_column_names();
        assert_eq!(cols, &["sample_name","organism"])
    }
}
