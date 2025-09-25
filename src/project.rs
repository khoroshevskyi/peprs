use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use polars::prelude::*;
use serde_yaml;

use crate::config::ProjectConfig;
use crate::error::Error;

pub struct Project {
    pub config: Option<ProjectConfig>,
    pub samples: Option<DataFrame>,
    pub subsamples: Option<Vec<DataFrame>>,
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
                .finish()?
                .collect()?,
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
        let config_file = File::open(&path)?;
        let reader = BufReader::new(config_file);
        let config: ProjectConfig = serde_yaml::from_reader(reader)?;

        let config_dir = path.as_ref().parent().unwrap_or(Path::new("."));

        // read in the sample table if it exists
        let samples = match &config.sample_table {
            Some(sample_table) => {
                let sample_table_path = config_dir.join(sample_table);
                Some(
                    LazyCsvReader::new(PlPath::new(sample_table_path.to_str().unwrap()))
                        .with_has_header(true)
                        .finish()?
                )
            }
            None => None,
        };

        let subsamples = match &config.subsample_table {
            // TODO: implement subsample table logic
            Some(_subsample_table) => None,
            None => None,
        };

        // TODO: implement logic for any remove, etc
        let samples = match (samples, &config.sample_modifiers) {
            (Some(mut samples_lazy), Some(modifiers)) => {
                if let Some(cols_to_remove) = &modifiers.remove {
                    samples_lazy = samples_lazy.drop(cols(cols_to_remove.clone()));
                }
                Some(samples_lazy.collect()?)
            }
            (Some(samples_lazy), None) => {
                Some(samples_lazy.collect()?)
            }
            (None, _) => None,
        };

         Ok(Self {
            config: Some(config),
            samples,
            subsamples,
        })
    }
}

impl Display for Project {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(samples) = &self.samples {
            samples.fmt(f)
        } else {
            Ok(())
        }
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
        "tests/example-peps/example_basic/project_config.yaml"
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
        println!("{}", proj.unwrap())
    }
}
