use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use polars::prelude::*;
use serde_yaml;

use crate::consts;
use crate::config::{ImplyCondition, ProjectConfig};
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
                .collect()?
        );

        Ok(Self {
            config,
            samples,
            subsamples,
        })
    }

    ///
    /// Create new Project object after parsing the project config. This
    /// is an internal function to enable moer abstact wrappers
    /// 
    fn new_from_parsed_config<P>(config: ProjectConfig, config_dir: P) -> Result<Self, Error> 
    where
        P: AsRef<Path>,
    {

        // read in the sample table if it exists
        // if the user has specified a sample table, read it in
        // assuming its in the same directory as the project config file.
        let mut samples_lf = match &config.sample_table {
            Some(sample_table) => {
                let sample_table_path = config_dir.as_ref().join(sample_table);
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
                // if let Some(imply_rules) = &modifiers.imply {
                //     for imply_rule in imply_rules {
                //         new_lf = new_lf.with_column(
                //             coalesce(
                //         imply_rule
                //                 .if_condition
                //                 .iter()
                //                 .map(|(attribute, condition)| {
                //                     match condition {
                //                         ImplyCondition::Single(c) => {
                //                            when(col(attribute).eq(lit(c.to_string())))
                //                         }
                //                         ImplyCondition::Multiple(c) => {
                //                             when(col(attribute).is_in(c, false))
                //                         }
                //                     }
                //                 })
                //                 .collect()
                //             )
                //         )
                //     }
                // }

                // after all potential modifications, re-assign
                samples_lf = Some(new_lf)
            }
        }

        // finally, collect the lazy frame
        let samples = match samples_lf {
            Some(lf) => {
                Some(lf.collect()?)
            },
            None => None
        };

        Ok(Self {
            config: Some(config),
            samples,
            subsamples,
        })
    }

    ///
    /// Create a new PEP project struct from a project configuration file
    /// that is a physical file on disk.
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

        Project::new_from_parsed_config(config, config_dir)
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
        self.samples
            .as_ref()
            .map_or(0, |df| df.height())
    }

    ///
    /// Check if the project contains no samples
    /// 
    pub fn is_empty(&self) -> bool {
        self.samples
            .as_ref()
            .is_none_or(|df| df.height() > 0)
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

        let samples = proj.unwrap().samples.unwrap();
        let cols = samples.get_column_names();
        assert_eq!(cols, &["sample_name", "organism"])
    }

    #[rstest]
    fn duplicate_pep_project(duplicate_pep: &'static str) {
        let proj = Project::from_config(duplicate_pep);
        assert_eq!(proj.is_ok(), true);

        let samples = proj.unwrap().samples.unwrap();
        let cols = samples.get_column_names();
        assert_eq!(cols, &["sample_name", "organism", "time", "animal"])
    }

    #[rstest]
    fn append_pep_project(append_pep: &'static str) {
        let proj = Project::from_config(append_pep);
        assert_eq!(proj.is_ok(), true);

        let samples = proj.unwrap().samples.unwrap();
        let cols = samples.get_column_names();
        assert_eq!(cols, &["sample_name", "organism", "time", "read_type"])
    }
}
