use std::path::Path;

use polars::prelude::*;

use crate::config::ProjectConfig;
use crate::error::Error;

pub struct Project {
    pub config: Option<ProjectConfig>,
    pub samples: DataFrame,
    pub subsamples: Option<Vec<DataFrame>>
}

impl Project {
    pub fn from_csv<P>(path: P) -> Result<Self, Error> 
    where P: AsRef<Path>
    {
        let df = LazyCsvReader::new(PlPath::new(path.as_ref().to_str().unwrap()))
            .with_has_header(true)
            .finish()?
            .collect()?;
        
        Ok(Self {
            config: None,
            samples: df,
            subsamples: None
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
        "tests/data/basic.csv"
    }


    #[rstest]
    fn pep_from_csv(basic_csv: &'static str) {
        let proj = Project::from_csv(basic_csv);
        assert_eq!(proj.is_ok(), true);
    }
}