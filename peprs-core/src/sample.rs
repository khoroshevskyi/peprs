use std::{collections::HashMap, ops::{Deref, DerefMut}};

use polars::prelude::*;

#[derive(Debug, Clone)]
pub struct Sample<'a>(HashMap<String, AnyValue<'a>>);

impl<'a> Sample<'a> {
    pub fn from_dataframe_row(df: &'a DataFrame, row_index: usize) -> PolarsResult<Self> {
        let mut sample = HashMap::new();
        
        for (col_name, series) in df.get_columns().iter().enumerate() {
            let column_name = df.get_column_names()[col_name].to_string();
            let value = series.get(row_index)?;
            sample.insert(column_name, value);
        }
        
        Ok(Sample(sample))
    }
}

impl<'a> Deref for Sample<'a> {
    type Target = HashMap<String, AnyValue<'a>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// We can also implement `DerefMut` if we want to allow mutable access.
impl<'a> DerefMut for Sample<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}