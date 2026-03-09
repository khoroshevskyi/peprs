use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use polars::prelude::AnyValue::Null;
use polars::prelude::*;

#[derive(Debug, Clone)]
pub struct Sample<'a>(HashMap<String, AnyValue<'a>>);

impl<'a> Sample<'a> {
    ///
    /// Create a new Sample object from a data frame and row index
    ///
    pub fn from_dataframe_row(df: &'a DataFrame, row_index: usize) -> PolarsResult<Self> {
        let mut sample = HashMap::new();

        for (col_name, series) in df.get_columns().iter().enumerate() {
            let column_name = df.get_column_names()[col_name].to_string();
            let value = series.get(row_index)?;
            sample.insert(column_name, value);
        }

        Ok(Sample(sample))
    }

    ///
    /// Create a new Sample by merging multiple rows with the same sample name.
    ///
    /// Columns where all values are identical are stored as scalars.
    /// Columns where values differ are collapsed into a list.
    ///
    pub fn from_df_duplicated_rows(df: &'a DataFrame, row_indexs: Vec<usize>) -> PolarsResult<Self> {
        if row_indexs.len() == 1 {
            return Self::from_dataframe_row(df, row_indexs[0]);
        }

        let mut sample = HashMap::new();

        for (col_idx, series) in df.get_columns().iter().enumerate() {
            let column_name = df.get_column_names()[col_idx].to_string();

            let values: Vec<AnyValue> = row_indexs
                .iter()
                .map(|&ri| series.get(ri))
                .collect::<PolarsResult<Vec<_>>>()?;

            let all_same = values.windows(2).all(|w| w[0] == w[1]);

            if all_same {
                sample.insert(column_name, values.into_iter().next().unwrap());
            } else {
                let list_series = Series::from_any_values(
                    PlSmallStr::from_str(&column_name),
                    &values,
                    false,
                )?;
                sample.insert(column_name, AnyValue::List(list_series));
            }
        }

        Ok(Sample(sample))
    }

    ///
    /// Convert the Sample into an owned HashMap, via cloning
    ///
    pub fn into_map(self) -> HashMap<String, String> {
        self.0
            .into_iter()
            .map(|(key, value)| (key, value.to_string()))
            .collect()
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

///
/// An iterator over the samples in a DataFrame.
///
pub struct SamplesIter<'a> {
    df: &'a DataFrame,
    column_names: Vec<String>,
    row_index: usize,
}

impl<'a> SamplesIter<'a> {
    pub fn new(df: &'a DataFrame) -> Self {
        let column_names = df
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        SamplesIter {
            df,
            column_names,
            row_index: 0,
        }
    }
}

impl<'a> Iterator for SamplesIter<'a> {
    type Item = Sample<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_index >= self.df.height() {
            return None;
        }

        let mut attributes = HashMap::new();

        // iterate over the columns of the dataframe
        for (i, series) in self.df.get_columns().iter().enumerate() {
            // we can safely unwrap here because we've already checked the row_index bounds.
            let value = series.get(self.row_index).unwrap();
            // use the cached column name, cloning it for insertion.
            let column_name = self.column_names[i].clone();
            attributes.insert(column_name, value);
        }

        // increment the index for the next call
        self.row_index += 1;

        Some(Sample(attributes))
    }
}
