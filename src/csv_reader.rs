// use chrono::prelude::*;
use polars::prelude::*;

pub fn read_sample_table(sample_table: &str) {
    let df_csv = CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(sample_table.into()))
        .unwrap()
        .finish()
        .unwrap();
    println!("{df_csv}");
}
