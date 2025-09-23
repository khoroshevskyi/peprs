pub mod common;

use chrono::prelude::*;
use common::csv_reader::read_sample_table;
use polars::prelude::*;

fn main() {
    read_sample_table("output.csv");
}
