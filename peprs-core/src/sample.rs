use std::collections::HashMap;

use polars::prelude::*;

pub type Sample<'a> = HashMap<String, AnyValue<'a>>;