use std::io::Cursor;
use std::path::Path;
use std::sync::LazyLock;

use polars::prelude::*;
use regex::Regex;
use serde_json::Value;

use crate::error::Error;

/// Convert a Polars `AnyValue` into a `serde_json::Value`, preserving type information.
pub fn any_value_to_json(any_value: AnyValue) -> Value {
    match any_value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => Value::Bool(b),
        AnyValue::String(s) => Value::String(s.to_string()),
        AnyValue::Float32(f) => Value::from(f),
        AnyValue::Float64(f) => Value::from(f),
        AnyValue::Int8(i) => Value::from(i),
        AnyValue::Int16(i) => Value::from(i),
        AnyValue::Int32(i) => Value::from(i),
        AnyValue::Int64(i) => Value::from(i),
        AnyValue::UInt8(u) => Value::from(u),
        AnyValue::UInt16(u) => Value::from(u),
        AnyValue::UInt32(u) => Value::from(u),
        AnyValue::UInt64(u) => Value::from(u),
        AnyValue::List(series) => {
            Value::Array(series.iter().map(|v| any_value_to_json(v)).collect())
        }
        av => Value::String(av.to_string()),
    }
}

static RE_BRACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{([^}]+)\}").unwrap());

///
/// Parses a template string and builds a polars `concat_str` expression.
/// e.g., `"/path/{sample_name}.bam"` becomes
/// `concat_str([lit("/path/"), col("sample_name"), lit(".bam")])`.
///
/// Environment variables (e.g. `${HOME}`) are expanded before parsing.
///
/// # Arguments
///
/// * `template` - The template string with `{column}` placeholders.
///
/// # Returns
///
/// A polars `Expr` that concatenates literal and column references.
///
pub fn build_derive_template_expr(template: &str) -> Result<Expr, PolarsError> {
    // expand environment variables like `${HOME}` first.
    let expanded_template = shellexpand::full(template)
        .map_err(|e| {
            PolarsError::ComputeError(
                format!("Failed to expand env var in template '{}': {}", template, e).into(),
            )
        })?
        .to_string();

    let mut parts: Vec<Expr> = Vec::new();
    let mut last_match_end = 0;

    // find all `{column}` placeholders and split the template into parts.
    for cap in RE_BRACE.captures_iter(&expanded_template) {
        let full_match = cap.get(0).unwrap();
        let col_name = cap.get(1).unwrap();

        // add the literal part before this match
        let literal_part = &expanded_template[last_match_end..full_match.start()];
        if !literal_part.is_empty() {
            parts.push(lit(literal_part.to_string()));
        }

        parts.push(col(col_name.as_str()));

        last_match_end = full_match.end();
    }

    let remaining_part = &expanded_template[last_match_end..];
    if !remaining_part.is_empty() {
        parts.push(lit(remaining_part.to_string()));
    }

    // if no placeholders found return the whole thing as a literal
    if parts.is_empty() {
        Ok(lit(expanded_template))
    } else {
        Ok(concat_str(parts, "", true))
    }
}

pub fn extract_template_columns(template: &str) -> Vec<String> {
    RE_BRACE
        .captures_iter(template)
        .map(|cap| cap.get(1).unwrap().as_str().to_string())
        .collect()
}

/// Resolve a CSV path: try local file first, then fetch as URL via ureq.
pub fn resolve_csv_to_dataframe(path: &Path) -> Result<DataFrame, Error> {
    if path.exists() {
        let df = LazyCsvReader::new(PlPath::new(path.to_str().unwrap()))
            .with_has_header(true)
            .with_infer_schema_length(Some(10_000))
            .finish()?
            .collect()?;
        return Ok(df);
    }

    #[cfg(feature = "native")]
    {
        let url = path
            .to_str()
            .ok_or_else(|| Error::config("Invalid UTF-8 in CSV path"))?;
        let mut response = ureq::get(url)
            .call()
            .map_err(|e| Error::config(format!("Failed to fetch CSV from '{url}': {e}")))?;

        let bytes = response
            .body_mut()
            .read_to_vec()
            .map_err(|e| Error::config(format!("Failed to read response from '{url}': {e}")))?;

        let cursor = Cursor::new(bytes);
        let df = CsvReadOptions::default()
            .with_has_header(true)
            .with_infer_schema_length(Some(10_000))
            .into_reader_with_file_handle(cursor)
            .finish()?;

        return Ok(df);
    }

    #[cfg(not(feature = "native"))]
    Err(Error::config(format!(
        "File not found: '{}' (URL fetching not available without 'native' feature)",
        path.display()
    )))
}
