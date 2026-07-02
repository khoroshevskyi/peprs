use std::fs::File;
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::LazyLock;

use crate::config::ProjectConfig;
use crate::error::Error;
use polars::prelude::*;
use regex::Regex;
use serde_json::Value;
use tracing::warn;

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
    // Missing env vars only produce a warning and are replaced with an empty string;
    // leaving the literal `${VAR}` would make the downstream `{...}` column parser
    // match `{VAR}` as a (nonexistent) column and fail.
    let expanded_template = shellexpand::full_with_context_no_errors(
        template,
        || std::env::var("HOME").ok(),
        |var: &str| match std::env::var(var) {
            Ok(val) => Some(val),
            Err(_) => {
                warn!(
                    "Env var '${{{}}}' in template '{}' is not set; substituting empty string",
                    var, template
                );
                Some(String::new())
            }
        },
    )
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

/// Read a YAML file containing sample data and convert it to a DataFrame.
/// Supports both list-of-dicts and dict-of-lists YAML structures.
pub fn resolve_yaml_to_dataframe(path: &Path) -> Result<DataFrame, Error> {
    let file = std::fs::File::open(path).map_err(|e| {
        Error::config(format!(
            "Failed to open YAML file '{}': {e}",
            path.display()
        ))
    })?;
    let value: Value = serde_yaml::from_reader(file).map_err(|e| {
        Error::config(format!(
            "Failed to parse YAML file '{}': {e}",
            path.display()
        ))
    })?;
    let json_str = value.to_string();
    let df = JsonReader::new(Cursor::new(json_str.as_bytes()))
        .finish()
        .map_err(|e| Error::config(format!("Failed to convert YAML to DataFrame: {e}")))?;
    Ok(df)
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

///
/// Generate the raw PEP files (sample table, subsample tables, config) as named byte buffers.
///
/// Shared by [`write_raw_folder_parts`] and [`write_raw_zip_parts`] so both sinks produce
/// identical content.
///
fn raw_pep_files(
    config: Option<&ProjectConfig>,
    samples: &mut DataFrame,
    subsamples: Option<&mut [DataFrame]>,
) -> Result<Vec<(String, Vec<u8>)>, Error> {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();

    let sample_table_name = "sample_table.csv";
    let mut sample_buf = Vec::new();
    CsvWriter::new(&mut sample_buf)
        .include_header(true)
        .with_separator(b',')
        .finish(samples)?;
    files.push((sample_table_name.to_string(), sample_buf));

    let mut subsample_names: Vec<String> = Vec::new();
    if let Some(sub_dfs) = subsamples {
        for (i, sub_df) in sub_dfs.iter_mut().enumerate() {
            let sub_name = format!("subsample_table_{}.csv", i + 1);
            let mut sub_buf = Vec::new();
            CsvWriter::new(&mut sub_buf)
                .include_header(true)
                .with_separator(b',')
                .finish(sub_df)?;
            files.push((sub_name.clone(), sub_buf));
            subsample_names.push(sub_name);
        }
    }

    if let Some(config) = config {
        let subsample_arg: Option<Vec<&str>> = if subsample_names.is_empty() {
            None
        } else {
            Some(subsample_names.iter().map(|s| s.as_str()).collect())
        };
        if let Some(config_value) = config.get_raw_config(Some(sample_table_name), subsample_arg) {
            let yaml = serde_yaml::to_string(&config_value)?;
            files.push(("project_config.yaml".to_string(), yaml.into_bytes()));
        }
    }

    Ok(files)
}

///
/// Write raw project parts (config, samples, subsamples) to a folder, without a [`Project`].
///
/// # Arguments
///
/// * `path` - Destination folder path (created if missing).
/// * `config` - Optional project config to embed as `project_config.yaml`.
/// * `samples` - Raw samples table written as `sample_table.csv`.
/// * `subsamples` - Optional subsample tables written as `subsample_table_{n}.csv`.
///
pub fn write_raw_folder_parts<P: AsRef<Path>>(
    path: P,
    config: Option<&ProjectConfig>,
    samples: &mut DataFrame,
    subsamples: Option<&mut [DataFrame]>,
) -> Result<(), Error> {
    let folder = path.as_ref();
    std::fs::create_dir_all(folder)?;
    for (name, bytes) in raw_pep_files(config, samples, subsamples)? {
        let mut file = File::create(folder.join(name))?;
        file.write_all(&bytes)?;
    }
    Ok(())
}

///
/// Write raw project parts (config, samples, subsamples) to a zip archive, without a [`Project`].
///
/// # Arguments
///
/// * `path` - Destination zip file path.
/// * `config` - Optional project config to embed as `project_config.yaml`.
/// * `samples` - Raw samples table written as `sample_table.csv`.
/// * `subsamples` - Optional subsample tables written as `subsample_table_{n}.csv`.
///
#[cfg(feature = "zip")]
pub fn write_raw_zip_parts<P: AsRef<Path>>(
    path: P,
    config: Option<&ProjectConfig>,
    samples: &mut DataFrame,
    subsamples: Option<&mut [DataFrame]>,
) -> Result<(), Error> {
    use ::zip::write::SimpleFileOptions;
    use ::zip::{CompressionMethod, ZipWriter};

    let file = File::create(path.as_ref())?;
    let mut zip = ZipWriter::new(file);
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);

    for (name, bytes) in raw_pep_files(config, samples, subsamples)? {
        zip.start_file(&name, options)?;
        zip.write_all(&bytes)?;
    }
    zip.finish()?;
    Ok(())
}

///
/// Parse a raw PEPHub project JSON string into its config, samples, and optional subsamples.
///
/// Expects the shape returned by `Api::get_raw`: keys `config`, `sample_list`, and optional
/// `subsample_list`.
///
pub fn parse_raw_pep(
    json: &str,
) -> Result<(ProjectConfig, DataFrame, Option<Vec<DataFrame>>), Error> {
    use std::io::Cursor;

    let raw_value: serde_json::Value = serde_json::from_str(json)?;

    // Config
    let config_value = raw_value
        .get("config")
        .ok_or_else(|| Error::invalid_format("Missing 'config' key"))?;
    let mut config: ProjectConfig = serde_json::from_value(config_value.clone())?;
    config.raw = Some(config_value.clone());

    // Samples
    let samples_obj = raw_value
        .get("samples")
        .ok_or_else(|| Error::invalid_format("Missing 'sample_list' key"))?;
    let samples_bytes = samples_obj.to_string();
    let samples = JsonReader::new(Cursor::new(samples_bytes.as_bytes())).finish()?;

    // Subsamples (optional)
    let subsamples = match raw_value.get("subsamples") {
        Some(serde_json::Value::Array(subs_list)) => {
            let mut dfs = Vec::new();
            for sub_item in subs_list {
                let sub_bytes = sub_item.to_string();
                let sub_df = JsonReader::new(Cursor::new(sub_bytes.as_bytes())).finish()?;
                dfs.push(sub_df);
            }
            Some(dfs)
        }
        Some(serde_json::Value::Null) | None => None,
        _ => return Err(Error::invalid_format("Invalid 'subsample_list' format")),
    };

    Ok((config, samples, subsamples))
}

///
/// Parse a raw PEPHub project JSON and write it to `path` as a folder or zip archive.
///
/// This is the pull-friendly entry point: it saves the raw project without building a
/// [`Project`], so a project with broken modifiers can still be persisted.
///
/// # Arguments
///
/// * `path` - Destination folder (or `.zip` file when `zipped`).
/// * `raw_json` - Raw project JSON as returned by `Api::get_raw`.
/// * `zipped` - If `true`, write a single zip archive; otherwise a folder.
///
pub fn save_raw_pep<P: AsRef<Path>>(path: P, raw_json: &str, zipped: bool) -> Result<(), Error> {
    let (config, mut samples, mut subsamples) = parse_raw_pep(raw_json)?;
    let sub = subsamples.as_deref_mut();
    match zipped {
        #[cfg(feature = "zip")]
        true => write_raw_zip_parts(path, Some(&config), &mut samples, sub),
        #[cfg(not(feature = "zip"))]
        true => Err(Error::Processing("zip feature not enabled".to_string())),
        false => write_raw_folder_parts(path, Some(&config), &mut samples, sub),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn missing_env_var_does_not_error() {
        let missing = "PEPRS_DEFINITELY_NOT_SET_XYZ_123";
        // ensure the var is not set in the test environment
        unsafe {
            std::env::remove_var(missing);
        }
        let template = format!("/prefix/${{{}}}/{{sample_name}}.bam", missing);
        let expr =
            build_derive_template_expr(&template).expect("missing env var should warn, not error");
        // missing env vars are substituted with empty string, so the unresolved
        // name must NOT appear as a column reference in the resulting expression.
        let debug = format!("{:?}", expr);
        assert!(
            !debug.contains(missing),
            "expected unresolved placeholder to be stripped, got: {debug}"
        );
    }

    #[test]
    fn present_env_var_expands() {
        unsafe {
            std::env::set_var("PEPRS_TEST_VAR_PRESENT", "resolved");
        }
        let expr = build_derive_template_expr("/x/${PEPRS_TEST_VAR_PRESENT}/{sample_name}")
            .expect("present env var should expand");
        let debug = format!("{:?}", expr);
        assert!(debug.contains("resolved"));
        assert!(!debug.contains("PEPRS_TEST_VAR_PRESENT"));
    }

    #[rstest]
    fn test_parse_and_save_raw_pep() {
        let raw = r#"{
            "config": {"pep_version": "2.1.0", "name": "test_proj", "description": "d"},
            "samples": [
                {"sample_name": "frog_1", "organism": "frog"},
                {"sample_name": "frog_2", "organism": "frog"}
            ],
            "subsamples": [
                [{"sample_name": "frog_1", "read": "r1"}]
            ]
        }"#;

        let (config, samples, subsamples) = parse_raw_pep(raw).unwrap();
        pretty_assertions::assert_eq!(config.name.as_deref(), Some("test_proj"));
        pretty_assertions::assert_eq!(samples.height(), 2);
        pretty_assertions::assert_eq!(subsamples.as_ref().unwrap().len(), 1);

        let dir = "/tmp/peprs_test_raw_pep";
        std::fs::remove_dir_all(dir).ok();
        save_raw_pep(dir, raw, false).unwrap();

        let folder = Path::new(dir);
        assert!(folder.join("sample_table.csv").exists());
        assert!(folder.join("subsample_table_1.csv").exists());
        assert!(folder.join("project_config.yaml").exists());

        let csv = std::fs::read_to_string(folder.join("sample_table.csv")).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        pretty_assertions::assert_eq!(lines.len(), 3); // header + 2 rows
        assert!(lines[0].contains("sample_name"));

        let yaml = std::fs::read_to_string(folder.join("project_config.yaml")).unwrap();
        assert!(yaml.contains("sample_table.csv"));
        assert!(yaml.contains("subsample_table_1.csv"));

        std::fs::remove_dir_all(dir).ok();
    }
}
