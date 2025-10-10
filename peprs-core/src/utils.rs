use std::sync::LazyLock;

use polars::prelude::*;
use regex::Regex;

static RE_BRACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{([^}]+)\}").unwrap());

/// Parses a template string and builds a polars `concat_str` expression.
/// e.g., "/path/{sample_name}.bam" -> concat_str([lit("/path/"), col("sample_name"), lit(".bam")])
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
