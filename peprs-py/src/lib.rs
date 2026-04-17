pub mod eido;
pub mod error;
pub mod project;
pub mod samples;
pub mod utils;

use pyo3::prelude::*;

use project::PyProject;
use samples::PySample;

/// CLI entry point callable from Python.
#[pyfunction]
fn _cli_main(py: Python<'_>) {
    let sys = py.import("sys").expect("failed to import sys");
    let argv: Vec<String> = sys
        .getattr("argv")
        .expect("failed to get sys.argv")
        .extract()
        .expect("failed to extract sys.argv");
    peprs_cli::run_with_args(argv);
}

/// A Python module implemented in Rust.
#[pymodule]
fn peprs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_tracing();
    m.add_class::<PyProject>()?;
    m.add_class::<PySample>()?;
    m.add_function(wrap_pyfunction!(_cli_main, m)?)?;
    eido::register_eido_module(m)?;
    Ok(())
}

/// Install a stderr `tracing` subscriber so `warn!`/`info!` from peprs-core
/// reach Python users. No-op if a subscriber is already registered (safe to
/// call multiple times / alongside Rust consumers that set their own).
/// Verbosity is controlled via the `PEPRS_LOG` env var (e.g. `PEPRS_LOG=debug`);
/// default is `warn`.
fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_env("PEPRS_LOG")
        .unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
