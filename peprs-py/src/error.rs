use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};

pub struct PeprsCoreError(peprs_core::error::Error);

// https://pyo3.rs/v0.26.0/function/error-handling.html#foreign-rust-error-types
impl std::convert::From<PeprsCoreError> for pyo3::PyErr {
    fn from(value: PeprsCoreError) -> Self {
        match value.0 {
            peprs_core::error::Error::Io(error) => PyIOError::new_err(error),
            peprs_core::error::Error::Yaml(error) => PyValueError::new_err(error.to_string()),
            peprs_core::error::Error::Config(error) => PyValueError::new_err(error),
            peprs_core::error::Error::Processing(error) => PyRuntimeError::new_err(error),
            peprs_core::error::Error::InvalidFormat(error) => PyValueError::new_err(error),
            peprs_core::error::Error::Polars(polars_error) => {
                PyRuntimeError::new_err(format!("Polars error occured: {}", polars_error))
            }
            peprs_core::error::Error::AmendmentNotFound(error) => PyValueError::new_err(error),
        }
    }
}

// https://pyo3.rs/v0.26.0/function/error-handling.html#foreign-rust-error-types
impl From<peprs_core::error::Error> for PeprsCoreError {
    fn from(value: peprs_core::error::Error) -> Self {
        Self(value)
    }
}
