use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};

///
/// Wrapper around [`peprs_core::error::Error`] for converting to Python exceptions.
///
pub struct PeprsCoreError(peprs_core::error::Error);

///
/// Converts a [`PeprsCoreError`] into a Python exception.
///
/// Maps each error variant to the appropriate PyO3 exception type:
/// IO/Zip -> `PyIOError`, Config/Format/Yaml/Json/Amendment -> `PyValueError`,
/// Processing/Polars -> `PyRuntimeError`.
///
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
            peprs_core::error::Error::ProjectMissingAttribute(error) => {
                PyValueError::new_err(error)
            }
            peprs_core::error::Error::Json(_) => {
                PyValueError::new_err(format!("JSON error: {}", value.0))
            }
            peprs_core::error::Error::Zip(error) => PyIOError::new_err(error.to_string()),
        }
    }
}

///
/// Converts a [`peprs_core::error::Error`] into a [`PeprsCoreError`].
///
impl From<peprs_core::error::Error> for PeprsCoreError {
    fn from(value: peprs_core::error::Error) -> Self {
        Self(value)
    }
}
