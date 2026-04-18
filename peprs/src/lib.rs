//! # peprs: *<small>Rust implementation of the PEP (Portable Encapsulated Projects) specification.</small>*
//!
//! `peprs` is a Rust library for loading, validating, and manipulating
//! biological sample metadata conforming to the [PEP specification](https://pep.databio.org/).
//!
//! This crate is a thin umbrella that re-exports the underlying workspace
//! crates behind feature flags — pick only what you need:
//!
//! - `core`    — load and manipulate PEP projects via [`peprs::core`]
//! - `eido`    — JSON-schema validation for PEP projects via [`peprs::eido`]
//! - `pephub`  — HTTP client for the PEPHub registry via [`peprs::pephub`]
//!
//! ```toml
//! [dependencies]
//! peprs = { version = "0.1", features = ["core", "eido"] }
//! ```

#[cfg(feature = "core")]
#[doc(inline)]
pub use peprs_core as core;

#[cfg(feature = "eido")]
#[doc(inline)]
pub use peprs_eido as eido;

#[cfg(feature = "pephub")]
#[doc(inline)]
pub use pephub_client as pephub;
