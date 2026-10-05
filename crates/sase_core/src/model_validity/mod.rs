//! Single routing classifier for macro `model` inputs.
//!
//! A `model` value is valid exactly when `%model:<value>` would be accepted
//! by the directive parser and would route to a provider without the silent
//! default-provider fallback. Classification runs over a
//! [`ModelValiditySnapshot`]: providers, the `model_to_provider` map,
//! alias names (without `@`), and the effort levels. It never resolves alias
//! targets, never takes the `consume` path, never reads the load-balance
//! cursor, never makes a network call, and never consults provider
//! availability or disable state.

mod classifier;

pub use classifier::{
    classify_model_value, ClassifyModelValueRequestWire,
    ClassifyModelValueResultWire, ModelValidityError, ModelValiditySnapshot,
};
