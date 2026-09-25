//! Extractor facade.

pub mod environment;
pub mod generic;
pub mod lint;
pub mod raw;
pub mod tests_output;

pub use environment::extract_environment;
pub use generic::extract_generic;
pub use lint::{
    extract_keep_sorted, extract_mypy, extract_prettier, extract_ruff,
    extract_ruff_format, extract_symvision, extract_toobig,
};
pub use tests_output::{extract_cargo_test, extract_pytest};
