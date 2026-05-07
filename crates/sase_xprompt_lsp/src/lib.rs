#![deny(clippy::print_stdout)]

pub mod catalog_cache;
pub mod logging;
pub mod lsp_convert;
pub mod server;

pub use server::{run_stdio, XpromptLspServer};
