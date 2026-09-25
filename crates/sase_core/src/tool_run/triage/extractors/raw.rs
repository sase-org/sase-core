//! Shared raw item shape for extractors.

use super::super::normalize::{clean_locator_path, display_text};

/// One un-collapsed extractor hit over normalized lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawItem {
    pub key: String,
    pub display: String,
    pub locator_paths: Vec<String>,
}

impl RawItem {
    pub fn new(
        key: String,
        display_source: &str,
        locator_paths: Vec<String>,
    ) -> Self {
        let mut paths: Vec<String> = locator_paths
            .into_iter()
            .filter_map(|path| clean_locator_path(&path))
            .collect();
        paths.sort();
        paths.dedup();
        Self {
            key,
            display: display_text(display_source),
            locator_paths: paths,
        }
    }
}
