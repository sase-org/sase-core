//! Builtin and fixture input-type registries.

use std::collections::BTreeMap;

use super::catalog::{builtin_catalog, CatalogEntry};

/// Catalog rows plus the plugin types known to this process.
///
/// `builtin()` has an empty plugin list. Tests and later phases pass a
/// fixture map of PEP 503 distribution names to declared type ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputTypeRegistry {
    entries: Vec<CatalogEntry>,
    plugins: BTreeMap<String, Vec<String>>,
}

impl InputTypeRegistry {
    /// Builtin scalars, `string`, `enum`, and `agent`, with no plugins.
    pub fn builtin() -> Self {
        Self {
            entries: builtin_catalog(),
            plugins: BTreeMap::new(),
        }
    }

    /// Builtin catalog plus a fixture plugin map (`distribution` → type ids).
    pub fn with_plugins(plugins: BTreeMap<String, Vec<String>>) -> Self {
        let mut canonical = BTreeMap::new();
        for (distribution, ids) in plugins {
            canonical
                .entry(super::resolve::pep503_normalize(&distribution))
                .or_insert_with(Vec::new)
                .extend(ids);
        }
        Self {
            entries: builtin_catalog(),
            plugins: canonical,
        }
    }

    pub fn entries(&self) -> &[CatalogEntry] {
        &self.entries
    }

    pub fn plugins(&self) -> &BTreeMap<String, Vec<String>> {
        &self.plugins
    }

    /// Type ids declared by `distribution`, after PEP 503 normalization.
    pub fn plugin_type_ids(&self, distribution: &str) -> Option<&[String]> {
        self.plugins
            .get(&super::resolve::pep503_normalize(distribution))
            .map(Vec::as_slice)
    }

    pub fn has_plugin(&self, distribution: &str) -> bool {
        self.plugins
            .contains_key(&super::resolve::pep503_normalize(distribution))
    }
}
