//! Agent tab identity: name canonicalization, tab keys, effective-tab
//! resolution, and catalog ordering.
//!
//! An agent tab is an exclusive, root-level, presentation-only placement of
//! top-level sase agents on the Agents tab. The stored field is always named
//! `agent_tab`. Membership is decided by the presentation root (a session
//! takes its first turn's tab, a clan takes its generation's `clan_tab`, a
//! workflow takes its workflow root's tab), so every display surface resolves
//! through the root and a container never splits across tabs.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Explicit `%tab:main` placeholder. It is stored as absent and opts out of
/// view and lineage inheritance.
pub const DEFAULT_AGENT_TAB_NAME: &str = "main";

/// Reserved names that can never become named tabs. Machine tabs are derived,
/// and `all` is a layout level, not a tab.
pub const RESERVED_AGENT_TAB_NAMES: &[&str] = &["local", "all"];

/// Longest accepted tab name: one leading alnum plus up to 31 trailing
/// `[a-z0-9_.-]` characters.
pub const MAX_AGENT_TAB_NAME_LEN: usize = 32;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentTabError {
    #[error(
        "agent tab name must not be empty; use %tab:<name> (for example %tab:sase), or omit %tab for the default tab"
    )]
    Empty,
    #[error(
        "machine tabs are derived; omit %tab to land on the local machine tab"
    )]
    DerivedMachineTab,
    #[error(
        "there is no 'all' tab; use o in the grouping picker to see all tabs"
    )]
    AllIsALayoutLevel,
    #[error(
        "agent tab name {0:?} must match ^[a-z0-9][a-z0-9_.-]{{0,31}}$ (lowercase letters, digits, underscore, dot, dash; at most 32 characters)"
    )]
    Invalid(String),
}

/// Canonicalized `%tab` value.
///
/// `Default` is the explicit `%tab:main` placeholder: it is stored as absent.
/// `Named` carries the canonical (trimmed, lowercased) tab name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentTabNameWire {
    Default,
    Named { name: String },
}

impl AgentTabNameWire {
    /// Stored form of the canonical name: `None` for the default tab.
    pub fn stored_name(&self) -> Option<&str> {
        match self {
            AgentTabNameWire::Default => None,
            AgentTabNameWire::Named { name } => Some(name.as_str()),
        }
    }
}

/// Trim, lowercase, and validate a raw `%tab` value against the tab grammar.
pub fn canonicalize_agent_tab_name(
    raw: &str,
) -> Result<AgentTabNameWire, AgentTabError> {
    let canonical = raw.trim().to_lowercase();
    if canonical.is_empty() {
        return Err(AgentTabError::Empty);
    }
    if canonical == DEFAULT_AGENT_TAB_NAME {
        return Ok(AgentTabNameWire::Default);
    }
    if canonical == "local" {
        return Err(AgentTabError::DerivedMachineTab);
    }
    if canonical == "all" {
        return Err(AgentTabError::AllIsALayoutLevel);
    }
    if !is_valid_agent_tab_name(&canonical) {
        return Err(AgentTabError::Invalid(raw.to_string()));
    }
    Ok(AgentTabNameWire::Named { name: canonical })
}

fn is_valid_agent_tab_name(canonical: &str) -> bool {
    if canonical.len() > MAX_AGENT_TAB_NAME_LEN {
        return false;
    }
    let mut chars = canonical.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() || first.is_ascii_digit() => {
        }
        _ => return false,
    }
    chars.all(|char| {
        char.is_ascii_lowercase()
            || char.is_ascii_digit()
            || matches!(char, '_' | '.' | '-')
    })
}

/// Stable identity of one agent tab.
///
/// The default tab keeps one key whether it is labeled `main` or `⌨ local`,
/// so enrolling a first remote relabels it without losing selection, folds,
/// or memory. Machine tabs are keyed by owner installation id; the alias is
/// only the label. `UnresolvedMachine` is a session-only key for origins with
/// no known installation id; selection is never persisted against it.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentTabKeyWire {
    Default,
    Machine { installation_id: String },
    UnresolvedMachine { alias: String },
    Named { name: String },
}

impl AgentTabKeyWire {
    pub fn kind_label(&self) -> &'static str {
        match self {
            AgentTabKeyWire::Default => "default",
            AgentTabKeyWire::Machine { .. }
            | AgentTabKeyWire::UnresolvedMachine { .. } => "machine",
            AgentTabKeyWire::Named { .. } => "named",
        }
    }
}

/// Ownership of a presentation root for effective-tab resolution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentTabOwnerWire {
    #[default]
    Local,
    Remote {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        installation_id: Option<String>,
        #[serde(default)]
        alias: String,
    },
}

/// Presentation root input for effective-tab resolution: the root's stored
/// tab plus its owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTabRootWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_tab: Option<String>,
    #[serde(default)]
    pub owner: AgentTabOwnerWire,
}

/// Viewer-relative effective-tab resolution. Pure, no I/O.
///
/// A stored named tab wins on every machine. Without one, non-machine mode
/// and locally owned roots land on the default tab; otherwise the root lands
/// on its owner's machine tab. Invalid stored values fall through to the
/// machine rules instead of failing the whole roster.
pub fn resolve_effective_agent_tab(
    root: &AgentTabRootWire,
    machine_mode: bool,
) -> AgentTabKeyWire {
    if let Some(stored) = root.agent_tab.as_deref() {
        if let Ok(AgentTabNameWire::Named { name }) =
            canonicalize_agent_tab_name(stored)
        {
            return AgentTabKeyWire::Named { name };
        }
    }
    if !machine_mode {
        return AgentTabKeyWire::Default;
    }
    match &root.owner {
        AgentTabOwnerWire::Local => AgentTabKeyWire::Default,
        AgentTabOwnerWire::Remote {
            installation_id,
            alias,
        } => match installation_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            Some(id) => AgentTabKeyWire::Machine {
                installation_id: id.to_string(),
            },
            None => AgentTabKeyWire::UnresolvedMachine {
                alias: alias.clone(),
            },
        },
    }
}

/// Catalog options for one roster: the machine-mode flag, the configured
/// machine order as `(installation_id, alias)` pairs, and the configured
/// named-tab `order` map.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTabCatalogOptionsWire {
    #[serde(default)]
    pub machine_mode: bool,
    #[serde(default)]
    pub machine_order: Vec<AgentTabMachineOrderWire>,
    #[serde(default)]
    pub named_order: BTreeMap<String, i64>,
}

/// One configured machine in display order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTabMachineOrderWire {
    pub installation_id: String,
    #[serde(default)]
    pub alias: String,
}

/// One ordered catalog entry: the tab key, its kind, display label, and the
/// number of input roots that resolve to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTabCatalogEntryWire {
    pub key: AgentTabKeyWire,
    pub kind: String,
    pub label: String,
    pub root_count: usize,
}

/// Batched catalog result: per-root keys index-aligned with the input roots,
/// plus the ordered catalog entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTabCatalogWire {
    pub keys: Vec<AgentTabKeyWire>,
    pub entries: Vec<AgentTabCatalogEntryWire>,
}

/// Build the per-root tab index and ordered catalog in one batched call per
/// roster, never one call per row.
///
/// Catalog order is identical on every machine: the default tab, then remote
/// machine tabs in configured-machine order followed by unconfigured origins
/// by alias, then named tabs by configured `order` followed by natural
/// case-insensitive name. Only keys with at least one root appear.
pub fn build_agent_tab_catalog(
    roots: &[AgentTabRootWire],
    options: &AgentTabCatalogOptionsWire,
) -> AgentTabCatalogWire {
    let keys: Vec<AgentTabKeyWire> = roots
        .iter()
        .map(|root| resolve_effective_agent_tab(root, options.machine_mode))
        .collect();

    let mut counts: BTreeMap<AgentTabKeyWire, usize> = BTreeMap::new();
    for key in &keys {
        *counts.entry(key.clone()).or_insert(0) += 1;
    }
    // First-seen alias per machine installation id, for origins with no
    // configured record. Configured aliases always win for the label.
    let mut seen_alias: BTreeMap<String, String> = BTreeMap::new();
    for root in roots {
        if let AgentTabOwnerWire::Remote {
            installation_id: Some(id),
            alias,
        } = &root.owner
        {
            seen_alias
                .entry(id.trim().to_string())
                .or_insert_with(|| alias.clone());
        }
    }
    let configured_alias: BTreeMap<&str, &str> = options
        .machine_order
        .iter()
        .map(|entry| (entry.installation_id.as_str(), entry.alias.as_str()))
        .collect();
    let machine_label = |id: &str| -> String {
        let alias = configured_alias
            .get(id)
            .copied()
            .filter(|alias| !alias.is_empty())
            .or_else(|| seen_alias.get(id).map(String::as_str))
            .unwrap_or(id);
        machine_tab_label(alias)
    };

    // Configured machine ids in order, then unconfigured ids by alias.
    let mut machine_ids: Vec<String> = Vec::new();
    let mut seen_ids: BTreeSet<String> = BTreeSet::new();
    for entry in &options.machine_order {
        let id = entry.installation_id.trim().to_string();
        if id.is_empty() || !seen_ids.insert(id.clone()) {
            continue;
        }
        if counts.contains_key(&AgentTabKeyWire::Machine {
            installation_id: id.clone(),
        }) {
            machine_ids.push(id);
        }
    }
    let mut unconfigured: Vec<(String, String)> = Vec::new();
    for key in counts.keys() {
        if let AgentTabKeyWire::Machine { installation_id } = key {
            if !seen_ids.contains(installation_id) {
                unconfigured.push((
                    installation_id.clone(),
                    machine_label(installation_id),
                ));
            }
        }
    }
    unconfigured.sort_by(|left, right| {
        left.1
            .to_lowercase()
            .cmp(&right.1.to_lowercase())
            .then_with(|| left.1.cmp(&right.1))
    });
    for (id, _) in unconfigured {
        if seen_ids.insert(id.clone()) {
            machine_ids.push(id);
        }
    }

    let mut entries = Vec::new();
    if let Some(count) = counts.get(&AgentTabKeyWire::Default) {
        entries.push(AgentTabCatalogEntryWire {
            key: AgentTabKeyWire::Default,
            kind: "default".to_string(),
            label: default_tab_label(options.machine_mode),
            root_count: *count,
        });
    }
    for id in machine_ids {
        let key = AgentTabKeyWire::Machine {
            installation_id: id.clone(),
        };
        let Some(count) = counts.get(&key) else {
            continue;
        };
        entries.push(AgentTabCatalogEntryWire {
            key,
            kind: "machine".to_string(),
            label: machine_label(&id),
            root_count: *count,
        });
    }
    let mut unresolved: Vec<String> = counts
        .keys()
        .filter_map(|key| match key {
            AgentTabKeyWire::UnresolvedMachine { alias } => Some(alias.clone()),
            _ => None,
        })
        .collect();
    unresolved.sort_by(|left, right| {
        left.to_lowercase()
            .cmp(&right.to_lowercase())
            .then_with(|| left.cmp(right))
    });
    for alias in unresolved {
        let key = AgentTabKeyWire::UnresolvedMachine {
            alias: alias.clone(),
        };
        let Some(count) = counts.get(&key) else {
            continue;
        };
        entries.push(AgentTabCatalogEntryWire {
            key,
            kind: "machine".to_string(),
            label: machine_tab_label(&alias),
            root_count: *count,
        });
    }
    let mut named: Vec<(String, usize)> = counts
        .iter()
        .filter_map(|(key, count)| match key {
            AgentTabKeyWire::Named { name } => Some((name.clone(), *count)),
            _ => None,
        })
        .collect();
    named.sort_by(|left, right| {
        named_sort_key(left.0.as_str(), &options.named_order)
            .cmp(&named_sort_key(right.0.as_str(), &options.named_order))
    });
    for (name, count) in named {
        entries.push(AgentTabCatalogEntryWire {
            key: AgentTabKeyWire::Named { name: name.clone() },
            kind: "named".to_string(),
            label: name.clone(),
            root_count: count,
        });
    }

    AgentTabCatalogWire { keys, entries }
}

fn default_tab_label(machine_mode: bool) -> String {
    if machine_mode {
        "⌨ local".to_string()
    } else {
        "main".to_string()
    }
}

fn machine_tab_label(alias: &str) -> String {
    if alias == "local" {
        "⌨ local·remote".to_string()
    } else {
        format!("⌨ {alias}")
    }
}

fn named_sort_key(name: &str, order: &BTreeMap<String, i64>) -> (i64, String) {
    (
        order.get(name).copied().unwrap_or(i64::MAX),
        name.to_lowercase(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_root(agent_tab: Option<&str>) -> AgentTabRootWire {
        AgentTabRootWire {
            agent_tab: agent_tab.map(str::to_string),
            owner: AgentTabOwnerWire::Local,
        }
    }

    fn remote_root(
        agent_tab: Option<&str>,
        installation_id: Option<&str>,
        alias: &str,
    ) -> AgentTabRootWire {
        AgentTabRootWire {
            agent_tab: agent_tab.map(str::to_string),
            owner: AgentTabOwnerWire::Remote {
                installation_id: installation_id.map(str::to_string),
                alias: alias.to_string(),
            },
        }
    }

    #[test]
    fn canonicalize_trims_lowercases_and_validates() {
        assert_eq!(
            canonicalize_agent_tab_name("  Sase  ").unwrap(),
            AgentTabNameWire::Named {
                name: "sase".to_string()
            }
        );
        assert_eq!(
            canonicalize_agent_tab_name("a-b_c.d9").unwrap(),
            AgentTabNameWire::Named {
                name: "a-b_c.d9".to_string()
            }
        );
        assert_eq!(
            canonicalize_agent_tab_name("MAIN").unwrap(),
            AgentTabNameWire::Default
        );
        assert_eq!(AgentTabNameWire::Default.stored_name(), None);
        assert_eq!(
            AgentTabNameWire::Named {
                name: "sase".to_string()
            }
            .stored_name(),
            Some("sase")
        );
    }

    #[test]
    fn canonicalize_rejects_reserved_empty_and_invalid() {
        assert_eq!(
            canonicalize_agent_tab_name("").unwrap_err(),
            AgentTabError::Empty
        );
        assert_eq!(
            canonicalize_agent_tab_name("   ").unwrap_err(),
            AgentTabError::Empty
        );
        assert_eq!(
            canonicalize_agent_tab_name("local").unwrap_err(),
            AgentTabError::DerivedMachineTab
        );
        assert_eq!(
            canonicalize_agent_tab_name("LOCAL").unwrap_err(),
            AgentTabError::DerivedMachineTab
        );
        assert_eq!(
            canonicalize_agent_tab_name("all").unwrap_err(),
            AgentTabError::AllIsALayoutLevel
        );
        assert!(canonicalize_agent_tab_name("-blog").is_err());
        assert!(canonicalize_agent_tab_name("has space").is_err());
        assert!(canonicalize_agent_tab_name("oops!").is_err());
        assert!(canonicalize_agent_tab_name(&"a".repeat(33)).is_err());
        assert!(canonicalize_agent_tab_name(&"a".repeat(32)).is_ok());
        let message = format!("{}", AgentTabError::DerivedMachineTab);
        assert!(message.contains("derived"));
        let message = format!("{}", AgentTabError::AllIsALayoutLevel);
        assert!(message.contains('o'));
    }

    #[test]
    fn effective_tab_named_wins_and_machine_rules_apply() {
        // Stored named tab wins on every machine, in both modes.
        for machine_mode in [false, true] {
            assert_eq!(
                resolve_effective_agent_tab(
                    &remote_root(Some("Sase"), Some("id-1"), "apollo"),
                    machine_mode,
                ),
                AgentTabKeyWire::Named {
                    name: "sase".to_string()
                }
            );
        }
        // No stored tab: default without machine mode, or locally owned.
        assert_eq!(
            resolve_effective_agent_tab(&local_root(None), false),
            AgentTabKeyWire::Default
        );
        assert_eq!(
            resolve_effective_agent_tab(&local_root(None), true),
            AgentTabKeyWire::Default
        );
        // Remote owner in machine mode: machine key.
        assert_eq!(
            resolve_effective_agent_tab(
                &remote_root(None, Some("id-1"), "apollo"),
                true
            ),
            AgentTabKeyWire::Machine {
                installation_id: "id-1".to_string()
            }
        );
        // Remote owner outside machine mode: default.
        assert_eq!(
            resolve_effective_agent_tab(
                &remote_root(None, Some("id-1"), "apollo"),
                false
            ),
            AgentTabKeyWire::Default
        );
        // Unknown installation id: session-only key.
        assert_eq!(
            resolve_effective_agent_tab(
                &remote_root(None, None, "apollo"),
                true
            ),
            AgentTabKeyWire::UnresolvedMachine {
                alias: "apollo".to_string()
            }
        );
        // Invalid stored values fall through to the machine rules.
        assert_eq!(
            resolve_effective_agent_tab(
                &remote_root(Some("has space"), Some("id-1"), "apollo"),
                true
            ),
            AgentTabKeyWire::Machine {
                installation_id: "id-1".to_string()
            }
        );
    }

    #[test]
    fn catalog_orders_default_machines_then_named() {
        let roots = vec![
            local_root(Some("blog")),
            local_root(None),
            remote_root(None, Some("id-apollo"), "apollo"),
            remote_root(None, Some("id-mac"), "mac"),
            local_root(Some("sase")),
            remote_root(None, None, "zeus"),
        ];
        let options = AgentTabCatalogOptionsWire {
            machine_mode: true,
            machine_order: vec![
                AgentTabMachineOrderWire {
                    installation_id: "id-mac".to_string(),
                    alias: "mac".to_string(),
                },
                AgentTabMachineOrderWire {
                    installation_id: "id-apollo".to_string(),
                    alias: "apollo".to_string(),
                },
            ],
            named_order: BTreeMap::new(),
        };
        let catalog = build_agent_tab_catalog(&roots, &options);
        assert_eq!(catalog.keys.len(), roots.len());
        assert_eq!(
            catalog.keys[0],
            AgentTabKeyWire::Named {
                name: "blog".to_string()
            }
        );
        assert_eq!(catalog.keys[1], AgentTabKeyWire::Default);
        let labels: Vec<&str> = catalog
            .entries
            .iter()
            .map(|entry| entry.label.as_str())
            .collect();
        assert_eq!(
            labels,
            vec!["⌨ local", "⌨ mac", "⌨ apollo", "⌨ zeus", "blog", "sase",]
        );
        assert_eq!(
            catalog
                .entries
                .iter()
                .map(|entry| entry.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["default", "machine", "machine", "machine", "named", "named",]
        );
        assert!(
            catalog
                .entries
                .iter()
                .map(|entry| entry.root_count)
                .sum::<usize>()
                == roots.len()
        );
    }

    #[test]
    fn catalog_hides_default_label_outside_machine_mode() {
        let roots = vec![local_root(None), local_root(Some("blog"))];
        let options = AgentTabCatalogOptionsWire {
            machine_mode: false,
            machine_order: Vec::new(),
            named_order: BTreeMap::new(),
        };
        let catalog = build_agent_tab_catalog(&roots, &options);
        let labels: Vec<&str> = catalog
            .entries
            .iter()
            .map(|entry| entry.label.as_str())
            .collect();
        assert_eq!(labels, vec!["main", "blog"]);
    }

    #[test]
    fn catalog_uses_configured_order_and_renamed_alias() {
        let roots = vec![
            local_root(Some("Zulu")),
            local_root(Some("alpha")),
            remote_root(None, Some("id-1"), "stale-alias"),
        ];
        let options = AgentTabCatalogOptionsWire {
            machine_mode: true,
            machine_order: vec![AgentTabMachineOrderWire {
                installation_id: "id-1".to_string(),
                alias: "apollo".to_string(),
            }],
            named_order: BTreeMap::from([("alpha".to_string(), 5)]),
        };
        let catalog = build_agent_tab_catalog(&roots, &options);
        let labels: Vec<&str> = catalog
            .entries
            .iter()
            .map(|entry| entry.label.as_str())
            .collect();
        // Renaming relabels the machine tab; configured order wins for named.
        // No root resolves to the default tab, so it stays out of the catalog.
        assert_eq!(labels, vec!["⌨ apollo", "alpha", "zulu"]);
        assert!(!catalog
            .entries
            .iter()
            .any(|entry| entry.label == "⌨ local·remote"));
    }

    #[test]
    fn catalog_disambiguates_remote_alias_named_local() {
        let roots =
            vec![remote_root(None, Some("id-1"), "local"), local_root(None)];
        let options = AgentTabCatalogOptionsWire {
            machine_mode: true,
            machine_order: Vec::new(),
            named_order: BTreeMap::new(),
        };
        let catalog = build_agent_tab_catalog(&roots, &options);
        let labels: Vec<&str> = catalog
            .entries
            .iter()
            .map(|entry| entry.label.as_str())
            .collect();
        assert_eq!(labels, vec!["⌨ local", "⌨ local·remote"]);
    }
}
