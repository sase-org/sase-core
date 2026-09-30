//! Serde wire types for the Jinja completion engine.
//!
//! All enums serialize snake_case. The engine is purely additive: no
//! existing wire type changes for this feature.

use serde::{Deserialize, Serialize};

use crate::editor::wire::{EditorPosition, EditorRange};

/// Schema version for the Jinja catalog and assist wires.
pub const JINJA_CATALOG_WIRE_SCHEMA_VERSION: u32 = 1;

/// Document scope a Jinja assist request runs in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaScopeKind {
    /// A top-level agent prompt.
    Prompt,
    /// An xprompt definition body.
    Xprompt,
}

/// Completion slot for the identifier token around the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaCompletionSlotKind {
    Variable,
    Member,
    Filter,
    Test,
    Statement,
    None,
}

/// Menu-row kind of one completion item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaCompletionItemKind {
    Variable,
    Member,
    Function,
    Filter,
    Test,
    Keyword,
}

/// Origin bucket of one completion item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaCompletionSource {
    Input,
    Local,
    Sase,
    Positional,
    Provider,
    Jinja,
}

/// Whether a candidate renders in the current scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaAvailabilityState {
    Available,
    Conditional,
}

/// Static availability rule of a catalog variable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaAvailabilityRule {
    /// Defined in every scope.
    Always,
    /// Defined while an agent run renders the prompt.
    Run,
    /// A run variable defined only under `%repeat`.
    RunNeedsRepeat,
    /// A run variable defined only under `%wait`.
    RunNeedsWait,
    /// Defined in `xprompt` scope only.
    XpromptOnly,
    /// Defined in `xprompt` scope with truthy frontmatter `skill`.
    XpromptSkillOnly,
}

/// Display tier of a catalog filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JinjaFilterTier {
    Sase,
    Common,
    Other,
}

/// Completion or hover request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaAssistRequestWire {
    pub text: String,
    pub position: EditorPosition,
    pub scope: JinjaScopeKind,
    pub frontmatter: Option<String>,
}

/// Scope-variable request for lint and input inference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaScopeRequestWire {
    pub text: String,
    pub scope: JinjaScopeKind,
    pub frontmatter: Option<String>,
}

/// Availability of one completion item in the current scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaAvailabilityWire {
    pub state: JinjaAvailabilityState,
    pub hint: Option<String>,
}

/// One ranked completion candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCompletionItemWire {
    pub name: String,
    pub insertion: String,
    pub kind: JinjaCompletionItemKind,
    pub source: JinjaCompletionSource,
    pub type_label: Option<String>,
    pub signature: Option<String>,
    pub summary: Option<String>,
    pub documentation: String,
    pub required: bool,
    pub default_display: Option<String>,
    pub choices: Vec<String>,
    pub availability: JinjaAvailabilityWire,
    pub legacy_for: Option<String>,
    pub closes: Option<String>,
    pub shadows: Option<String>,
    /// Character ranges into `name` that matched the prefix.
    pub match_runs: Vec<(u32, u32)>,
    /// Final index in the ranked list.
    pub rank: u32,
}

/// Ranked completion response for one cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCompletionWire {
    pub slot: JinjaCompletionSlotKind,
    pub namespace: Option<String>,
    pub prefix: String,
    pub replacement_range: EditorRange,
    pub items: Vec<JinjaCompletionItemWire>,
    pub shared_extension: String,
}

/// One name that would fail in the current scope, with its reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaUnavailableWire {
    pub name: String,
    pub reason: String,
}

/// Scope-variable list backing the unknown-variable lint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaScopeVariablesWire {
    pub known: Vec<String>,
    pub positional_pattern: bool,
    pub unavailable: Vec<JinjaUnavailableWire>,
}

/// One member of a namespace variable such as `wait`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaNamespaceMemberWire {
    pub name: String,
    pub type_label: String,
    pub summary: String,
}

/// One sase-injected variable in the static catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCatalogVariableWire {
    pub name: String,
    pub type_label: String,
    pub group: JinjaCompletionSource,
    pub summary: String,
    pub documentation: String,
    pub availability_rule: JinjaAvailabilityRule,
    pub legacy_for: Option<String>,
    pub members: Vec<JinjaNamespaceMemberWire>,
}

/// One filter in the static catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCatalogFilterWire {
    pub name: String,
    pub signature: String,
    pub summary: String,
    pub tier: JinjaFilterTier,
}

/// One test in the static catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCatalogTestWire {
    pub name: String,
    pub summary: String,
}

/// One Jinja global in the static catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCatalogGlobalWire {
    pub name: String,
    pub signature: String,
    pub summary: String,
}

/// One statement keyword in the static catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCatalogStatementWire {
    pub name: String,
    pub closer: Option<String>,
    pub summary: String,
}

/// The static Jinja catalog: variables, filters, tests, Jinja globals,
/// and statements, with their docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JinjaCatalogWire {
    pub variables: Vec<JinjaCatalogVariableWire>,
    pub filters: Vec<JinjaCatalogFilterWire>,
    pub tests: Vec<JinjaCatalogTestWire>,
    pub jinja_globals: Vec<JinjaCatalogGlobalWire>,
    pub statements: Vec<JinjaCatalogStatementWire>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enums_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&JinjaScopeKind::Xprompt).unwrap(),
            "\"xprompt\""
        );
        assert_eq!(
            serde_json::to_string(&JinjaCompletionSlotKind::None).unwrap(),
            "\"none\""
        );
        assert_eq!(
            serde_json::to_string(&JinjaAvailabilityRule::RunNeedsRepeat)
                .unwrap(),
            "\"run_needs_repeat\""
        );
        assert_eq!(
            serde_json::to_string(&JinjaFilterTier::Common).unwrap(),
            "\"common\""
        );
        assert_eq!(
            serde_json::to_string(&JinjaCompletionSource::Positional).unwrap(),
            "\"positional\""
        );
    }
}
