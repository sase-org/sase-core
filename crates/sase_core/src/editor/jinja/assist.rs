//! Ranked Jinja completion and scope variables.
//!
//! Combines the static catalog with document scope into fuzzy-matched,
//! availability-aware candidates. `None` means the cursor is outside any
//! tag; an inert region also yields `None`. A `none` slot yields `Some`
//! with empty items so Jinja owns the position.

use std::collections::HashSet;

use super::catalog::jinja_catalog;
use super::context::{jinja_completion_slot, JinjaSlot};
use super::docs;
use super::scope::{jinja_document_scope, JinjaDocumentScope, JinjaLocalKind};
use super::wire::{
    JinjaAssistRequestWire, JinjaAvailabilityRule, JinjaAvailabilityState,
    JinjaAvailabilityWire, JinjaCompletionItemKind, JinjaCompletionItemWire,
    JinjaCompletionSlotKind, JinjaCompletionSource, JinjaCompletionWire,
    JinjaFilterTier, JinjaScopeKind,
};
use crate::editor::fuzzy::fuzzy_match;
use crate::editor::token::DocumentSnapshot;
use crate::editor::wire::EditorRange;

const GROUP_BLOCK_LOCAL: u32 = 0;
const GROUP_INPUT: u32 = 1;
const GROUP_DOC_LOCAL: u32 = 2;
const GROUP_SASE: u32 = 3;
const GROUP_POSITIONAL_PROVIDER: u32 = 4;
const GROUP_JINJA: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Availability {
    Available,
    Conditional,
    Unavailable,
}

/// Ranked completion for one cursor.
///
/// Returns `None` only outside tags (including inert regions). A `none`
/// slot returns `Some` with empty items.
pub fn jinja_completion(
    req: &JinjaAssistRequestWire,
) -> Option<JinjaCompletionWire> {
    let document = DocumentSnapshot::new(req.text.as_str());
    let cursor = document.position_to_byte_offset(req.position)?;
    let slot = jinja_completion_slot(&req.text, cursor)?;
    let range = document
        .byte_range_to_range(slot.token_start, slot.token_end)
        .unwrap_or(EditorRange {
            start: req.position,
            end: req.position,
        });
    let scope =
        jinja_document_scope(&req.text, cursor, req.frontmatter.as_deref());
    let slot_kind = match slot.slot {
        JinjaSlot::Variable => JinjaCompletionSlotKind::Variable,
        JinjaSlot::Member => JinjaCompletionSlotKind::Member,
        JinjaSlot::Filter => JinjaCompletionSlotKind::Filter,
        JinjaSlot::Test => JinjaCompletionSlotKind::Test,
        JinjaSlot::Statement => JinjaCompletionSlotKind::Statement,
        JinjaSlot::None => JinjaCompletionSlotKind::None,
    };
    if slot.slot == JinjaSlot::None {
        return Some(JinjaCompletionWire {
            slot: slot_kind,
            namespace: slot.namespace.clone(),
            prefix: slot.prefix.clone(),
            replacement_range: range,
            items: Vec::new(),
            shared_extension: String::new(),
        });
    }
    let mut candidates = match slot.slot {
        JinjaSlot::Variable => variable_candidates(req.scope, &scope),
        JinjaSlot::Member => {
            member_candidates(req.scope, &scope, slot.namespace.as_deref())
        }
        JinjaSlot::Filter => filter_candidates(),
        JinjaSlot::Test => test_candidates(),
        JinjaSlot::Statement => statement_candidates(&scope),
        JinjaSlot::None => Vec::new(),
    };
    apply_shadowing(&mut candidates);
    // Drop unavailable before matching; hover still resolves them.
    candidates.retain(|candidate| {
        candidate.availability != Availability::Unavailable
    });
    let prefix = slot.prefix.clone();
    let mut matched = Vec::new();
    for mut candidate in candidates {
        let Some(match_result) = fuzzy_match(&prefix, &candidate.name) else {
            continue;
        };
        candidate.fuzzy_tier = match_result.tier;
        candidate.fuzzy_score = match_result.score;
        candidate.match_runs = match_result.runs;
        matched.push(candidate);
    }
    sort_candidates(&mut matched, &prefix);
    apply_legacy_ordering(&mut matched);
    let shared_extension = shared_extension(&matched, &prefix);
    let items = matched
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| candidate.into_wire(index as u32))
        .collect();
    Some(JinjaCompletionWire {
        slot: slot_kind,
        namespace: slot.namespace.clone(),
        prefix,
        replacement_range: range,
        items,
        shared_extension,
    })
}

struct RawCandidate {
    name: String,
    insertion: String,
    kind: JinjaCompletionItemKind,
    source: JinjaCompletionSource,
    type_label: Option<String>,
    signature: Option<String>,
    summary: Option<String>,
    required: bool,
    default_display: Option<String>,
    choices: Vec<String>,
    availability: Availability,
    availability_hint: Option<String>,
    legacy_for: Option<String>,
    closes: Option<String>,
    shadows: Option<String>,
    group_rank: u32,
    filter_tier_rank: u32,
    order_index: usize,
    fuzzy_tier: u8,
    fuzzy_score: i32,
    match_runs: Vec<(u32, u32)>,
    catalog_documentation: Option<String>,
    member_namespace: Option<String>,
}

impl RawCandidate {
    fn catalog_doc(
        &self,
        source_label: &'static str,
        default_type: &str,
    ) -> String {
        docs::catalog_variable_markdown(&docs::CatalogVariableDoc {
            name: &self.name,
            type_label: self.type_label.as_deref().unwrap_or(default_type),
            source_label,
            summary: self.summary.as_deref().unwrap_or(""),
            documentation: self.catalog_documentation.as_deref().unwrap_or(""),
            conditional_hint: self.availability_hint.as_deref(),
            legacy_for: self.legacy_for.as_deref(),
            shadows: self.shadows.as_deref(),
        })
    }

    fn into_wire(self, rank: u32) -> JinjaCompletionItemWire {
        let documentation = self.documentation();
        let (state, hint) = match self.availability {
            Availability::Available => {
                (JinjaAvailabilityState::Available, None)
            }
            Availability::Conditional => (
                JinjaAvailabilityState::Conditional,
                self.availability_hint.clone(),
            ),
            Availability::Unavailable => {
                (JinjaAvailabilityState::Available, None)
            }
        };
        JinjaCompletionItemWire {
            name: self.name,
            insertion: self.insertion,
            kind: self.kind,
            source: self.source,
            type_label: self.type_label,
            signature: self.signature,
            summary: self.summary,
            documentation,
            required: self.required,
            default_display: self.default_display,
            choices: self.choices,
            availability: JinjaAvailabilityWire { state, hint },
            legacy_for: self.legacy_for,
            closes: self.closes,
            shadows: self.shadows,
            match_runs: self.match_runs,
            rank,
        }
    }

    fn documentation(&self) -> String {
        match self.source {
            JinjaCompletionSource::Input => docs::input_markdown(
                &self.name,
                self.type_label.as_deref().unwrap_or("line"),
                self.summary.as_deref(),
                self.required,
                self.default_display.as_deref(),
                &self.choices,
                self.shadows.as_deref(),
            ),
            JinjaCompletionSource::Local
                if self.kind == JinjaCompletionItemKind::Variable =>
            {
                docs::local_markdown(
                    &self.name,
                    self.type_label.as_deref().unwrap_or("unknown"),
                    self.summary.as_deref().unwrap_or("Template local."),
                    self.shadows.as_deref(),
                )
            }
            JinjaCompletionSource::Local | JinjaCompletionSource::Sase => {
                if self.kind == JinjaCompletionItemKind::Member {
                    return docs::member_markdown(
                        self.member_namespace.as_deref().unwrap_or(""),
                        &self.name,
                        self.type_label.as_deref().unwrap_or("unknown"),
                        self.summary.as_deref().unwrap_or(""),
                        source_label(self.source),
                    );
                }
                // Catalog-backed sase variable (or the catalog `loop`
                // entry, which completion skips in favor of locals).
                self.catalog_doc(source_label(self.source), "unknown")
            }
            JinjaCompletionSource::Positional => {
                if self.catalog_documentation.is_some() {
                    self.catalog_doc("positional", "list")
                } else {
                    docs::positional_markdown(
                        &self.name,
                        self.type_label.as_deref().unwrap_or("any"),
                        self.summary.as_deref().unwrap_or(""),
                        self.shadows.as_deref(),
                    )
                }
            }
            JinjaCompletionSource::Provider => {
                self.catalog_doc("provider", "str")
            }
            JinjaCompletionSource::Jinja => match self.kind {
                JinjaCompletionItemKind::Function => docs::global_markdown(
                    &self.name,
                    self.signature.as_deref().unwrap_or("()"),
                    self.summary.as_deref().unwrap_or(""),
                    self.shadows.as_deref(),
                ),
                JinjaCompletionItemKind::Filter => docs::filter_markdown(
                    &self.name,
                    self.signature.as_deref().unwrap_or("()"),
                    self.summary.as_deref().unwrap_or(""),
                    if self.filter_tier_rank == 0 {
                        "sase"
                    } else {
                        "jinja"
                    },
                ),
                JinjaCompletionItemKind::Test => docs::test_markdown(
                    &self.name,
                    self.summary.as_deref().unwrap_or(""),
                ),
                JinjaCompletionItemKind::Keyword => docs::statement_markdown(
                    &self.name,
                    self.summary.as_deref().unwrap_or(""),
                    self.closes.as_deref(),
                ),
                _ => self.catalog_doc("jinja", "unknown"),
            },
        }
    }
}

fn source_label(source: JinjaCompletionSource) -> &'static str {
    match source {
        JinjaCompletionSource::Input => "input",
        JinjaCompletionSource::Local => "local",
        JinjaCompletionSource::Sase => "sase",
        JinjaCompletionSource::Positional => "positional",
        JinjaCompletionSource::Provider => "provider",
        JinjaCompletionSource::Jinja => "jinja",
    }
}

fn variable_candidates(
    scope_kind: JinjaScopeKind,
    scope: &JinjaDocumentScope,
) -> Vec<RawCandidate> {
    let mut out = Vec::new();
    // Block-scoped locals innermost first, then inputs, then
    // document-level locals, per the ranking principle.
    let mut block_locals = scope
        .locals
        .iter()
        .filter(|local| is_block_scoped(local.kind))
        .collect::<Vec<_>>();
    block_locals.sort_by_key(|local| std::cmp::Reverse(local.scope_start));
    for local in block_locals {
        out.push(local_candidate(
            local.name.as_str(),
            local.kind,
            GROUP_BLOCK_LOCAL,
            out.len(),
        ));
    }
    for input in &scope.inputs {
        out.push(RawCandidate {
            name: input.name.clone(),
            insertion: input.name.clone(),
            kind: JinjaCompletionItemKind::Variable,
            source: JinjaCompletionSource::Input,
            type_label: Some(input.type_name.clone()),
            signature: None,
            summary: input.description.clone(),
            required: input.required,
            default_display: input.default_display.clone(),
            choices: input.choices.clone(),
            availability: Availability::Available,
            availability_hint: None,
            legacy_for: None,
            closes: None,
            shadows: None,
            group_rank: GROUP_INPUT,
            filter_tier_rank: 0,
            order_index: out.len(),
            fuzzy_tier: 0,
            fuzzy_score: 0,
            match_runs: Vec::new(),
            catalog_documentation: None,
            member_namespace: None,
        });
    }
    for local in scope
        .locals
        .iter()
        .filter(|local| !is_block_scoped(local.kind))
    {
        out.push(local_candidate(
            local.name.as_str(),
            local.kind,
            GROUP_DOC_LOCAL,
            out.len(),
        ));
    }
    let catalog = jinja_catalog();
    for variable in &catalog.variables {
        if variable.name == "loop" {
            continue;
        }
        let availability =
            evaluate_rule(variable.availability_rule, scope_kind, scope);
        let (state, hint) = match &availability {
            Availability::Available => (Availability::Available, None),
            Availability::Conditional => (
                Availability::Conditional,
                Some(conditional_hint(variable.availability_rule, scope_kind)),
            ),
            Availability::Unavailable => (Availability::Unavailable, None),
        };
        let group_rank = match variable.group {
            JinjaCompletionSource::Sase => GROUP_SASE,
            JinjaCompletionSource::Positional
            | JinjaCompletionSource::Provider => GROUP_POSITIONAL_PROVIDER,
            JinjaCompletionSource::Local => GROUP_DOC_LOCAL,
            JinjaCompletionSource::Input => GROUP_INPUT,
            JinjaCompletionSource::Jinja => GROUP_JINJA,
        };
        out.push(RawCandidate {
            name: variable.name.clone(),
            insertion: variable.name.clone(),
            kind: JinjaCompletionItemKind::Variable,
            source: variable.group,
            type_label: Some(variable.type_label.clone()),
            signature: None,
            summary: Some(variable.summary.clone()),
            required: false,
            default_display: None,
            choices: Vec::new(),
            availability: state,
            availability_hint: hint,
            legacy_for: variable.legacy_for.clone(),
            closes: None,
            shadows: None,
            group_rank,
            filter_tier_rank: 0,
            order_index: out.len(),
            fuzzy_tier: 0,
            fuzzy_score: 0,
            match_runs: Vec::new(),
            catalog_documentation: Some(variable.documentation.clone()),
            member_namespace: None,
        });
    }
    // Dynamic positional `_1` .. `_k`.
    if scope_kind == JinjaScopeKind::Xprompt {
        let count = scope.inputs.len().max(1);
        for index in 1..=count {
            let name = format!("_{index}");
            if out.iter().any(|candidate| candidate.name == name) {
                continue;
            }
            out.push(RawCandidate {
                name: name.clone(),
                insertion: name,
                kind: JinjaCompletionItemKind::Variable,
                source: JinjaCompletionSource::Positional,
                type_label: Some("any".to_string()),
                signature: None,
                summary: Some(format!("Positional argument {index}.")),
                required: false,
                default_display: None,
                choices: Vec::new(),
                availability: Availability::Available,
                availability_hint: None,
                legacy_for: None,
                closes: None,
                shadows: None,
                group_rank: GROUP_POSITIONAL_PROVIDER,
                filter_tier_rank: 0,
                order_index: out.len(),
                fuzzy_tier: 0,
                fuzzy_score: 0,
                match_runs: Vec::new(),
                catalog_documentation: None,
                member_namespace: None,
            });
        }
    }
    for global in &catalog.jinja_globals {
        out.push(RawCandidate {
            name: global.name.clone(),
            insertion: global.name.clone(),
            kind: JinjaCompletionItemKind::Function,
            source: JinjaCompletionSource::Jinja,
            type_label: Some("callable".to_string()),
            signature: Some(global.signature.clone()),
            summary: Some(global.summary.clone()),
            required: false,
            default_display: None,
            choices: Vec::new(),
            availability: Availability::Available,
            availability_hint: None,
            legacy_for: None,
            closes: None,
            shadows: None,
            group_rank: GROUP_JINJA,
            filter_tier_rank: 0,
            order_index: out.len(),
            fuzzy_tier: 0,
            fuzzy_score: 0,
            match_runs: Vec::new(),
            catalog_documentation: None,
            member_namespace: None,
        });
    }
    out
}

fn local_candidate(
    name: &str,
    kind: JinjaLocalKind,
    group_rank: u32,
    order_index: usize,
) -> RawCandidate {
    let (type_label, summary) = match kind {
        JinjaLocalKind::Set => {
            ("unknown", "Template local defined by `{% set %}`.")
        }
        JinjaLocalKind::ForTarget => {
            ("unknown", "Loop variable of `{% for %}`.")
        }
        JinjaLocalKind::Loop => (
            "for-loop",
            "Jinja loop object for the enclosing `{% for %}`.",
        ),
        JinjaLocalKind::MacroName => {
            ("macro", "Macro defined by `{% macro %}`.")
        }
        JinjaLocalKind::MacroParam => ("unknown", "Macro parameter."),
        JinjaLocalKind::MacroSpecial => (
            "unknown",
            "Implicit macro variable (`varargs`, `kwargs`, `caller`).",
        ),
        JinjaLocalKind::With => {
            ("unknown", "Temporary assignment in `{% with %}`.")
        }
        JinjaLocalKind::Import => (
            "unknown",
            "Name imported by `{% import %}` or `{% from %}`.",
        ),
    };
    RawCandidate {
        name: name.to_string(),
        insertion: name.to_string(),
        kind: JinjaCompletionItemKind::Variable,
        source: JinjaCompletionSource::Local,
        type_label: Some(type_label.to_string()),
        signature: None,
        summary: Some(summary.to_string()),
        required: false,
        default_display: None,
        choices: Vec::new(),
        availability: Availability::Available,
        availability_hint: None,
        legacy_for: None,
        closes: None,
        shadows: None,
        group_rank,
        filter_tier_rank: 0,
        order_index,
        fuzzy_tier: 0,
        fuzzy_score: 0,
        match_runs: Vec::new(),
        catalog_documentation: None,
        member_namespace: None,
    }
}

fn is_block_scoped(kind: JinjaLocalKind) -> bool {
    matches!(
        kind,
        JinjaLocalKind::ForTarget
            | JinjaLocalKind::Loop
            | JinjaLocalKind::MacroParam
            | JinjaLocalKind::MacroSpecial
            | JinjaLocalKind::With
    )
}

fn member_candidates(
    scope_kind: JinjaScopeKind,
    scope: &JinjaDocumentScope,
    namespace: Option<&str>,
) -> Vec<RawCandidate> {
    let Some(namespace) = namespace else {
        return Vec::new();
    };
    let catalog = jinja_catalog();
    if namespace == "wait" {
        let Some(holder) =
            catalog.variables.iter().find(|entry| entry.name == "wait")
        else {
            return Vec::new();
        };
        if evaluate_rule(holder.availability_rule, scope_kind, scope)
            == Availability::Unavailable
        {
            return Vec::new();
        }
        return holder
            .members
            .iter()
            .enumerate()
            .map(|(index, member)| RawCandidate {
                name: member.name.clone(),
                insertion: member.name.clone(),
                kind: JinjaCompletionItemKind::Member,
                source: JinjaCompletionSource::Sase,
                type_label: Some(member.type_label.clone()),
                signature: None,
                summary: Some(member.summary.clone()),
                required: false,
                default_display: None,
                choices: Vec::new(),
                availability: Availability::Available,
                availability_hint: None,
                legacy_for: None,
                closes: None,
                shadows: None,
                group_rank: 0,
                filter_tier_rank: 0,
                order_index: index,
                fuzzy_tier: 0,
                fuzzy_score: 0,
                match_runs: Vec::new(),
                catalog_documentation: None,
                member_namespace: Some("wait".to_string()),
            })
            .collect();
    }
    if namespace == "loop" {
        let Some(holder) =
            catalog.variables.iter().find(|entry| entry.name == "loop")
        else {
            return Vec::new();
        };
        return holder
            .members
            .iter()
            .enumerate()
            .map(|(index, member)| RawCandidate {
                name: member.name.clone(),
                insertion: member.name.clone(),
                kind: JinjaCompletionItemKind::Member,
                source: JinjaCompletionSource::Local,
                type_label: Some(member.type_label.clone()),
                signature: None,
                summary: Some(member.summary.clone()),
                required: false,
                default_display: None,
                choices: Vec::new(),
                availability: Availability::Available,
                availability_hint: None,
                legacy_for: None,
                closes: None,
                shadows: None,
                group_rank: 0,
                filter_tier_rank: 0,
                order_index: index,
                fuzzy_tier: 0,
                fuzzy_score: 0,
                match_runs: Vec::new(),
                catalog_documentation: None,
                member_namespace: Some("loop".to_string()),
            })
            .collect();
    }
    Vec::new()
}

fn filter_candidates() -> Vec<RawCandidate> {
    let catalog = jinja_catalog();
    catalog
        .filters
        .iter()
        .enumerate()
        .map(|(index, filter)| {
            let (source, tier_rank) = match filter.tier {
                JinjaFilterTier::Sase => (JinjaCompletionSource::Sase, 0),
                JinjaFilterTier::Common => (JinjaCompletionSource::Jinja, 1),
                JinjaFilterTier::Other => (JinjaCompletionSource::Jinja, 2),
            };
            RawCandidate {
                name: filter.name.clone(),
                insertion: filter.name.clone(),
                kind: JinjaCompletionItemKind::Filter,
                source,
                type_label: None,
                signature: Some(filter.signature.clone()),
                summary: Some(filter.summary.clone()),
                required: false,
                default_display: None,
                choices: Vec::new(),
                availability: Availability::Available,
                availability_hint: None,
                legacy_for: None,
                closes: None,
                shadows: None,
                group_rank: 0,
                filter_tier_rank: tier_rank,
                order_index: index,
                fuzzy_tier: 0,
                fuzzy_score: 0,
                match_runs: Vec::new(),
                catalog_documentation: None,
                member_namespace: None,
            }
        })
        .collect()
}

fn test_candidates() -> Vec<RawCandidate> {
    let catalog = jinja_catalog();
    catalog
        .tests
        .iter()
        .enumerate()
        .map(|(index, test)| RawCandidate {
            name: test.name.clone(),
            insertion: test.name.clone(),
            kind: JinjaCompletionItemKind::Test,
            source: JinjaCompletionSource::Jinja,
            type_label: None,
            signature: None,
            summary: Some(test.summary.clone()),
            required: false,
            default_display: None,
            choices: Vec::new(),
            availability: Availability::Available,
            availability_hint: None,
            legacy_for: None,
            closes: None,
            shadows: None,
            group_rank: 0,
            filter_tier_rank: 0,
            order_index: index,
            fuzzy_tier: 0,
            fuzzy_score: 0,
            match_runs: Vec::new(),
            catalog_documentation: None,
            member_namespace: None,
        })
        .collect()
}

fn statement_candidates(scope: &JinjaDocumentScope) -> Vec<RawCandidate> {
    let catalog = jinja_catalog();
    let mut out = Vec::new();
    for block in &scope.open_blocks {
        let Some(statement) = catalog
            .statements
            .iter()
            .find(|entry| entry.name == block.closer)
        else {
            continue;
        };
        out.push(RawCandidate {
            name: statement.name.clone(),
            insertion: statement.name.clone(),
            kind: JinjaCompletionItemKind::Keyword,
            source: JinjaCompletionSource::Jinja,
            type_label: None,
            signature: None,
            summary: Some(statement.summary.clone()),
            required: false,
            default_display: None,
            choices: Vec::new(),
            availability: Availability::Available,
            availability_hint: None,
            legacy_for: None,
            closes: Some(block.keyword.clone()),
            shadows: None,
            group_rank: 0,
            filter_tier_rank: 0,
            order_index: out.len(),
            fuzzy_tier: 0,
            fuzzy_score: 0,
            match_runs: Vec::new(),
            catalog_documentation: None,
            member_namespace: None,
        });
    }
    let innermost = scope
        .open_blocks
        .first()
        .map(|block| block.keyword.as_str());
    if innermost == Some("if") {
        for name in ["elif", "else"] {
            if let Some(statement) =
                catalog.statements.iter().find(|entry| entry.name == name)
            {
                out.push(RawCandidate {
                    name: statement.name.clone(),
                    insertion: statement.name.clone(),
                    kind: JinjaCompletionItemKind::Keyword,
                    source: JinjaCompletionSource::Jinja,
                    type_label: None,
                    signature: None,
                    summary: Some(statement.summary.clone()),
                    required: false,
                    default_display: None,
                    choices: Vec::new(),
                    availability: Availability::Available,
                    availability_hint: None,
                    legacy_for: None,
                    closes: None,
                    shadows: None,
                    group_rank: 0,
                    filter_tier_rank: 0,
                    order_index: out.len(),
                    fuzzy_tier: 0,
                    fuzzy_score: 0,
                    match_runs: Vec::new(),
                    catalog_documentation: None,
                    member_namespace: None,
                });
            }
        }
    } else if innermost == Some("for") {
        if let Some(statement) =
            catalog.statements.iter().find(|entry| entry.name == "else")
        {
            out.push(RawCandidate {
                name: statement.name.clone(),
                insertion: statement.name.clone(),
                kind: JinjaCompletionItemKind::Keyword,
                source: JinjaCompletionSource::Jinja,
                type_label: None,
                signature: None,
                summary: Some(statement.summary.clone()),
                required: false,
                default_display: None,
                choices: Vec::new(),
                availability: Availability::Available,
                availability_hint: None,
                legacy_for: None,
                closes: None,
                shadows: None,
                group_rank: 0,
                filter_tier_rank: 0,
                order_index: out.len(),
                fuzzy_tier: 0,
                fuzzy_score: 0,
                match_runs: Vec::new(),
                catalog_documentation: None,
                member_namespace: None,
            });
        }
    }
    for statement in &catalog.statements {
        let is_opener = statement.closer.is_some();
        let is_single = matches!(
            statement.name.as_str(),
            "extends" | "include" | "import" | "from"
        );
        if !is_opener && !is_single {
            continue;
        }
        out.push(RawCandidate {
            name: statement.name.clone(),
            insertion: statement.name.clone(),
            kind: JinjaCompletionItemKind::Keyword,
            source: JinjaCompletionSource::Jinja,
            type_label: None,
            signature: None,
            summary: Some(statement.summary.clone()),
            required: false,
            default_display: None,
            choices: Vec::new(),
            availability: Availability::Available,
            availability_hint: None,
            legacy_for: None,
            closes: None,
            shadows: None,
            group_rank: 0,
            filter_tier_rank: 0,
            order_index: out.len(),
            fuzzy_tier: 0,
            fuzzy_score: 0,
            match_runs: Vec::new(),
            catalog_documentation: None,
            member_namespace: None,
        });
    }
    out
}

fn apply_shadowing(candidates: &mut Vec<RawCandidate>) {
    let mut builtin_names = HashSet::new();
    for candidate in candidates.iter() {
        if matches!(
            candidate.source,
            JinjaCompletionSource::Sase
                | JinjaCompletionSource::Positional
                | JinjaCompletionSource::Provider
                | JinjaCompletionSource::Jinja
        ) {
            builtin_names.insert(candidate.name.clone());
        }
    }
    // First occurrence wins; mark winners that shadow a builtin.
    let mut first_index = std::collections::HashMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        first_index.entry(candidate.name.clone()).or_insert(index);
    }
    for (name, winner) in &first_index {
        if builtin_names.contains(name)
            && matches!(
                candidates[*winner].source,
                JinjaCompletionSource::Input | JinjaCompletionSource::Local
            )
        {
            candidates[*winner].shadows = Some(name.clone());
        }
    }
    // Drop later duplicates, keeping the first occurrence.
    let mut emitted = HashSet::new();
    candidates.retain(|candidate| emitted.insert(candidate.name.clone()));
}

fn sort_candidates(candidates: &mut [RawCandidate], prefix: &str) {
    candidates.sort_by(|left, right| {
        (right.name == prefix)
            .cmp(&(left.name == prefix))
            .then_with(|| left.fuzzy_tier.cmp(&right.fuzzy_tier))
            .then_with(|| {
                availability_rank(left.availability)
                    .cmp(&availability_rank(right.availability))
            })
            .then_with(|| left.group_rank.cmp(&right.group_rank))
            .then_with(|| left.filter_tier_rank.cmp(&right.filter_tier_rank))
            .then_with(|| right.fuzzy_score.cmp(&left.fuzzy_score))
            .then_with(|| left.order_index.cmp(&right.order_index))
            .then_with(|| left.name.cmp(&right.name))
    });
}

fn availability_rank(availability: Availability) -> u32 {
    match availability {
        Availability::Available => 0,
        Availability::Conditional => 1,
        Availability::Unavailable => 2,
    }
}

fn apply_legacy_ordering(candidates: &mut Vec<RawCandidate>) {
    let mut index_by_name = std::collections::HashMap::new();
    for (index, candidate) in candidates.iter().enumerate() {
        index_by_name.entry(candidate.name.clone()).or_insert(index);
    }
    let moves = candidates
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| {
            let target = candidate.legacy_for.as_deref()?;
            let target_index = index_by_name.get(target).copied()?;
            if target_index == index {
                return None;
            }
            Some((index, target_index))
        })
        .collect::<Vec<_>>();
    // Apply back-to-front so indexes stay valid; recompute after each.
    for (index, _) in moves.into_iter().rev() {
        let Some(target) = candidates[index].legacy_for.clone() else {
            continue;
        };
        let Some(target_pos) =
            candidates.iter().position(|item| item.name == target)
        else {
            continue;
        };
        if target_pos == index || target_pos + 1 == index {
            continue;
        }
        let item = candidates.remove(index);
        let target_pos =
            candidates.iter().position(|entry| entry.name == target);
        if let Some(target_pos) = target_pos {
            candidates.insert(target_pos + 1, item);
        } else {
            candidates.push(item);
        }
    }
}

fn shared_extension(candidates: &[RawCandidate], prefix: &str) -> String {
    let prefixed = candidates
        .iter()
        .filter(|candidate| candidate.fuzzy_tier == 0)
        .collect::<Vec<_>>();
    if prefixed.len() < 2 {
        return String::new();
    }
    let mut shared = prefixed[0].name.clone();
    for candidate in &prefixed[1..] {
        shared = common_prefix_case_insensitive(&shared, &candidate.name);
        if shared.len() <= prefix.len() {
            return String::new();
        }
    }
    if shared.len() > prefix.len() && shared.is_char_boundary(prefix.len()) {
        shared[prefix.len()..].to_string()
    } else {
        String::new()
    }
}

fn common_prefix_case_insensitive(left: &str, right: &str) -> String {
    let mut end = 0;
    for ((left_idx, left_ch), (_, right_ch)) in
        left.char_indices().zip(right.char_indices())
    {
        if !left_ch.eq_ignore_ascii_case(&right_ch) {
            break;
        }
        end = left_idx + left_ch.len_utf8();
    }
    left[..end].to_string()
}

pub(crate) fn evaluate_rule(
    rule: JinjaAvailabilityRule,
    scope_kind: JinjaScopeKind,
    scope: &JinjaDocumentScope,
) -> Availability {
    let has_inputs = !scope.inputs.is_empty();
    // Run rule: prompt documents that declare inputs render before any
    // agent run exists, so run-time names are undefined (see sase-1dd).
    if scope_kind == JinjaScopeKind::Prompt
        && has_inputs
        && matches!(
            rule,
            JinjaAvailabilityRule::Run
                | JinjaAvailabilityRule::RunNeedsRepeat
                | JinjaAvailabilityRule::RunNeedsWait
        )
    {
        return Availability::Unavailable;
    }
    match rule {
        JinjaAvailabilityRule::Always => Availability::Available,
        JinjaAvailabilityRule::Run => Availability::Available,
        JinjaAvailabilityRule::RunNeedsRepeat => {
            if scope_kind == JinjaScopeKind::Prompt {
                if scope.has_repeat {
                    Availability::Available
                } else {
                    Availability::Conditional
                }
            } else {
                Availability::Conditional
            }
        }
        JinjaAvailabilityRule::RunNeedsWait => {
            if scope_kind == JinjaScopeKind::Prompt {
                if scope.has_wait {
                    Availability::Available
                } else {
                    Availability::Conditional
                }
            } else {
                Availability::Conditional
            }
        }
        JinjaAvailabilityRule::XpromptOnly => {
            if scope_kind == JinjaScopeKind::Xprompt {
                Availability::Available
            } else {
                Availability::Unavailable
            }
        }
        JinjaAvailabilityRule::XpromptSkillOnly => {
            if scope_kind == JinjaScopeKind::Xprompt && scope.skill {
                Availability::Available
            } else {
                Availability::Unavailable
            }
        }
    }
}

pub(crate) fn conditional_hint(
    rule: JinjaAvailabilityRule,
    scope_kind: JinjaScopeKind,
) -> String {
    match rule {
        JinjaAvailabilityRule::RunNeedsRepeat => {
            if scope_kind == JinjaScopeKind::Prompt {
                "Only defined under `%repeat` \u{2014} add `%repeat:N`"
                    .to_string()
            } else {
                "Only defined when the launching prompt uses `%repeat`"
                    .to_string()
            }
        }
        JinjaAvailabilityRule::RunNeedsWait => {
            if scope_kind == JinjaScopeKind::Prompt {
                "Only defined under `%wait` \u{2014} add `%wait:<name>`"
                    .to_string()
            } else {
                "Only defined when the launching prompt uses `%wait`"
                    .to_string()
            }
        }
        _ => String::new(),
    }
}

pub(crate) fn unavailable_reason(
    rule: JinjaAvailabilityRule,
    scope_kind: JinjaScopeKind,
    scope: &JinjaDocumentScope,
) -> String {
    if scope_kind == JinjaScopeKind::Prompt
        && !scope.inputs.is_empty()
        && matches!(
            rule,
            JinjaAvailabilityRule::Run
                | JinjaAvailabilityRule::RunNeedsRepeat
                | JinjaAvailabilityRule::RunNeedsWait
        )
    {
        return "Unavailable when the prompt declares inputs \u{2014} it renders before any agent run exists"
            .to_string();
    }
    match rule {
        JinjaAvailabilityRule::XpromptOnly => macro_only_reason(),
        JinjaAvailabilityRule::XpromptSkillOnly => skill_only_reason(),
        _ => "Unavailable in this scope".to_string(),
    }
}

pub(crate) fn macro_only_reason() -> String {
    "Only defined in xprompt scope".to_string()
}

pub(crate) fn skill_only_reason() -> String {
    "Only defined in xprompt scope with truthy frontmatter `skill`".to_string()
}

#[cfg(test)]
mod tests {
    use super::super::scope_vars::jinja_scope_variables;
    use super::super::wire::JinjaScopeRequestWire;
    use super::*;
    use crate::editor::wire::{EditorPosition, EditorRange};

    fn request(
        text: &str,
        cursor: usize,
        scope: JinjaScopeKind,
    ) -> JinjaAssistRequestWire {
        let document = DocumentSnapshot::new(text);
        let position = document.byte_offset_to_position(cursor).unwrap();
        JinjaAssistRequestWire {
            text: text.to_string(),
            position,
            scope,
            frontmatter: None,
        }
    }

    fn names(items: &[JinjaCompletionItemWire]) -> Vec<&str> {
        items.iter().map(|item| item.name.as_str()).collect()
    }

    #[test]
    fn empty_prefix_orders_inputs_before_sase_before_jinja() {
        let text = "---\ninput:\n  topic: word\n---\n{{ }}";
        let cursor = text.find("{{ }}").unwrap() + 3;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        let ordered = names(&completion.items);
        let topic = ordered.iter().position(|name| *name == "topic").unwrap();
        let root = ordered.iter().position(|name| *name == "root").unwrap();
        let range = ordered.iter().position(|name| *name == "range").unwrap();
        assert!(topic < root, "{ordered:?}");
        assert!(root < range, "{ordered:?}");
    }

    #[test]
    fn block_locals_come_first_with_empty_prefix() {
        let text = "{% set x = 1 %}{{ }}";
        let cursor = text.len() - 3;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        assert_eq!(completion.items[0].name, "x");
        assert_eq!(completion.items[0].source, JinjaCompletionSource::Local);
    }

    #[test]
    fn fuzzy_prefix_prefers_match_quality_over_group() {
        let text = "{{ roo }}";
        let cursor = text.find("roo").unwrap() + 3;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        assert_eq!(completion.items[0].name, "root");
        assert!(!completion.items[0].match_runs.is_empty());
    }

    #[test]
    fn exact_match_wins() {
        let text = "{{ root }}";
        let cursor = text.find("root").unwrap() + 4;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        assert_eq!(completion.items[0].name, "root");
    }

    #[test]
    fn exact_match_beats_better_group() {
        // `rooted` is a declared input (better group) with only a prefix
        // match; `root` is an exact builtin match and must still win.
        let text = "---\ninput:\n  rooted: word\n---\n{{ root }}";
        let cursor = text.find("root }}").unwrap() + 4;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        assert_eq!(completion.items[0].name, "root");
    }

    #[test]
    fn conditional_sorts_after_available() {
        let text = "{{ }}";
        let completion =
            jinja_completion(&request(text, 3, JinjaScopeKind::Prompt))
                .unwrap();
        let ordered = names(&completion.items);
        let root = ordered.iter().position(|name| *name == "root").unwrap();
        let n = ordered.iter().position(|name| *name == "n").unwrap();
        assert!(root < n, "{ordered:?}");
        let entry = completion
            .items
            .iter()
            .find(|item| item.name == "n")
            .unwrap();
        assert_eq!(
            entry.availability.state,
            JinjaAvailabilityState::Conditional
        );
        assert!(entry
            .availability
            .hint
            .as_deref()
            .unwrap()
            .contains("%repeat"));
    }

    #[test]
    fn repeat_makes_n_available() {
        let text = "%repeat:2\n{{ }}";
        let cursor = text.len() - 3;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        let entry = completion
            .items
            .iter()
            .find(|item| item.name == "n")
            .unwrap();
        assert_eq!(entry.availability.state, JinjaAvailabilityState::Available);
    }

    #[test]
    fn run_names_unavailable_with_inputs() {
        let text = "---\ninput:\n  topic: word\n---\n{{ }}";
        let cursor = text.find("{{ }}").unwrap() + 3;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        assert!(!names(&completion.items).contains(&"patch_name"));
        let scope_req = JinjaScopeRequestWire {
            text: text.to_string(),
            scope: JinjaScopeKind::Prompt,
            frontmatter: None,
        };
        let variables = jinja_scope_variables(&scope_req);
        assert!(variables.known.contains(&"topic".to_string()));
        assert!(variables
            .unavailable
            .iter()
            .any(|entry| entry.name == "patch_name"));
    }

    #[test]
    fn xprompt_scope_offers_positional_and_marks_pattern() {
        let text = "{{ }}";
        let completion =
            jinja_completion(&request(text, 3, JinjaScopeKind::Xprompt))
                .unwrap();
        assert!(names(&completion.items).contains(&"_args"));
        assert!(names(&completion.items).contains(&"_1"));
        let variables = jinja_scope_variables(&JinjaScopeRequestWire {
            text: text.to_string(),
            scope: JinjaScopeKind::Xprompt,
            frontmatter: None,
        });
        assert!(variables.positional_pattern);
        assert!(variables.known.contains(&"_args".to_string()));
        assert!(
            !variables.positional_pattern
                || variables.known.contains(&"_1".to_string())
        );
    }

    #[test]
    fn provider_vars_need_skill_flag() {
        let text = "{{ }}";
        let without =
            jinja_completion(&request(text, 3, JinjaScopeKind::Xprompt))
                .unwrap();
        assert!(!names(&without.items).contains(&"provider_name"));
        let skill = "---\nskill: true\n---\n{{ }}";
        let cursor = skill.find("{{ }}").unwrap() + 3;
        let with =
            jinja_completion(&request(skill, cursor, JinjaScopeKind::Xprompt))
                .unwrap();
        assert!(names(&with.items).contains(&"provider_name"));
    }

    #[test]
    fn input_shadows_builtin_with_note() {
        let text = "---\ninput:\n  n: word\n---\n{{ }}";
        let cursor = text.find("{{ }}").unwrap() + 3;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Xprompt))
                .unwrap();
        let matches = completion
            .items
            .iter()
            .filter(|item| item.name == "n")
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].source, JinjaCompletionSource::Input);
        assert_eq!(matches[0].shadows.as_deref(), Some("n"));
        assert!(matches[0]
            .documentation
            .contains("Shadows the sase built-in"));
    }

    #[test]
    fn legacy_alias_sorts_directly_after_canonical() {
        let text = "{{ }}";
        let completion =
            jinja_completion(&request(text, 3, JinjaScopeKind::Prompt))
                .unwrap();
        let ordered = names(&completion.items);
        let patch = ordered
            .iter()
            .position(|name| *name == "patch_name")
            .expect("patch_name is offered in prompt scope");
        let cl = ordered
            .iter()
            .position(|name| *name == "cl_name")
            .expect("cl_name is offered in prompt scope");
        assert_eq!(cl, patch + 1, "{ordered:?}");
    }

    #[test]
    fn filters_order_by_tier() {
        let text = "{{ x | }}";
        let cursor = text.find('|').unwrap() + 2;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        assert_eq!(completion.slot, JinjaCompletionSlotKind::Filter);
        let ordered = names(&completion.items);
        let plan = ordered
            .iter()
            .position(|name| *name == "plan_ref_path")
            .unwrap();
        let join = ordered.iter().position(|name| *name == "join").unwrap();
        let abs = ordered.iter().position(|name| *name == "abs").unwrap();
        assert!(plan < join, "{ordered:?}");
        assert!(join < abs, "{ordered:?}");
    }

    #[test]
    fn members_cover_wait_and_loop_but_not_unknown() {
        let text = "{{ wait. }}";
        let completion = jinja_completion(&request(
            text,
            text.find('.').unwrap() + 1,
            JinjaScopeKind::Prompt,
        ))
        .unwrap();
        assert!(names(&completion.items).contains(&"chats"));
        let text = "{{ loop. }}";
        let completion = jinja_completion(&request(
            text,
            text.find('.').unwrap() + 1,
            JinjaScopeKind::Prompt,
        ))
        .unwrap();
        assert!(names(&completion.items).contains(&"index"));
        let text = "{{ nope. }}";
        let completion = jinja_completion(&request(
            text,
            text.find('.').unwrap() + 1,
            JinjaScopeKind::Prompt,
        ))
        .unwrap();
        assert!(completion.items.is_empty());
    }

    #[test]
    fn statements_offer_closers_first() {
        let text = "{% for x in y %}{% %}";
        let cursor = text.rfind("{% %").unwrap() + 3;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        assert_eq!(completion.slot, JinjaCompletionSlotKind::Statement);
        assert_eq!(completion.items[0].name, "endfor");
        assert_eq!(completion.items[0].closes.as_deref(), Some("for"));
    }

    #[test]
    fn closed_raw_block_offers_no_endraw() {
        let text = "{% raw %}x{% endraw %} {% ";
        let completion = jinja_completion(&request(
            text,
            text.len(),
            JinjaScopeKind::Prompt,
        ))
        .unwrap();
        assert_eq!(completion.slot, JinjaCompletionSlotKind::Statement);
        assert!(
            !names(&completion.items).contains(&"endraw"),
            "{:?}",
            names(&completion.items)
        );
    }

    #[test]
    fn unclosed_raw_block_offers_endraw_first() {
        let text = "{% raw %}x{% ";
        let completion = jinja_completion(&request(
            text,
            text.len(),
            JinjaScopeKind::Prompt,
        ))
        .unwrap();
        assert_eq!(completion.slot, JinjaCompletionSlotKind::Statement);
        assert_eq!(completion.items[0].name, "endraw");
    }

    #[test]
    fn fenced_for_block_offers_no_endfor() {
        let text = "```\n{% for a in b %}\n```\n{% ";
        let completion = jinja_completion(&request(
            text,
            text.len(),
            JinjaScopeKind::Prompt,
        ))
        .unwrap();
        assert_eq!(completion.slot, JinjaCompletionSlotKind::Statement);
        assert!(
            !names(&completion.items).contains(&"endfor"),
            "{:?}",
            names(&completion.items)
        );
    }

    #[test]
    fn none_slot_returns_empty_items() {
        let text = "{{ \"a\" }}";
        let completion =
            jinja_completion(&request(text, 4, JinjaScopeKind::Prompt))
                .unwrap();
        assert_eq!(completion.slot, JinjaCompletionSlotKind::None);
        assert!(completion.items.is_empty());
    }

    #[test]
    fn outside_tag_returns_none() {
        assert!(
            jinja_completion(&request("hello", 5, JinjaScopeKind::Prompt))
                .is_none()
        );
    }

    #[test]
    fn inline_code_opener_returns_none_at_trailing_text() {
        let text = "Use `{{` to open. hello";
        assert!(
            jinja_completion(&request(
                text,
                text.len(),
                JinjaScopeKind::Prompt
            ))
            .is_none(),
            "{text}"
        );
    }

    #[test]
    fn ranks_are_final_indexes_and_shared_extension() {
        let text = "{{ patch_ }}";
        let cursor = text.find("patch_").unwrap() + 6;
        let completion =
            jinja_completion(&request(text, cursor, JinjaScopeKind::Prompt))
                .unwrap();
        for (index, item) in completion.items.iter().enumerate() {
            assert_eq!(item.rank, index as u32);
        }
        // Both `patch_name` and friends share little beyond the prefix;
        // the extension is empty or a strict continuation.
        assert!(
            completion.shared_extension.is_empty()
                || "patch_name".ends_with(&completion.shared_extension)
                || completion
                    .items
                    .iter()
                    .all(|item| item.name.starts_with("patch_"))
        );
        let _ = EditorRange {
            start: EditorPosition {
                line: 0,
                character: 0,
            },
            end: EditorPosition {
                line: 0,
                character: 0,
            },
        };
    }
}
