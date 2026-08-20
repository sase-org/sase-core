//! Shared snippet composition, trigger validation, and call-graph analysis.
//!
//! Call scanning reuses the xprompt reference parser so `#[trigger]`,
//! `#[trigger(value)]`, `#[trigger:value]`, quoting, escaping, and boundary
//! rules cannot drift from expansion. Graph analysis inspects raw explicit
//! templates before expansion removes call sites. Generated aliases resolve
//! to the explicit identity used by panel navigation.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::editor::{
    find_matching_bracket_for_args, parse_xprompt_reference_body,
};
use crate::snippet_session::iter_unescaped_tabstops;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComposedSnippetCatalog {
    pub templates: BTreeMap<String, String>,
    pub alias_provenance: BTreeMap<String, String>,
    pub triggers: BTreeMap<String, SnippetTriggerValidation>,
    pub calls: BTreeMap<String, Vec<SnippetCall>>,
    pub outbound: BTreeMap<String, Vec<String>>,
    pub inbound: BTreeMap<String, Vec<String>>,
    pub diagnostics: Vec<SnippetDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnippetTriggerValidation {
    pub trigger: String,
    pub valid: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnippetCall {
    pub authored_target: String,
    pub canonical_target: Option<String>,
    pub positional_args: Vec<String>,
    pub span: SnippetSourceSpan,
    pub status: SnippetCallStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnippetSourceSpan {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnippetCallStatus {
    Resolved,
    Missing,
    Cycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnippetDiagnostic {
    pub code: String,
    pub message: String,
    pub trigger: String,
    pub target: Option<String>,
    pub span: Option<SnippetSourceSpan>,
    pub cycle: Option<Vec<String>>,
}

pub fn validate_snippet_trigger(trigger: &str) -> SnippetTriggerValidation {
    if trigger.is_empty() {
        return SnippetTriggerValidation {
            trigger: trigger.to_string(),
            valid: false,
            reason: Some("empty".to_string()),
        };
    }
    if !trigger
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return SnippetTriggerValidation {
            trigger: trigger.to_string(),
            valid: false,
            reason: Some("invalid_characters".to_string()),
        };
    }
    SnippetTriggerValidation {
        trigger: trigger.to_string(),
        valid: true,
        reason: None,
    }
}

pub fn is_valid_snippet_trigger(trigger: &str) -> bool {
    validate_snippet_trigger(trigger).valid
}

pub fn compose_snippet_catalog(
    explicit_templates: &BTreeMap<String, String>,
) -> ComposedSnippetCatalog {
    let resolved_explicit = resolve_snippet_references(explicit_templates);
    let mut combined_templates = resolved_explicit.clone();
    let mut alias_provenance = BTreeMap::new();

    for (trigger, template) in &resolved_explicit {
        let alias = uppercase_first_scalar(trigger);
        if alias == *trigger || combined_templates.contains_key(&alias) {
            continue;
        }
        combined_templates
            .insert(alias.clone(), uppercase_first_scalar(template));
        alias_provenance.insert(alias, trigger.clone());
    }

    let analysis =
        analyze_snippet_relations(explicit_templates, &alias_provenance);
    ComposedSnippetCatalog {
        templates: resolve_snippet_references(&combined_templates),
        alias_provenance,
        triggers: analysis.triggers,
        calls: analysis.calls,
        outbound: analysis.outbound,
        inbound: analysis.inbound,
        diagnostics: analysis.diagnostics,
    }
}

struct SnippetRelationAnalysis {
    triggers: BTreeMap<String, SnippetTriggerValidation>,
    calls: BTreeMap<String, Vec<SnippetCall>>,
    outbound: BTreeMap<String, Vec<String>>,
    inbound: BTreeMap<String, Vec<String>>,
    diagnostics: Vec<SnippetDiagnostic>,
}

fn analyze_snippet_relations(
    explicit_templates: &BTreeMap<String, String>,
    alias_provenance: &BTreeMap<String, String>,
) -> SnippetRelationAnalysis {
    let mut triggers = BTreeMap::new();
    let mut raw_calls = BTreeMap::<String, Vec<RawSnippetCall>>::new();
    let mut graph = BTreeMap::<String, BTreeSet<String>>::new();

    for trigger in explicit_templates.keys() {
        triggers.insert(trigger.clone(), validate_snippet_trigger(trigger));
        graph.entry(trigger.clone()).or_default();
    }

    for (trigger, template) in explicit_templates {
        let scanned = iter_raw_snippet_calls(template);
        for raw in &scanned {
            if let Some(canonical) = canonical_target(
                &raw.name,
                explicit_templates,
                alias_provenance,
            ) {
                graph.entry(trigger.clone()).or_default().insert(canonical);
            }
        }
        raw_calls.insert(trigger.clone(), scanned);
    }

    let reachable = reachable_from(&graph);
    let mut calls = BTreeMap::new();
    let mut outbound = BTreeMap::new();
    let mut inbound = BTreeMap::new();
    let mut diagnostics = Vec::new();

    for trigger in explicit_templates.keys() {
        inbound.insert(trigger.clone(), Vec::new());
    }

    for (trigger, validation) in &triggers {
        if !validation.valid {
            diagnostics.push(SnippetDiagnostic {
                code: "invalid_trigger".to_string(),
                message: invalid_trigger_message(trigger, validation),
                trigger: trigger.clone(),
                target: None,
                span: None,
                cycle: None,
            });
        }
    }

    for (trigger, scanned) in &raw_calls {
        let mut trigger_calls = Vec::new();
        let mut seen_outbound = BTreeSet::new();
        let mut trigger_outbound = Vec::new();

        for raw in scanned {
            let authored = raw.name.clone();
            let canonical = canonical_target(
                &authored,
                explicit_templates,
                alias_provenance,
            );
            let status = call_status(trigger, canonical.as_deref(), &reachable);
            let span = SnippetSourceSpan {
                start: raw.start,
                end: raw.end,
            };
            if status != SnippetCallStatus::Resolved {
                diagnostics.push(call_diagnostic(
                    trigger,
                    &authored,
                    canonical.as_deref(),
                    span,
                    status,
                    &graph,
                ));
            }
            let relation_key =
                canonical.clone().unwrap_or_else(|| authored.clone());
            if seen_outbound.insert(relation_key.clone()) {
                trigger_outbound.push(relation_key);
            }
            if let Some(canonical) = canonical.as_ref() {
                if let Some(sources) = inbound.get_mut(canonical) {
                    if !sources.iter().any(|source| source == trigger) {
                        sources.push(trigger.clone());
                    }
                }
            }
            trigger_calls.push(SnippetCall {
                authored_target: authored,
                canonical_target: canonical,
                positional_args: raw.positional_args.clone(),
                span,
                status,
            });
        }

        calls.insert(trigger.clone(), trigger_calls);
        outbound.insert(trigger.clone(), trigger_outbound);
    }

    for sources in inbound.values_mut() {
        sources.sort();
    }
    diagnostics.sort_by(|left, right| {
        (
            left.trigger.as_str(),
            left.span.map(|span| span.start).unwrap_or(0),
            left.span.map(|span| span.end).unwrap_or(0),
            left.code.as_str(),
            left.target.as_deref().unwrap_or(""),
        )
            .cmp(&(
                right.trigger.as_str(),
                right.span.map(|span| span.start).unwrap_or(0),
                right.span.map(|span| span.end).unwrap_or(0),
                right.code.as_str(),
                right.target.as_deref().unwrap_or(""),
            ))
    });

    SnippetRelationAnalysis {
        triggers,
        calls,
        outbound,
        inbound,
        diagnostics,
    }
}

fn invalid_trigger_message(
    trigger: &str,
    validation: &SnippetTriggerValidation,
) -> String {
    match validation.reason.as_deref() {
        Some("empty") => "snippet trigger is empty".to_string(),
        _ => format!(
            "snippet trigger '{trigger}' is not alphanumeric or underscore"
        ),
    }
}

fn canonical_target(
    authored: &str,
    explicit_templates: &BTreeMap<String, String>,
    alias_provenance: &BTreeMap<String, String>,
) -> Option<String> {
    if explicit_templates.contains_key(authored) {
        Some(authored.to_string())
    } else {
        alias_provenance.get(authored).cloned()
    }
}

fn call_status(
    source: &str,
    canonical: Option<&str>,
    reachable: &BTreeMap<String, BTreeSet<String>>,
) -> SnippetCallStatus {
    let Some(target) = canonical else {
        return SnippetCallStatus::Missing;
    };
    if reachable
        .get(target)
        .is_some_and(|nodes| nodes.contains(source))
    {
        SnippetCallStatus::Cycle
    } else {
        SnippetCallStatus::Resolved
    }
}

fn call_diagnostic(
    source: &str,
    authored: &str,
    canonical: Option<&str>,
    span: SnippetSourceSpan,
    status: SnippetCallStatus,
    graph: &BTreeMap<String, BTreeSet<String>>,
) -> SnippetDiagnostic {
    match status {
        SnippetCallStatus::Missing => SnippetDiagnostic {
            code: "missing_target".to_string(),
            message: format!(
                "snippet '{source}' calls missing target '{authored}'"
            ),
            trigger: source.to_string(),
            target: Some(authored.to_string()),
            span: Some(span),
            cycle: None,
        },
        SnippetCallStatus::Cycle => {
            let target = canonical.unwrap_or(authored);
            let cycle = cycle_path(source, target, graph);
            let code = if cycle.len() <= 1 {
                "direct_cycle"
            } else {
                "indirect_cycle"
            };
            SnippetDiagnostic {
                code: code.to_string(),
                message: format!(
                    "snippet '{source}' has a {kind} cyclic call to '{target}'",
                    kind = if cycle.len() <= 1 {
                        "direct"
                    } else {
                        "indirect"
                    },
                ),
                trigger: source.to_string(),
                target: Some(target.to_string()),
                span: Some(span),
                cycle: Some(cycle),
            }
        }
        SnippetCallStatus::Resolved => {
            unreachable!("resolved calls do not produce relation diagnostics")
        }
    }
}

fn cycle_path(
    source: &str,
    target: &str,
    graph: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    if source == target {
        return vec![source.to_string()];
    }
    let Some(path) = bfs_path(graph, target, source) else {
        return vec![source.to_string(), target.to_string()];
    };
    let mut cycle = vec![source.to_string()];
    cycle.extend(path.into_iter().filter(|node| node != source));
    cycle
}

fn reachable_from(
    graph: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut reachable = BTreeMap::new();
    for start in graph.keys() {
        let mut seen = BTreeSet::new();
        let mut stack = vec![start.clone()];
        while let Some(node) = stack.pop() {
            let Some(neighbors) = graph.get(&node) else {
                continue;
            };
            for next in neighbors {
                if seen.insert(next.clone()) {
                    stack.push(next.clone());
                }
            }
        }
        reachable.insert(start.clone(), seen);
    }
    reachable
}

fn bfs_path(
    graph: &BTreeMap<String, BTreeSet<String>>,
    start: &str,
    goal: &str,
) -> Option<Vec<String>> {
    if start == goal {
        return Some(vec![start.to_string()]);
    }
    let mut parent = BTreeMap::<String, String>::new();
    let mut seen = BTreeSet::from([start.to_string()]);
    let mut queue = VecDeque::from([start.to_string()]);
    while let Some(node) = queue.pop_front() {
        let Some(neighbors) = graph.get(&node) else {
            continue;
        };
        for next in neighbors {
            if !seen.insert(next.clone()) {
                continue;
            }
            parent.insert(next.clone(), node.clone());
            if next == goal {
                let mut path = vec![goal.to_string()];
                let mut cursor = goal.to_string();
                while cursor != start {
                    cursor = parent.get(&cursor)?.clone();
                    path.push(cursor.clone());
                }
                path.reverse();
                return Some(path);
            }
            queue.push_back(next.clone());
        }
    }
    None
}

fn resolve_snippet_references(
    catalog: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut resolver = SnippetReferenceResolver::new(catalog);
    catalog
        .keys()
        .map(|trigger| (trigger.clone(), resolver.resolve(trigger)))
        .collect()
}

fn uppercase_first_scalar(value: &str) -> String {
    let Some(first) = value.chars().next() else {
        return String::new();
    };
    let mut uppercased = String::with_capacity(value.len());
    uppercased.extend(first.to_uppercase());
    uppercased.push_str(&value[first.len_utf8()..]);
    uppercased
}

struct SnippetReferenceResolver<'a> {
    catalog: &'a BTreeMap<String, String>,
    memo: BTreeMap<String, String>,
}

struct RawSnippetCall {
    start: usize,
    end: usize,
    name: String,
    positional_args: Vec<String>,
}

impl<'a> SnippetReferenceResolver<'a> {
    fn new(catalog: &'a BTreeMap<String, String>) -> Self {
        Self {
            catalog,
            memo: BTreeMap::new(),
        }
    }

    fn resolve(&mut self, trigger: &str) -> String {
        if let Some(resolved) = self.memo.get(trigger) {
            return resolved.clone();
        }
        let Some(template) = self.catalog.get(trigger) else {
            return String::new();
        };
        let mut visiting = BTreeSet::new();
        visiting.insert(trigger.to_string());
        let resolved = self.resolve_template(template, &mut visiting);
        self.memo.insert(trigger.to_string(), resolved.clone());
        resolved
    }

    fn resolve_template(
        &mut self,
        template: &str,
        visiting: &mut BTreeSet<String>,
    ) -> String {
        let mut segments = Vec::<(usize, String)>::new();
        let mut next_source_id = 1usize;
        let mut used_reference = false;
        let mut literal_start = 0usize;

        for raw in iter_raw_snippet_calls(template) {
            if !self.catalog.contains_key(&raw.name)
                || visiting.contains(&raw.name)
            {
                continue;
            }

            if literal_start < raw.start {
                segments
                    .push((0, template[literal_start..raw.start].to_string()));
            }
            let target = self.resolve_reference(
                &raw.name,
                &raw.positional_args,
                visiting,
            );
            segments.push((next_source_id, target));
            next_source_id += 1;
            used_reference = true;
            literal_start = raw.end;
        }

        if !used_reference {
            return template.to_string();
        }
        if literal_start < template.len() {
            segments.push((0, template[literal_start..].to_string()));
        }
        renumber_snippet_segments(&segments)
    }

    fn resolve_reference(
        &mut self,
        trigger: &str,
        positional_args: &[String],
        visiting: &mut BTreeSet<String>,
    ) -> String {
        let template = if let Some(resolved) = self.memo.get(trigger) {
            resolved.clone()
        } else {
            visiting.insert(trigger.to_string());
            let resolved = self.resolve_template(
                self.catalog
                    .get(trigger)
                    .map(String::as_str)
                    .unwrap_or_default(),
                visiting,
            );
            visiting.remove(trigger);
            self.memo.insert(trigger.to_string(), resolved.clone());
            resolved
        };
        let fragment = remove_zero_tabstops(&template);
        apply_positional_args(&fragment, positional_args)
    }
}

fn iter_raw_snippet_calls(template: &str) -> Vec<RawSnippetCall> {
    let mut calls = Vec::new();
    let mut cursor = 0usize;
    while cursor < template.len() {
        if !starts_snippet_reference(template, cursor) {
            cursor += 1;
            continue;
        }
        let Some(close) = find_matching_bracket_for_args(template, cursor + 1)
        else {
            cursor += 1;
            continue;
        };
        let body = &template[cursor + 2..close];
        if let Some(reference) = parse_xprompt_reference_body(body) {
            calls.push(RawSnippetCall {
                start: cursor,
                end: close + 1,
                name: reference.name,
                positional_args: reference.positional_args,
            });
        }
        cursor = close + 1;
    }
    calls
}

fn starts_snippet_reference(text: &str, index: usize) -> bool {
    if text.as_bytes().get(index..index + 2) != Some(b"#[") {
        return false;
    }
    if index == 0 {
        return true;
    }
    text[..index].chars().next_back().is_some_and(|ch| {
        ch.is_whitespace() || matches!(ch, '(' | '[' | '{' | '"' | '\'')
    })
}

fn apply_positional_args(fragment: &str, positional_args: &[String]) -> String {
    if positional_args.is_empty() {
        return fragment.to_string();
    }

    let replacements = positional_args
        .iter()
        .enumerate()
        .map(|(index, value)| (index + 1, escape_snippet_arg(value)))
        .collect::<BTreeMap<_, _>>();
    let mut rendered = String::new();
    let mut cursor = 0usize;
    for (start, end, number) in iter_unescaped_tabstops(fragment) {
        rendered.push_str(&fragment[cursor..start]);
        if let Some(replacement) = replacements.get(&number) {
            rendered.push_str(replacement);
        } else {
            rendered.push_str(&fragment[start..end]);
        }
        cursor = end;
    }
    rendered.push_str(&fragment[cursor..]);
    rendered
}

fn escape_snippet_arg(value: &str) -> String {
    value.replace('$', "\\$")
}

fn remove_zero_tabstops(template: &str) -> String {
    let mut rendered = String::new();
    let mut cursor = 0usize;
    for (start, end, number) in iter_unescaped_tabstops(template) {
        rendered.push_str(&template[cursor..start]);
        if number != 0 {
            rendered.push_str(&template[start..end]);
        }
        cursor = end;
    }
    rendered.push_str(&template[cursor..]);
    rendered
}

fn renumber_snippet_segments(segments: &[(usize, String)]) -> String {
    let mut assignments = BTreeMap::<(usize, usize), usize>::new();
    let mut next_tabstop = 1usize;
    let mut rendered = String::new();

    for (source_id, text) in segments {
        let mut cursor = 0usize;
        for (start, end, number) in iter_unescaped_tabstops(text) {
            rendered.push_str(&text[cursor..start]);
            if number != 0 {
                let key = (*source_id, number);
                let assigned = assignments.entry(key).or_insert_with(|| {
                    let value = next_tabstop;
                    next_tabstop += 1;
                    value
                });
                rendered.push_str(&format!("${assigned}"));
            }
            cursor = end;
        }
        rendered.push_str(&text[cursor..]);
    }

    rendered.push_str("$0");
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(trigger, template)| {
                (trigger.to_string(), template.to_string())
            })
            .collect()
    }

    fn span_of(template: &str, call: &str) -> SnippetSourceSpan {
        let start = template.find(call).expect(call);
        SnippetSourceSpan {
            start,
            end: start + call.len(),
        }
    }

    #[test]
    fn composes_capitalized_aliases_and_preserves_remaining_case() {
        let composed =
            compose_snippet_catalog(&catalog(&[("foo", "foo bar BAZ")]));

        assert_eq!(composed.templates["foo"], "foo bar BAZ");
        assert_eq!(composed.templates["Foo"], "Foo bar BAZ");
        assert_eq!(composed.alias_provenance["Foo"], "foo");
    }

    #[test]
    fn composes_unicode_aliases_and_handles_unchanged_leading_scalars() {
        let composed = compose_snippet_catalog(&catalog(&[
            ("éclair", "élan suite"),
            ("ßeta", "ßeta suite"),
            ("Already", "already"),
            ("1digit", "digit"),
            ("_private", "private"),
            ("empty", ""),
            ("punct", "$1 lower$0"),
        ]));

        assert_eq!(composed.templates["Éclair"], "Élan suite");
        assert_eq!(composed.alias_provenance["Éclair"], "éclair");
        assert_eq!(composed.templates["SSeta"], "SSeta suite");
        assert_eq!(composed.alias_provenance["SSeta"], "ßeta");
        assert!(!composed.alias_provenance.contains_key("Already"));
        assert!(!composed.alias_provenance.contains_key("1digit"));
        assert!(!composed.alias_provenance.contains_key("_private"));
        assert_eq!(composed.templates["Empty"], "");
        assert_eq!(composed.templates["Punct"], "$1 lower$0");
        assert!(!composed.triggers["éclair"].valid);
        assert_eq!(
            composed.triggers["éclair"].reason.as_deref(),
            Some("invalid_characters")
        );
        assert!(composed.triggers["1digit"].valid);
        assert!(composed.triggers["_private"].valid);
    }

    #[test]
    fn explicit_capitalized_trigger_wins_alias_collision() {
        let composed = compose_snippet_catalog(&catalog(&[
            ("foo", "lower source"),
            ("Foo", "authored capital"),
        ]));

        assert_eq!(composed.templates["foo"], "lower source");
        assert_eq!(composed.templates["Foo"], "authored capital");
        assert!(composed.alias_provenance.is_empty());
        assert!(composed.triggers["foo"].valid);
        assert!(composed.triggers["Foo"].valid);
        assert!(composed.calls["Foo"].is_empty());
        assert!(composed.inbound["Foo"].is_empty());
    }

    #[test]
    fn aliases_use_composed_templates_and_references_can_target_aliases() {
        let composed = compose_snippet_catalog(&catalog(&[
            ("base", "base $1$0"),
            ("wrapper", "#[base] end $1$0"),
            ("capital_ref", "#[Base] then $1$0"),
        ]));

        assert_eq!(composed.templates["Base"], "Base $1$0");
        assert_eq!(composed.templates["wrapper"], "base $1 end $2$0");
        assert_eq!(composed.templates["Wrapper"], "Base $1 end $2$0");
        assert_eq!(composed.templates["capital_ref"], "Base $1 then $2$0");
        assert_eq!(composed.templates["Capital_ref"], "Base $1 then $2$0");

        let wrapper_call = &composed.calls["wrapper"][0];
        assert_eq!(wrapper_call.authored_target, "base");
        assert_eq!(wrapper_call.canonical_target.as_deref(), Some("base"));
        assert_eq!(wrapper_call.status, SnippetCallStatus::Resolved);
        let alias_call = &composed.calls["capital_ref"][0];
        assert_eq!(alias_call.authored_target, "Base");
        assert_eq!(alias_call.canonical_target.as_deref(), Some("base"));
        assert_eq!(alias_call.status, SnippetCallStatus::Resolved);
        assert_eq!(composed.outbound["capital_ref"], vec!["base"]);
        assert_eq!(composed.inbound["base"], vec!["capital_ref", "wrapper"]);
        assert!(!composed.calls.contains_key("Base"));
        assert!(composed.diagnostics.is_empty());
    }

    #[test]
    fn snippet_reference_golden_vectors() {
        // Cross-language parity contract: identical to the Python
        // `_SNIPPET_REFERENCE_GOLDEN_VECTORS` table.
        type SnippetEntry<'a> = (&'a str, &'a str);
        type SnippetGoldenCase<'a> = (Vec<SnippetEntry<'a>>, &'a str, &'a str);
        let cases: Vec<SnippetGoldenCase> = vec![
            (
                vec![
                    ("greet", "Hello $1!$0"),
                    ("welcome", "#[greet] Welcome to $1.$0"),
                ],
                "welcome",
                "Hello $1! Welcome to $2.$0",
            ),
            (
                vec![
                    ("pair", "$1 and $2$0"),
                    ("outer", "#[pair(a, b)] done$0"),
                ],
                "outer",
                "a and b done$0",
            ),
            (
                vec![("pair", "$1 and $2$0"), ("outer", "#[pair(a)] $1$0")],
                "outer",
                "a and $1 $2$0",
            ),
            (
                vec![("say", "$1 says hi, $1$0"), ("outer", "#[say] $1$0")],
                "outer",
                "$1 says hi, $1 $2$0",
            ),
            (
                vec![
                    ("a", "A $1$0"),
                    ("b", "B $1$0"),
                    ("outer", "#[a] #[b] $1$0"),
                ],
                "outer",
                "A $1 B $2 $3$0",
            ),
            (
                vec![
                    ("leaf", "Leaf $1$0"),
                    ("mid", "M #[leaf] $1$0"),
                    ("outer", "O #[mid] $1$0"),
                ],
                "outer",
                "O M Leaf $1 $2 $3$0",
            ),
            (
                vec![("outer", "#[missing] $1$0")],
                "outer",
                "#[missing] $1$0",
            ),
            (vec![("outer", "#[outer] $1$0")], "outer", "#[outer] $1$0"),
            (vec![("a", "#[b]$0"), ("b", "#[a]$0")], "a", "#[a]$0"),
            (
                vec![("greet", "Hello $1$0"), ("outer", "#[greet:World]$0")],
                "outer",
                "Hello World$0",
            ),
            (
                vec![
                    ("wrap", "<$1>$0"),
                    ("outer", "#[wrap([[multi, line]])] $1$0"),
                ],
                "outer",
                "<multi, line> $1$0",
            ),
            (
                vec![("user", "User $1$0"), ("xp", "#[user] xp $1$0")],
                "xp",
                "User $1 xp $2$0",
            ),
            (
                vec![("xp", "XP $1$0"), ("user", "User #[xp] $1$0")],
                "user",
                "User XP $1 $2$0",
            ),
            (
                vec![("bar", "BAR$0"), ("outer", "foo#[bar] #[bar]$0")],
                "outer",
                "foo#[bar] BAR$0",
            ),
            (
                vec![("price", "Cost $1$0"), ("outer", "#[price($5)] $1$0")],
                "outer",
                r"Cost \$5 $1$0",
            ),
        ];

        for (catalog_entries, trigger, expected) in cases {
            let composed = compose_snippet_catalog(&catalog(&catalog_entries));
            assert_eq!(composed.templates[trigger], expected, "{trigger}");
        }
    }

    #[test]
    fn analyzes_nested_positional_quoted_and_duplicate_calls() {
        let outer = "start #[mid] #[pair(a, b)] #[wrap([[multi, line]])] #[leaf] #[leaf]$0";
        let composed = compose_snippet_catalog(&catalog(&[
            ("leaf", "Leaf $1$0"),
            ("mid", "M #[leaf] $1$0"),
            ("pair", "$1 and $2$0"),
            ("wrap", "<$1>$0"),
            ("outer", outer),
        ]));

        assert_eq!(composed.calls["outer"].len(), 5);
        assert_eq!(composed.calls["outer"][0].authored_target, "mid");
        assert_eq!(composed.calls["outer"][1].positional_args, vec!["a", "b"]);
        assert_eq!(
            composed.calls["outer"][1].span,
            span_of(outer, "#[pair(a, b)]")
        );
        assert_eq!(
            composed.calls["outer"][2].positional_args,
            vec!["multi, line"]
        );
        assert_eq!(
            composed.calls["outer"][2].span,
            span_of(outer, "#[wrap([[multi, line]])]")
        );
        assert_eq!(composed.calls["outer"][3].authored_target, "leaf");
        assert_eq!(composed.calls["outer"][4].authored_target, "leaf");
        assert_eq!(
            composed.outbound["outer"],
            vec!["mid", "pair", "wrap", "leaf"]
        );
        assert_eq!(composed.calls["mid"][0].authored_target, "leaf");
        assert_eq!(composed.inbound["leaf"], vec!["mid", "outer"]);
        assert!(composed.diagnostics.is_empty());
    }

    #[test]
    fn colon_form_records_positional_argument() {
        let template = "#[greet:World]$0";
        let composed = compose_snippet_catalog(&catalog(&[
            ("greet", "Hello $1$0"),
            ("outer", template),
        ]));
        let call = &composed.calls["outer"][0];
        assert_eq!(call.authored_target, "greet");
        assert_eq!(call.positional_args, vec!["World"]);
        assert_eq!(call.span, span_of(template, "#[greet:World]"));
        assert_eq!(call.status, SnippetCallStatus::Resolved);
    }

    #[test]
    fn missing_target_and_boundary_rules() {
        let template = "foo#[missing] #[missing] #[gone]$0";
        let composed =
            compose_snippet_catalog(&catalog(&[("outer", template)]));

        assert_eq!(composed.templates["outer"], template);
        assert_eq!(composed.calls["outer"].len(), 2);
        assert_eq!(composed.calls["outer"][0].authored_target, "missing");
        assert!(composed.calls["outer"][0].canonical_target.is_none());
        assert_eq!(
            composed.calls["outer"][0].status,
            SnippetCallStatus::Missing
        );
        assert_eq!(composed.calls["outer"][1].authored_target, "gone");
        assert_eq!(composed.outbound["outer"], vec!["missing", "gone"]);
        assert_eq!(
            composed
                .diagnostics
                .iter()
                .map(|d| d.code.as_str())
                .collect::<Vec<_>>(),
            vec!["missing_target", "missing_target"]
        );
        let first_valid =
            template.find(" #[missing]").expect("bounded missing call") + 1;
        assert_eq!(
            composed.calls["outer"][0].span,
            SnippetSourceSpan {
                start: first_valid,
                end: first_valid + "#[missing]".len(),
            }
        );
    }

    #[test]
    fn direct_and_indirect_cycles() {
        let composed = compose_snippet_catalog(&catalog(&[
            ("selfish", "#[selfish] $1$0"),
            ("a", "#[b]$0"),
            ("b", "#[a]$0"),
            ("ok", "#[a] done$0"),
        ]));

        assert_eq!(
            composed.calls["selfish"][0].status,
            SnippetCallStatus::Cycle
        );
        assert_eq!(
            composed.calls["selfish"][0].canonical_target.as_deref(),
            Some("selfish")
        );
        assert_eq!(composed.calls["a"][0].status, SnippetCallStatus::Cycle);
        assert_eq!(composed.calls["b"][0].status, SnippetCallStatus::Cycle);
        assert_eq!(composed.calls["ok"][0].status, SnippetCallStatus::Resolved);
        assert_eq!(
            composed.calls["ok"][0].canonical_target.as_deref(),
            Some("a")
        );

        let by_trigger: BTreeMap<_, _> = composed
            .diagnostics
            .iter()
            .map(|d| (d.trigger.as_str(), d))
            .collect();
        assert_eq!(by_trigger["selfish"].code, "direct_cycle");
        assert_eq!(
            by_trigger["selfish"].cycle.as_deref(),
            Some(&["selfish".to_string()][..])
        );
        assert_eq!(by_trigger["a"].code, "indirect_cycle");
        assert_eq!(
            by_trigger["a"].cycle.as_deref(),
            Some(&["a".to_string(), "b".to_string()][..])
        );
        assert_eq!(
            by_trigger["b"].cycle.as_deref(),
            Some(&["b".to_string(), "a".to_string()][..])
        );
        assert!(!composed.diagnostics.iter().any(|d| d.trigger == "ok"));
    }

    #[test]
    fn alias_self_cycle_lands_on_explicit_identity() {
        let composed =
            compose_snippet_catalog(&catalog(&[("foo", "#[Foo]$0")]));
        let call = &composed.calls["foo"][0];
        assert_eq!(call.authored_target, "Foo");
        assert_eq!(call.canonical_target.as_deref(), Some("foo"));
        assert_eq!(call.status, SnippetCallStatus::Cycle);
        assert_eq!(composed.diagnostics[0].code, "direct_cycle");
        assert_eq!(
            composed.diagnostics[0].cycle.as_deref(),
            Some(&["foo".to_string()][..])
        );
        assert_eq!(composed.templates["foo"], "#[Foo]$0");
    }

    #[test]
    fn alias_pair_is_an_indirect_cycle_on_explicit_identities() {
        let composed = compose_snippet_catalog(&catalog(&[
            ("left", "#[Right]$0"),
            ("right", "#[Left]$0"),
        ]));
        assert_eq!(composed.calls["left"][0].authored_target, "Right");
        assert_eq!(
            composed.calls["left"][0].canonical_target.as_deref(),
            Some("right")
        );
        assert_eq!(composed.calls["left"][0].status, SnippetCallStatus::Cycle);
        assert_eq!(composed.calls["right"][0].status, SnippetCallStatus::Cycle);
        assert_eq!(
            composed.diagnostics[0].cycle.as_deref(),
            Some(&["left".to_string(), "right".to_string()][..])
        );
        assert_eq!(composed.diagnostics[0].code, "indirect_cycle");
        assert_eq!(composed.inbound["left"], vec!["right"]);
        assert_eq!(composed.inbound["right"], vec!["left"]);
    }

    #[test]
    fn inbound_ordering_is_deterministic_and_unique() {
        let composed = compose_snippet_catalog(&catalog(&[
            ("target", "body$0"),
            ("z_src", "#[target] #[target]$0"),
            ("a_src", "#[target]$0"),
            ("m_src", "#[Target]$0"),
        ]));

        assert_eq!(composed.inbound["target"], vec!["a_src", "m_src", "z_src"]);
        assert_eq!(composed.outbound["z_src"], vec!["target"]);
        assert_eq!(composed.calls["z_src"].len(), 2);
    }

    #[test]
    fn unicode_template_spans_use_byte_offsets() {
        let template = "café #[base] 🎉";
        let composed = compose_snippet_catalog(&catalog(&[
            ("base", "body$0"),
            ("outer", template),
        ]));
        let call = &composed.calls["outer"][0];
        let expected = span_of(template, "#[base]");
        assert_eq!(call.span, expected);
        assert_eq!(
            &template.as_bytes()[expected.start..expected.end],
            b"#[base]"
        );
        assert_eq!(call.status, SnippetCallStatus::Resolved);
    }

    #[test]
    fn validate_snippet_trigger_rejects_empty_and_punctuation() {
        assert!(!validate_snippet_trigger("").valid);
        assert_eq!(
            validate_snippet_trigger("").reason.as_deref(),
            Some("empty")
        );
        assert!(!validate_snippet_trigger("bad-name!").valid);
        assert_eq!(
            validate_snippet_trigger("bad-name!").reason.as_deref(),
            Some("invalid_characters")
        );
        assert!(validate_snippet_trigger("fix_it2").valid);
        assert!(is_valid_snippet_trigger("fix_it2"));
        assert!(!is_valid_snippet_trigger("bad-name!"));
    }

    #[test]
    fn invalid_trigger_does_not_change_expansion() {
        let composed = compose_snippet_catalog(&catalog(&[
            ("bad-name!", "still expands #[ok]$0"),
            ("ok", "OK$0"),
        ]));
        assert_eq!(composed.templates["bad-name!"], "still expands OK$0");
        assert!(!composed.triggers["bad-name!"].valid);
        assert_eq!(composed.diagnostics[0].code, "invalid_trigger");
        assert_eq!(
            composed.calls["bad-name!"][0].status,
            SnippetCallStatus::Resolved
        );
    }
}
