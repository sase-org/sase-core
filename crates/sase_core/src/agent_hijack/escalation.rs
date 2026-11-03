//! Never-downgrade escalation policy for hijacked agents.
//!
//! Pure policy: rung lookup, then target selection. Python resolves
//! `alias_candidates` from `models.yml` (already filtered for provider
//! disables); this module only orders and compares plain data.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Wire schema version for the escalation request/result types.
pub const AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION: u32 = 1;

/// Model facts recorded at hijack time, mirroring `agent_meta.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackModelFactsWire {
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub alias_trail: Vec<String>,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
}

/// Escalation request: facts plus the ladder and pre-resolved candidates.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackEscalationRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub model_facts: AgentHijackModelFactsWire,
    #[serde(default)]
    pub ladder: Vec<String>,
    #[serde(default)]
    pub alias_candidates: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub lineage_to_rung: Option<String>,
    #[serde(default)]
    pub stalled_concrete: Option<String>,
}

/// Escalation result: either a concrete target or a disabled reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackEscalationResultWire {
    pub schema_version: u32,
    pub available: bool,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub from_rung: Option<String>,
    #[serde(default)]
    pub to_rung: Option<String>,
    #[serde(default)]
    pub concrete: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Normalize `large` ↔ `@large` to the `@`-prefixed canonical form.
pub fn normalize_hijack_rung(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with('@') {
        trimmed.to_owned()
    } else if trimmed.is_empty() {
        String::new()
    } else {
        format!("@{trimmed}")
    }
}

/// Split `provider/model@effort` into `(provider/model, effort)`.
///
/// Only a trailing `@` token that looks like an effort level is treated as
/// effort; anything else stays in the concrete string.
fn split_concrete_effort(concrete: &str) -> (&str, Option<&str>) {
    match concrete.rfind('@') {
        Some(at) if at > 0 => {
            let candidate = &concrete[at + 1..];
            if is_effort_like(candidate) {
                (&concrete[..at], Some(candidate))
            } else {
                (concrete, None)
            }
        }
        _ => (concrete, None),
    }
}

fn is_effort_like(token: &str) -> bool {
    matches!(
        token,
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
    )
}

/// Split `provider/model` into `(provider, model)`.
fn split_provider_model(base: &str) -> (Option<&str>, &str) {
    match base.split_once('/') {
        Some((provider, model))
            if !provider.is_empty() && !model.is_empty() =>
        {
            (Some(provider), model)
        }
        _ => (None, base),
    }
}

fn stalled_concrete_for(request: &AgentHijackEscalationRequestWire) -> String {
    if let Some(concrete) = request.stalled_concrete.as_deref() {
        let trimmed = concrete.trim().to_owned();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    let facts = &request.model_facts;
    match (facts.provider.as_deref(), facts.model.as_deref()) {
        (Some(provider), Some(model))
            if !provider.trim().is_empty() && !model.trim().is_empty() =>
        {
            let base = if model.contains('/') {
                model.trim().to_owned()
            } else {
                format!("{}/{}", provider.trim(), model.trim())
            };
            match facts.effort.as_deref() {
                Some(effort) if !effort.trim().is_empty() => {
                    format!("{}@{}", base, effort.trim())
                }
                _ => base,
            }
        }
        (None, Some(model)) if !model.trim().is_empty() => {
            let base = model.trim().to_owned();
            match facts.effort.as_deref() {
                Some(effort) if !effort.trim().is_empty() => {
                    format!("{}@{}", base, effort.trim())
                }
                _ => base,
            }
        }
        _ => String::new(),
    }
}

/// Rung lookup, in order: lineage, then highest ladder rung in
/// alias/alias_trail, then highest rung whose candidate set contains the
/// stalled concrete. Returns the ladder index of the current rung, if known.
fn current_rung_index(
    request: &AgentHijackEscalationRequestWire,
) -> Option<usize> {
    let ladder: Vec<String> = request
        .ladder
        .iter()
        .map(|r| normalize_hijack_rung(r))
        .collect();
    if ladder.is_empty() {
        return None;
    }
    // 1. Successor lineage.
    if let Some(lineage) = request.lineage_to_rung.as_deref() {
        let normalized = normalize_hijack_rung(lineage);
        if let Some(index) =
            ladder.iter().position(|entry| *entry == normalized)
        {
            return Some(index);
        }
    }
    // 2. Highest configured ladder rung in alias / trail (aliases stored
    // without `@`).
    let facts = &request.model_facts;
    let mut candidates: Vec<String> = Vec::new();
    if let Some(alias) = facts.alias.as_deref() {
        candidates.push(alias.to_owned());
    }
    candidates.extend(facts.alias_trail.iter().cloned());
    let mut best: Option<usize> = None;
    for candidate in candidates {
        let normalized = normalize_hijack_rung(&candidate);
        if let Some(index) = ladder.iter().position(|e| *e == normalized) {
            best = Some(best.map_or(index, |b: usize| b.max(index)));
        }
    }
    if best.is_some() {
        return best;
    }
    // 3. Highest rung whose candidate set contains the stalled concrete.
    let stalled = stalled_concrete_for(request);
    if !stalled.is_empty() {
        for (index, rung) in ladder.iter().enumerate().rev() {
            let key_variants =
                [rung.clone(), rung.trim_start_matches('@').to_owned()];
            for key in key_variants {
                if let Some(list) = request.alias_candidates.get(&key) {
                    if list.iter().any(|c| c.trim() == stalled.trim()) {
                        return Some(index);
                    }
                }
            }
        }
    }
    None
}

fn candidates_for<'a>(
    request: &'a AgentHijackEscalationRequestWire,
    rung: &str,
) -> Vec<&'a String> {
    let bare = rung.trim_start_matches('@').to_owned();
    let mut out: Vec<&String> = Vec::new();
    for key in [rung.to_owned(), bare] {
        if let Some(list) = request.alias_candidates.get(&key) {
            out.extend(list.iter());
        }
    }
    out
}

fn disabled(reason: &str) -> AgentHijackEscalationResultWire {
    AgentHijackEscalationResultWire {
        schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
        available: false,
        kind: None,
        from_rung: None,
        to_rung: None,
        concrete: None,
        reason: Some(reason.to_owned()),
    }
}

/// Compute the never-downgrade escalation target for a hijacked run.
pub fn agent_hijack_escalation_target(
    request: &AgentHijackEscalationRequestWire,
) -> AgentHijackEscalationResultWire {
    let ladder: Vec<String> = request
        .ladder
        .iter()
        .map(|r| normalize_hijack_rung(r))
        .collect();
    if ladder.is_empty() {
        return disabled("escalation ladder is empty");
    }
    let stalled = stalled_concrete_for(request);
    let (stalled_base, _) = split_concrete_effort(&stalled);
    let (stalled_provider, stalled_model) = split_provider_model(stalled_base);

    let current = current_rung_index(request);
    let (from_rung, to_rung, kind) = match current {
        None => (None, ladder[0].clone(), "escalate".to_owned()),
        Some(index) if index + 1 < ladder.len() => (
            Some(ladder[index].clone()),
            ladder[index + 1].clone(),
            "escalate".to_owned(),
        ),
        Some(index) => (
            Some(ladder[index].clone()),
            ladder[index].clone(),
            "switch".to_owned(),
        ),
    };

    let candidates = candidates_for(request, &to_rung);
    if candidates.is_empty() {
        return disabled(
            "no different model available; use Continue or Instruct",
        );
    }

    if kind == "switch" {
        for candidate in candidates {
            let (base, _) = split_concrete_effort(candidate.trim());
            let (provider, model) = split_provider_model(base);
            if provider != stalled_provider || model != stalled_model {
                // Skip candidates identical to the stalled run as well: a
                // provider/model match already implies it, but an empty
                // stalled concrete must not match everything.
                if candidate.trim() == stalled.trim() {
                    continue;
                }
                return AgentHijackEscalationResultWire {
                    schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
                    available: true,
                    kind: Some(kind),
                    from_rung,
                    to_rung: Some(to_rung),
                    concrete: Some(candidate.trim().to_owned()),
                    reason: None,
                };
            }
        }
        return disabled(
            "no different model available; use Continue or Instruct",
        );
    }

    for candidate in candidates {
        if candidate.trim() == stalled.trim() {
            continue;
        }
        // Never return the identical concrete model for escalate either.
        return AgentHijackEscalationResultWire {
            schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
            available: true,
            kind: Some(kind),
            from_rung,
            to_rung: Some(to_rung),
            concrete: Some(candidate.trim().to_owned()),
            reason: None,
        };
    }
    disabled("no different model available; use Continue or Instruct")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn req(
        facts: AgentHijackModelFactsWire,
        ladder: Vec<&str>,
        candidates: Vec<(&str, Vec<&str>)>,
        lineage: Option<&str>,
        stalled: Option<&str>,
    ) -> AgentHijackEscalationRequestWire {
        AgentHijackEscalationRequestWire {
            schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
            model_facts: facts,
            ladder: ladder.into_iter().map(str::to_owned).collect(),
            alias_candidates: candidates
                .into_iter()
                .map(|(k, v)| {
                    (k.to_owned(), v.into_iter().map(str::to_owned).collect())
                })
                .collect(),
            lineage_to_rung: lineage.map(str::to_owned),
            stalled_concrete: stalled.map(str::to_owned),
        }
    }

    fn facts(
        alias: Option<&str>,
        trail: Vec<&str>,
        provider: Option<&str>,
        model: Option<&str>,
        effort: Option<&str>,
    ) -> AgentHijackModelFactsWire {
        AgentHijackModelFactsWire {
            alias: alias.map(str::to_owned),
            alias_trail: trail.into_iter().map(str::to_owned).collect(),
            origin: None,
            provider: provider.map(str::to_owned),
            model: model.map(str::to_owned),
            effort: effort.map(str::to_owned),
        }
    }

    fn ladder_candidates() -> Vec<(&'static str, Vec<&'static str>)> {
        vec![
            (
                "@large",
                vec![
                    "claude/opus@high",
                    "codex/gpt-6.1-sol@xhigh",
                    "grok/grok-4.7@xhigh",
                ],
            ),
            (
                "@xlarge",
                vec![
                    "claude/opus@xhigh",
                    "codex/gpt-6.1-sol@xhigh",
                    "grok/grok-4.7@xhigh",
                ],
            ),
        ]
    }

    #[test]
    fn below_first_rung_goes_to_first_rung() {
        for alias in ["xsmall", "medium", "@xsmall", "@medium"] {
            let r = req(
                facts(
                    Some(alias),
                    vec![],
                    Some("claude"),
                    Some("x"),
                    Some("low"),
                ),
                vec!["@large", "@xlarge"],
                ladder_candidates(),
                None,
                None,
            );
            let out = agent_hijack_escalation_target(&r);
            assert!(out.available, "alias {alias}");
            assert_eq!(out.kind.as_deref(), Some("escalate"));
            assert_eq!(out.to_rung.as_deref(), Some("@large"));
            assert_eq!(out.concrete.as_deref(), Some("claude/opus@high"));
        }
    }

    #[test]
    fn on_large_goes_to_xlarge() {
        let r = req(
            facts(
                Some("large"),
                vec![],
                Some("claude"),
                Some("opus"),
                Some("high"),
            ),
            vec!["@large", "@xlarge"],
            ladder_candidates(),
            None,
            Some("claude/opus@high"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.kind.as_deref(), Some("escalate"));
        assert_eq!(out.from_rung.as_deref(), Some("@large"));
        assert_eq!(out.to_rung.as_deref(), Some("@xlarge"));
        assert_eq!(out.concrete.as_deref(), Some("claude/opus@xhigh"));
    }

    #[test]
    fn on_top_rung_switches_provider() {
        let r = req(
            facts(
                Some("xlarge"),
                vec![],
                Some("claude"),
                Some("opus"),
                Some("xhigh"),
            ),
            vec!["@large", "@xlarge"],
            ladder_candidates(),
            None,
            Some("claude/opus@xhigh"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.kind.as_deref(), Some("switch"));
        assert_eq!(out.to_rung.as_deref(), Some("@xlarge"));
        // First candidate identical to the stalled run is skipped.
        assert_eq!(out.concrete.as_deref(), Some("codex/gpt-6.1-sol@xhigh"));
    }

    #[test]
    fn explicit_concrete_uses_candidate_membership() {
        // No alias at all; concrete lives in @large so current rung is @large.
        let r = req(
            facts(
                None,
                vec![],
                Some("codex"),
                Some("gpt-6.1-sol"),
                Some("xhigh"),
            ),
            vec!["@large", "@xlarge"],
            ladder_candidates(),
            None,
            Some("codex/gpt-6.1-sol@xhigh"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        // codex/gpt-6.1-sol@xhigh is in both rungs; highest match is @xlarge,
        // so the result is a switch to the first differing provider/model.
        assert_eq!(out.kind.as_deref(), Some("switch"));
        assert_eq!(out.concrete.as_deref(), Some("claude/opus@xhigh"));
    }

    #[test]
    fn custom_alias_trail_selects_highest_rung() {
        let r = req(
            facts(
                Some("custom-thing"),
                vec!["team-large", "large"],
                Some("x"),
                Some("y"),
                Some("high"),
            ),
            vec!["@large", "@xlarge"],
            ladder_candidates(),
            None,
            Some("x/y@high"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.from_rung.as_deref(), Some("@large"));
        assert_eq!(out.to_rung.as_deref(), Some("@xlarge"));
    }

    #[test]
    fn unknown_provenance_goes_to_first_rung() {
        let r = req(
            facts(None, vec![], None, None, None),
            vec!["@large", "@xlarge"],
            ladder_candidates(),
            None,
            Some("mystery/model@low"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.kind.as_deref(), Some("escalate"));
        assert_eq!(out.to_rung.as_deref(), Some("@large"));
    }

    #[test]
    fn lineage_wins_over_alias() {
        let r = req(
            facts(Some("xsmall"), vec![], Some("a"), Some("b"), Some("low")),
            vec!["@large", "@xlarge"],
            ladder_candidates(),
            Some("@large"),
            Some("a/b@low"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.from_rung.as_deref(), Some("@large"));
        assert_eq!(out.to_rung.as_deref(), Some("@xlarge"));
    }

    #[test]
    fn identical_candidate_is_skipped() {
        // On @large with stalled a/b@high, the @xlarge first candidate is
        // identical, so escalate skips it for the next one.
        let mut map = BTreeMap::new();
        map.insert(
            "@large".to_owned(),
            vec!["a/b@high".to_owned(), "c/d@high".to_owned()],
        );
        map.insert(
            "@xlarge".to_owned(),
            vec!["a/b@high".to_owned(), "e/f@xhigh".to_owned()],
        );
        let r = AgentHijackEscalationRequestWire {
            schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
            model_facts: facts(
                Some("large"),
                vec![],
                Some("a"),
                Some("b"),
                Some("high"),
            ),
            ladder: vec!["@large".to_owned(), "@xlarge".to_owned()],
            alias_candidates: map,
            lineage_to_rung: None,
            stalled_concrete: Some("a/b@high".to_owned()),
        };
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.kind.as_deref(), Some("escalate"));
        assert_eq!(out.to_rung.as_deref(), Some("@xlarge"));
        assert_eq!(out.concrete.as_deref(), Some("e/f@xhigh"));
    }

    #[test]
    fn no_differing_candidate_disables() {
        let mut map = BTreeMap::new();
        map.insert("@xlarge".to_owned(), vec!["only/one@xhigh".to_owned()]);
        let r = AgentHijackEscalationRequestWire {
            schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
            model_facts: facts(
                Some("xlarge"),
                vec![],
                Some("only"),
                Some("one"),
                Some("xhigh"),
            ),
            ladder: vec!["@large".to_owned(), "@xlarge".to_owned()],
            alias_candidates: map,
            lineage_to_rung: None,
            stalled_concrete: Some("only/one@xhigh".to_owned()),
        };
        let out = agent_hijack_escalation_target(&r);
        assert!(!out.available);
        assert_eq!(
            out.reason.as_deref(),
            Some("no different model available; use Continue or Instruct")
        );
    }

    #[test]
    fn bare_rung_names_normalize() {
        let r = req(
            facts(Some("large"), vec![], Some("a"), Some("b"), Some("high")),
            vec!["large", "xlarge"],
            vec![("large", vec!["a/b@high"]), ("xlarge", vec!["a/b@xhigh"])],
            None,
            Some("a/b@high"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.to_rung.as_deref(), Some("@xlarge"));
    }

    #[test]
    fn default_model_origin_still_escalates() {
        let mut f =
            facts(None, vec![], Some("claude"), Some("opus"), Some("high"));
        f.origin = Some("default_model".to_owned());
        let r = req(
            f,
            vec!["@large", "@xlarge"],
            ladder_candidates(),
            None,
            Some("claude/opus@high"),
        );
        let out = agent_hijack_escalation_target(&r);
        assert!(out.available);
        assert_eq!(out.kind.as_deref(), Some("escalate"));
    }
}
