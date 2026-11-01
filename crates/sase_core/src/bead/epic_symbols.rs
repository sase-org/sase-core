//! Parsing and freshness assessment for Symvision ``--epic-symbol`` rows.

use std::collections::{BTreeMap, BTreeSet};

use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct EpicSymbolRowWire {
    pub bead_id: String,
    pub symbol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpicSymbolFreshnessKindWire {
    StaleCheckout,
    LocalInvalidRow,
    UpstreamUnresolved,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpicSymbolFreshnessRequestWire {
    pub local_rows: Vec<EpicSymbolRowWire>,
    pub head_rows: Option<Vec<EpicSymbolRowWire>>,
    pub statuses: BTreeMap<String, String>,
    pub local_head: Option<String>,
    pub upstream_revision: Option<String>,
    pub upstream_fetch_succeeded: bool,
    pub upstream_rows: Option<Vec<EpicSymbolRowWire>>,
    pub upstream_descends_from_head: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpicSymbolFreshnessAssessmentWire {
    pub bead_id: String,
    pub symbol: String,
    pub status: Option<String>,
    pub classification: EpicSymbolFreshnessKindWire,
    pub reason: String,
    pub local_head: Option<String>,
    pub upstream_revision: Option<String>,
    pub upstream_descends_from_head: Option<bool>,
}

/// Parse all supported quoted, bare, and ``=`` forms, skipping malformed tokens.
pub fn parse_epic_symbol_entries(text: &str) -> Vec<EpicSymbolRowWire> {
    let token =
        Regex::new(r#"--epic-symbol(?:\s+|=)(?:"([^"]+)"|'([^']+)'|(\S+))"#)
            .expect("epic-symbol token regex is valid");
    token
        .captures_iter(text)
        .filter_map(|captures| {
            let body = captures
                .get(1)
                .or_else(|| captures.get(2))
                .or_else(|| captures.get(3))?
                .as_str();
            parse_body(body)
        })
        .collect()
}

fn parse_body(body: &str) -> Option<EpicSymbolRowWire> {
    let (bead_id, suffix) = body.split_once('(')?;
    let symbol = suffix.strip_suffix(')')?;
    if bead_id.is_empty()
        || bead_id.chars().any(char::is_whitespace)
        || symbol.is_empty()
        || symbol.contains(')')
    {
        return None;
    }
    Some(EpicSymbolRowWire {
        bead_id: bead_id.to_string(),
        symbol: symbol.to_string(),
    })
}

/// Classify known closed-bead rows using only explicitly supplied evidence.
pub fn assess_epic_symbol_freshness(
    request: &EpicSymbolFreshnessRequestWire,
) -> Vec<EpicSymbolFreshnessAssessmentWire> {
    let local_rows = unique_rows(&request.local_rows);
    let head_rows = request.head_rows.as_ref().map(|rows| unique_row_set(rows));
    let upstream_rows = request
        .upstream_rows
        .as_ref()
        .map(|rows| unique_row_set(rows));

    local_rows
        .into_iter()
        .filter_map(|row| {
            let status = request.statuses.get(&row.bead_id).cloned();
            if status.as_deref().is_some_and(|status| status != "closed") {
                return None;
            }

            let row_key = (row.bead_id.clone(), row.symbol.clone());
            let upstream_is_fresh = request.upstream_fetch_succeeded
                && request.upstream_revision.is_some()
                && upstream_rows.is_some();

            let (classification, reason) = if status.is_none() {
                (
                    EpicSymbolFreshnessKindWire::Uncertain,
                    "bead status is unavailable; preserve Symvision's status lookup behavior"
                        .to_string(),
                )
            } else if upstream_is_fresh
                && upstream_rows
                    .as_ref()
                    .is_some_and(|rows| rows.contains(&row_key))
            {
                (
                    EpicSymbolFreshnessKindWire::UpstreamUnresolved,
                    "the freshly fetched upstream Justfile still contains this closed-bead row"
                        .to_string(),
                )
            } else if head_rows
                .as_ref()
                .is_some_and(|rows| !rows.contains(&row_key))
            {
                (
                    EpicSymbolFreshnessKindWire::LocalInvalidRow,
                    "the closed-bead row is absent from local HEAD and was introduced in this checkout"
                        .to_string(),
                )
            } else if !request.upstream_fetch_succeeded {
                (
                    EpicSymbolFreshnessKindWire::Uncertain,
                    "the default branch could not be fetched; upstream cleanliness is unknown"
                        .to_string(),
                )
            } else if request.upstream_revision.is_none() || upstream_rows.is_none() {
                (
                    EpicSymbolFreshnessKindWire::Uncertain,
                    "the fetched upstream revision or its Justfile rows are unavailable"
                        .to_string(),
                )
            } else if head_rows.is_none() || request.local_head.is_none() {
                (
                    EpicSymbolFreshnessKindWire::Uncertain,
                    "local HEAD history or its Justfile rows are unavailable".to_string(),
                )
            } else if request.upstream_descends_from_head == Some(false) {
                (
                    EpicSymbolFreshnessKindWire::Uncertain,
                    "the fetched default branch does not descend from local HEAD".to_string(),
                )
            } else if request.upstream_descends_from_head != Some(true) {
                (
                    EpicSymbolFreshnessKindWire::Uncertain,
                    "Git could not establish whether the fetched default branch descends from local HEAD"
                        .to_string(),
                )
            } else if head_rows
                .as_ref()
                .is_some_and(|rows| rows.contains(&row_key))
                && upstream_rows
                    .as_ref()
                    .is_some_and(|rows| !rows.contains(&row_key))
            {
                (
                    EpicSymbolFreshnessKindWire::StaleCheckout,
                    "the row is inherited from local HEAD, while a descendant upstream revision has removed it"
                        .to_string(),
                )
            } else {
                (
                    EpicSymbolFreshnessKindWire::Uncertain,
                    "the supplied evidence does not prove whether this row is stale"
                        .to_string(),
                )
            };

            Some(EpicSymbolFreshnessAssessmentWire {
                bead_id: row.bead_id,
                symbol: row.symbol,
                status,
                classification,
                reason,
                local_head: request.local_head.clone(),
                upstream_revision: request.upstream_revision.clone(),
                upstream_descends_from_head: request.upstream_descends_from_head,
            })
        })
        .collect()
}

fn unique_rows(rows: &[EpicSymbolRowWire]) -> Vec<EpicSymbolRowWire> {
    let mut seen = BTreeSet::new();
    rows.iter()
        .filter(|row| seen.insert((row.bead_id.clone(), row.symbol.clone())))
        .cloned()
        .collect()
}

fn unique_row_set(rows: &[EpicSymbolRowWire]) -> BTreeSet<(String, String)> {
    rows.iter()
        .map(|row| (row.bead_id.clone(), row.symbol.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(bead_id: &str, symbol: &str) -> EpicSymbolRowWire {
        EpicSymbolRowWire {
            bead_id: bead_id.to_string(),
            symbol: symbol.to_string(),
        }
    }

    fn request() -> EpicSymbolFreshnessRequestWire {
        EpicSymbolFreshnessRequestWire {
            local_rows: vec![row("sase-1", "OldSymbol")],
            head_rows: Some(vec![row("sase-1", "OldSymbol")]),
            statuses: BTreeMap::from([(
                "sase-1".to_string(),
                "closed".to_string(),
            )]),
            local_head: Some("abc123".to_string()),
            upstream_revision: Some("def456".to_string()),
            upstream_fetch_succeeded: true,
            upstream_rows: Some(vec![]),
            upstream_descends_from_head: Some(true),
        }
    }

    #[test]
    fn parser_preserves_supported_forms_and_skips_malformed_tokens() {
        let parsed = parse_epic_symbol_entries(
            r#"--epic-symbol "sase-nb(encode_feature_flags_env)" --epic-symbol 'sase-o8.4(PlaceholderRankingMetadata)' --epic-symbol=sase-n4(UsageLimitSettings) --epic-symbol sase-n4.5(ProviderDisableWriteOutcome) --epic-symbol "not-a-body" --epic-symbol "sase-x(Foo""#,
        );
        assert_eq!(
            parsed,
            vec![
                row("sase-nb", "encode_feature_flags_env"),
                row("sase-o8.4", "PlaceholderRankingMetadata"),
                row("sase-n4", "UsageLimitSettings"),
                row("sase-n4.5", "ProviderDisableWriteOutcome"),
            ]
        );
    }

    #[test]
    fn classifies_inherited_row_removed_from_descendant_upstream() {
        let result = assess_epic_symbol_freshness(&request());
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].classification,
            EpicSymbolFreshnessKindWire::StaleCheckout
        );
        assert_eq!(result[0].local_head.as_deref(), Some("abc123"));
        assert_eq!(result[0].upstream_revision.as_deref(), Some("def456"));
    }

    #[test]
    fn distinguishes_upstream_row_and_locally_introduced_row() {
        let mut upstream = request();
        upstream.upstream_rows = Some(vec![row("sase-1", "OldSymbol")]);
        assert_eq!(
            assess_epic_symbol_freshness(&upstream)[0].classification,
            EpicSymbolFreshnessKindWire::UpstreamUnresolved
        );

        let mut local = request();
        local.head_rows = Some(vec![]);
        assert_eq!(
            assess_epic_symbol_freshness(&local)[0].classification,
            EpicSymbolFreshnessKindWire::LocalInvalidRow
        );
    }

    #[test]
    fn open_or_unknown_status_and_uncertain_history_are_not_stale_proof() {
        let mut open = request();
        open.statuses
            .insert("sase-1".to_string(), "open".to_string());
        assert!(assess_epic_symbol_freshness(&open).is_empty());

        let mut unknown = request();
        unknown.statuses.clear();
        assert_eq!(
            assess_epic_symbol_freshness(&unknown)[0].classification,
            EpicSymbolFreshnessKindWire::Uncertain
        );

        let mut missing_objects = request();
        missing_objects.upstream_fetch_succeeded = false;
        missing_objects.upstream_revision = None;
        missing_objects.upstream_rows = None;
        assert_eq!(
            assess_epic_symbol_freshness(&missing_objects)[0].classification,
            EpicSymbolFreshnessKindWire::Uncertain
        );

        let mut missing_fetched_tree = request();
        missing_fetched_tree.upstream_rows = None;
        assert_eq!(
            assess_epic_symbol_freshness(&missing_fetched_tree)[0]
                .classification,
            EpicSymbolFreshnessKindWire::Uncertain
        );

        let mut unavailable_head = request();
        unavailable_head.head_rows = None;
        unavailable_head.local_head = None;
        unavailable_head.upstream_descends_from_head = None;
        assert_eq!(
            assess_epic_symbol_freshness(&unavailable_head)[0].classification,
            EpicSymbolFreshnessKindWire::Uncertain
        );

        let mut divergent = request();
        divergent.upstream_descends_from_head = Some(false);
        assert_eq!(
            assess_epic_symbol_freshness(&divergent)[0].classification,
            EpicSymbolFreshnessKindWire::Uncertain
        );
    }

    #[test]
    fn deduplicates_rows_for_the_same_closed_bead_and_symbol() {
        let mut duplicated = request();
        duplicated.local_rows.push(row("sase-1", "OldSymbol"));
        assert_eq!(assess_epic_symbol_freshness(&duplicated).len(), 1);
    }
}
