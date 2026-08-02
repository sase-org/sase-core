use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{prompt_literal_zone_ranges, scan_artifact_refs};

use super::fuzzy::{
    compare_fuzzy_prepared, compare_prepared_text, fuzzy_match_prepared,
    FuzzyQuery, FuzzyText,
};
use super::token::DocumentSnapshot;
use super::wire::EditorPosition;
use super::{compare_fuzzy, fuzzy_match, FuzzyMatch};

const ARTIFACT_REF_PREFIX_LOOKBACK: usize = 128;
pub const AT_REFERENCE_MAX_GROUP_ROWS: usize = 200;
/// Builtin artifact reference kinds, in menu-ranking order.
///
/// New kinds are appended so existing rows keep their position. This is the
/// single source of truth; nothing else may hardcode the list.
pub const BUILTIN_ARTIFACT_REF_KINDS: &[&str] =
    &["commit", "chat", "bug", "file", "bead", "agent"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtReferenceStage {
    Kind,
    Payload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtReferenceGroup {
    Artifact,
    File,
    Payload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferencePathQueryWire {
    pub directory: String,
    pub partial: String,
    pub show_hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceContextWire {
    pub stage: AtReferenceStage,
    pub candidate_span: (usize, usize),
    pub replacement_span: (usize, usize),
    pub query_span: (usize, usize),
    pub query: String,
    pub kind: Option<String>,
    pub path_query: Option<AtReferencePathQueryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceKindRowWire {
    pub kind: String,
    pub builtin: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferencePathRowWire {
    pub name: String,
    pub is_dir: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferencePayloadRowWire {
    pub payload: String,
    pub label: String,
    pub detail: String,
    pub age: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub rank: Option<u32>,
    #[serde(default)]
    pub body: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceInventoryWire {
    pub kinds: Vec<AtReferenceKindRowWire>,
    pub paths: Vec<AtReferencePathRowWire>,
    pub payloads: Vec<AtReferencePayloadRowWire>,
    #[serde(default)]
    pub truncated_payloads: usize,
}

/// Caller policy for one at-reference menu build.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(default)]
pub struct AtReferenceMenuOptionsWire {
    /// Include Kind-stage local path rows even when an artifact kind
    /// prefix-matches the query. Set by an explicit user request.
    pub include_files: bool,
}

/// Payload rows and their reusable native fuzzy-match metadata.
#[derive(Clone, Debug)]
pub struct AtReferencePayloadIndex {
    rows: Vec<IndexedPayloadRow>,
}

#[derive(Clone, Debug)]
struct IndexedPayloadRow {
    row: AtReferencePayloadRowWire,
    payload: FuzzyText,
    title: FuzzyText,
    qualified: Option<IndexedQualifiedPayload>,
}

#[derive(Clone, Debug)]
struct IndexedQualifiedPayload {
    text: String,
    fuzzy: FuzzyText,
}

impl AtReferencePayloadIndex {
    pub fn new(rows: Vec<AtReferencePayloadRowWire>) -> Self {
        Self {
            rows: rows
                .into_iter()
                .map(|row| {
                    let qualified = qualified_payload_text(&row).map(|text| {
                        IndexedQualifiedPayload {
                            fuzzy: FuzzyText::new(&text),
                            text,
                        }
                    });
                    IndexedPayloadRow {
                        payload: FuzzyText::new(&row.payload),
                        title: FuzzyText::new(&row.label),
                        qualified,
                        row,
                    }
                })
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceRowWire {
    pub group: AtReferenceGroup,
    pub label: String,
    pub title: String,
    pub insertion: String,
    pub is_dir: bool,
    pub detail: String,
    pub builtin: bool,
    pub label_match: Vec<(u32, u32)>,
    pub title_match: Vec<(u32, u32)>,
    pub match_tier: u8,
    #[serde(default)]
    pub body: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceMenuWire {
    pub rows: Vec<AtReferenceRowWire>,
    pub shared_extension: String,
    pub artifact_count: usize,
    pub file_count: usize,
    pub payload_count: usize,
    pub truncated_payloads: usize,
    /// Kind-stage path rows existed but were withheld by the gate.
    #[serde(default)]
    pub files_suppressed: bool,
}

pub fn detect_at_reference_context(
    document: &DocumentSnapshot,
    position: EditorPosition,
    _known_kinds: &[String],
) -> Option<AtReferenceContextWire> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let literal_ranges = prompt_literal_zone_ranges(text);

    if let Some(candidate) =
        scan_artifact_refs(text).into_iter().find(|candidate| {
            cursor >= candidate.sigil_span.end
                && cursor <= candidate.candidate_span.end
                && !literal_ranges.iter().any(|literal| {
                    ranges_intersect(
                        (
                            candidate.candidate_span.start,
                            candidate.candidate_span.end,
                        ),
                        *literal,
                    )
                })
        })
    {
        let candidate_span =
            (candidate.candidate_span.start, candidate.candidate_span.end);
        let (stage, replacement_span, query_span, kind) =
            if cursor <= candidate.separator_span.start {
                (
                    AtReferenceStage::Kind,
                    (candidate.kind_span.start, candidate.separator_span.end),
                    (
                        candidate.kind_span.start,
                        cursor.min(candidate.kind_span.end),
                    ),
                    None,
                )
            } else {
                (
                    AtReferenceStage::Payload,
                    (candidate.payload_span.start, candidate.payload_span.end),
                    (
                        candidate.payload_span.start,
                        cursor.min(candidate.payload_span.end),
                    ),
                    Some(candidate.kind),
                )
            };
        return context_from_spans(
            text,
            stage,
            candidate_span,
            replacement_span,
            query_span,
            kind,
        );
    }

    let (candidate_start, candidate_end) =
        incomplete_at_reference_candidate(text, cursor)?;
    if literal_ranges.iter().any(|literal| {
        ranges_intersect((candidate_start, candidate_end), *literal)
    }) {
        return None;
    }
    context_from_spans(
        text,
        AtReferenceStage::Kind,
        (candidate_start, candidate_end),
        (candidate_start + 1, candidate_end),
        (candidate_start + 1, cursor),
        None,
    )
}

pub fn build_at_reference_menu(
    context: &AtReferenceContextWire,
    inventory: &AtReferenceInventoryWire,
) -> AtReferenceMenuWire {
    build_at_reference_menu_inner(
        context,
        inventory,
        None,
        AtReferenceMenuOptionsWire::default(),
    )
}

/// Build an at-reference menu using a reusable payload index.
///
/// The index replaces `inventory.payloads`; kinds, paths, and truncation
/// metadata continue to come from the inventory wire.
pub fn build_at_reference_menu_with_payload_index(
    context: &AtReferenceContextWire,
    inventory: &AtReferenceInventoryWire,
    payload_index: &AtReferencePayloadIndex,
) -> AtReferenceMenuWire {
    build_at_reference_menu_inner(
        context,
        inventory,
        Some(payload_index),
        AtReferenceMenuOptionsWire::default(),
    )
}

/// Build an at-reference menu with explicit caller policy.
pub fn build_at_reference_menu_with_options(
    context: &AtReferenceContextWire,
    inventory: &AtReferenceInventoryWire,
    payload_index: Option<&AtReferencePayloadIndex>,
    options: AtReferenceMenuOptionsWire,
) -> AtReferenceMenuWire {
    build_at_reference_menu_inner(context, inventory, payload_index, options)
}

fn build_at_reference_menu_inner(
    context: &AtReferenceContextWire,
    inventory: &AtReferenceInventoryWire,
    payload_index: Option<&AtReferencePayloadIndex>,
    options: AtReferenceMenuOptionsWire,
) -> AtReferenceMenuWire {
    match context.stage {
        AtReferenceStage::Kind => build_kind_menu(context, inventory, options),
        AtReferenceStage::Payload => {
            build_payload_menu(context, inventory, payload_index)
        }
    }
}

pub fn is_builtin_at_reference_kind(kind: &str) -> bool {
    BUILTIN_ARTIFACT_REF_KINDS.contains(&kind)
}

fn context_from_spans(
    text: &str,
    stage: AtReferenceStage,
    candidate_span: (usize, usize),
    replacement_span: (usize, usize),
    query_span: (usize, usize),
    kind: Option<String>,
) -> Option<AtReferenceContextWire> {
    let query = text.get(query_span.0..query_span.1)?.to_string();
    let path_query =
        (stage == AtReferenceStage::Kind).then(|| split_path_query(&query));
    Some(AtReferenceContextWire {
        stage,
        candidate_span,
        replacement_span,
        query_span,
        query,
        kind,
        path_query,
    })
}

fn incomplete_at_reference_candidate(
    text: &str,
    cursor: usize,
) -> Option<(usize, usize)> {
    let floor = cursor.saturating_sub(ARTIFACT_REF_PREFIX_LOOKBACK);
    let mut start = cursor;
    while start > floor {
        let previous = previous_char_boundary(text, start)?;
        let character = text[previous..].chars().next()?;
        if character.is_whitespace() || matches!(character, '"' | '\'' | '`') {
            break;
        }
        start = previous;
    }
    if text.get(start..start + 1) != Some("@") || cursor < start + 1 {
        return None;
    }
    if start > 0 {
        let previous = text[..start].chars().next_back()?;
        if !previous.is_whitespace() && !matches!(previous, '"' | '\'' | '`') {
            return None;
        }
    }

    let mut end = cursor;
    while end < text.len() && end - start <= ARTIFACT_REF_PREFIX_LOOKBACK {
        let character = text[end..].chars().next()?;
        if character.is_whitespace() || matches!(character, '"' | '\'' | '`') {
            break;
        }
        end += character.len_utf8();
    }
    let body = text.get(start + 1..end)?;
    if body.contains(':') || !body.bytes().all(is_at_reference_body_byte) {
        return None;
    }
    Some((start, end))
}

fn is_at_reference_body_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'_' | b'-' | b'/' | b'.' | b'~')
}

fn split_path_query(query: &str) -> AtReferencePathQueryWire {
    let (directory, partial) = query
        .rfind('/')
        .map(|separator| query.split_at(separator + 1))
        .unwrap_or(("", query));
    AtReferencePathQueryWire {
        directory: directory.to_string(),
        partial: partial.to_string(),
        show_hidden: partial.starts_with('.'),
    }
}

fn build_kind_menu(
    context: &AtReferenceContextWire,
    inventory: &AtReferenceInventoryWire,
    options: AtReferenceMenuOptionsWire,
) -> AtReferenceMenuWire {
    let mut kinds = inventory
        .kinds
        .iter()
        .filter_map(|row| {
            fuzzy_match(&context.query, &row.kind)
                .map(|match_result| (row, match_result))
        })
        .collect::<Vec<_>>();
    if context.query.is_empty() {
        kinds.sort_by(|left, right| compare_kind_rows(left.0, right.0));
    } else {
        kinds.sort_by(compare_kind_matches);
    }

    let mut seen_kinds = BTreeSet::new();
    kinds.retain(|(row, _)| seen_kinds.insert(row.kind.clone()));
    let kind_prefix_hit =
        kinds.iter().any(|(_, match_result)| match_result.tier == 0);
    let gate_files = kind_prefix_hit && !options.include_files;
    let artifact_count = kinds.len();
    let artifact_extension = shared_match_extension(
        &kinds,
        &context.query,
        |(row, match_result)| (&row.kind, match_result.tier),
    );
    let artifact_rows = kinds
        .into_iter()
        .take(AT_REFERENCE_MAX_GROUP_ROWS)
        .map(|(row, match_result)| AtReferenceRowWire {
            group: AtReferenceGroup::Artifact,
            label: row.kind.clone(),
            title: String::new(),
            insertion: format!("@{}:", row.kind),
            is_dir: false,
            detail: row.detail.clone(),
            builtin: row.builtin,
            label_match: match_result.runs,
            title_match: Vec::new(),
            match_tier: match_result.tier,
            body: String::new(),
        })
        .collect::<Vec<_>>();

    let path_query = context
        .path_query
        .clone()
        .unwrap_or_else(|| split_path_query(&context.query));
    let mut paths = inventory
        .paths
        .iter()
        .filter_map(|row| {
            if !path_query.show_hidden && row.name.starts_with('.') {
                return None;
            }
            fuzzy_match(&path_query.partial, &row.name)
                .map(|match_result| (row, match_result))
        })
        .collect::<Vec<_>>();
    paths.sort_by(|(left_row, left_match), (right_row, right_match)| {
        right_row.is_dir.cmp(&left_row.is_dir).then_with(|| {
            if path_query.partial.is_empty() {
                compare_case_insensitive(&left_row.name, &right_row.name)
            } else {
                compare_fuzzy(
                    (left_match, &left_row.name),
                    (right_match, &right_row.name),
                )
            }
        })
    });
    let files_suppressed = gate_files && !paths.is_empty();
    let file_count = if gate_files { 0 } else { paths.len() };
    let file_extension = shared_match_extension(
        &paths,
        &path_query.partial,
        |(row, match_result)| (&row.name, match_result.tier),
    );
    let file_rows = if gate_files {
        Vec::new()
    } else {
        paths
            .into_iter()
            .take(AT_REFERENCE_MAX_GROUP_ROWS)
            .map(|(row, match_result)| {
                let label = if row.is_dir {
                    format!("{}/", row.name.trim_end_matches('/'))
                } else {
                    row.name.clone()
                };
                AtReferenceRowWire {
                    group: AtReferenceGroup::File,
                    insertion: format!("@{}{}", path_query.directory, label),
                    label,
                    title: String::new(),
                    is_dir: row.is_dir,
                    detail: if row.is_dir {
                        "directory".to_string()
                    } else {
                        "file".to_string()
                    },
                    builtin: false,
                    label_match: match_result.runs,
                    title_match: Vec::new(),
                    match_tier: match_result.tier,
                    body: String::new(),
                }
            })
            .collect::<Vec<_>>()
    };

    let shared_extension = if artifact_count == 0 {
        file_extension
    } else {
        artifact_extension
    };
    let mut rows = artifact_rows;
    rows.extend(file_rows);
    AtReferenceMenuWire {
        rows,
        shared_extension,
        artifact_count,
        file_count,
        payload_count: 0,
        truncated_payloads: inventory.truncated_payloads,
        files_suppressed,
    }
}

struct MatchedPayload<'a> {
    row: &'a AtReferencePayloadRowWire,
    payload_match: Option<FuzzyMatch>,
    title_match: Option<FuzzyMatch>,
    qualified_match: Option<QualifiedPayloadMatch<'a>>,
    prepared_payload: Option<&'a FuzzyText>,
    prepared_title: Option<&'a FuzzyText>,
}

struct QualifiedPayloadMatch<'a> {
    matched: FuzzyMatch,
    text: Cow<'a, str>,
    prepared: Option<&'a FuzzyText>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PayloadMatchTarget {
    Payload,
    Title,
    Qualified,
}

impl MatchedPayload<'_> {
    fn best_match(&self) -> (&FuzzyMatch, &str, PayloadMatchTarget) {
        let payload = self.payload_match.as_ref().map(|matched| {
            (
                matched,
                self.row.payload.as_str(),
                PayloadMatchTarget::Payload,
            )
        });
        let title = self.title_match.as_ref().map(|matched| {
            (matched, self.row.label.as_str(), PayloadMatchTarget::Title)
        });
        let qualified = self.qualified_match.as_ref().map(|qualified| {
            (
                &qualified.matched,
                qualified.text.as_ref(),
                PayloadMatchTarget::Qualified,
            )
        });
        [payload, title, qualified]
            .into_iter()
            .flatten()
            .reduce(|best, candidate| {
                if compare_fuzzy((candidate.0, candidate.1), (best.0, best.1))
                    == Ordering::Less
                {
                    candidate
                } else {
                    best
                }
            })
            .expect("matched payload has at least one match")
    }

    fn best_prepared_match(
        &self,
    ) -> Option<(&FuzzyMatch, &str, &FuzzyText, PayloadMatchTarget)> {
        let payload =
            self.payload_match.as_ref().zip(self.prepared_payload).map(
                |(matched, prepared)| {
                    (
                        matched,
                        self.row.payload.as_str(),
                        prepared,
                        PayloadMatchTarget::Payload,
                    )
                },
            );
        let title = self.title_match.as_ref().zip(self.prepared_title).map(
            |(matched, prepared)| {
                (
                    matched,
                    self.row.label.as_str(),
                    prepared,
                    PayloadMatchTarget::Title,
                )
            },
        );
        let qualified = self.qualified_match.as_ref().and_then(|qualified| {
            Some((
                &qualified.matched,
                qualified.text.as_ref(),
                qualified.prepared?,
                PayloadMatchTarget::Qualified,
            ))
        });
        [payload, title, qualified].into_iter().flatten().reduce(
            |best, candidate| {
                if compare_fuzzy_prepared(
                    (candidate.0, candidate.1, candidate.2),
                    (best.0, best.1, best.2),
                ) == Ordering::Less
                {
                    candidate
                } else {
                    best
                }
            },
        )
    }
}

fn build_payload_menu(
    context: &AtReferenceContextWire,
    inventory: &AtReferenceInventoryWire,
    payload_index: Option<&AtReferencePayloadIndex>,
) -> AtReferenceMenuWire {
    let kind = context.kind.as_deref().unwrap_or_default();
    let query = FuzzyQuery::new(&context.query);
    let mut matches = if let Some(payload_index) = payload_index {
        payload_index
            .rows
            .iter()
            .filter_map(|indexed| {
                let qualified_match =
                    indexed.qualified.as_ref().and_then(|qualified| {
                        Some(QualifiedPayloadMatch {
                            matched: fuzzy_match_prepared(
                                &query,
                                &qualified.fuzzy,
                            )?,
                            text: Cow::Borrowed(qualified.text.as_str()),
                            prepared: Some(&qualified.fuzzy),
                        })
                    });
                matched_payload(
                    &indexed.row,
                    fuzzy_match_prepared(&query, &indexed.payload),
                    fuzzy_match_prepared(&query, &indexed.title),
                    qualified_match,
                    Some(&indexed.payload),
                    Some(&indexed.title),
                )
            })
            .collect::<Vec<_>>()
    } else {
        inventory
            .payloads
            .iter()
            .filter_map(|row| {
                let qualified_match =
                    qualified_payload_text(row).and_then(|text| {
                        Some(QualifiedPayloadMatch {
                            matched: fuzzy_match(&context.query, &text)?,
                            text: Cow::Owned(text),
                            prepared: None,
                        })
                    });
                matched_payload(
                    row,
                    fuzzy_match(&context.query, &row.payload),
                    fuzzy_match(&context.query, &row.label),
                    qualified_match,
                    None,
                    None,
                )
            })
            .collect::<Vec<_>>()
    };
    if !context.query.is_empty() {
        matches.sort_by(|left, right| {
            compare_payload_matches(left, right)
                .then_with(|| compare_payload_text(left, right, true))
                .then_with(|| compare_payload_text(left, right, false))
        });
    }
    let payload_count = matches.len();
    let shared_extension =
        shared_match_extension(&matches, &context.query, |matched| {
            (matched.row.payload.as_str(), matched.best_match().0.tier)
        });
    let rows = matches
        .into_iter()
        .take(AT_REFERENCE_MAX_GROUP_ROWS)
        .map(|matched| {
            let (best_match, _, best_target) = matched.best_match();
            let match_tier = best_match.tier;
            let row = matched.row;
            let detail = match (row.detail.is_empty(), row.age.is_empty()) {
                (false, false) => format!("{} · {}", row.detail, row.age),
                (false, true) => row.detail.clone(),
                (true, false) => row.age.clone(),
                (true, true) => String::new(),
            };
            let (label_match, title_match) = if best_target
                == PayloadMatchTarget::Qualified
            {
                qualified_match_runs(row, best_match)
            } else {
                (
                    matched
                        .payload_match
                        .map_or_else(Vec::new, |match_result| {
                            match_result.runs
                        }),
                    matched.title_match.map_or_else(Vec::new, |match_result| {
                        match_result.runs
                    }),
                )
            };
            AtReferenceRowWire {
                group: AtReferenceGroup::Payload,
                label: row.payload.clone(),
                title: row.label.clone(),
                insertion: format!("@{kind}:{}", row.payload),
                is_dir: false,
                detail,
                builtin: false,
                label_match,
                title_match,
                match_tier,
                body: row.body.clone(),
            }
        })
        .collect::<Vec<_>>();
    AtReferenceMenuWire {
        shared_extension,
        rows,
        artifact_count: 0,
        file_count: 0,
        payload_count,
        truncated_payloads: inventory.truncated_payloads,
        files_suppressed: false,
    }
}

fn matched_payload<'a>(
    row: &'a AtReferencePayloadRowWire,
    payload_match: Option<FuzzyMatch>,
    title_match: Option<FuzzyMatch>,
    qualified_match: Option<QualifiedPayloadMatch<'a>>,
    prepared_payload: Option<&'a FuzzyText>,
    prepared_title: Option<&'a FuzzyText>,
) -> Option<MatchedPayload<'a>> {
    if payload_match.is_none()
        && title_match.is_none()
        && qualified_match.is_none()
    {
        return None;
    }
    Some(MatchedPayload {
        row,
        payload_match,
        title_match,
        qualified_match,
        prepared_payload,
        prepared_title,
    })
}

fn qualified_payload_text(row: &AtReferencePayloadRowWire) -> Option<String> {
    if row.scope.is_empty() || !row.payload.starts_with(&row.scope) {
        return None;
    }
    let remainder = row.payload.get(row.scope.len()..)?;
    let separator = remainder.chars().next()?;
    let prefix_end = row.scope.len() + separator.len_utf8();
    Some(format!("{}{}", &row.payload[..prefix_end], row.label))
}

type MatchRuns = Vec<(u32, u32)>;

fn qualified_match_runs(
    row: &AtReferencePayloadRowWire,
    matched: &FuzzyMatch,
) -> (MatchRuns, MatchRuns) {
    let scope_end = row.scope.chars().count() as u32;
    let title_start = scope_end + 1;
    let mut label_runs = Vec::new();
    let mut title_runs = Vec::new();
    for &(start, end) in &matched.runs {
        if end <= scope_end {
            label_runs.push((start, end));
        } else if start >= title_start {
            title_runs.push((start - title_start, end - title_start));
        }
    }
    (label_runs, title_runs)
}

fn compare_payload_matches(
    left: &MatchedPayload<'_>,
    right: &MatchedPayload<'_>,
) -> Ordering {
    match (left.best_prepared_match(), right.best_prepared_match()) {
        (Some(left_match), Some(right_match)) => compare_payload_quality(
            left_match.0,
            left.row.rank,
            right_match.0,
            right.row.rank,
        )
        .then_with(|| {
            compare_fuzzy_prepared(
                (left_match.0, left_match.1, left_match.2),
                (right_match.0, right_match.1, right_match.2),
            )
        }),
        _ => {
            let left_match = left.best_match();
            let right_match = right.best_match();
            compare_payload_quality(
                left_match.0,
                left.row.rank,
                right_match.0,
                right.row.rank,
            )
            .then_with(|| {
                compare_fuzzy(
                    (left_match.0, left_match.1),
                    (right_match.0, right_match.1),
                )
            })
        }
    }
}

fn compare_payload_quality(
    left_match: &FuzzyMatch,
    left_rank: Option<u32>,
    right_match: &FuzzyMatch,
    right_rank: Option<u32>,
) -> Ordering {
    left_match
        .tier
        .cmp(&right_match.tier)
        .then_with(|| right_match.score.cmp(&left_match.score))
        .then_with(|| match (left_rank, right_rank) {
            (Some(left), Some(right)) => left.cmp(&right),
            _ => Ordering::Equal,
        })
}

fn compare_payload_text(
    left: &MatchedPayload<'_>,
    right: &MatchedPayload<'_>,
    payload: bool,
) -> Ordering {
    let (left_text, right_text, left_prepared, right_prepared) = if payload {
        (
            left.row.payload.as_str(),
            right.row.payload.as_str(),
            left.prepared_payload,
            right.prepared_payload,
        )
    } else {
        (
            left.row.label.as_str(),
            right.row.label.as_str(),
            left.prepared_title,
            right.prepared_title,
        )
    };
    match (left_prepared, right_prepared) {
        (Some(left_prepared), Some(right_prepared)) => compare_prepared_text(
            (left_text, left_prepared),
            (right_text, right_prepared),
        ),
        _ => compare_case_insensitive(left_text, right_text),
    }
}

fn compare_kind_matches(
    left: &(&AtReferenceKindRowWire, FuzzyMatch),
    right: &(&AtReferenceKindRowWire, FuzzyMatch),
) -> Ordering {
    left.1
        .tier
        .cmp(&right.1.tier)
        .then_with(|| right.1.score.cmp(&left.1.score))
        .then_with(|| {
            match (builtin_kind_rank(left.0), builtin_kind_rank(right.0)) {
                (Some(left), Some(right)) => left.cmp(&right),
                _ => Ordering::Equal,
            }
        })
        .then_with(|| {
            compare_fuzzy((&left.1, &left.0.kind), (&right.1, &right.0.kind))
        })
        .then_with(|| compare_kind_rows(left.0, right.0))
}

fn compare_kind_rows(
    left: &AtReferenceKindRowWire,
    right: &AtReferenceKindRowWire,
) -> Ordering {
    let left_rank = builtin_kind_rank(left);
    let right_rank = builtin_kind_rank(right);
    match (left_rank, right_rank) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => compare_case_insensitive(&left.kind, &right.kind),
    }
}

fn builtin_kind_rank(row: &AtReferenceKindRowWire) -> Option<usize> {
    BUILTIN_ARTIFACT_REF_KINDS
        .iter()
        .position(|kind| *kind == row.kind)
}

fn compare_case_insensitive(left: &str, right: &str) -> Ordering {
    left.to_lowercase()
        .cmp(&right.to_lowercase())
        .then_with(|| left.cmp(right))
}

fn shared_match_extension<'a, T>(
    rows: &'a [T],
    query: &str,
    label_and_tier: impl Fn(&'a T) -> (&'a str, u8),
) -> String {
    if rows.len() < 2 {
        return String::new();
    }
    let (first_label, first_tier) = label_and_tier(&rows[0]);
    if first_tier != 0 {
        return String::new();
    }
    let mut prefix = first_label.to_string();
    for row in &rows[1..] {
        let (label, match_tier) = label_and_tier(row);
        if match_tier != 0 {
            return String::new();
        }
        prefix = common_prefix_case_insensitive(&prefix, label);
    }
    if prefix.len() > query.len() && prefix.is_char_boundary(query.len()) {
        prefix[query.len()..].to_string()
    } else {
        String::new()
    }
}

fn common_prefix_case_insensitive(left: &str, right: &str) -> String {
    let mut end = 0;
    for ((left_idx, left_char), (_, right_char)) in
        left.char_indices().zip(right.char_indices())
    {
        if !left_char.eq_ignore_ascii_case(&right_char) {
            break;
        }
        end = left_idx + left_char.len_utf8();
    }
    left[..end].to_string()
}

fn previous_char_boundary(text: &str, byte_index: usize) -> Option<usize> {
    text.get(..byte_index)?
        .char_indices()
        .last()
        .map(|(index, _)| index)
}

fn ranges_intersect(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 < right.1 && right.0 < left.1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(text: &str, cursor: usize) -> Option<AtReferenceContextWire> {
        let document = DocumentSnapshot::new(text);
        let position = document.byte_offset_to_position(cursor).unwrap();
        detect_at_reference_context(
            &document,
            position,
            &["plan".to_string(), "chat".to_string()],
        )
    }

    fn kind(kind: &str, builtin: bool) -> AtReferenceKindRowWire {
        AtReferenceKindRowWire {
            kind: kind.to_string(),
            builtin,
            detail: format!("{kind} detail"),
        }
    }

    fn path(name: &str, is_dir: bool) -> AtReferencePathRowWire {
        AtReferencePathRowWire {
            name: name.to_string(),
            is_dir,
        }
    }

    fn payload(
        payload: &str,
        title: &str,
        detail: &str,
        age: &str,
    ) -> AtReferencePayloadRowWire {
        AtReferencePayloadRowWire {
            payload: payload.to_string(),
            label: title.to_string(),
            detail: detail.to_string(),
            age: age.to_string(),
            scope: String::new(),
            rank: None,
            body: String::new(),
        }
    }

    fn row(label: &str, match_tier: u8) -> AtReferenceRowWire {
        AtReferenceRowWire {
            group: AtReferenceGroup::Artifact,
            label: label.to_string(),
            title: String::new(),
            insertion: format!("@{label}:"),
            is_dir: false,
            detail: String::new(),
            builtin: false,
            label_match: Vec::new(),
            title_match: Vec::new(),
            match_tier,
            body: String::new(),
        }
    }

    fn slice_runs<'a>(text: &'a str, runs: &[(u32, u32)]) -> Vec<&'a str> {
        let byte_offsets = text
            .char_indices()
            .map(|(offset, _)| offset)
            .chain(std::iter::once(text.len()))
            .collect::<Vec<_>>();
        runs.iter()
            .map(|&(start, end)| {
                &text[byte_offsets[start as usize]..byte_offsets[end as usize]]
            })
            .collect()
    }

    fn payload_menus(
        context: &AtReferenceContextWire,
        payloads: Vec<AtReferencePayloadRowWire>,
    ) -> (AtReferenceMenuWire, AtReferenceMenuWire) {
        let wire = build_at_reference_menu(
            context,
            &AtReferenceInventoryWire {
                payloads: payloads.clone(),
                ..Default::default()
            },
        );
        let index = AtReferencePayloadIndex::new(payloads);
        let indexed = build_at_reference_menu_with_payload_index(
            context,
            &AtReferenceInventoryWire::default(),
            &index,
        );
        (wire, indexed)
    }

    #[test]
    fn detects_kind_and_payload_at_every_cursor_position() {
        for text in ["@", "@p", "@plan"] {
            for cursor in 1..=text.len() {
                let detected = context(text, cursor).unwrap();
                assert_eq!(detected.stage, AtReferenceStage::Kind, "{text}");
                assert!(detected.path_query.is_some());
            }
        }
        for text in ["@plan:", "@plan:a/b.md"] {
            let separator = text.find(':').unwrap();
            for cursor in 1..=text.len() {
                let detected = context(text, cursor).unwrap();
                assert_eq!(
                    detected.stage,
                    if cursor <= separator {
                        AtReferenceStage::Kind
                    } else {
                        AtReferenceStage::Payload
                    },
                    "{text} at {cursor}"
                );
            }
        }
    }

    #[test]
    fn detects_path_shaped_kind_queries() {
        for (text, directory, partial, show_hidden) in [
            ("@src/", "src/", "", false),
            ("@src/fo", "src/", "fo", false),
            ("@~/dev/", "~/dev/", "", false),
            ("@../x", "../", "x", false),
            ("@.sase/", ".sase/", "", false),
            ("@src/.g", "src/", ".g", true),
            ("@/etc/h", "/etc/", "h", false),
        ] {
            let detected = context(text, text.len()).unwrap();
            assert_eq!(detected.stage, AtReferenceStage::Kind);
            assert_eq!(
                detected.path_query,
                Some(AtReferencePathQueryWire {
                    directory: directory.to_string(),
                    partial: partial.to_string(),
                    show_hidden,
                }),
                "{text}"
            );
        }
    }

    #[test]
    fn rejects_prose_invalid_characters_and_literal_zones() {
        for (text, cursor) in [
            ("mail@example.com", 16),
            ("word@", 5),
            ("@foo!", 4),
            ("```\n@plan\n```", 9),
            ("`@plan`", 6),
        ] {
            assert!(context(text, cursor).is_none(), "{text:?}");
        }
    }

    #[test]
    fn bare_menu_groups_builtins_then_sorted_kinds_and_visible_paths() {
        let detected = context("@", 1).unwrap();
        let inventory = AtReferenceInventoryWire {
            kinds: vec![
                kind("zeta", false),
                kind("file", true),
                kind("chat", true),
                kind("commit", true),
                kind("bug", true),
                kind("Alpha", false),
            ],
            paths: vec![
                path("z.txt", false),
                path(".git", true),
                path("src", true),
                path("A.txt", false),
            ],
            payloads: vec![],
            ..Default::default()
        };
        let menu = build_at_reference_menu_with_options(
            &detected,
            &inventory,
            None,
            AtReferenceMenuOptionsWire {
                include_files: true,
            },
        );
        assert_eq!(
            menu.rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec![
                "@commit:", "@chat:", "@bug:", "@file:", "@Alpha:", "@zeta:",
                "@src/", "@A.txt", "@z.txt"
            ]
        );
        assert_eq!(menu.artifact_count, 6);
        assert_eq!(menu.file_count, 3);
        assert!(!menu.files_suppressed);

        let gated = build_at_reference_menu(&detected, &inventory);
        assert_eq!(
            gated
                .rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["@commit:", "@chat:", "@bug:", "@file:", "@Alpha:", "@zeta:"]
        );
        assert_eq!(gated.artifact_count, 6);
        assert_eq!(gated.file_count, 0);
        assert!(gated.files_suppressed);
    }

    #[test]
    fn kind_and_file_groups_filter_independently() {
        let detected = context("@pl", 3).unwrap();
        let inventory = AtReferenceInventoryWire {
            kinds: vec![kind("plan", false), kind("chat", true)],
            paths: vec![path("plans", true), path("src", true)],
            payloads: vec![],
            ..Default::default()
        };
        let menu = build_at_reference_menu(&detected, &inventory);
        assert_eq!(
            menu.rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["@plan:"]
        );
        assert_eq!(menu.file_count, 0);
        assert!(menu.files_suppressed);

        let revealed = build_at_reference_menu_with_options(
            &detected,
            &inventory,
            None,
            AtReferenceMenuOptionsWire {
                include_files: true,
            },
        );
        assert_eq!(
            revealed
                .rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["@plan:", "@plans/"]
        );
        assert_eq!(revealed.file_count, 1);
        assert!(!revealed.files_suppressed);

        let path_context = context("@src/", 5).unwrap();
        let path_menu = build_at_reference_menu(
            &path_context,
            &AtReferenceInventoryWire {
                kinds: vec![kind("plan", false)],
                paths: vec![path("lib.rs", false)],
                payloads: vec![],
                ..Default::default()
            },
        );
        assert_eq!(path_menu.rows[0].insertion, "@src/lib.rs");
        assert_eq!(path_menu.rows[0].group, AtReferenceGroup::File);
        assert!(!path_menu.files_suppressed);
    }

    #[test]
    fn fuzzy_kind_matches_do_not_gate_file_rows() {
        let inventory = AtReferenceInventoryWire {
            kinds: vec![kind("research", false)],
            paths: vec![path("rsch-notes", false), path("src", true)],
            ..Default::default()
        };

        let subsequence =
            build_at_reference_menu(&context("@rsch", 5).unwrap(), &inventory);
        assert_eq!(
            subsequence
                .rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["@research:", "@rsch-notes"]
        );
        assert!(!subsequence.files_suppressed);

        let short =
            build_at_reference_menu(&context("@sr", 3).unwrap(), &inventory);
        assert!(short
            .rows
            .iter()
            .any(|row| row.group == AtReferenceGroup::File));
        assert!(!short.files_suppressed);
    }

    #[test]
    fn gated_menu_without_matching_paths_does_not_offer_an_empty_reveal() {
        let menu = build_at_reference_menu(
            &context("@pl", 3).unwrap(),
            &AtReferenceInventoryWire {
                kinds: vec![kind("plan", false)],
                paths: vec![path("src", true)],
                ..Default::default()
            },
        );
        assert_eq!(menu.artifact_count, 1);
        assert_eq!(menu.file_count, 0);
        assert!(!menu.files_suppressed);
    }

    #[test]
    fn dotfile_visibility_tracks_the_trailing_partial() {
        let inventory = AtReferenceInventoryWire {
            kinds: vec![],
            paths: vec![path(".git", true), path("src", true)],
            payloads: vec![],
            ..Default::default()
        };
        let hidden =
            build_at_reference_menu(&context("@", 1).unwrap(), &inventory);
        assert_eq!(hidden.file_count, 1);

        let visible =
            build_at_reference_menu(&context("@.", 2).unwrap(), &inventory);
        assert_eq!(visible.file_count, 1);
        assert_eq!(visible.rows[0].insertion, "@.git/");
    }

    #[test]
    fn caps_each_group_but_records_pre_cap_counts() {
        let inventory = AtReferenceInventoryWire {
            kinds: (0..205)
                .map(|index| kind(&format!("kind{index:03}"), false))
                .collect(),
            paths: (0..207)
                .map(|index| path(&format!("file{index:03}"), false))
                .collect(),
            payloads: vec![],
            ..Default::default()
        };
        let menu = build_at_reference_menu_with_options(
            &context("@", 1).unwrap(),
            &inventory,
            None,
            AtReferenceMenuOptionsWire {
                include_files: true,
            },
        );
        assert_eq!(menu.artifact_count, 205);
        assert_eq!(menu.file_count, 207);
        assert_eq!(menu.rows.len(), AT_REFERENCE_MAX_GROUP_ROWS * 2);
    }

    #[test]
    fn shared_extension_uses_only_the_leading_non_empty_group() {
        let inventory = AtReferenceInventoryWire {
            kinds: vec![kind("plan", false), kind("placeholder", false)],
            paths: vec![path("pluto", true), path("plugin", true)],
            payloads: vec![],
            ..Default::default()
        };
        let menu =
            build_at_reference_menu(&context("@p", 2).unwrap(), &inventory);
        assert_eq!(menu.shared_extension, "la");
        assert_eq!(menu.file_count, 0);
        assert!(menu.files_suppressed);

        let files_only = build_at_reference_menu(
            &context("@src/f", 6).unwrap(),
            &AtReferenceInventoryWire {
                kinds: inventory.kinds,
                paths: vec![path("foo", false), path("format", false)],
                payloads: vec![],
                ..Default::default()
            },
        );
        assert_eq!(files_only.shared_extension, "o");
    }

    #[test]
    fn fuzzy_kind_menu_keeps_builtin_ties_and_reports_runs() {
        let inventory = AtReferenceInventoryWire {
            kinds: vec![
                kind("chat", true),
                kind("research", false),
                kind("commit", true),
            ],
            ..Default::default()
        };
        let menu =
            build_at_reference_menu(&context("@c", 2).unwrap(), &inventory);
        assert_eq!(
            menu.rows
                .iter()
                .map(|row| row.label.as_str())
                .collect::<Vec<_>>(),
            vec!["commit", "chat", "research"]
        );
        assert!(menu
            .rows
            .iter()
            .take(2)
            .all(|row| row.match_tier == 0 && row.label_match == vec![(0, 1)]));

        let fuzzy =
            build_at_reference_menu(&context("@rsch", 5).unwrap(), &inventory);
        assert_eq!(fuzzy.rows[0].label, "research");
        assert_eq!(fuzzy.rows[0].match_tier, 3);
        assert_eq!(
            slice_runs("research", &fuzzy.rows[0].label_match),
            vec!["r", "s", "ch"]
        );
    }

    #[test]
    fn path_menu_fuzzy_matches_only_the_trailing_partial() {
        let menu = build_at_reference_menu(
            &context("@src/fcb", 8).unwrap(),
            &AtReferenceInventoryWire {
                paths: vec![
                    path("_file_completion_base.py", false),
                    path("fixtures", true),
                ],
                ..Default::default()
            },
        );
        assert_eq!(menu.file_count, 1);
        assert_eq!(menu.rows[0].insertion, "@src/_file_completion_base.py");
        assert_eq!(menu.rows[0].match_tier, 3);
        assert_eq!(
            slice_runs("_file_completion_base.py", &menu.rows[0].label_match),
            vec!["f", "c", "b"]
        );
    }

    #[test]
    fn payload_menu_is_path_first_and_ranks_both_match_fields() {
        let bundled =
            "202607/sase_sites_hub_and_pages/sase_sites_hub_and_pages.md";
        let menu = build_at_reference_menu(
            &context("@research:site", 14).unwrap(),
            &AtReferenceInventoryWire {
                payloads: vec![
                    payload(
                        bundled,
                        "SASE Sites Hub and Pages",
                        "research",
                        "3d",
                    ),
                    payload("site_notes.md", "Notes", "research", "1d"),
                    payload("other.md", "No match", "research", "now"),
                ],
                ..Default::default()
            },
        );
        assert_eq!(menu.payload_count, 2);
        assert_eq!(menu.rows[0].label, "site_notes.md");

        let row = menu.rows.iter().find(|row| row.label == bundled).unwrap();
        assert_eq!(row.title, "SASE Sites Hub and Pages");
        assert_eq!(row.insertion, format!("@research:{bundled}"));
        assert_eq!(row.detail, "research · 3d");
        assert_eq!(row.match_tier, 2);
        assert_eq!(slice_runs(&row.label, &row.label_match), vec!["site"]);
        assert_eq!(slice_runs(&row.title, &row.title_match), vec!["Site"]);
    }

    #[test]
    fn indexed_payload_menu_matches_the_wire_inventory_path() {
        let payloads = vec![
            payload(
                "202607/sase_sites_hub_and_pages.md",
                "SASE Sites",
                "research",
                "3d",
            ),
            payload("other.md", "No match", "research", "now"),
        ];
        let context = context("@research:site", 14).unwrap();
        let expected = build_at_reference_menu(
            &context,
            &AtReferenceInventoryWire {
                payloads: payloads.clone(),
                truncated_payloads: 7,
                ..Default::default()
            },
        );
        let index = AtReferencePayloadIndex::new(payloads);
        assert_eq!(index.len(), 2);
        assert!(!index.is_empty());

        let indexed = build_at_reference_menu_with_payload_index(
            &context,
            &AtReferenceInventoryWire {
                payloads: vec![payload(
                    "ignored.md",
                    "Ignored",
                    "research",
                    "",
                )],
                truncated_payloads: 7,
                ..Default::default()
            },
            &index,
        );

        assert_eq!(indexed, expected);
    }

    #[test]
    fn payload_menu_includes_title_only_matches() {
        let menu = build_at_reference_menu(
            &context("@plan:needle", 12).unwrap(),
            &AtReferenceInventoryWire {
                payloads: vec![payload(
                    "202607/other.md",
                    "Needle in a haystack",
                    "plan",
                    "",
                )],
                ..Default::default()
            },
        );
        assert_eq!(menu.rows[0].label, "202607/other.md");
        assert_eq!(menu.rows[0].title, "Needle in a haystack");
        assert!(menu.rows[0].label_match.is_empty());
        assert_eq!(menu.rows[0].title_match, vec![(0, 6)]);
        assert_eq!(menu.rows[0].match_tier, 0);
    }

    #[test]
    fn scoped_payload_matches_repo_and_title_as_one_qualified_target() {
        let mut commit = payload(
            "sase-core@5143cb981f0a",
            "fix(stats): expose occupancy",
            "",
            "now",
        );
        commit.scope = "sase-core".to_string();
        let detected = context("@commit:core@fix", 16).unwrap();
        let (wire, indexed) = payload_menus(&detected, vec![commit]);

        assert_eq!(wire, indexed);
        assert_eq!(wire.payload_count, 1);
        assert_eq!(wire.rows[0].label, "sase-core@5143cb981f0a");
        assert_eq!(wire.rows[0].title, "fix(stats): expose occupancy");
    }

    #[test]
    fn qualified_match_highlights_each_field_and_drops_straddling_runs() {
        let mut commit = payload(
            "sase-core@5143cb981f0a",
            "fix(stats): expose occupancy",
            "",
            "now",
        );
        commit.scope = "sase-core".to_string();

        let split_context = context("@commit:cofi", 12).unwrap();
        let (wire, indexed) =
            payload_menus(&split_context, vec![commit.clone()]);
        assert_eq!(wire, indexed);
        assert_eq!(
            slice_runs(&wire.rows[0].label, &wire.rows[0].label_match),
            vec!["co"]
        );
        assert_eq!(
            slice_runs(&wire.rows[0].title, &wire.rows[0].title_match),
            vec!["fi"]
        );

        let straddling_context = context("@commit:core@fix", 16).unwrap();
        let (wire, indexed) = payload_menus(&straddling_context, vec![commit]);
        assert_eq!(wire, indexed);
        assert!(wire.rows[0].label_match.is_empty());
        assert!(wire.rows[0].title_match.is_empty());
    }

    #[test]
    fn payload_rank_breaks_quality_ties_but_unranked_rows_keep_text_order() {
        let mut recent = payload("zulu", "Needle", "", "now");
        recent.rank = Some(0);
        let mut older = payload("able", "Needle", "", "1d");
        older.rank = Some(1);
        let detected = context("@plan:needle", 12).unwrap();

        let (ranked, ranked_indexed) =
            payload_menus(&detected, vec![older.clone(), recent.clone()]);
        assert_eq!(ranked, ranked_indexed);
        assert_eq!(
            ranked
                .rows
                .iter()
                .map(|row| row.label.as_str())
                .collect::<Vec<_>>(),
            vec!["zulu", "able"]
        );

        recent.rank = None;
        older.rank = None;
        let (unranked, unranked_indexed) =
            payload_menus(&detected, vec![recent, older]);
        assert_eq!(unranked, unranked_indexed);
        assert_eq!(
            unranked
                .rows
                .iter()
                .map(|row| row.label.as_str())
                .collect::<Vec<_>>(),
            vec!["able", "zulu"]
        );
    }

    #[test]
    fn malformed_scope_degrades_to_an_unscoped_row() {
        let mut commit = payload(
            "sase-core@5143cb981f0a",
            "fix(stats): expose occupancy",
            "",
            "now",
        );
        commit.scope = "different-repo".to_string();
        let detected = context("@commit:core@fix", 16).unwrap();
        let (wire, indexed) = payload_menus(&detected, vec![commit]);

        assert_eq!(wire, indexed);
        assert_eq!(wire.payload_count, 0);
    }

    #[test]
    fn payload_wire_defaults_new_metadata_when_deserializing_old_rows() {
        let row: AtReferencePayloadRowWire = serde_json::from_str(
            r#"{"payload":"item","label":"Item","detail":"","age":""}"#,
        )
        .unwrap();

        assert_eq!(row.scope, "");
        assert_eq!(row.rank, None);
        assert_eq!(row.body, "");
    }

    #[test]
    fn payload_menu_preserves_empty_query_provider_order() {
        let detected = context("@plan:", 6).unwrap();
        let menu = build_at_reference_menu(
            &detected,
            &AtReferenceInventoryWire {
                payloads: vec![
                    payload("zulu", "Alpha label", "plan", "2h"),
                    payload("able", "Second", "", ""),
                    payload("nope", "No match", "", ""),
                ],
                ..Default::default()
            },
        );
        assert_eq!(
            menu.rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["@plan:zulu", "@plan:able", "@plan:nope"]
        );
        assert_eq!(menu.rows[0].label, "zulu");
        assert_eq!(menu.rows[0].title, "Alpha label");
        assert_eq!(menu.rows[0].detail, "plan · 2h");
    }

    #[test]
    fn payload_caps_report_matches_and_caller_truncation() {
        let inventory = AtReferenceInventoryWire {
            payloads: (0..205)
                .map(|index| payload(&format!("item{index:03}"), "", "", ""))
                .collect(),
            truncated_payloads: 17,
            ..Default::default()
        };
        let menu =
            build_at_reference_menu(&context("@plan:", 6).unwrap(), &inventory);
        assert_eq!(menu.payload_count, 205);
        assert_eq!(menu.truncated_payloads, 17);
        assert_eq!(menu.rows.len(), AT_REFERENCE_MAX_GROUP_ROWS);
    }

    #[test]
    fn shared_extension_requires_an_all_prefix_leading_group() {
        assert_eq!(
            shared_match_extension(
                &[row("plan", 0), row("plans", 0)],
                "pl",
                |row| (row.label.as_str(), row.match_tier),
            ),
            "an"
        );
        assert_eq!(
            shared_match_extension(
                &[row("plan", 0), row("plans", 3)],
                "pl",
                |row| (row.label.as_str(), row.match_tier),
            ),
            ""
        );
    }
}
