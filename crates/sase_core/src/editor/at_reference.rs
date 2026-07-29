use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{prompt_literal_zone_ranges, scan_artifact_refs};

use super::token::DocumentSnapshot;
use super::wire::EditorPosition;

const ARTIFACT_REF_PREFIX_LOOKBACK: usize = 128;
pub const AT_REFERENCE_MAX_GROUP_ROWS: usize = 200;
pub const BUILTIN_ARTIFACT_REF_KINDS: &[&str] =
    &["commit", "chat", "bug", "file"];

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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceInventoryWire {
    pub kinds: Vec<AtReferenceKindRowWire>,
    pub paths: Vec<AtReferencePathRowWire>,
    pub payloads: Vec<AtReferencePayloadRowWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceRowWire {
    pub group: AtReferenceGroup,
    pub label: String,
    pub insertion: String,
    pub is_dir: bool,
    pub detail: String,
    pub builtin: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtReferenceMenuWire {
    pub rows: Vec<AtReferenceRowWire>,
    pub shared_extension: String,
    pub artifact_count: usize,
    pub file_count: usize,
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
    match context.stage {
        AtReferenceStage::Kind => build_kind_menu(context, inventory),
        AtReferenceStage::Payload => build_payload_menu(context, inventory),
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
) -> AtReferenceMenuWire {
    let query = context.query.to_lowercase();
    let mut kinds = inventory
        .kinds
        .iter()
        .filter(|row| row.kind.to_lowercase().starts_with(&query))
        .collect::<Vec<_>>();
    kinds.sort_by(|left, right| compare_kind_rows(left, right));

    let mut seen_kinds = BTreeSet::new();
    kinds.retain(|row| seen_kinds.insert(row.kind.clone()));
    let artifact_count = kinds.len();
    let artifact_rows = kinds
        .into_iter()
        .take(AT_REFERENCE_MAX_GROUP_ROWS)
        .map(|row| AtReferenceRowWire {
            group: AtReferenceGroup::Artifact,
            label: row.kind.clone(),
            insertion: format!("@{}:", row.kind),
            is_dir: false,
            detail: row.detail.clone(),
            builtin: row.builtin,
        })
        .collect::<Vec<_>>();

    let path_query = context
        .path_query
        .clone()
        .unwrap_or_else(|| split_path_query(&context.query));
    let partial_lower = path_query.partial.to_lowercase();
    let mut paths = inventory
        .paths
        .iter()
        .filter(|row| {
            (path_query.show_hidden || !row.name.starts_with('.'))
                && row.name.to_lowercase().starts_with(&partial_lower)
        })
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        right
            .is_dir
            .cmp(&left.is_dir)
            .then_with(|| compare_case_insensitive(&left.name, &right.name))
    });
    let file_count = paths.len();
    let file_rows = paths
        .into_iter()
        .take(AT_REFERENCE_MAX_GROUP_ROWS)
        .map(|row| {
            let label = if row.is_dir {
                format!("{}/", row.name.trim_end_matches('/'))
            } else {
                row.name.clone()
            };
            AtReferenceRowWire {
                group: AtReferenceGroup::File,
                insertion: format!("@{}{}", path_query.directory, label),
                label,
                is_dir: row.is_dir,
                detail: if row.is_dir {
                    "directory".to_string()
                } else {
                    "file".to_string()
                },
                builtin: false,
            }
        })
        .collect::<Vec<_>>();

    let shared_extension = if artifact_rows.is_empty() {
        shared_row_extension(&file_rows, &path_query.partial)
    } else {
        shared_row_extension(&artifact_rows, &context.query)
    };
    let mut rows = artifact_rows;
    rows.extend(file_rows);
    AtReferenceMenuWire {
        rows,
        shared_extension,
        artifact_count,
        file_count,
    }
}

fn build_payload_menu(
    context: &AtReferenceContextWire,
    inventory: &AtReferenceInventoryWire,
) -> AtReferenceMenuWire {
    let query = context.query.to_lowercase();
    let kind = context.kind.as_deref().unwrap_or_default();
    let rows = inventory
        .payloads
        .iter()
        .filter(|row| {
            row.payload.to_lowercase().starts_with(&query)
                || row.label.to_lowercase().starts_with(&query)
        })
        .take(AT_REFERENCE_MAX_GROUP_ROWS)
        .map(|row| {
            let detail = match (row.detail.is_empty(), row.age.is_empty()) {
                (false, false) => format!("{} · {}", row.detail, row.age),
                (false, true) => row.detail.clone(),
                (true, false) => row.age.clone(),
                (true, true) => String::new(),
            };
            AtReferenceRowWire {
                group: AtReferenceGroup::Payload,
                label: row.label.clone(),
                insertion: format!("@{kind}:{}", row.payload),
                is_dir: false,
                detail,
                builtin: false,
            }
        })
        .collect::<Vec<_>>();
    AtReferenceMenuWire {
        shared_extension: shared_row_extension(&rows, &context.query),
        rows,
        artifact_count: 0,
        file_count: 0,
    }
}

fn compare_kind_rows(
    left: &AtReferenceKindRowWire,
    right: &AtReferenceKindRowWire,
) -> Ordering {
    let left_rank = BUILTIN_ARTIFACT_REF_KINDS
        .iter()
        .position(|kind| *kind == left.kind);
    let right_rank = BUILTIN_ARTIFACT_REF_KINDS
        .iter()
        .position(|kind| *kind == right.kind);
    match (left_rank, right_rank) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => compare_case_insensitive(&left.kind, &right.kind),
    }
}

fn compare_case_insensitive(left: &str, right: &str) -> Ordering {
    left.to_lowercase()
        .cmp(&right.to_lowercase())
        .then_with(|| left.cmp(right))
}

fn shared_row_extension(rows: &[AtReferenceRowWire], query: &str) -> String {
    if rows.len() < 2 {
        return String::new();
    }
    let mut prefix = rows[0].label.clone();
    for row in &rows[1..] {
        prefix = common_prefix_case_insensitive(&prefix, &row.label);
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
        let menu = build_at_reference_menu(
            &detected,
            &AtReferenceInventoryWire {
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
    }

    #[test]
    fn kind_and_file_groups_filter_independently() {
        let detected = context("@pl", 3).unwrap();
        let menu = build_at_reference_menu(
            &detected,
            &AtReferenceInventoryWire {
                kinds: vec![kind("plan", false), kind("chat", true)],
                paths: vec![path("plans", true), path("src", true)],
                payloads: vec![],
            },
        );
        assert_eq!(
            menu.rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["@plan:", "@plans/"]
        );

        let path_context = context("@src/", 5).unwrap();
        let path_menu = build_at_reference_menu(
            &path_context,
            &AtReferenceInventoryWire {
                kinds: vec![kind("plan", false)],
                paths: vec![path("lib.rs", false)],
                payloads: vec![],
            },
        );
        assert_eq!(path_menu.rows[0].insertion, "@src/lib.rs");
        assert_eq!(path_menu.rows[0].group, AtReferenceGroup::File);
    }

    #[test]
    fn dotfile_visibility_tracks_the_trailing_partial() {
        let inventory = AtReferenceInventoryWire {
            kinds: vec![],
            paths: vec![path(".git", true), path("src", true)],
            payloads: vec![],
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
        };
        let menu =
            build_at_reference_menu(&context("@", 1).unwrap(), &inventory);
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
        };
        let menu =
            build_at_reference_menu(&context("@p", 2).unwrap(), &inventory);
        assert_eq!(menu.shared_extension, "la");

        let files_only = build_at_reference_menu(
            &context("@src/f", 6).unwrap(),
            &AtReferenceInventoryWire {
                kinds: inventory.kinds,
                paths: vec![path("foo", false), path("format", false)],
                payloads: vec![],
            },
        );
        assert_eq!(files_only.shared_extension, "o");
    }

    #[test]
    fn payload_menu_preserves_order_and_matches_payload_or_label() {
        let detected = context("@plan:a", 7).unwrap();
        let menu = build_at_reference_menu(
            &detected,
            &AtReferenceInventoryWire {
                kinds: vec![],
                paths: vec![],
                payloads: vec![
                    AtReferencePayloadRowWire {
                        payload: "zulu".to_string(),
                        label: "Alpha label".to_string(),
                        detail: "plan".to_string(),
                        age: "2h".to_string(),
                    },
                    AtReferencePayloadRowWire {
                        payload: "able".to_string(),
                        label: "Second".to_string(),
                        detail: String::new(),
                        age: String::new(),
                    },
                    AtReferencePayloadRowWire {
                        payload: "nope".to_string(),
                        label: "No match".to_string(),
                        detail: String::new(),
                        age: String::new(),
                    },
                ],
            },
        );
        assert_eq!(
            menu.rows
                .iter()
                .map(|row| row.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["@plan:zulu", "@plan:able"]
        );
        assert_eq!(menu.rows[0].detail, "plan · 2h");
    }
}
