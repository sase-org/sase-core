//! Pure bead full-text search.

use std::borrow::Cow;
use std::path::Path;

use regex::{Regex, RegexBuilder};

use super::read::{list_issues_in_issues, read_store_issues};
use super::wire::{
    BeadError, BeadResolutionWire, BeadSearchMatchWire, BeadTierWire,
    IssueTypeWire, IssueWire, PhaseSizeWire, StatusWire,
};

pub const BEAD_SEARCH_FIELD_NAMES: &[&str] = &[
    "id",
    "title",
    "description",
    "notes",
    "design",
    "refs",
    "plus_one_evidence",
    "close_history",
    "owner",
    "assignee",
    "model",
    "size",
    "changespec_name",
    "changespec_bug_id",
    "external_ref",
    "status",
    "type",
    "tier",
];

const REGEX_SIZE_LIMIT: usize = 10 * 1024 * 1024;

pub fn search_issues(
    beads_dir: &Path,
    query: &str,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
    limit: Option<usize>,
    regex: bool,
) -> Result<Vec<BeadSearchMatchWire>, BeadError> {
    search_issues_in_issues(
        read_store_issues(beads_dir)?,
        query,
        statuses,
        issue_types,
        tiers,
        limit,
        regex,
    )
}

pub(crate) fn search_issues_in_issues(
    issues: Vec<IssueWire>,
    query: &str,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
    limit: Option<usize>,
    regex: bool,
) -> Result<Vec<BeadSearchMatchWire>, BeadError> {
    let matcher = SearchMatcher::new(query, regex)?;
    search_issues_in_issues_with_matcher(
        issues,
        &matcher,
        statuses,
        issue_types,
        tiers,
        limit,
    )
}

pub(crate) fn search_issues_in_issues_with_matcher(
    issues: Vec<IssueWire>,
    matcher: &SearchMatcher,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
    limit: Option<usize>,
) -> Result<Vec<BeadSearchMatchWire>, BeadError> {
    let filtered = list_issues_in_issues(issues, statuses, issue_types, tiers)?;
    let max = limit.unwrap_or(0);
    let mut matches = Vec::new();
    // `list_issues_in_issues` returns candidates sorted by `created_at`
    // ascending; iterate in reverse so newer matches come before older ones,
    // and so `--limit` keeps the newest matches.
    for issue in filtered.into_iter().rev() {
        let matched_fields = matched_field_names(&issue, matcher);
        if matched_fields.is_empty() {
            continue;
        }
        matches.push(BeadSearchMatchWire {
            issue,
            matched_fields,
        });
        if max > 0 && matches.len() >= max {
            break;
        }
    }
    Ok(matches)
}

#[derive(Debug, Clone)]
pub(crate) struct SearchMatcher {
    query: String,
    kind: SearchMatcherKind,
}

#[derive(Debug, Clone)]
enum SearchMatcherKind {
    Literal { needle: String },
    Regex(Regex),
}

impl SearchMatcher {
    pub(crate) fn new(query: &str, regex: bool) -> Result<Self, BeadError> {
        if query.trim().is_empty() {
            return Err(BeadError::validation("search query cannot be empty"));
        }

        let kind = if regex {
            RegexBuilder::new(query)
                .case_insensitive(true)
                .size_limit(REGEX_SIZE_LIMIT)
                .build()
                .map(SearchMatcherKind::Regex)
                .map_err(|err| {
                    BeadError::validation(format!(
                        "invalid search regex: {err}"
                    ))
                })?
        } else {
            SearchMatcherKind::Literal {
                needle: query.to_lowercase(),
            }
        };

        Ok(Self {
            query: query.to_string(),
            kind,
        })
    }

    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    pub(crate) fn is_regex(&self) -> bool {
        matches!(self.kind, SearchMatcherKind::Regex(_))
    }

    pub(crate) fn is_match(&self, value: &str) -> bool {
        match &self.kind {
            SearchMatcherKind::Literal { needle } => {
                value.to_lowercase().contains(needle)
            }
            SearchMatcherKind::Regex(regex) => regex.is_match(value),
        }
    }

    pub(crate) fn byte_ranges(&self, value: &str) -> Vec<(usize, usize)> {
        match &self.kind {
            SearchMatcherKind::Literal { needle } => {
                case_insensitive_byte_ranges(value, needle)
            }
            SearchMatcherKind::Regex(regex) => regex
                .find_iter(value)
                .filter(|matched| matched.start() < matched.end())
                .map(|matched| (matched.start(), matched.end()))
                .collect(),
        }
    }
}

fn matched_field_names(
    issue: &IssueWire,
    matcher: &SearchMatcher,
) -> Vec<String> {
    searchable_fields(issue)
        .into_iter()
        .filter(|field| matcher.is_match(&field.value))
        .map(|field| field.name.to_string())
        .collect()
}

fn case_insensitive_byte_ranges(
    text: &str,
    needle: &str,
) -> Vec<(usize, usize)> {
    if needle.is_empty() {
        return Vec::new();
    }
    let (folded, offsets) = folded_with_offsets(text);
    let mut ranges = Vec::new();
    let mut search_start = 0;
    while let Some(relative_start) = folded[search_start..].find(needle) {
        let folded_start = search_start + relative_start;
        let folded_end = folded_start + needle.len();
        let Some(original_start) = offsets
            .iter()
            .find(|(start, end, _, _)| {
                *start <= folded_start && folded_start < *end
            })
            .map(|(_, _, original_start, _)| *original_start)
        else {
            break;
        };
        let Some(original_end) = offsets
            .iter()
            .rev()
            .find(|(start, end, _, _)| {
                *start < folded_end && folded_end <= *end
            })
            .map(|(_, _, _, original_end)| *original_end)
        else {
            break;
        };
        ranges.push((original_start, original_end));
        search_start = folded_end;
    }
    ranges
}

fn folded_with_offsets(
    text: &str,
) -> (String, Vec<(usize, usize, usize, usize)>) {
    let mut folded = String::new();
    let mut offsets = Vec::new();
    for (original_start, ch) in text.char_indices() {
        let original_end = original_start + ch.len_utf8();
        for lower in ch.to_lowercase() {
            let folded_start = folded.len();
            folded.push(lower);
            let folded_end = folded.len();
            offsets.push((
                folded_start,
                folded_end,
                original_start,
                original_end,
            ));
        }
    }
    (folded, offsets)
}

struct SearchField<'a> {
    name: &'static str,
    value: Cow<'a, str>,
}

fn field<'a>(
    name: &'static str,
    value: impl Into<Cow<'a, str>>,
) -> SearchField<'a> {
    SearchField {
        name,
        value: value.into(),
    }
}

fn searchable_fields(issue: &IssueWire) -> Vec<SearchField<'_>> {
    let mut fields = vec![
        field("id", issue.id.as_str()),
        field("title", issue.title.as_str()),
        field("description", issue.description.as_str()),
        field("notes", issue.notes.as_str()),
        field("design", issue.design.as_str()),
        field("refs", issue.refs.join("\n")),
        field(
            "plus_one_evidence",
            issue
                .plus_one_evidence
                .iter()
                .flat_map(|evidence| {
                    [
                        Some(evidence.reporter.as_str()),
                        Some(evidence.note.as_str()),
                        evidence.observed_since.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .chain(evidence.refs.iter().map(String::as_str))
                })
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        field(
            "close_history",
            issue
                .close_history
                .iter()
                .flat_map(|record| {
                    [
                        record.close_reason.as_deref(),
                        record
                            .resolution
                            .as_ref()
                            .map(BeadResolutionWire::as_str),
                        Some(record.closed_at.as_str()),
                        Some(record.reopened_at.as_str()),
                    ]
                })
                .flatten()
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        field("owner", issue.owner.as_str()),
        field("assignee", issue.assignee.as_str()),
        field("model", issue.model.as_str()),
        field("changespec_name", issue.changespec_name.as_str()),
        field("changespec_bug_id", issue.changespec_bug_id.as_str()),
        field("external_ref", issue.external_ref.as_str()),
        field("status", status_value(&issue.status)),
        field("type", issue_type_value(&issue.issue_type)),
    ];
    if let Some(size) = &issue.size {
        fields.push(field("size", phase_size_value(size)));
    }
    if let Some(tier) = &issue.tier {
        fields.push(field("tier", tier_value(tier)));
    }
    fields
}

fn status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::Ready => "ready",
        StatusWire::Snoozed => "snoozed",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn issue_type_value(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
        IssueTypeWire::Task => "task",
        IssueTypeWire::Flag => "flag",
    }
}

fn tier_value(tier: &BeadTierWire) -> &'static str {
    match tier {
        BeadTierWire::Plan => "plan",
        BeadTierWire::Epic => "epic",
    }
}

fn phase_size_value(size: &PhaseSizeWire) -> &'static str {
    size.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;

    use super::super::wire::{
        BeadCloseRecordWire, BeadReopenCauseWire, TaskPlusOneEvidenceWire,
    };

    use tempfile::tempdir;

    #[test]
    fn matches_every_searchable_field() {
        let cases = vec![
            (
                "id",
                phase_issue_with(|issue| issue.id = "needle-id".to_string()),
                "needle-id",
            ),
            (
                "title",
                phase_issue_with(|issue| {
                    issue.title = "Needle title".to_string()
                }),
                "needle",
            ),
            (
                "description",
                phase_issue_with(|issue| {
                    issue.description = "Needle description".to_string();
                }),
                "needle",
            ),
            (
                "notes",
                phase_issue_with(|issue| {
                    issue.notes = "Needle notes".to_string()
                }),
                "needle",
            ),
            (
                "design",
                phase_issue_with(|issue| {
                    issue.design = "plans/needle.md".to_string();
                }),
                "needle",
            ),
            (
                "refs",
                phase_issue_with(|issue| {
                    issue.refs = vec!["research:202607/needle.md".to_string()];
                }),
                "needle",
            ),
            (
                "plus_one_evidence",
                task_issue_with(|issue| {
                    issue.plus_one_evidence = vec![TaskPlusOneEvidenceWire {
                        timestamp: "2026-08-01T15:00:00Z".to_string(),
                        observed_since: Some(
                            "2026-01-01T00:00:00Z".to_string(),
                        ),
                        reporter: "agent.beta".to_string(),
                        note: "Saw this before the close landed.".to_string(),
                        refs: Vec::new(),
                    }];
                }),
                "2026-01-01T00:00:00Z",
            ),
            (
                "close_history",
                phase_issue_with(|issue| {
                    issue.close_history = vec![close_record_with(|record| {
                        record.close_reason =
                            Some("Needle close reason".to_string());
                    })];
                }),
                "needle",
            ),
            (
                "owner",
                phase_issue_with(|issue| {
                    issue.owner = "needle@example.com".to_string();
                }),
                "needle",
            ),
            (
                "assignee",
                phase_issue_with(|issue| {
                    issue.assignee = "needle@example.com".to_string();
                }),
                "needle",
            ),
            (
                "model",
                phase_issue_with(|issue| {
                    issue.model = "needle/model".to_string()
                }),
                "needle",
            ),
            (
                "size",
                phase_issue_with(|issue| {
                    issue.size = Some(PhaseSizeWire::Large)
                }),
                "large",
            ),
            (
                "changespec_name",
                plan_issue_with(|issue| {
                    issue.changespec_name = "needle_changespec".to_string();
                }),
                "needle",
            ),
            (
                "changespec_bug_id",
                plan_issue_with(|issue| {
                    issue.changespec_bug_id = "NEEDLE-123".to_string();
                    issue.changespec_name = "has_bug".to_string();
                }),
                "needle",
            ),
            (
                "external_ref",
                task_issue_with(|issue| {
                    issue.external_ref = "bug:sase#42".to_string();
                }),
                "sase#42",
            ),
            (
                "status",
                task_issue_with(|issue| issue.status = StatusWire::Ready),
                "ready",
            ),
            ("type", task_issue_with(|_| {}), "task"),
            ("tier", plan_issue_with(|_| {}), "epic"),
        ];

        for (field_name, issue, query) in cases {
            let results = search_issues_in_issues(
                vec![issue],
                query,
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();
            assert_eq!(results.len(), 1, "field {field_name}");
            assert_eq!(
                results[0].matched_fields,
                vec![field_name.to_string()],
                "field {field_name}"
            );
        }
    }

    #[test]
    fn field_names_constant_matches_all_collectable_fields() {
        let mut field_names = searchable_fields(&plan_issue_with(|_| {}))
            .into_iter()
            .map(|field| field.name)
            .collect::<Vec<_>>();
        field_names.extend(
            searchable_fields(&phase_issue_with(|issue| {
                issue.size = Some(PhaseSizeWire::Small);
            }))
            .into_iter()
            .map(|field| field.name),
        );
        field_names.sort_unstable();
        field_names.dedup();
        let mut expected = BEAD_SEARCH_FIELD_NAMES.to_vec();
        expected.sort_unstable();

        assert_eq!(field_names, expected);
    }

    #[test]
    fn matches_an_archived_close_reason() {
        let issue = phase_issue_with(|issue| {
            issue.close_history = vec![close_record_with(|record| {
                record.close_reason =
                    Some("superseded by the kaleidoscope rewrite".to_string());
            })];
        });

        let results = search_issues_in_issues(
            vec![issue],
            "kaleidoscope",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1"]);
        assert_eq!(results[0].matched_fields, vec!["close_history"]);
    }

    #[test]
    fn matches_an_archived_resolution() {
        let issue = phase_issue_with(|issue| {
            issue.close_history = vec![close_record_with(|record| {
                record.resolution = Some(BeadResolutionWire::Superseded);
            })];
        });

        let results = search_issues_in_issues(
            vec![issue],
            "superseded",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1"]);
        assert_eq!(results[0].matched_fields, vec!["close_history"]);
    }

    #[test]
    fn issues_without_close_history_do_not_match_an_archived_reason() {
        let archived = phase_issue_with(|issue| {
            issue.close_history = vec![close_record_with(|record| {
                record.close_reason =
                    Some("closed for kaleidoscope reasons".to_string());
            })];
        });
        let untouched = task_issue_with(|_| {});

        let results = search_issues_in_issues(
            vec![archived, untouched],
            "kaleidoscope",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1"]);
    }

    #[test]
    fn matches_case_insensitive_unicode_substrings() {
        let issue = phase_issue_with(|issue| {
            issue.title = "CAFÉ auth".to_string();
        });

        let results = search_issues_in_issues(
            vec![issue],
            "café",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1"]);
        assert_eq!(results[0].matched_fields, vec!["title"]);
    }

    #[test]
    fn closed_issues_match_by_default() {
        let issue = phase_issue_with(|issue| {
            issue.status = StatusWire::Closed;
            issue.title = "Auth cleanup".to_string();
        });

        let results = search_issues_in_issues(
            vec![issue],
            "auth",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1"]);
    }

    #[test]
    fn filters_status_type_and_tier_together() {
        let open_phase = phase_issue_with(|issue| {
            issue.id = "beads-1.1".to_string();
            issue.title = "Auth phase".to_string();
        });
        let closed_phase = phase_issue_with(|issue| {
            issue.id = "beads-1.2".to_string();
            issue.title = "Auth closed phase".to_string();
            issue.status = StatusWire::Closed;
            issue.created_at = "2026-01-01T00:02:00Z".to_string();
        });
        let epic_plan = plan_issue_with(|issue| {
            issue.id = "beads-2".to_string();
            issue.title = "Auth epic".to_string();
            issue.tier = Some(BeadTierWire::Epic);
            issue.created_at = "2026-01-01T00:03:00Z".to_string();
        });
        let results = search_issues_in_issues(
            vec![open_phase, closed_phase, epic_plan],
            "auth",
            Some(&["open".to_string()]),
            Some(&["plan".to_string()]),
            Some(&["epic".to_string()]),
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-2"]);
    }

    #[test]
    fn limit_keeps_newest_matches() {
        let issues = (1..=3)
            .map(|idx| {
                phase_issue_with(|issue| {
                    issue.id = format!("beads-1.{idx}");
                    issue.title = "Needle".to_string();
                    issue.created_at = format!("2026-01-01T00:0{idx}:00Z");
                })
            })
            .collect();

        let results = search_issues_in_issues(
            issues,
            "needle",
            None,
            None,
            None,
            Some(2),
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.3", "beads-1.2"]);
    }

    #[test]
    fn zero_limit_is_unlimited() {
        let issues = (1..=2)
            .map(|idx| {
                phase_issue_with(|issue| {
                    issue.id = format!("beads-1.{idx}");
                    issue.title = "Needle".to_string();
                    issue.created_at = format!("2026-01-01T00:0{idx}:00Z");
                })
            })
            .collect();

        let results = search_issues_in_issues(
            issues,
            "needle",
            None,
            None,
            None,
            Some(0),
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.2", "beads-1.1"]);
    }

    #[test]
    fn rejects_empty_or_whitespace_query() {
        for query in ["", "   ", "\n\t"] {
            let err = search_issues_in_issues(
                vec![phase_issue_with(|_| {})],
                query,
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap_err();
            assert_eq!(err.kind, "validation");
            assert_eq!(err.message, "search query cannot be empty");
        }
    }

    #[test]
    fn returns_newest_matches_before_older_matches() {
        let later = phase_issue_with(|issue| {
            issue.id = "beads-1.2".to_string();
            issue.title = "Needle later".to_string();
            issue.created_at = "2026-01-01T00:02:00Z".to_string();
        });
        let earlier = phase_issue_with(|issue| {
            issue.id = "beads-1.1".to_string();
            issue.title = "Needle earlier".to_string();
            issue.created_at = "2026-01-01T00:01:00Z".to_string();
        });

        // Pass earlier-then-later input to prove the order comes from
        // `created_at`, not from input position.
        let results = search_issues_in_issues(
            vec![earlier, later],
            "needle",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.2", "beads-1.1"]);
    }

    #[test]
    fn no_match_returns_empty_results() {
        let results = search_issues_in_issues(
            vec![phase_issue_with(|_| {})],
            "missing",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn literal_search_treats_regex_metacharacters_literally() {
        let issue = phase_issue_with(|issue| {
            issue.title = "Fix auth.* route".to_string();
        });
        let regex_like_but_literal = phase_issue_with(|issue| {
            issue.id = "beads-1.2".to_string();
            issue.title = "Fix authxyz route".to_string();
            issue.created_at = "2026-01-01T00:02:00Z".to_string();
        });

        let results = search_issues_in_issues(
            vec![issue, regex_like_but_literal],
            "auth.*",
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1"]);
    }

    #[test]
    fn literal_match_truth_uses_plain_case_folded_containment() {
        let matcher =
            SearchMatcher::new("auth.*", false).expect("valid matcher");

        assert!(matcher.is_match("Fix auth.* route"));
        assert!(!matcher.is_match("Fix authxyz route"));
        assert_eq!(matcher.byte_ranges("Fix auth.* route"), vec![(4, 10)]);
    }

    #[test]
    fn regex_search_matches_patterns_case_insensitively_by_default() {
        let later = phase_issue_with(|issue| {
            issue.id = "beads-1.2".to_string();
            issue.title = "AUTH service".to_string();
            issue.created_at = "2026-01-01T00:02:00Z".to_string();
        });
        let earlier = phase_issue_with(|issue| {
            issue.id = "beads-1.1".to_string();
            issue.title = "auth token".to_string();
            issue.created_at = "2026-01-01T00:01:00Z".to_string();
        });

        let results = search_issues_in_issues(
            vec![earlier, later],
            r"auth\s+(service|token)",
            None,
            None,
            None,
            Some(1),
            true,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.2"]);
        assert_eq!(results[0].matched_fields, vec!["title"]);
    }

    #[test]
    fn regex_search_allows_inline_case_sensitivity() {
        let lowercase = phase_issue_with(|issue| {
            issue.id = "beads-1.1".to_string();
            issue.title = "needle".to_string();
        });
        let uppercase = phase_issue_with(|issue| {
            issue.id = "beads-1.2".to_string();
            issue.title = "Needle".to_string();
            issue.created_at = "2026-01-01T00:02:00Z".to_string();
        });

        let results = search_issues_in_issues(
            vec![lowercase, uppercase],
            r"(?-i:Needle)",
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.2"]);
    }

    #[test]
    fn invalid_regex_is_a_validation_error() {
        let err = search_issues_in_issues(
            vec![phase_issue_with(|_| {})],
            "[",
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.starts_with("invalid search regex: "));
    }

    #[test]
    fn zero_width_regex_matches_truth_but_not_highlight_ranges() {
        let zero_width =
            SearchMatcher::new(r"\b", true).expect("valid regex matcher");
        assert!(zero_width.byte_ranges("auth token").is_empty());
        assert!(zero_width.is_match("auth token"));

        let word =
            SearchMatcher::new(r"\bauth\b", true).expect("valid matcher");
        assert_eq!(word.byte_ranges("auth token"), vec![(0, 4)]);
    }

    #[test]
    fn zero_width_regex_matches_fields() {
        let issue = phase_issue_with(|issue| {
            issue.title = "Auth boundary".to_string();
        });

        let results = search_issues_in_issues(
            vec![issue],
            r"\b",
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1"]);
        assert!(results[0].matched_fields.contains(&"title".to_string()));
    }

    #[test]
    fn public_search_loads_store_and_returns_wire_matches() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        let issue = phase_issue_with(|issue| {
            issue.title = "Store needle".to_string();
        });
        fs::write(
            beads_dir.join("issues.jsonl"),
            format!("{}\n", serde_json::to_string(&issue).unwrap()),
        )
        .unwrap();

        let results =
            search_issues(&beads_dir, "needle", None, None, None, None, false)
                .unwrap();

        assert_eq!(
            results,
            vec![BeadSearchMatchWire {
                issue,
                matched_fields: vec!["title".to_string()],
            }]
        );
    }

    fn ids(results: &[BeadSearchMatchWire]) -> Vec<&str> {
        results
            .iter()
            .map(|result| result.issue.id.as_str())
            .collect()
    }

    fn close_record_with(
        update: impl FnOnce(&mut BeadCloseRecordWire),
    ) -> BeadCloseRecordWire {
        let mut record = BeadCloseRecordWire {
            closed_at: "2026-01-01T00:02:00Z".to_string(),
            close_reason: None,
            resolution: None,
            reopened_at: "2026-01-01T00:03:00Z".to_string(),
            reopened_via: BeadReopenCauseWire::PlusOne,
            reopened_by: None,
        };
        update(&mut record);
        record
    }

    fn phase_issue_with(update: impl FnOnce(&mut IssueWire)) -> IssueWire {
        let mut issue = IssueWire {
            id: "beads-1.1".to_string(),
            title: "Neutral item".to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Phase,
            tier: None,
            parent_id: Some("beads-1".to_string()),
            owner: String::new(),
            assignee: String::new(),
            created_at: "2026-01-01T00:01:00Z".to_string(),
            created_by: String::new(),
            updated_at: "2026-01-01T00:01:00Z".to_string(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            close_history: Vec::new(),
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            refs: Vec::new(),
            plus_one_evidence: Vec::new(),
            snooze: None,
            flag: None,
            model: String::new(),
            size: None,
            task_type: None,
            task_type_fields: BTreeMap::new(),
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            external_ref: String::new(),
            dependencies: Vec::new(),
        };
        update(&mut issue);
        issue
    }

    fn plan_issue_with(update: impl FnOnce(&mut IssueWire)) -> IssueWire {
        let mut issue = phase_issue_with(|_| {});
        issue.id = "beads-1".to_string();
        issue.title = "Neutral item".to_string();
        issue.issue_type = IssueTypeWire::Plan;
        issue.tier = Some(BeadTierWire::Epic);
        issue.parent_id = None;
        update(&mut issue);
        issue
    }

    fn task_issue_with(update: impl FnOnce(&mut IssueWire)) -> IssueWire {
        let mut issue = phase_issue_with(|_| {});
        issue.id = "beads-2".to_string();
        issue.issue_type = IssueTypeWire::Task;
        issue.parent_id = None;
        update(&mut issue);
        issue
    }
}
