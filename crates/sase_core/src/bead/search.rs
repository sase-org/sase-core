//! Pure bead full-text search.

use std::borrow::Cow;
use std::path::Path;

use super::read::{list_issues_in_issues, read_store_issues};
use super::wire::{
    BeadError, BeadSearchMatchWire, BeadTierWire, IssueTypeWire, IssueWire,
    StatusWire,
};

pub const BEAD_SEARCH_FIELD_NAMES: &[&str] = &[
    "id",
    "title",
    "description",
    "notes",
    "design",
    "owner",
    "assignee",
    "model",
    "changespec_name",
    "changespec_bug_id",
    "status",
    "type",
    "tier",
];

pub fn search_issues(
    beads_dir: &Path,
    query: &str,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
    limit: Option<usize>,
) -> Result<Vec<BeadSearchMatchWire>, BeadError> {
    search_issues_in_issues(
        read_store_issues(beads_dir)?,
        query,
        statuses,
        issue_types,
        tiers,
        limit,
    )
}

fn search_issues_in_issues(
    issues: Vec<IssueWire>,
    query: &str,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
    limit: Option<usize>,
) -> Result<Vec<BeadSearchMatchWire>, BeadError> {
    if query.trim().is_empty() {
        return Err(BeadError::validation("search query cannot be empty"));
    }

    let needle = query.to_lowercase();
    let filtered = list_issues_in_issues(issues, statuses, issue_types, tiers)?;
    let max = limit.unwrap_or(0);
    let mut matches = Vec::new();
    for issue in filtered {
        let matched_fields = matched_field_names(&issue, &needle);
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

fn matched_field_names(issue: &IssueWire, needle: &str) -> Vec<String> {
    searchable_fields(issue)
        .into_iter()
        .filter(|field| field.value.to_lowercase().contains(needle))
        .map(|field| field.name.to_string())
        .collect()
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
        field("owner", issue.owner.as_str()),
        field("assignee", issue.assignee.as_str()),
        field("model", issue.model.as_str()),
        field("changespec_name", issue.changespec_name.as_str()),
        field("changespec_bug_id", issue.changespec_bug_id.as_str()),
        field("status", status_value(&issue.status)),
        field("type", issue_type_value(&issue.issue_type)),
    ];
    if let Some(tier) = &issue.tier {
        fields.push(field("tier", tier_value(tier)));
    }
    fields
}

fn status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn issue_type_value(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
    }
}

fn tier_value(tier: &BeadTierWire) -> &'static str {
    match tier {
        BeadTierWire::Plan => "plan",
        BeadTierWire::Epic => "epic",
        BeadTierWire::Legend => "legend",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
                "status",
                phase_issue_with(|issue| issue.status = StatusWire::Closed),
                "closed",
            ),
            ("type", phase_issue_with(|_| {}), "phase"),
            (
                "tier",
                plan_issue_with(|issue| {
                    issue.tier = Some(BeadTierWire::Legend)
                }),
                "legend",
            ),
        ];

        for (field_name, issue, query) in cases {
            let results = search_issues_in_issues(
                vec![issue],
                query,
                None,
                None,
                None,
                None,
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
    fn field_names_constant_matches_collected_plan_fields() {
        let issue = plan_issue_with(|issue| {
            issue.tier = Some(BeadTierWire::Legend);
        });
        let field_names = searchable_fields(&issue)
            .into_iter()
            .map(|field| field.name)
            .collect::<Vec<_>>();

        assert_eq!(field_names, BEAD_SEARCH_FIELD_NAMES);
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
        let legend_plan = plan_issue_with(|issue| {
            issue.id = "beads-3".to_string();
            issue.title = "Auth legend".to_string();
            issue.tier = Some(BeadTierWire::Legend);
            issue.created_at = "2026-01-01T00:04:00Z".to_string();
        });

        let results = search_issues_in_issues(
            vec![open_phase, closed_phase, epic_plan, legend_plan],
            "auth",
            Some(&["open".to_string()]),
            Some(&["plan".to_string()]),
            Some(&["epic".to_string()]),
            None,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-2"]);
    }

    #[test]
    fn applies_limit_after_filtering_and_matching() {
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
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1", "beads-1.2"]);
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
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1", "beads-1.2"]);
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
            )
            .unwrap_err();
            assert_eq!(err.kind, "validation");
            assert_eq!(err.message, "search query cannot be empty");
        }
    }

    #[test]
    fn keeps_list_ordering() {
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

        let results = search_issues_in_issues(
            vec![later, earlier],
            "needle",
            None,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(ids(&results), vec!["beads-1.1", "beads-1.2"]);
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
        )
        .unwrap();

        assert!(results.is_empty());
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
            search_issues(&beads_dir, "needle", None, None, None, None)
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
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            is_ready_to_work: false,
            epic_count: None,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
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
}
