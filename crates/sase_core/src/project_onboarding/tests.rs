use std::collections::BTreeMap;

use super::*;

fn classify_input() -> ProjectCandidateInputWire {
    ProjectCandidateInputWire {
        schema_version: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
        workflow_type: "gh".to_string(),
        sidecar_roles: vec![
            "plans".into(),
            "beads".into(),
            "agents".into(),
            "research".into(),
            "sdd".into(),
        ],
        owned_namespaces: vec!["sase-org".into(), "bbugyi200".into()],
        ..ProjectCandidateInputWire::default()
    }
}

fn remote(identity: &str, name: &str) -> ProjectCandidateRemoteWire {
    ProjectCandidateRemoteWire {
        namespace: "sase-org".into(),
        identity_key: identity.into(),
        repo_ref: format!("sase-org/{name}"),
        display: format!("sase-org/{name}"),
        name: name.into(),
        ..ProjectCandidateRemoteWire::default()
    }
}

fn local(identity: &str, owner: &str, name: &str) -> ProjectCandidateLocalWire {
    ProjectCandidateLocalWire {
        identity_key: identity.into(),
        repo_ref: format!("{owner}/{name}"),
        display: format!("{owner}/{name}"),
        owner: owner.into(),
        name: name.into(),
        path: format!("/tmp/{owner}/{name}"),
        numbered_copy: false,
    }
}

fn row_by_name<'a>(
    result: &'a ProjectCandidateListWire,
    name: &str,
) -> &'a ProjectCandidateRowWire {
    result
        .groups
        .iter()
        .flat_map(|group| group.rows.iter())
        .chain(result.search_only.iter())
        .find(|row| {
            row.display.ends_with(name) || row.identity_key.ends_with(name)
        })
        .unwrap_or_else(|| panic!("missing row {name}"))
}

fn group_ids(result: &ProjectCandidateListWire) -> Vec<&str> {
    result
        .groups
        .iter()
        .map(|group| group.id.as_str())
        .collect()
}

fn hidden_reason(result: &ProjectCandidateListWire, name: &str) -> String {
    row_by_name(result, name).hidden_reason.clone()
}

fn gh_form() -> RepoCreationFormWire {
    RepoCreationFormWire {
        workflow_type: "gh".into(),
        namespace: "sase-org".into(),
        fields: vec![
            field("description", FIELD_KIND_LINE, "Description", ""),
            RepoCreationFieldWire {
                name: "visibility".into(),
                label: "Visibility".into(),
                kind: FIELD_KIND_CHOICE.into(),
                group: "basics".into(),
                default: "private".into(),
                choices: vec![
                    choice("public", "Public", ""),
                    choice("private", "Private", ""),
                    choice("internal", "Internal", "enterprise orgs only"),
                ],
                ..RepoCreationFieldWire::default()
            },
            bool_field("add_readme", "README", "true"),
            search_field("gitignore", "gitignore"),
            search_field("license", "License"),
            RepoCreationFieldWire {
                name: "template".into(),
                label: "Template".into(),
                kind: FIELD_KIND_LINE.into(),
                group: "advanced".into(),
                excludes: vec![
                    "add_readme".into(),
                    "gitignore".into(),
                    "license".into(),
                    "team".into(),
                ],
                ..RepoCreationFieldWire::default()
            },
            RepoCreationFieldWire {
                name: "include_all_branches".into(),
                label: "Include all branches".into(),
                kind: FIELD_KIND_BOOL.into(),
                group: "advanced".into(),
                default: "false".into(),
                requires: vec!["template".into()],
                ..RepoCreationFieldWire::default()
            },
            field("homepage", FIELD_KIND_LINE, "Homepage", ""),
            RepoCreationFieldWire {
                name: "team".into(),
                label: "Team".into(),
                kind: FIELD_KIND_SEARCH.into(),
                group: "advanced".into(),
                disabled_reason: String::new(),
                ..RepoCreationFieldWire::default()
            },
            bool_field("issues", "Issues", "true"),
            bool_field("wiki", "Wiki", "true"),
        ],
        can_create: Some(true),
        ..RepoCreationFormWire::default()
    }
}

fn field(
    name: &str,
    kind: &str,
    label: &str,
    default: &str,
) -> RepoCreationFieldWire {
    RepoCreationFieldWire {
        name: name.into(),
        label: label.into(),
        kind: kind.into(),
        group: "basics".into(),
        default: default.into(),
        ..RepoCreationFieldWire::default()
    }
}

fn bool_field(name: &str, label: &str, default: &str) -> RepoCreationFieldWire {
    field(name, FIELD_KIND_BOOL, label, default)
}

fn search_field(name: &str, label: &str) -> RepoCreationFieldWire {
    field(name, FIELD_KIND_SEARCH, label, "")
}

fn choice(value: &str, label: &str, disabled: &str) -> RepoCreationChoiceWire {
    RepoCreationChoiceWire {
        value: value.into(),
        label: label.into(),
        disabled_reason: disabled.into(),
        ..RepoCreationChoiceWire::default()
    }
}

fn evaluate(values: Vec<(&str, &str)>, mode: &str) -> CreationFormStateWire {
    let request = CreationFormRequestWire {
        schema_version: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
        form: gh_form(),
        values: values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        mode: mode.into(),
    };
    evaluate_repo_creation_form(&request).unwrap()
}

fn field_state<'a>(
    state: &'a CreationFormStateWire,
    name: &str,
) -> &'a CreationFormFieldStateWire {
    state
        .fields
        .iter()
        .find(|field| field.name == name)
        .unwrap_or_else(|| panic!("missing field {name}"))
}

#[test]
fn schema_mismatch_is_rejected() {
    let mut input = classify_input();
    input.schema_version = 99;
    let error = classify_project_candidates(&input).unwrap_err();
    assert!(matches!(
        error,
        ProjectOnboardingError::UnsupportedSchema {
            actual: 99,
            expected: 1
        }
    ));
}

#[test]
fn getter_reports_schema_version() {
    assert_eq!(project_onboarding_wire_schema_version(), 1);
}

#[test]
fn sidecar_by_inventory_is_hidden() {
    let mut input = classify_input();
    input
        .remote
        .push(remote("github.com/sase-org/sase--plans", "sase--plans"));
    input.inventory.push(ProjectCandidateInventoryWire {
        identity_key: "github.com/sase-org/sase--plans".into(),
        kind: INVENTORY_SIDECAR.into(),
        project_key: "sase".into(),
        open_count: 3,
        last_open_at: "2026-10-01T00:00:00Z".into(),
    });
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(hidden_reason(&result, "sase--plans"), HIDDEN_SIDECAR);
    assert_eq!(result.hidden[0].reason, HIDDEN_SIDECAR);
    assert_eq!(result.hidden[0].count, 1);
}

#[test]
fn sidecar_by_role_suffix_is_hidden() {
    let mut input = classify_input();
    input
        .remote
        .push(remote("github.com/sase-org/forge--beads", "forge--beads"));
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(hidden_reason(&result, "forge--beads"), HIDDEN_SIDECAR);
}

#[test]
fn user_repo_with_non_role_double_dash_stays_suggested() {
    let mut input = classify_input();
    input
        .remote
        .push(remote("github.com/sase-org/foo--bar", "foo--bar"));
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(row_by_name(&result, "foo--bar").group, GROUP_SUGGESTED);
    assert!(row_by_name(&result, "foo--bar").hidden_reason.is_empty());
}

#[test]
fn fork_and_archived_are_hidden() {
    let mut input = classify_input();
    let mut fork = remote("github.com/sase-org/forked", "forked");
    fork.is_fork = true;
    let mut archived = remote("github.com/sase-org/old", "old");
    archived.is_archived = true;
    input.remote.extend([fork, archived]);
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(hidden_reason(&result, "forked"), HIDDEN_FORK);
    assert_eq!(hidden_reason(&result, "old"), HIDDEN_ARCHIVED);
}

#[test]
fn numbered_copy_and_third_party_local_are_hidden() {
    let mut input = classify_input();
    let mut numbered = local("github.com/sase-org/sase", "sase-org", "sase");
    numbered.numbered_copy = true;
    numbered.path = "/tmp/sase_2".into();
    let third = local("github.com/other/tool", "other", "tool");
    input.local.extend([numbered, third]);
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(hidden_reason(&result, "sase"), HIDDEN_NUMBERED_COPY);
    assert_eq!(hidden_reason(&result, "tool"), HIDDEN_THIRD_PARTY_LOCAL);
}

#[test]
fn all_numbered_local_set_stays_hidden_as_numbered_copy() {
    let mut input = classify_input();
    let mut first = local("github.com/sase-org/sase", "sase-org", "sase");
    first.numbered_copy = true;
    first.path = "/tmp/sase_2".into();
    let mut second = local("github.com/sase-org/sase", "sase-org", "sase");
    second.numbered_copy = true;
    second.path = "/tmp/sase_3".into();
    input.local.extend([first, second]);
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(hidden_reason(&result, "sase"), HIDDEN_NUMBERED_COPY);
    assert!(result.groups.is_empty());
}

#[test]
fn primary_checkout_plus_numbered_sibling_stays_local_in_both_orders() {
    for primary_first in [true, false] {
        let mut input = classify_input();
        let mut primary = local("github.com/sase-org/sase", "sase-org", "sase");
        primary.path = "/tmp/sase-org/sase".into();
        let mut sibling = local("github.com/sase-org/sase", "sase-org", "sase");
        sibling.numbered_copy = true;
        sibling.path = "/tmp/sase-org/sase_2".into();
        if primary_first {
            input.local.extend([primary, sibling]);
        } else {
            input.local.extend([sibling, primary]);
        }
        let result = classify_project_candidates(&input).unwrap();
        let row = row_by_name(&result, "sase");
        assert!(
            row.hidden_reason.is_empty(),
            "primary_first={primary_first}: hidden as {}",
            row.hidden_reason
        );
        assert_eq!(row.group, GROUP_LOCAL);
        assert_eq!(row.local_path, "/tmp/sase-org/sase");
    }
}

#[test]
fn inventory_only_identity_seeds_no_selectable_row() {
    for kind in [INVENTORY_EXTERNAL, INVENTORY_LINKED, INVENTORY_SIDECAR] {
        let mut input = classify_input();
        input.inventory.push(ProjectCandidateInventoryWire {
            identity_key: "github.com/steveyegge/beads".into(),
            kind: kind.into(),
            open_count: 4,
            last_open_at: "2026-10-01T00:00:00Z".into(),
            ..ProjectCandidateInventoryWire::default()
        });
        let result = classify_project_candidates(&input).unwrap();
        assert!(
            result.groups.is_empty(),
            "kind {kind} seeded a group: {:?}",
            result.groups
        );
        assert!(
            result.search_only.is_empty(),
            "kind {kind} seeded search_only: {:?}",
            result.search_only
        );
        assert!(
            result.hidden.is_empty(),
            "kind {kind} seeded hidden: {:?}",
            result.hidden
        );
    }
}

#[test]
fn enabled_project_is_search_only_and_disabled_gets_a_group() {
    let mut input = classify_input();
    input
        .remote
        .push(remote("github.com/sase-org/sase", "sase"));
    input.remote.push(remote("github.com/bbugyi200/job", "job"));
    input.projects.push(ProjectCandidateProjectWire {
        key: "gh_sase-org__sase".into(),
        name: "sase".into(),
        state: PROJECT_STATE_ENABLED.into(),
        identity_keys: vec!["github.com/sase-org/sase".into()],
        ..ProjectCandidateProjectWire::default()
    });
    input.projects.push(ProjectCandidateProjectWire {
        key: "job".into(),
        name: "job".into(),
        state: PROJECT_STATE_DISABLED.into(),
        identity_keys: vec!["github.com/bbugyi200/job".into()],
        ..ProjectCandidateProjectWire::default()
    });
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(hidden_reason(&result, "sase"), HIDDEN_ENABLED_PROJECT);
    assert_eq!(row_by_name(&result, "job").group, GROUP_DISABLED);
    assert_eq!(group_ids(&result), vec![GROUP_DISABLED]);
}

#[test]
fn sibling_project_is_hidden_as_enabled_project() {
    let mut input = classify_input();
    input
        .remote
        .push(remote("github.com/sase-org/sase-backing", "sase-backing"));
    input.projects.push(ProjectCandidateProjectWire {
        key: "sase".into(),
        name: "sase".into(),
        state: PROJECT_STATE_SIBLING.into(),
        identity_keys: vec!["github.com/sase-org/sase-backing".into()],
        ..ProjectCandidateProjectWire::default()
    });
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(
        hidden_reason(&result, "sase-backing"),
        HIDDEN_ENABLED_PROJECT
    );
}

#[test]
fn linked_inventory_forms_the_linked_group_and_chip() {
    let mut input = classify_input();
    input
        .remote
        .push(remote("github.com/sase-org/sase-core", "sase-core"));
    input.inventory.push(ProjectCandidateInventoryWire {
        identity_key: "github.com/sase-org/sase-core".into(),
        kind: INVENTORY_LINKED.into(),
        project_key: "sase".into(),
        open_count: 12,
        last_open_at: "2026-10-10T00:00:00Z".into(),
    });
    let result = classify_project_candidates(&input).unwrap();
    let row = row_by_name(&result, "sase-core");
    assert_eq!(row.group, GROUP_LINKED);
    assert_eq!(row.linked_by, vec!["sase".to_string()]);
}

#[test]
fn resume_journal_beats_other_rules_and_complete_is_ignored() {
    let mut input = classify_input();
    input
        .remote
        .push(remote("github.com/sase-org/forge", "forge"));
    input
        .remote
        .push(remote("github.com/sase-org/done", "done"));
    input.journals.push(ProjectCandidateJournalWire {
        identity_key: "github.com/sase-org/forge".into(),
        repo_ref: "sase-org/forge".into(),
        display: "sase-org/forge".into(),
        status: JOURNAL_RUNNING.into(),
        stopped_at: "checkout".into(),
        updated_at: "2026-10-11T00:00:00Z".into(),
    });
    input.journals.push(ProjectCandidateJournalWire {
        identity_key: "github.com/sase-org/done".into(),
        repo_ref: "sase-org/done".into(),
        display: "sase-org/done".into(),
        status: JOURNAL_COMPLETE.into(),
        updated_at: "2026-10-11T00:00:00Z".into(),
        ..ProjectCandidateJournalWire::default()
    });
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(row_by_name(&result, "forge").group, GROUP_RESUME);
    assert_eq!(row_by_name(&result, "done").group, GROUP_SUGGESTED);
}

#[test]
fn failed_journal_is_resume() {
    let mut input = classify_input();
    input.journals.push(ProjectCandidateJournalWire {
        identity_key: "github.com/sase-org/broken".into(),
        repo_ref: "sase-org/broken".into(),
        display: "sase-org/broken".into(),
        status: JOURNAL_FAILED.into(),
        stopped_at: "checkout".into(),
        updated_at: "2026-10-11T00:00:00Z".into(),
    });
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(row_by_name(&result, "broken").group, GROUP_RESUME);
    assert_eq!(
        row_by_name(&result, "broken").journal_status,
        JOURNAL_FAILED
    );
}

#[test]
fn identity_casefold_merges_remote_and_local() {
    let mut input = classify_input();
    let mut remote = remote("GitHub.com/Sase-Org/Dotfiles", "dotfiles");
    remote.description = "my dots".into();
    remote.visibility = "private".into();
    remote.pushed_at = "2026-10-01T00:00:00Z".into();
    input.remote.push(remote);
    input.local.push(local(
        "github.com/sase-org/dotfiles",
        "sase-org",
        "dotfiles",
    ));
    let result = classify_project_candidates(&input).unwrap();
    let row = row_by_name(&result, "dotfiles");
    assert_eq!(row.group, GROUP_LOCAL);
    assert_eq!(row.description, "my dots");
    assert_eq!(row.visibility, "private");
    assert_eq!(row.local_path, "/tmp/sase-org/dotfiles");
    assert_eq!(result.groups.len(), 1);
}

#[test]
fn ranking_uses_open_count_then_recency_then_display() {
    let mut input = classify_input();
    let mut a = remote("github.com/sase-org/aaa", "aaa");
    a.pushed_at = "2026-01-01T00:00:00Z".into();
    let mut b = remote("github.com/sase-org/bbb", "bbb");
    b.pushed_at = "2026-02-01T00:00:00Z".into();
    let mut c = remote("github.com/sase-org/ccc", "ccc");
    c.pushed_at = "2026-02-01T00:00:00Z".into();
    input.remote.extend([a, b, c]);
    input.inventory.push(ProjectCandidateInventoryWire {
        identity_key: "github.com/sase-org/aaa".into(),
        kind: INVENTORY_EXTERNAL.into(),
        open_count: 9,
        last_open_at: "2026-09-01T00:00:00Z".into(),
        ..ProjectCandidateInventoryWire::default()
    });
    input.inventory.push(ProjectCandidateInventoryWire {
        identity_key: "github.com/sase-org/bbb".into(),
        kind: INVENTORY_EXTERNAL.into(),
        open_count: 1,
        last_open_at: "2026-10-01T00:00:00Z".into(),
        ..ProjectCandidateInventoryWire::default()
    });
    input.inventory.push(ProjectCandidateInventoryWire {
        identity_key: "github.com/sase-org/ccc".into(),
        kind: INVENTORY_EXTERNAL.into(),
        open_count: 1,
        last_open_at: "2026-08-01T00:00:00Z".into(),
        ..ProjectCandidateInventoryWire::default()
    });
    let result = classify_project_candidates(&input).unwrap();
    let names: Vec<&str> = result.groups[0]
        .rows
        .iter()
        .map(|row| row.display.as_str())
        .collect();
    assert_eq!(names, vec!["sase-org/aaa", "sase-org/bbb", "sase-org/ccc"]);
}

#[test]
fn ranking_display_tie_is_casefolded_ascending() {
    let mut input = classify_input();
    input.remote.push(remote("github.com/sase-org/Zed", "Zed"));
    input
        .remote
        .push(remote("github.com/sase-org/alpha", "alpha"));
    let result = classify_project_candidates(&input).unwrap();
    let names: Vec<&str> = result.groups[0]
        .rows
        .iter()
        .map(|row| row.display.as_str())
        .collect();
    assert_eq!(names, vec!["sase-org/alpha", "sase-org/Zed"]);
}

#[test]
fn empty_groups_are_omitted_and_namespaces_pass_through() {
    let mut input = classify_input();
    input.namespaces.push(ProjectCandidateNamespaceWire {
        name: "sase-org".into(),
        kind_label: "org".into(),
        status: NAMESPACE_OK.into(),
        ..ProjectCandidateNamespaceWire::default()
    });
    let result = classify_project_candidates(&input).unwrap();
    assert!(result.groups.is_empty());
    assert_eq!(result.namespaces[0].name, "sase-org");
}

#[test]
fn owned_local_clone_is_local_group() {
    let mut input = classify_input();
    input.local.push(local(
        "github.com/bbugyi200/dotfiles",
        "bbugyi200",
        "dotfiles",
    ));
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(row_by_name(&result, "dotfiles").group, GROUP_LOCAL);
}

#[test]
fn template_excludes_readme_gitignore_license_and_team() {
    let state =
        evaluate(vec![("template", "sase-org/template")], FORM_MODE_LENIENT);
    assert!(state.valid);
    for name in ["add_readme", "gitignore", "license", "team"] {
        let field = field_state(&state, name);
        assert!(!field.available, "{name} should be greyed");
        assert!(
            field.reason.contains("Template"),
            "{name} reason {}",
            field.reason
        );
        assert!(!state.effective_options.contains_key(name));
    }
    assert_eq!(
        state.effective_options.get("template").map(String::as_str),
        Some("sase-org/template")
    );
}

#[test]
fn include_all_branches_requires_template() {
    let lenient =
        evaluate(vec![("include_all_branches", "true")], FORM_MODE_LENIENT);
    let field = field_state(&lenient, "include_all_branches");
    assert!(!field.available);
    assert!(field.reason.contains("Template"));
    assert!(lenient.valid);
    assert!(!lenient
        .effective_options
        .contains_key("include_all_branches"));

    let strict =
        evaluate(vec![("include_all_branches", "true")], FORM_MODE_STRICT);
    assert!(!strict.valid);
    assert!(strict.errors.iter().any(|error| {
        error.field == "include_all_branches" && error.code == ERROR_REQUIRES
    }));
}

#[test]
fn include_all_branches_is_available_with_template() {
    let state = evaluate(
        vec![
            ("template", "sase-org/template"),
            ("include_all_branches", "yes"),
        ],
        FORM_MODE_STRICT,
    );
    let field = field_state(&state, "include_all_branches");
    assert!(field.available);
    assert!(field.is_set);
    assert_eq!(field.value, "true");
    assert_eq!(
        state.effective_options.get("include_all_branches").unwrap(),
        "true"
    );
}

#[test]
fn unknown_option_is_an_error() {
    let state = evaluate(vec![("clone", "true")], FORM_MODE_STRICT);
    assert!(!state.valid);
    assert_eq!(state.errors[0].code, ERROR_UNKNOWN_OPTION);
    assert_eq!(state.errors[0].field, "clone");
}

#[test]
fn required_empty_field_errors() {
    let mut form = gh_form();
    form.fields.push(RepoCreationFieldWire {
        name: "owner_team".into(),
        label: "Owner team".into(),
        kind: FIELD_KIND_LINE.into(),
        required: true,
        ..RepoCreationFieldWire::default()
    });
    let request = CreationFormRequestWire {
        schema_version: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
        form,
        values: BTreeMap::new(),
        mode: FORM_MODE_LENIENT.into(),
    };
    let state = evaluate_repo_creation_form(&request).unwrap();
    assert!(!state.valid);
    assert!(state.errors.iter().any(|error| {
        error.field == "owner_team" && error.code == ERROR_REQUIRED
    }));
}

#[test]
fn disabled_choice_and_invalid_choice() {
    let disabled = evaluate(vec![("visibility", "internal")], FORM_MODE_STRICT);
    assert!(!disabled.valid);
    assert!(disabled.errors.iter().any(|error| {
        error.field == "visibility" && error.code == ERROR_DISABLED_CHOICE
    }));

    let invalid = evaluate(vec![("visibility", "secret")], FORM_MODE_STRICT);
    assert!(!invalid.valid);
    assert!(invalid.errors.iter().any(|error| {
        error.field == "visibility" && error.code == ERROR_INVALID_CHOICE
    }));
}

#[test]
fn search_kind_accepts_typed_values() {
    let state = evaluate(vec![("gitignore", "CustomLang")], FORM_MODE_STRICT);
    assert!(state.valid);
    assert_eq!(
        state.effective_options.get("gitignore").unwrap(),
        "CustomLang"
    );
}

#[test]
fn strict_conflict_when_excluded_field_is_supplied() {
    let state = evaluate(
        vec![("template", "sase-org/template"), ("license", "MIT")],
        FORM_MODE_STRICT,
    );
    assert!(!state.valid);
    assert!(state.errors.iter().any(|error| {
        error.field == "license" && error.code == ERROR_CONFLICT
    }));
    let lenient = evaluate(
        vec![("template", "sase-org/template"), ("license", "MIT")],
        FORM_MODE_LENIENT,
    );
    assert!(lenient.valid);
    assert!(!lenient.effective_options.contains_key("license"));
    assert!(!field_state(&lenient, "license").available);
}

#[test]
fn defaults_fill_effective_options_and_bools_normalize() {
    let state = evaluate(
        vec![("add_readme", "yes"), ("wiki", "off")],
        FORM_MODE_LENIENT,
    );
    assert!(state.valid);
    assert_eq!(
        state.effective_options.get("visibility").unwrap(),
        "private"
    );
    assert_eq!(state.effective_options.get("add_readme").unwrap(), "true");
    assert_eq!(state.effective_options.get("issues").unwrap(), "true");
    // An explicitly-off bool is a real setting, not an absence.
    assert_eq!(state.effective_options.get("wiki").unwrap(), "false");
    assert!(!field_state(&state, "wiki").is_set);
    assert_eq!(field_state(&state, "wiki").value, "false");
}

#[test]
fn explicit_false_on_default_true_bool_is_kept_in_both_modes() {
    for mode in [FORM_MODE_LENIENT, FORM_MODE_STRICT] {
        let state = evaluate(vec![("issues", "false")], mode);
        assert!(state.valid, "mode {mode}: {:?}", state.errors);
        assert_eq!(state.effective_options.get("issues").unwrap(), "false");
        assert!(!field_state(&state, "issues").is_set);
    }
}

#[test]
fn strict_ignores_off_values_of_excluded_fields_but_keeps_conflicts() {
    // add_readme=false requests nothing, so template plus an off bool is
    // harmless even in strict mode.
    let harmless = evaluate(
        vec![("template", "sase-org/template"), ("add_readme", "false")],
        FORM_MODE_STRICT,
    );
    assert!(harmless.valid, "{:?}", harmless.errors);
    assert!(!harmless.effective_options.contains_key("add_readme"));
    assert!(!field_state(&harmless, "add_readme").available);

    // gitignore=Python requests the excluded behavior, so it stays an error.
    let conflict = evaluate(
        vec![("template", "sase-org/template"), ("gitignore", "Python")],
        FORM_MODE_STRICT,
    );
    assert!(!conflict.valid);
    assert!(conflict.errors.iter().any(|error| {
        error.field == "gitignore" && error.code == ERROR_CONFLICT
    }));
}

#[test]
fn strict_template_plus_excluded_field_table() {
    struct Case {
        label: &'static str,
        values: Vec<(&'static str, &'static str)>,
        expect_valid: bool,
        conflict_field: Option<&'static str>,
    }
    let cases = [
        Case {
            label: "add_readme=true equals the default and still conflicts",
            values: vec![
                ("template", "sase-org/template"),
                ("add_readme", "true"),
            ],
            expect_valid: false,
            conflict_field: Some("add_readme"),
        },
        Case {
            label: "add_readme=false is omitted and valid",
            values: vec![
                ("template", "sase-org/template"),
                ("add_readme", "false"),
            ],
            expect_valid: true,
            conflict_field: None,
        },
        Case {
            label: "empty add_readme is not a request",
            values: vec![("template", "sase-org/template"), ("add_readme", "")],
            expect_valid: true,
            conflict_field: None,
        },
        Case {
            label: "non-empty gitignore still conflicts",
            values: vec![
                ("template", "sase-org/template"),
                ("gitignore", "Python"),
            ],
            expect_valid: false,
            conflict_field: Some("gitignore"),
        },
    ];
    for case in cases {
        let state = evaluate(case.values.clone(), FORM_MODE_STRICT);
        assert_eq!(
            state.valid, case.expect_valid,
            "{}: errors {:?}",
            case.label, state.errors
        );
        if let Some(field) = case.conflict_field {
            assert!(
                state.errors.iter().any(|error| {
                    error.field == field && error.code == ERROR_CONFLICT
                }),
                "{}: missing conflict on {field}: {:?}",
                case.label,
                state.errors
            );
            assert!(
                !state.effective_options.contains_key(field),
                "{}",
                case.label
            );
        } else {
            assert!(
                !state.effective_options.contains_key("add_readme"),
                "{}",
                case.label
            );
            assert!(
                !field_state(&state, "add_readme").available,
                "{}",
                case.label
            );
        }
        let lenient = evaluate(case.values, FORM_MODE_LENIENT);
        assert!(lenient.valid, "lenient stayed valid for {}", case.label);
    }
}

#[test]
fn form_schema_mismatch_and_bad_mode_are_rejected() {
    let mut request = CreationFormRequestWire {
        schema_version: 2,
        form: gh_form(),
        values: BTreeMap::new(),
        mode: FORM_MODE_LENIENT.into(),
    };
    assert!(matches!(
        evaluate_repo_creation_form(&request).unwrap_err(),
        ProjectOnboardingError::UnsupportedSchema { actual: 2, .. }
    ));
    request.schema_version = PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION;
    request.mode = "maybe".into();
    let error = evaluate_repo_creation_form(&request).unwrap_err();
    assert!(error.to_string().contains("unsupported creation form mode"));
}

#[test]
fn namespaces_pass_through_on_classified_output() {
    let mut input = classify_input();
    input.namespaces.push(ProjectCandidateNamespaceWire {
        name: "bbugyi200".into(),
        kind_label: "you".into(),
        status: NAMESPACE_LOADING.into(),
        truncated: true,
        ..ProjectCandidateNamespaceWire::default()
    });
    input
        .remote
        .push(remote("github.com/sase-org/sase", "sase"));
    let result = classify_project_candidates(&input).unwrap();
    assert_eq!(result.namespaces[0].kind_label, "you");
    assert!(result.namespaces[0].truncated);
}
