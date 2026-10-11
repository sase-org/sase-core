use super::*;

fn target() -> ProjectOnboardingTargetWire {
    ProjectOnboardingTargetWire {
        workflow_type: "gh".into(),
        provider_display: "GitHub".into(),
        identity_key: "github.com/sase-org/bob".into(),
        repo_ref: "sase-org/bob".into(),
        display: "sase-org/bob".into(),
        host: "github.com".into(),
        owner: "sase-org".into(),
        name: "bob".into(),
        web_url: "https://github.com/sase-org/bob".into(),
        checkout_dir: "/tmp/bob".into(),
        project_key_base: "gh_sase-org__bob".into(),
        project_name_base: "bob".into(),
        storage_policy: STORAGE_SEPARATE_REPO.into(),
    }
}

fn add_input() -> ProjectOnboardingPlanInputWire {
    ProjectOnboardingPlanInputWire {
        schema_version: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
        request: ProjectOnboardingRequestWire {
            mode: MODE_ADD.into(),
            actor: ACTOR_HUMAN.into(),
            target: target(),
            requested: ProjectOnboardingRequestedWire {
                setup: SETUP_AUTO.into(),
                ..ProjectOnboardingRequestedWire::default()
            },
            creation: None,
        },
        facts: ProjectOnboardingFactsWire {
            remote: ProjectOnboardingRemoteFactsWire {
                status: REMOTE_FOUND.into(),
                visibility: "private".into(),
                can_push: Some(true),
                sase_managed: Some(false),
                repo_id: "R_bob".into(),
                default_branch: "main".into(),
                ..ProjectOnboardingRemoteFactsWire::default()
            },
            checkout: ProjectOnboardingCheckoutFactsWire {
                state: CHECKOUT_ABSENT.into(),
                ..ProjectOnboardingCheckoutFactsWire::default()
            },
            ..ProjectOnboardingFactsWire::default()
        },
    }
}

fn create_input() -> ProjectOnboardingPlanInputWire {
    let mut input = add_input();
    input.request.mode = MODE_CREATE.into();
    input.request.creation = Some(ProjectOnboardingCreationWire {
        visibility: "private".into(),
        summary: vec!["README".into(), "MIT".into()],
    });
    input.facts.remote.status = REMOTE_NOT_FOUND.into();
    input.facts.remote.repo_id.clear();
    input.facts.can_create = Some(true);
    input
}

fn plan_of(
    input: &ProjectOnboardingPlanInputWire,
) -> ProjectOnboardingPlanWire {
    plan_project_onboarding(input).unwrap()
}

fn step<'a>(
    plan: &'a ProjectOnboardingPlanWire,
    id: &str,
) -> &'a ProjectOnboardingStepWire {
    plan.steps
        .iter()
        .find(|step| step.id == id)
        .unwrap_or_else(|| panic!("missing step {id}"))
}

fn step_ids(plan: &ProjectOnboardingPlanWire) -> Vec<&str> {
    plan.steps.iter().map(|step| step.id.as_str()).collect()
}

fn blocker_codes(plan: &ProjectOnboardingPlanWire) -> Vec<&str> {
    plan.blockers
        .iter()
        .map(|item| item.code.as_str())
        .collect()
}

fn warning_codes(plan: &ProjectOnboardingPlanWire) -> Vec<&str> {
    plan.warnings
        .iter()
        .map(|item| item.code.as_str())
        .collect()
}

fn occupying(key: &str, name: &str) -> ProjectOnboardingProjectFactsWire {
    ProjectOnboardingProjectFactsWire {
        key: key.into(),
        name: name.into(),
        ..ProjectOnboardingProjectFactsWire::default()
    }
}

fn matching_record() -> ProjectOnboardingRecordFactsWire {
    ProjectOnboardingRecordFactsWire {
        key: "gh_sase-org__bob".into(),
        name: "bob".into(),
        state: PROJECT_STATE_ENABLED.into(),
        ..ProjectOnboardingRecordFactsWire::default()
    }
}

#[test]
fn plan_schema_mismatch_is_rejected() {
    let mut input = add_input();
    input.schema_version = 9;
    let error = plan_project_onboarding(&input).unwrap_err();
    assert!(matches!(
        error,
        ProjectOnboardingError::UnsupportedSchema {
            actual: 9,
            expected: 1
        }
    ));
}

#[test]
fn add_fresh_clone_is_ready_with_checkout_manage_and_init() {
    let plan = plan_of(&add_input());
    assert_eq!(plan.status, PLAN_STATUS_READY);
    assert_eq!(
        step_ids(&plan),
        vec![
            STEP_CHECKOUT,
            STEP_REENABLE,
            STEP_IDENTITY,
            STEP_MANAGE,
            STEP_CURRENT,
            STEP_INIT
        ]
    );
    assert_eq!(step(&plan, STEP_CHECKOUT).state, STEP_PENDING);
    assert_eq!(step(&plan, STEP_CHECKOUT).operation, OPERATION_CREATE);
    assert!(step(&plan, STEP_CHECKOUT).label.contains("/tmp/bob"));
    assert_eq!(step(&plan, STEP_REENABLE).state, STEP_SKIPPED);
    assert_eq!(step(&plan, STEP_IDENTITY).state, STEP_SKIPPED);
    assert_eq!(step(&plan, STEP_MANAGE).state, STEP_PENDING);
    assert_eq!(step(&plan, STEP_MANAGE).operation, OPERATION_CREATE);
    assert!(step(&plan, STEP_MANAGE).remote);
    assert_eq!(step(&plan, STEP_CURRENT).state, STEP_SKIPPED);
    assert_eq!(step(&plan, STEP_INIT).state, STEP_PENDING);
    assert!(step(&plan, STEP_INIT).handoff);
    assert!(step(&plan, STEP_INIT).requires_tty);
    assert_eq!(step(&plan, STEP_INIT).operation, OPERATION_VALIDATE);
    assert_eq!(plan.setup.code, SETUP_CODE_UNMANAGED_CAN_PUSH);
    assert!(plan.setup.enabled);
    assert_eq!(plan.identity.key, "gh_sase-org__bob");
    assert_eq!(plan.identity.name, "bob");
    assert_eq!(plan.identity.key_source, KEY_SOURCE_ALLOCATED);
    assert_eq!(plan.identity.name_source, NAME_SOURCE_ALLOCATED);
}

#[test]
fn empty_dir_is_pending_checkout() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_EMPTY_DIR.into();
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CHECKOUT).state, STEP_PENDING);
    assert_eq!(step(&plan, STEP_CHECKOUT).operation, OPERATION_CREATE);
}

#[test]
fn matching_clone_without_record_registers() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CHECKOUT).state, STEP_PENDING);
    assert_eq!(step(&plan, STEP_CHECKOUT).operation, OPERATION_UPDATE);
    assert!(step(&plan, STEP_CHECKOUT).label.contains("Register"));
}

#[test]
fn matching_clone_with_record_is_done() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CHECKOUT).state, STEP_DONE);
    assert_eq!(plan.identity.key_source, KEY_SOURCE_EXISTING);
    assert_eq!(plan.identity.name_source, NAME_SOURCE_EXISTING);
}

#[test]
fn mismatch_and_not_git_are_checkout_conflicts() {
    for state in [CHECKOUT_MISMATCH, CHECKOUT_NOT_GIT] {
        let mut input = add_input();
        input.facts.checkout.state = state.into();
        let plan = plan_of(&input);
        assert!(blocker_codes(&plan).contains(&BLOCKER_CHECKOUT_CONFLICT));
        assert!(plan
            .blockers
            .iter()
            .any(|item| item.message.contains("/tmp/bob")));
        assert_eq!(plan.status, PLAN_STATUS_BLOCKED);
    }
}

#[test]
fn add_remote_not_found_is_blocked() {
    let mut input = add_input();
    input.facts.remote.status = REMOTE_NOT_FOUND.into();
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_NOT_FOUND));
    assert_eq!(plan.status, PLAN_STATUS_BLOCKED);
}

#[test]
fn add_remote_unavailable_without_matching_checkout_is_unknown() {
    let mut input = add_input();
    input.facts.remote.status = REMOTE_UNAVAILABLE.into();
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_REMOTE_UNKNOWN));
}

#[test]
fn add_remote_unavailable_with_matching_checkout_is_a_warning() {
    let mut input = add_input();
    input.facts.remote.status = REMOTE_UNAVAILABLE.into();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    let plan = plan_of(&input);
    assert!(!blocker_codes(&plan).contains(&BLOCKER_REMOTE_UNKNOWN));
    assert!(warning_codes(&plan).contains(&WARNING_REMOTE_UNVERIFIED));
}

#[test]
fn create_remote_is_pending_when_not_found() {
    let plan = plan_of(&create_input());
    assert_eq!(step_ids(&plan)[0], STEP_CREATE_REMOTE);
    assert_eq!(step(&plan, STEP_CREATE_REMOTE).state, STEP_PENDING);
    assert!(step(&plan, STEP_CREATE_REMOTE).remote);
    assert_eq!(step(&plan, STEP_CREATE_REMOTE).operation, OPERATION_CREATE);
    assert_eq!(plan.setup.code, SETUP_CODE_NEW_REPO);
    assert!(plan.setup.enabled);
    assert_eq!(plan.status, PLAN_STATUS_READY);
}

#[test]
fn found_remote_without_journal_is_remote_exists() {
    let mut input = create_input();
    input.facts.remote.status = REMOTE_FOUND.into();
    input.facts.remote.repo_id = "R_other".into();
    input.facts.remote.is_empty = Some(false);
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_REMOTE_EXISTS));
    assert!(plan
        .blockers
        .iter()
        .any(|item| item.hint.contains("sase project add sase-org/bob")));
    assert_eq!(step(&plan, STEP_CREATE_REMOTE).state, STEP_PENDING);
    assert_eq!(plan.status, PLAN_STATUS_BLOCKED);
}

#[test]
fn journal_repo_id_match_adopts_the_remote() {
    let mut input = create_input();
    input.facts.remote.status = REMOTE_FOUND.into();
    input.facts.remote.repo_id = "R_bob".into();
    input.facts.journal = Some(ProjectOnboardingJournalFactsWire {
        intent_recorded_at: "2026-10-11T00:00:00Z".into(),
        remote_created: true,
        repo_id: "R_bob".into(),
    });
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CREATE_REMOTE).state, STEP_DONE);
    assert!(!blocker_codes(&plan).contains(&BLOCKER_REMOTE_EXISTS));
}

#[test]
fn journal_without_id_adopts_empty_remote_of_requested_visibility() {
    let mut input = create_input();
    input.facts.remote.status = REMOTE_FOUND.into();
    input.facts.remote.visibility = "private".into();
    input.facts.remote.is_empty = Some(true);
    input.facts.remote.repo_id = "R_new".into();
    input.facts.journal = Some(ProjectOnboardingJournalFactsWire {
        intent_recorded_at: "2026-10-11T00:00:00Z".into(),
        remote_created: true,
        repo_id: String::new(),
    });
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CREATE_REMOTE).state, STEP_DONE);
}

#[test]
fn journal_without_id_does_not_adopt_a_non_empty_remote() {
    let mut input = create_input();
    input.facts.remote.status = REMOTE_FOUND.into();
    input.facts.remote.visibility = "private".into();
    input.facts.remote.is_empty = Some(false);
    input.facts.journal = Some(ProjectOnboardingJournalFactsWire {
        intent_recorded_at: "2026-10-11T00:00:00Z".into(),
        remote_created: false,
        repo_id: String::new(),
    });
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_REMOTE_EXISTS));
}

#[test]
fn create_remote_unavailable_is_unknown() {
    let mut input = create_input();
    input.facts.remote.status = REMOTE_UNAVAILABLE.into();
    input.facts.remote.message = "gh auth expired".into();
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_REMOTE_UNKNOWN));
    assert!(plan
        .blockers
        .iter()
        .any(|item| item.message.contains("gh auth expired")));
}

#[test]
fn create_agent_is_human_only() {
    let mut input = create_input();
    input.request.actor = ACTOR_AGENT.into();
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_HUMAN_ONLY));
    assert_eq!(plan.setup.code, SETUP_CODE_AGENT);
    assert!(!plan.setup.enabled);
    assert_eq!(plan.status, PLAN_STATUS_BLOCKED);
}

#[test]
fn no_create_rights_and_invalid_options_block_create() {
    let mut input = create_input();
    input.facts.can_create = Some(false);
    input.facts.creation_errors.push(CreationFormErrorWire {
        field: "license".into(),
        code: ERROR_CONFLICT.into(),
        message: "not available with Template".into(),
    });
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_NO_CREATE_RIGHTS));
    assert!(blocker_codes(&plan).contains(&BLOCKER_INVALID_OPTION));
}

#[test]
fn reenable_is_pending_for_a_disabled_record() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(ProjectOnboardingRecordFactsWire {
        key: "gh_sase-org__bob".into(),
        name: "bob".into(),
        state: PROJECT_STATE_DISABLED.into(),
        ..ProjectOnboardingRecordFactsWire::default()
    });
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_REENABLE).state, STEP_PENDING);
    assert_eq!(step(&plan, STEP_REENABLE).operation, OPERATION_UPDATE);
}

#[test]
fn identity_is_pending_when_requested_name_differs() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    input.request.requested.name = "alice".into();
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_IDENTITY).state, STEP_PENDING);
    assert_eq!(plan.identity.name, "alice");
    assert_eq!(plan.identity.name_source, NAME_SOURCE_REQUESTED);
}

#[test]
fn identity_is_skipped_when_requested_matches_record() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    input.request.requested.name = "bob".into();
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_IDENTITY).state, STEP_SKIPPED);
}

#[test]
fn invalid_requested_name_is_blocked() {
    let mut input = add_input();
    input.request.requested.name = "../escape".into();
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_INVALID_NAME));
    assert_eq!(plan.status, PLAN_STATUS_BLOCKED);
}

#[test]
fn taken_requested_name_suggests_underscore_suffix() {
    let mut input = add_input();
    input.request.requested.name = "sase".into();
    input
        .facts
        .projects
        .push(occupying("gh_sase-org__sase", "sase"));
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_NAME_TAKEN));
    let taken = plan
        .blockers
        .iter()
        .find(|item| item.code == BLOCKER_NAME_TAKEN)
        .unwrap();
    assert!(taken.message.contains("sase"));
    assert_eq!(taken.hint, "try sase_1");
    assert_eq!(plan.identity.name, "sase_1");
    assert_eq!(plan.identity.name_source, NAME_SOURCE_ALLOCATED);
}

#[test]
fn taken_alias_is_blocked_with_suggestion() {
    let mut input = add_input();
    input.request.requested.aliases = vec!["sase".into()];
    input
        .facts
        .projects
        .push(occupying("gh_sase-org__sase", "sase"));
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_ALIAS_TAKEN));
    assert!(plan
        .blockers
        .iter()
        .any(|item| item.code == BLOCKER_ALIAS_TAKEN
            && item.hint == "try sase_1"));
    assert_eq!(step(&plan, STEP_IDENTITY).state, STEP_PENDING);
}

#[test]
fn gh_key_collision_uses_dash_two_suffix() {
    let mut input = add_input();
    input
        .facts
        .projects
        .push(occupying("gh_sase-org__bob", "other"));
    let plan = plan_of(&input);
    assert_eq!(plan.identity.key, "gh_sase-org__bob-2");
    assert_eq!(plan.identity.key_source, KEY_SOURCE_ALLOCATED);
    assert!(!blocker_codes(&plan).contains(&BLOCKER_NAME_TAKEN));
}

#[test]
fn bare_git_key_collision_is_name_taken_without_suffix() {
    let mut input = add_input();
    input.request.target.workflow_type = WORKFLOW_GIT.into();
    input.request.target.project_key_base = "forge".into();
    input.request.target.project_name_base = "forge".into();
    input.request.target.identity_key = "git:/tmp/forge.git".into();
    input.request.target.repo_ref = "forge".into();
    input.request.target.display = "forge".into();
    input.request.target.storage_policy = "in_tree".into();
    input.facts.projects.push(occupying("forge", "forge"));
    let plan = plan_of(&input);
    assert_eq!(plan.identity.key, "forge");
    assert!(blocker_codes(&plan).contains(&BLOCKER_NAME_TAKEN));
    assert!(plan
        .blockers
        .iter()
        .any(|item| item.hint.contains("bare git")));
}

#[test]
fn auto_setup_from_an_agent_is_off() {
    let mut input = add_input();
    input.request.actor = ACTOR_AGENT.into();
    let plan = plan_of(&input);
    assert_eq!(plan.setup.code, SETUP_CODE_AGENT);
    assert!(!plan.setup.enabled);
    assert_eq!(step(&plan, STEP_MANAGE).state, STEP_SKIPPED);
    assert_eq!(step(&plan, STEP_INIT).state, STEP_SKIPPED);
    assert!(!blocker_codes(&plan).contains(&BLOCKER_HUMAN_ONLY));
}

#[test]
fn auto_setup_on_a_managed_repo_skips_init() {
    let mut input = add_input();
    input.facts.remote.sase_managed = Some(true);
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    let plan = plan_of(&input);
    assert_eq!(plan.setup.code, SETUP_CODE_ALREADY_MANAGED);
    assert!(!plan.setup.enabled);
    assert_eq!(step(&plan, STEP_MANAGE).state, STEP_SKIPPED);
    assert_eq!(step(&plan, STEP_INIT).state, STEP_SKIPPED);
    assert_eq!(plan.status, PLAN_STATUS_CURRENT);
}

#[test]
fn auto_setup_without_push_rights_warns_not_launchable() {
    let mut input = add_input();
    input.facts.remote.can_push = Some(false);
    let plan = plan_of(&input);
    assert_eq!(plan.setup.code, SETUP_CODE_NO_PUSH);
    assert!(!plan.setup.enabled);
    assert!(warning_codes(&plan).contains(&WARNING_NOT_LAUNCHABLE));
    assert!(plan
        .warnings
        .iter()
        .any(|item| item.message.contains("sase init -M")));
}

#[test]
fn auto_setup_with_unknown_push_warns() {
    let mut input = add_input();
    input.facts.remote.can_push = None;
    let plan = plan_of(&input);
    assert_eq!(plan.setup.code, SETUP_CODE_UNKNOWN_PUSH);
    assert!(!plan.setup.enabled);
    assert!(warning_codes(&plan).contains(&WARNING_UNKNOWN_PUSH));
}

#[test]
fn explicit_setup_from_an_agent_is_human_only() {
    let mut input = add_input();
    input.request.actor = ACTOR_AGENT.into();
    input.request.requested.setup = SETUP_ON.into();
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_HUMAN_ONLY));
    assert_eq!(plan.setup.code, SETUP_CODE_ON);
    assert!(plan.setup.enabled);
}

#[test]
fn explicit_setup_without_push_rights_is_blocked() {
    let mut input = add_input();
    input.request.requested.setup = SETUP_ON.into();
    input.facts.remote.can_push = Some(false);
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_NO_PUSH));
}

#[test]
fn explicit_setup_on_managed_repo_keeps_init_pending() {
    let mut input = add_input();
    input.request.requested.setup = SETUP_ON.into();
    input.facts.remote.sase_managed = Some(true);
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    let plan = plan_of(&input);
    assert!(plan.setup.enabled);
    assert_eq!(step(&plan, STEP_MANAGE).state, STEP_DONE);
    assert_eq!(step(&plan, STEP_INIT).state, STEP_PENDING);
    assert_eq!(plan.status, PLAN_STATUS_CURRENT);
}

#[test]
fn explicit_setup_off_on_unmanaged_separate_repo_warns() {
    let mut input = add_input();
    input.request.requested.setup = SETUP_OFF.into();
    let plan = plan_of(&input);
    assert_eq!(plan.setup.code, SETUP_CODE_OFF);
    assert!(!plan.setup.enabled);
    assert!(warning_codes(&plan).contains(&WARNING_NOT_LAUNCHABLE));
    assert_eq!(step(&plan, STEP_MANAGE).state, STEP_SKIPPED);
    assert_eq!(step(&plan, STEP_INIT).state, STEP_SKIPPED);
}

#[test]
fn local_managed_counts_as_managed() {
    let mut input = add_input();
    input.facts.remote.sase_managed = None;
    input.facts.local_managed = Some(true);
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    let plan = plan_of(&input);
    assert_eq!(plan.setup.code, SETUP_CODE_ALREADY_MANAGED);
}

#[test]
fn config_dirty_and_wrong_branch_block_manage() {
    let mut input = add_input();
    input.facts.checkout.config_dirty = true;
    input.facts.checkout.on_default_branch = Some(false);
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_CONFIG_DIRTY));
    assert!(blocker_codes(&plan).contains(&BLOCKER_NOT_ON_DEFAULT_BRANCH));
}

#[test]
fn make_current_is_pending() {
    let mut input = add_input();
    input.request.requested.make_current = true;
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CURRENT).state, STEP_PENDING);
}

#[test]
fn in_tree_init_does_not_require_tty() {
    let mut input = add_input();
    input.request.target.storage_policy = "in_tree".into();
    let plan = plan_of(&input);
    assert!(!step(&plan, STEP_INIT).requires_tty);
    assert!(step(&plan, STEP_INIT).handoff);
}

#[test]
fn status_is_current_when_only_handoff_is_pending() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    input.facts.remote.sase_managed = Some(true);
    input.request.requested.setup = SETUP_ON.into();
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CHECKOUT).state, STEP_DONE);
    assert_eq!(step(&plan, STEP_MANAGE).state, STEP_DONE);
    assert_eq!(step(&plan, STEP_INIT).state, STEP_PENDING);
    assert_eq!(plan.status, PLAN_STATUS_CURRENT);
}

#[test]
fn resume_after_checkout_continues_at_manage() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    let plan = plan_of(&input);
    assert_eq!(step(&plan, STEP_CHECKOUT).state, STEP_DONE);
    assert_eq!(step(&plan, STEP_MANAGE).state, STEP_PENDING);
    assert_eq!(step(&plan, STEP_INIT).state, STEP_PENDING);
    assert_eq!(plan.status, PLAN_STATUS_READY);
}

#[test]
fn fingerprint_is_stable_across_step_states() {
    let fresh = plan_of(&add_input());
    let mut later = add_input();
    later.facts.checkout.state = CHECKOUT_MATCHING.into();
    later.facts.record = Some(matching_record());
    later.facts.remote.sase_managed = Some(true);
    let later = plan_of(&later);
    assert_eq!(fresh.intent_fingerprint, later.intent_fingerprint);
    assert_eq!(fresh.intent_fingerprint.len(), 64);
    assert!(fresh
        .intent_fingerprint
        .chars()
        .all(|ch| ch.is_ascii_hexdigit()));
}

#[test]
fn fingerprint_changes_with_requested_name_and_ignores_facts() {
    let mut renamed = add_input();
    renamed.request.requested.name = "alice".into();
    let original = plan_of(&add_input());
    let renamed = plan_of(&renamed);
    assert_ne!(original.intent_fingerprint, renamed.intent_fingerprint);
}

#[test]
fn own_record_is_excluded_from_collision_checks() {
    let mut input = add_input();
    input.facts.checkout.state = CHECKOUT_MATCHING.into();
    input.facts.record = Some(matching_record());
    input
        .facts
        .projects
        .push(occupying("gh_sase-org__bob", "bob"));
    input.request.requested.name = "bob".into();
    let plan = plan_of(&input);
    assert!(!blocker_codes(&plan).contains(&BLOCKER_NAME_TAKEN));
    assert_eq!(plan.identity.key, "gh_sase-org__bob");
    assert_eq!(plan.identity.name, "bob");
}

#[test]
fn home_is_reserved_and_allocates_a_suffix() {
    let mut input = add_input();
    input.request.requested.name = "home".into();
    let plan = plan_of(&input);
    assert!(blocker_codes(&plan).contains(&BLOCKER_NAME_TAKEN));
    assert_eq!(plan.identity.name, "home_1");
}

#[test]
fn unknown_mode_is_rejected() {
    let mut input = add_input();
    input.request.mode = "import".into();
    let error = plan_project_onboarding(&input).unwrap_err();
    assert!(error.to_string().contains("unsupported onboarding mode"));
}
