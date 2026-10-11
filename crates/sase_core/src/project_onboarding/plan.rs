//! Onboarding plan derivation: setup, identity, steps, and intent fingerprint.

use std::collections::BTreeSet;

use serde::Serialize;
use unicode_casefold::UnicodeCaseFold;

use super::wire::{
    ProjectOnboardingBlockerWire, ProjectOnboardingFactsWire,
    ProjectOnboardingIdentityWire, ProjectOnboardingJournalFactsWire,
    ProjectOnboardingPlanInputWire, ProjectOnboardingPlanWire,
    ProjectOnboardingProjectFactsWire, ProjectOnboardingRecordFactsWire,
    ProjectOnboardingRemoteFactsWire, ProjectOnboardingRequestWire,
    ProjectOnboardingRequestedWire, ProjectOnboardingSetupWire,
    ProjectOnboardingStepWire, ProjectOnboardingTargetWire,
    ProjectOnboardingWarningWire, ACTOR_AGENT, ACTOR_HUMAN,
    BLOCKER_ALIAS_TAKEN, BLOCKER_CHECKOUT_CONFLICT, BLOCKER_CONFIG_DIRTY,
    BLOCKER_HUMAN_ONLY, BLOCKER_INVALID_NAME, BLOCKER_INVALID_OPTION,
    BLOCKER_NAME_TAKEN, BLOCKER_NOT_FOUND, BLOCKER_NOT_ON_DEFAULT_BRANCH,
    BLOCKER_NO_CREATE_RIGHTS, BLOCKER_NO_PUSH, BLOCKER_REMOTE_EXISTS,
    BLOCKER_REMOTE_UNKNOWN, CHECKOUT_ABSENT, CHECKOUT_EMPTY_DIR,
    CHECKOUT_MATCHING, CHECKOUT_MISMATCH, CHECKOUT_NOT_GIT,
    KEY_SOURCE_ALLOCATED, KEY_SOURCE_EXISTING, MODE_ADD, MODE_CREATE,
    NAME_SOURCE_ALLOCATED, NAME_SOURCE_EXISTING, NAME_SOURCE_REQUESTED,
    OPERATION_CREATE, OPERATION_UPDATE, OPERATION_VALIDATE,
    PLAN_STATUS_BLOCKED, PLAN_STATUS_CURRENT, PLAN_STATUS_READY,
    PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION, PROJECT_STATE_DISABLED,
    REMOTE_FOUND, REMOTE_NOT_FOUND, REMOTE_UNAVAILABLE, SETUP_AUTO,
    SETUP_CODE_AGENT, SETUP_CODE_ALREADY_MANAGED, SETUP_CODE_NEW_REPO,
    SETUP_CODE_NO_PUSH, SETUP_CODE_OFF, SETUP_CODE_ON, SETUP_CODE_UNKNOWN_PUSH,
    SETUP_CODE_UNMANAGED_CAN_PUSH, SETUP_OFF, SETUP_ON, STEP_CHECKOUT,
    STEP_CREATE_REMOTE, STEP_CURRENT, STEP_DONE, STEP_IDENTITY, STEP_INIT,
    STEP_MANAGE, STEP_PENDING, STEP_REENABLE, STEP_SKIPPED,
    STORAGE_SEPARATE_REPO, WARNING_NOT_LAUNCHABLE, WARNING_REMOTE_UNVERIFIED,
    WARNING_UNKNOWN_PUSH, WORKFLOW_GIT,
};
use super::ProjectOnboardingError;
use crate::project_spec::is_valid_sase_project_name;

/// Derive the ordered onboarding plan from intent and observed facts.
pub fn plan_project_onboarding(
    input: &ProjectOnboardingPlanInputWire,
) -> Result<ProjectOnboardingPlanWire, ProjectOnboardingError> {
    ProjectOnboardingError::check_schema(input.schema_version)?;
    let request = &input.request;
    let facts = &input.facts;
    let mode = parse_mode(&request.mode)?;
    let actor = parse_actor(&request.actor)?;
    let setup_choice = parse_setup(&request.requested.setup)?;
    let remote_status = parse_remote_status(&facts.remote.status)?;
    let checkout_state = parse_checkout_state(&facts.checkout.state)?;

    let mut blockers = Vec::new();
    let mut warnings = Vec::new();
    let identity = preview_identity(request, facts, &mut blockers);
    let setup = resolve_setup(
        mode,
        actor,
        setup_choice,
        facts,
        &request.target,
        &mut blockers,
        &mut warnings,
    );
    let steps = build_steps(
        &StepBuild {
            mode,
            actor,
            checkout_state,
            remote_status,
            request,
            facts,
            identity: &identity,
            setup: &setup,
        },
        &mut blockers,
        &mut warnings,
    );
    let status = if !blockers.is_empty() {
        PLAN_STATUS_BLOCKED
    } else if steps
        .iter()
        .all(|step| step.state != STEP_PENDING || step.handoff)
    {
        PLAN_STATUS_CURRENT
    } else {
        PLAN_STATUS_READY
    };
    Ok(ProjectOnboardingPlanWire {
        schema_version: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
        mode: mode.to_string(),
        status: status.to_string(),
        intent_fingerprint: intent_fingerprint(request)?,
        identity,
        setup,
        steps,
        warnings,
        blockers,
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Add,
    Create,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Add => MODE_ADD,
            Self::Create => MODE_CREATE,
        }
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Actor {
    Human,
    Agent,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SetupChoice {
    Auto,
    On,
    Off,
}

fn parse_mode(value: &str) -> Result<Mode, ProjectOnboardingError> {
    match value.trim() {
        MODE_ADD => Ok(Mode::Add),
        MODE_CREATE => Ok(Mode::Create),
        other => Err(ProjectOnboardingError::Validation(format!(
            "unsupported onboarding mode: {other}"
        ))),
    }
}

fn parse_actor(value: &str) -> Result<Actor, ProjectOnboardingError> {
    match value.trim() {
        ACTOR_HUMAN => Ok(Actor::Human),
        ACTOR_AGENT => Ok(Actor::Agent),
        other => Err(ProjectOnboardingError::Validation(format!(
            "unsupported onboarding actor: {other}"
        ))),
    }
}

fn parse_setup(value: &str) -> Result<SetupChoice, ProjectOnboardingError> {
    match value.trim() {
        "" | SETUP_AUTO => Ok(SetupChoice::Auto),
        SETUP_ON => Ok(SetupChoice::On),
        SETUP_OFF => Ok(SetupChoice::Off),
        other => Err(ProjectOnboardingError::Validation(format!(
            "unsupported setup request: {other}"
        ))),
    }
}

fn parse_remote_status(value: &str) -> Result<&str, ProjectOnboardingError> {
    match value.trim() {
        "" | REMOTE_UNAVAILABLE => Ok(REMOTE_UNAVAILABLE),
        REMOTE_FOUND => Ok(REMOTE_FOUND),
        REMOTE_NOT_FOUND => Ok(REMOTE_NOT_FOUND),
        other => Err(ProjectOnboardingError::Validation(format!(
            "unsupported remote status: {other}"
        ))),
    }
}

fn parse_checkout_state(value: &str) -> Result<&str, ProjectOnboardingError> {
    match value.trim() {
        "" | CHECKOUT_ABSENT => Ok(CHECKOUT_ABSENT),
        CHECKOUT_MATCHING => Ok(CHECKOUT_MATCHING),
        CHECKOUT_MISMATCH => Ok(CHECKOUT_MISMATCH),
        CHECKOUT_NOT_GIT => Ok(CHECKOUT_NOT_GIT),
        CHECKOUT_EMPTY_DIR => Ok(CHECKOUT_EMPTY_DIR),
        other => Err(ProjectOnboardingError::Validation(format!(
            "unsupported checkout state: {other}"
        ))),
    }
}

fn preview_identity(
    request: &ProjectOnboardingRequestWire,
    facts: &ProjectOnboardingFactsWire,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
) -> ProjectOnboardingIdentityWire {
    let exclude = facts.record.as_ref().map(|record| record.key.as_str());
    let occupied = occupied_refs(&facts.projects, exclude);
    let (key, key_source) = preview_key(
        &request.target,
        facts.record.as_ref(),
        &facts.projects,
        &occupied,
        blockers,
    );
    let (name, name_source) = preview_name(
        &request.target,
        &request.requested,
        facts.record.as_ref(),
        &facts.projects,
        &occupied,
        blockers,
    );
    let aliases = preview_aliases(
        &request.requested,
        facts.record.as_ref(),
        &facts.projects,
        &occupied,
        blockers,
    );
    ProjectOnboardingIdentityWire {
        key,
        name,
        aliases,
        key_source: key_source.to_string(),
        name_source: name_source.to_string(),
    }
}

fn preview_key(
    target: &ProjectOnboardingTargetWire,
    record: Option<&ProjectOnboardingRecordFactsWire>,
    projects: &[ProjectOnboardingProjectFactsWire],
    occupied: &BTreeSet<String>,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
) -> (String, &'static str) {
    if let Some(record) = record {
        return (record.key.clone(), KEY_SOURCE_EXISTING);
    }
    let base = target.project_key_base.trim();
    if base.is_empty() {
        blockers.push(blocker(
            BLOCKER_INVALID_NAME,
            "project key base is empty",
            "",
        ));
        return (String::new(), KEY_SOURCE_ALLOCATED);
    }
    if !is_taken(base, occupied) {
        return (base.to_string(), KEY_SOURCE_ALLOCATED);
    }
    if target.workflow_type.trim() == WORKFLOW_GIT {
        let owner = taken_by(base, projects);
        blockers.push(blocker(
            BLOCKER_NAME_TAKEN,
            format!(
                "key {base} is taken{}",
                owner.map(|name| format!(" by {name}")).unwrap_or_default()
            ),
            "bare git projects cannot reuse a claimed name",
        ));
        return (base.to_string(), KEY_SOURCE_ALLOCATED);
    }
    let mut suffix = 2_u32;
    loop {
        let candidate = format!("{base}-{suffix}");
        if !is_taken(&candidate, occupied) {
            return (candidate, KEY_SOURCE_ALLOCATED);
        }
        suffix = suffix.saturating_add(1);
        if suffix > 10_000 {
            blockers.push(blocker(
                BLOCKER_NAME_TAKEN,
                format!("key {base} is taken"),
                "",
            ));
            return (base.to_string(), KEY_SOURCE_ALLOCATED);
        }
    }
}

fn preview_name(
    target: &ProjectOnboardingTargetWire,
    requested: &ProjectOnboardingRequestedWire,
    record: Option<&ProjectOnboardingRecordFactsWire>,
    projects: &[ProjectOnboardingProjectFactsWire],
    occupied: &BTreeSet<String>,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
) -> (String, &'static str) {
    let requested_name = requested.name.trim();
    if !requested_name.is_empty() && !is_valid_sase_project_name(requested_name)
    {
        blockers.push(blocker(
            BLOCKER_INVALID_NAME,
            format!("invalid project name {requested_name}"),
            "use a single path component without separators",
        ));
        return (requested_name.to_string(), NAME_SOURCE_REQUESTED);
    }
    let (base, source_if_free) = if !requested_name.is_empty() {
        (requested_name, NAME_SOURCE_REQUESTED)
    } else if let Some(record) = record {
        if record.name.trim().is_empty() {
            (target.project_name_base.trim(), NAME_SOURCE_ALLOCATED)
        } else {
            return (record.name.clone(), NAME_SOURCE_EXISTING);
        }
    } else {
        (target.project_name_base.trim(), NAME_SOURCE_ALLOCATED)
    };
    if base.is_empty() {
        blockers.push(blocker(
            BLOCKER_INVALID_NAME,
            "project name is empty",
            "",
        ));
        return (String::new(), NAME_SOURCE_ALLOCATED);
    }
    if !is_valid_sase_project_name(base) {
        blockers.push(blocker(
            BLOCKER_INVALID_NAME,
            format!("invalid project name {base}"),
            "use a single path component without separators",
        ));
        return (base.to_string(), source_if_free);
    }
    if !is_taken(base, occupied) {
        return (base.to_string(), source_if_free);
    }
    let allocated = allocate_name(base, occupied);
    if !requested_name.is_empty() {
        let owner = taken_by(base, projects);
        blockers.push(blocker(
            BLOCKER_NAME_TAKEN,
            format!(
                "name {base} is taken{}",
                owner.map(|name| format!(" by {name}")).unwrap_or_default()
            ),
            format!("try {allocated}"),
        ));
    }
    (allocated, NAME_SOURCE_ALLOCATED)
}

fn preview_aliases(
    requested: &ProjectOnboardingRequestedWire,
    record: Option<&ProjectOnboardingRecordFactsWire>,
    projects: &[ProjectOnboardingProjectFactsWire],
    occupied: &BTreeSet<String>,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
) -> Vec<String> {
    if requested.aliases.is_empty() {
        return record.map(|item| item.aliases.clone()).unwrap_or_default();
    }
    for alias in &requested.aliases {
        let trimmed = alias.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !is_valid_sase_project_name(trimmed) {
            blockers.push(blocker(
                BLOCKER_INVALID_NAME,
                format!("invalid alias {trimmed}"),
                "use a single path component without separators",
            ));
            continue;
        }
        if is_taken(trimmed, occupied) {
            let allocated = allocate_name(trimmed, occupied);
            let owner = taken_by(trimmed, projects);
            blockers.push(blocker(
                BLOCKER_ALIAS_TAKEN,
                format!(
                    "alias {trimmed} is taken{}",
                    owner.map(|name| format!(" by {name}")).unwrap_or_default()
                ),
                format!("try {allocated}"),
            ));
        }
    }
    requested.aliases.clone()
}

fn resolve_setup(
    mode: Mode,
    actor: Actor,
    choice: SetupChoice,
    facts: &ProjectOnboardingFactsWire,
    target: &ProjectOnboardingTargetWire,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
    warnings: &mut Vec<ProjectOnboardingWarningWire>,
) -> ProjectOnboardingSetupWire {
    let managed = is_managed(facts);
    let separate = target.storage_policy.trim() == STORAGE_SEPARATE_REPO;
    match choice {
        SetupChoice::On => {
            if actor == Actor::Agent {
                blockers.push(blocker(
                    BLOCKER_HUMAN_ONLY,
                    "agents cannot run the commit-and-push setup step",
                    "propose setup through /sase_gate, or omit --setup",
                ));
            }
            if facts.remote.can_push == Some(false) {
                blockers.push(blocker(
                    BLOCKER_NO_PUSH,
                    "cannot push the SASE-managed marker",
                    "run sase init -M on a machine with push rights",
                ));
            }
            ProjectOnboardingSetupWire {
                enabled: true,
                code: SETUP_CODE_ON.to_string(),
                reason: "setup was requested".to_string(),
            }
        }
        SetupChoice::Off => {
            if !managed && separate {
                warnings.push(warning(
                    WARNING_NOT_LAUNCHABLE,
                    "agents cannot launch here until someone runs sase init -M",
                ));
            }
            ProjectOnboardingSetupWire {
                enabled: false,
                code: SETUP_CODE_OFF.to_string(),
                reason: "setup was disabled".to_string(),
            }
        }
        SetupChoice::Auto => {
            if actor == Actor::Agent {
                return ProjectOnboardingSetupWire {
                    enabled: false,
                    code: SETUP_CODE_AGENT.to_string(),
                    reason: "agents never run the commit-and-push setup step"
                        .to_string(),
                };
            }
            if mode == Mode::Create {
                return ProjectOnboardingSetupWire {
                    enabled: true,
                    code: SETUP_CODE_NEW_REPO.to_string(),
                    reason: "new repositories are set up by default"
                        .to_string(),
                };
            }
            if managed {
                return ProjectOnboardingSetupWire {
                    enabled: false,
                    code: SETUP_CODE_ALREADY_MANAGED.to_string(),
                    reason:
                        "already SASE-managed — sidecars connect on first launch"
                            .to_string(),
                };
            }
            match facts.remote.can_push {
                Some(true) => ProjectOnboardingSetupWire {
                    enabled: true,
                    code: SETUP_CODE_UNMANAGED_CAN_PUSH.to_string(),
                    reason: "needed before agents can launch here".to_string(),
                },
                Some(false) => {
                    warnings.push(warning(
                        WARNING_NOT_LAUNCHABLE,
                        "no push rights — run sase init -M before agents can launch here",
                    ));
                    ProjectOnboardingSetupWire {
                        enabled: false,
                        code: SETUP_CODE_NO_PUSH.to_string(),
                        reason: "no push rights — run sase init -M".to_string(),
                    }
                }
                None => {
                    warnings.push(warning(
                        WARNING_UNKNOWN_PUSH,
                        "push rights are unknown; setup is left off",
                    ));
                    ProjectOnboardingSetupWire {
                        enabled: false,
                        code: SETUP_CODE_UNKNOWN_PUSH.to_string(),
                        reason: "push rights are unknown".to_string(),
                    }
                }
            }
        }
    }
}

struct StepBuild<'a> {
    mode: Mode,
    actor: Actor,
    checkout_state: &'a str,
    remote_status: &'a str,
    request: &'a ProjectOnboardingRequestWire,
    facts: &'a ProjectOnboardingFactsWire,
    identity: &'a ProjectOnboardingIdentityWire,
    setup: &'a ProjectOnboardingSetupWire,
}

struct StepFlags {
    remote: bool,
    requires_tty: bool,
    handoff: bool,
}

const STEP_LOCAL: StepFlags = StepFlags {
    remote: false,
    requires_tty: false,
    handoff: false,
};

const STEP_REMOTE: StepFlags = StepFlags {
    remote: true,
    requires_tty: false,
    handoff: false,
};

fn build_steps(
    ctx: &StepBuild<'_>,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
    warnings: &mut Vec<ProjectOnboardingWarningWire>,
) -> Vec<ProjectOnboardingStepWire> {
    let mut steps = Vec::new();
    if ctx.mode == Mode::Create {
        steps.push(create_remote_step(
            ctx.actor,
            ctx.remote_status,
            ctx.request,
            ctx.facts,
            blockers,
        ));
    }
    steps.push(checkout_step(
        ctx.mode,
        ctx.checkout_state,
        ctx.remote_status,
        ctx.request,
        ctx.facts,
        blockers,
        warnings,
    ));
    steps.push(reenable_step(ctx.facts.record.as_ref(), ctx.identity));
    steps.push(identity_step(
        ctx.request,
        ctx.facts.record.as_ref(),
        ctx.identity,
    ));
    steps.push(manage_step(ctx.setup, ctx.facts, blockers));
    steps.push(current_step(
        ctx.request.requested.make_current,
        ctx.identity,
    ));
    steps.push(init_step(ctx.setup, &ctx.request.target));
    steps
}

fn create_remote_step(
    actor: Actor,
    remote_status: &str,
    request: &ProjectOnboardingRequestWire,
    facts: &ProjectOnboardingFactsWire,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
) -> ProjectOnboardingStepWire {
    if actor == Actor::Agent {
        blockers.push(blocker(
            BLOCKER_HUMAN_ONLY,
            "creating a remote repository is human-only",
            "propose sase project create through /sase_gate",
        ));
    }
    if facts.can_create == Some(false) {
        blockers.push(blocker(
            BLOCKER_NO_CREATE_RIGHTS,
            "the signed-in account cannot create a repository here",
            "choose an owner you can create in",
        ));
    }
    for error in &facts.creation_errors {
        let field = if error.field.is_empty() {
            "option".to_string()
        } else {
            error.field.clone()
        };
        blockers.push(blocker(
            BLOCKER_INVALID_OPTION,
            if error.message.is_empty() {
                format!("invalid {field}")
            } else {
                error.message.clone()
            },
            "",
        ));
    }
    let visibility = request
        .creation
        .as_ref()
        .map(|item| item.visibility.as_str())
        .unwrap_or("");
    let owned = journal_owns_remote(
        facts.journal.as_ref(),
        &facts.remote,
        remote_status,
        visibility,
    );
    let (state, extra_blocker) = match remote_status {
        REMOTE_FOUND if owned => (STEP_DONE, None),
        REMOTE_FOUND => (
            STEP_PENDING,
            Some(blocker(
                BLOCKER_REMOTE_EXISTS,
                format!("{} already exists", display_of(request)),
                format!("sase project add {}", request.target.repo_ref),
            )),
        ),
        REMOTE_UNAVAILABLE => (
            STEP_PENDING,
            Some(blocker(
                BLOCKER_REMOTE_UNKNOWN,
                remote_unknown_message(&facts.remote),
                "",
            )),
        ),
        _ => (STEP_PENDING, None),
    };
    if let Some(item) = extra_blocker {
        blockers.push(item);
    }
    let summary = request
        .creation
        .as_ref()
        .map(|item| item.summary.join(" · "))
        .unwrap_or_default();
    let detail = match (visibility, summary.as_str()) {
        ("", "") => display_of(request),
        ("", summary) => summary.to_string(),
        (visibility, "") => format!("{visibility} {}", display_of(request)),
        (visibility, summary) => {
            format!("{visibility} {} · {summary}", display_of(request))
        }
    };
    step(
        STEP_CREATE_REMOTE,
        OPERATION_CREATE,
        format!("Create {}", display_of(request)),
        detail,
        state,
        STEP_REMOTE,
    )
}

fn checkout_step(
    mode: Mode,
    checkout_state: &str,
    remote_status: &str,
    request: &ProjectOnboardingRequestWire,
    facts: &ProjectOnboardingFactsWire,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
    warnings: &mut Vec<ProjectOnboardingWarningWire>,
) -> ProjectOnboardingStepWire {
    let path = request.target.checkout_dir.as_str();
    let matching = checkout_state == CHECKOUT_MATCHING;
    let has_record = facts.record.is_some();
    if matches!(checkout_state, CHECKOUT_MISMATCH | CHECKOUT_NOT_GIT) {
        blockers.push(blocker(
            BLOCKER_CHECKOUT_CONFLICT,
            if path.is_empty() {
                "checkout path conflicts".to_string()
            } else {
                format!("{path} is not a matching clone")
            },
            if path.is_empty() {
                String::new()
            } else {
                format!("choose a different path than {path}")
            },
        ));
    }
    if mode == Mode::Add && remote_status == REMOTE_NOT_FOUND {
        blockers.push(blocker(
            BLOCKER_NOT_FOUND,
            format!("{} was not found", display_of(request)),
            "",
        ));
    }
    if remote_status == REMOTE_UNAVAILABLE {
        if matching {
            warnings.push(warning(
                WARNING_REMOTE_UNVERIFIED,
                format!(
                    "could not verify {}: {}",
                    display_of(request),
                    if facts.remote.message.is_empty() {
                        "remote unavailable".to_string()
                    } else {
                        facts.remote.message.clone()
                    }
                ),
            ));
        } else if mode == Mode::Add {
            blockers.push(blocker(
                BLOCKER_REMOTE_UNKNOWN,
                remote_unknown_message(&facts.remote),
                "",
            ));
        }
    }
    let (operation, label, state) = match checkout_state {
        CHECKOUT_MATCHING if has_record => {
            (OPERATION_UPDATE, "Register existing clone", STEP_DONE)
        }
        CHECKOUT_MATCHING => {
            (OPERATION_UPDATE, "Register existing clone", STEP_PENDING)
        }
        CHECKOUT_EMPTY_DIR | CHECKOUT_ABSENT => (
            OPERATION_CREATE,
            "Clone into the checkout and register",
            STEP_PENDING,
        ),
        _ => (
            OPERATION_CREATE,
            "Clone into the checkout and register",
            STEP_PENDING,
        ),
    };
    let label =
        if matches!(checkout_state, CHECKOUT_ABSENT | CHECKOUT_EMPTY_DIR)
            && !path.is_empty()
        {
            format!("Clone into {path} and register")
        } else {
            label.to_string()
        };
    step(STEP_CHECKOUT, operation, label, path, state, STEP_LOCAL)
}

fn reenable_step(
    record: Option<&ProjectOnboardingRecordFactsWire>,
    identity: &ProjectOnboardingIdentityWire,
) -> ProjectOnboardingStepWire {
    let disabled =
        record.is_some_and(|item| item.state.trim() == PROJECT_STATE_DISABLED);
    let name = if identity.name.is_empty() {
        "project".to_string()
    } else {
        identity.name.clone()
    };
    step(
        STEP_REENABLE,
        OPERATION_UPDATE,
        format!("Re-enable {name}"),
        "",
        if disabled { STEP_PENDING } else { STEP_SKIPPED },
        STEP_LOCAL,
    )
}

fn identity_step(
    request: &ProjectOnboardingRequestWire,
    record: Option<&ProjectOnboardingRecordFactsWire>,
    identity: &ProjectOnboardingIdentityWire,
) -> ProjectOnboardingStepWire {
    let requested_name = request.requested.name.trim();
    let name_pending = if requested_name.is_empty() {
        false
    } else if let Some(record) = record {
        requested_name != record.name
    } else {
        requested_name != request.target.project_name_base.trim()
            || identity.name != requested_name
    };
    let alias_pending = if request.requested.aliases.is_empty() {
        false
    } else if let Some(record) = record {
        request.requested.aliases != record.aliases
    } else {
        true
    };
    let pending = name_pending || alias_pending;
    let detail = if identity.aliases.is_empty() {
        identity.name.clone()
    } else {
        format!("{} ({})", identity.name, identity.aliases.join(", "))
    };
    step(
        STEP_IDENTITY,
        OPERATION_UPDATE,
        "Set project name and aliases",
        detail,
        if pending { STEP_PENDING } else { STEP_SKIPPED },
        STEP_LOCAL,
    )
}

fn manage_step(
    setup: &ProjectOnboardingSetupWire,
    facts: &ProjectOnboardingFactsWire,
    blockers: &mut Vec<ProjectOnboardingBlockerWire>,
) -> ProjectOnboardingStepWire {
    let managed = is_managed(facts);
    if setup.enabled && !managed {
        if facts.checkout.config_dirty {
            blockers.push(blocker(
                BLOCKER_CONFIG_DIRTY,
                "sase/sase.yml has local edits",
                "commit or stash sase/sase.yml, then retry",
            ));
        }
        if facts.checkout.on_default_branch == Some(false) {
            blockers.push(blocker(
                BLOCKER_NOT_ON_DEFAULT_BRANCH,
                "checkout is not on the default branch",
                "switch to the default branch, then retry",
            ));
        }
    }
    let (operation, state) = if !setup.enabled {
        (OPERATION_UPDATE, STEP_SKIPPED)
    } else if managed {
        (OPERATION_UPDATE, STEP_DONE)
    } else {
        (OPERATION_CREATE, STEP_PENDING)
    };
    let detail = if state == STEP_SKIPPED {
        setup.reason.clone()
    } else {
        "commit and push sase/sase.yml is_sase_managed".to_string()
    };
    step(
        STEP_MANAGE,
        operation,
        "Mark the repository as SASE-managed",
        detail,
        state,
        STEP_REMOTE,
    )
}

fn current_step(
    make_current: bool,
    identity: &ProjectOnboardingIdentityWire,
) -> ProjectOnboardingStepWire {
    let name = if identity.name.is_empty() {
        "project".to_string()
    } else {
        identity.name.clone()
    };
    step(
        STEP_CURRENT,
        OPERATION_UPDATE,
        format!("Make {name} the current project"),
        "",
        if make_current {
            STEP_PENDING
        } else {
            STEP_SKIPPED
        },
        STEP_LOCAL,
    )
}

fn init_step(
    setup: &ProjectOnboardingSetupWire,
    target: &ProjectOnboardingTargetWire,
) -> ProjectOnboardingStepWire {
    let requires_tty = target.storage_policy.trim() == STORAGE_SEPARATE_REPO;
    let (state, detail) = if setup.enabled {
        (
            STEP_PENDING,
            if requires_tty {
                "init preview; sidecar creation asks in a terminal".to_string()
            } else {
                "then the init preview".to_string()
            },
        )
    } else {
        (STEP_SKIPPED, setup.reason.clone())
    };
    step(
        STEP_INIT,
        OPERATION_VALIDATE,
        "Initialize SASE",
        detail,
        state,
        StepFlags {
            remote: false,
            requires_tty: requires_tty && setup.enabled,
            handoff: true,
        },
    )
}

fn journal_owns_remote(
    journal: Option<&ProjectOnboardingJournalFactsWire>,
    remote: &ProjectOnboardingRemoteFactsWire,
    remote_status: &str,
    requested_visibility: &str,
) -> bool {
    if remote_status != REMOTE_FOUND {
        return false;
    }
    let Some(journal) = journal else {
        return false;
    };
    if !journal.repo_id.is_empty() {
        return !remote.repo_id.is_empty() && journal.repo_id == remote.repo_id;
    }
    remote.is_empty == Some(true)
        && !requested_visibility.is_empty()
        && remote.visibility.eq_ignore_ascii_case(requested_visibility)
}

fn is_managed(facts: &ProjectOnboardingFactsWire) -> bool {
    facts.remote.sase_managed == Some(true) || facts.local_managed == Some(true)
}

fn occupied_refs(
    projects: &[ProjectOnboardingProjectFactsWire],
    exclude_key: Option<&str>,
) -> BTreeSet<String> {
    let mut occupied = BTreeSet::new();
    occupied.insert(fold_ref("home"));
    let exclude = exclude_key.map(fold_ref).unwrap_or_default();
    for project in projects {
        if !exclude.is_empty() && fold_ref(&project.key) == exclude {
            continue;
        }
        occupy(&mut occupied, &project.key);
        occupy(&mut occupied, &project.name);
        for alias in &project.aliases {
            occupy(&mut occupied, alias);
        }
    }
    occupied
}

fn occupy(occupied: &mut BTreeSet<String>, value: &str) {
    let folded = fold_ref(value);
    if !folded.is_empty() {
        occupied.insert(folded);
    }
}

fn is_taken(value: &str, occupied: &BTreeSet<String>) -> bool {
    occupied.contains(&fold_ref(value))
}

fn taken_by(
    value: &str,
    projects: &[ProjectOnboardingProjectFactsWire],
) -> Option<String> {
    let folded = fold_ref(value);
    if folded == fold_ref("home") {
        return Some("home".to_string());
    }
    for project in projects {
        if fold_ref(&project.key) == folded
            || fold_ref(&project.name) == folded
            || project
                .aliases
                .iter()
                .any(|alias| fold_ref(alias) == folded)
        {
            if project.name.is_empty() {
                return Some(project.key.clone());
            }
            return Some(project.name.clone());
        }
    }
    None
}

fn allocate_name(base: &str, occupied: &BTreeSet<String>) -> String {
    if !is_taken(base, occupied) {
        return base.to_string();
    }
    let mut suffix = 1_u32;
    loop {
        let candidate = format!("{base}_{suffix}");
        if !is_taken(&candidate, occupied) {
            return candidate;
        }
        suffix = suffix.saturating_add(1);
        if suffix > 10_000 {
            return format!("{base}_{suffix}");
        }
    }
}

fn fold_ref(value: &str) -> String {
    value.trim().case_fold().collect()
}

fn display_of(request: &ProjectOnboardingRequestWire) -> String {
    if !request.target.display.is_empty() {
        request.target.display.clone()
    } else if !request.target.repo_ref.is_empty() {
        request.target.repo_ref.clone()
    } else {
        request.target.identity_key.clone()
    }
}

fn remote_unknown_message(remote: &ProjectOnboardingRemoteFactsWire) -> String {
    if remote.message.is_empty() {
        "the remote repository could not be verified".to_string()
    } else {
        remote.message.clone()
    }
}

fn step(
    id: &str,
    operation: &str,
    label: impl Into<String>,
    detail: impl Into<String>,
    state: &str,
    flags: StepFlags,
) -> ProjectOnboardingStepWire {
    ProjectOnboardingStepWire {
        id: id.to_string(),
        operation: operation.to_string(),
        label: label.into(),
        detail: detail.into(),
        state: state.to_string(),
        remote: flags.remote,
        requires_tty: flags.requires_tty,
        handoff: flags.handoff,
    }
}

fn blocker(
    code: &str,
    message: impl Into<String>,
    hint: impl Into<String>,
) -> ProjectOnboardingBlockerWire {
    ProjectOnboardingBlockerWire {
        code: code.to_string(),
        message: message.into(),
        hint: hint.into(),
    }
}

fn warning(
    code: &str,
    message: impl Into<String>,
) -> ProjectOnboardingWarningWire {
    ProjectOnboardingWarningWire {
        code: code.to_string(),
        message: message.into(),
    }
}

#[derive(Serialize)]
struct IntentFingerprint<'a> {
    mode: &'a str,
    identity_key: &'a str,
    #[serde(rename = "ref")]
    repo_ref: &'a str,
    requested: &'a ProjectOnboardingRequestedWire,
    creation: Option<&'a super::wire::ProjectOnboardingCreationWire>,
}

fn intent_fingerprint(
    request: &ProjectOnboardingRequestWire,
) -> Result<String, ProjectOnboardingError> {
    let payload = IntentFingerprint {
        mode: request.mode.trim(),
        identity_key: request.target.identity_key.as_str(),
        repo_ref: request.target.repo_ref.as_str(),
        requested: &request.requested,
        creation: request.creation.as_ref(),
    };
    let value = serde_json::to_value(&payload).map_err(|error| {
        ProjectOnboardingError::Validation(format!(
            "unable to encode intent fingerprint: {error}"
        ))
    })?;
    crate::finalizer::canonical_json_sha256(&value).map_err(|error| {
        ProjectOnboardingError::Validation(format!(
            "unable to hash intent fingerprint: {error}"
        ))
    })
}
