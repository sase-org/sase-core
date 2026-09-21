//! Typed launch-unit planning, identity directives, waits, holds, and
//! queue spellings (`typed_units.rs`, `identity.rs`, `plan_resolution.rs`).
use serde_json::json;

use crate::agent_launch::directive_scan::parse_directive_args_with_names;
use crate::agent_launch::{
    agent_unit_dispatch_prompt, plan_typed_launch_units,
    plan_typed_launch_units_with_flags, prompt_has_identity_directive,
    AgentLaunchFanoutPlanError, AgentUnitWire, LaunchPlanWire,
    LaunchUnitPayloadWire, WaitTargetWire, LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
};

#[test]
fn typed_launch_plan_builds_mixed_proc_agent_wait_graph() {
    let prompt =
        "%proc(\"just check\")\n---\n%wait\n%id:reviewer\n%model:opus\nReview";

    let plan =
        plan_typed_launch_units(prompt, Some("multi_prompt"), Some("sase"))
            .unwrap();

    assert_eq!(plan.schema_version, LAUNCH_PLAN_WIRE_SCHEMA_VERSION);
    assert_eq!(plan.launch_kind, "multi_prompt");
    assert_eq!(plan.units.len(), 2);
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Proc(proc_unit) => {
            assert_eq!(proc_unit.code.source, "just check");
            assert_eq!(proc_unit.code.language, "bash");
            assert_eq!(proc_unit.selected_project.as_deref(), Some("sase"));
            assert!(proc_unit.workspace);
        }
        other => panic!("expected proc payload, got {other:?}"),
    }
    match &plan.units[1].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.identity.as_deref(), Some("reviewer"));
            assert_eq!(agent.model.as_deref(), Some("opus"));
            assert_eq!(agent.prompt, "Review");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    assert_eq!(
        plan.units[1].waits,
        vec![WaitTargetWire::Logical {
            logical_id: "unit-1".to_string(),
            source: Some("%wait".to_string())
        }]
    );
    assert!(plan.approval_preview[1].contains("proc"));
    assert_eq!(plan.content_digest.len(), 64);
}

#[test]
fn typed_launch_plan_captures_if_fence_without_duplicate_form_error() {
    let prompt = "%if::\n\n```bash\ntest -f pyproject.toml\n```\nReview";

    let plan =
        plan_typed_launch_units(prompt, Some("auto"), Some("sase")).unwrap();

    assert_eq!(plan.units.len(), 1);
    let condition = plan.units[0].condition.as_ref().expect("conditioned unit");
    assert_eq!(condition.code.language, "bash");
    assert!(condition.code.source.contains("test -f pyproject.toml"));
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.prompt, "Review");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
}

#[test]
fn typed_launch_plan_preserves_prose_if_proc_mentions() {
    let prompt = "stop un-admitted %if/%proc units\n%proc mentions stay prose";

    let plan =
        plan_typed_launch_units(prompt, Some("auto"), Some("sase")).unwrap();

    assert_eq!(plan.units.len(), 1);
    assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.prompt, prompt);
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_plan_rejects_bare_proc_without_body() {
    let err =
        plan_typed_launch_units("%proc\nDo work", Some("auto"), Some("sase"))
            .unwrap_err();
    assert!(err.to_string().contains("%proc requires a body"), "{err}");
}

#[test]
fn typed_launch_plan_rejects_invalid_if_forms_without_owned_fence() {
    let err =
        plan_typed_launch_units("%if:true\nReview", Some("auto"), Some("sase"))
            .unwrap_err();

    assert!(err.to_string().contains("static omission"));

    let paren_err = plan_typed_launch_units(
        "%if(true)\nReview",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();
    match paren_err {
        AgentLaunchFanoutPlanError::TypedLaunchPlan { diagnostics } => {
            assert!(diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid-if-form"));
        }
        other => panic!("expected typed launch diagnostic, got {other:?}"),
    }
}

#[test]
fn typed_launch_plan_keeps_fenced_proc_options() {
    let prompt = "%proc(timeout=\"20m\", cwd=\"docs\", workspace=\"true\")::\n\n```bash\njust docs-check\n```\n";

    let plan =
        plan_typed_launch_units(prompt, Some("auto"), Some("sase")).unwrap();

    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Proc(proc_unit) => {
            assert_eq!(proc_unit.timeout.as_deref(), Some("20m"));
            assert_eq!(proc_unit.cwd.as_deref(), Some("docs"));
            assert!(proc_unit.workspace);
            assert!(proc_unit.code.source.contains("just docs-check"));
        }
        other => panic!("expected proc payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_plan_captures_bare_fenced_proc() {
    let prompt = "%proc::\n```bash\njust check\n```\n";

    let plan =
        plan_typed_launch_units(prompt, Some("auto"), Some("sase")).unwrap();

    assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Proc(proc_unit) => {
            assert_eq!(proc_unit.code.source, "just check\n");
            assert_eq!(proc_unit.code.language, "bash");
        }
        other => panic!("expected proc payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_plan_resolves_forward_proc_wait() {
    let prompt =
        "%wait(proc=build)\nReview\n---\n%id:build\n%proc(\"echo ready\")";

    let plan =
        plan_typed_launch_units(prompt, Some("multi_prompt"), Some("sase"))
            .unwrap();

    assert_eq!(
        plan.units[0].waits,
        vec![WaitTargetWire::Logical {
            logical_id: "unit-2".to_string(),
            source: Some("%wait(proc=build)".to_string())
        }]
    );
    match &plan.units[1].payload {
        LaunchUnitPayloadWire::Proc(proc_unit) => {
            assert_eq!(proc_unit.shell_name.as_deref(), Some("build"));
        }
        other => panic!("expected proc payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_plan_rejects_agent_directives_on_proc() {
    let err = plan_typed_launch_units(
        "%model:opus\n%proc(\"just check\")",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();

    assert!(err.to_string().contains("not valid on %proc"));
    match err {
        AgentLaunchFanoutPlanError::TypedLaunchPlan { diagnostics } => {
            assert!(
                diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.code
                        == "agent-directive-on-proc")
            );
        }
        other => panic!("expected typed launch diagnostic, got {other:?}"),
    }
}

#[test]
fn typed_launch_plan_rejects_wait_cycles() {
    let err = plan_typed_launch_units(
        "%wait(unit=unit-2)\nFirst\n---\n%wait(unit=unit-1)\nSecond",
        Some("multi_prompt"),
        Some("sase"),
    )
    .unwrap_err();

    assert!(err.to_string().contains("cycle"));
}

#[test]
fn typed_launch_future_hold_cycle_rejects_wait_on_sibling() {
    let err = plan_typed_launch_units_with_flags(
        "%hold(future)\n%wait(unit=unit-2)\nFirst\n---\nSecond",
        Some("multi_prompt"),
        Some("sase"),
        &[],
    )
    .unwrap_err();

    assert!(err.to_string().contains("future"), "{err}");
    match err {
        AgentLaunchFanoutPlanError::TypedLaunchPlan { diagnostics } => {
            assert!(diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "hold-cycle"));
        }
        other => panic!("expected typed launch diagnostic, got {other:?}"),
    }
}

#[test]
fn typed_launch_future_hold_cycle_rejects_two_future_siblings() {
    let err = plan_typed_launch_units_with_flags(
        "%hold(future)\nFirst\n---\n%hold(future)\nSecond",
        Some("multi_prompt"),
        Some("sase"),
        &[],
    )
    .unwrap_err();

    assert!(err.to_string().contains("future"), "{err}");
}

#[test]
fn typed_launch_future_hold_without_cycle_is_allowed() {
    let plan = plan_typed_launch_units_with_flags(
        "%hold(future)\nFirst",
        Some("multi_prompt"),
        Some("sase"),
        &[],
    )
    .unwrap();

    assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
}

#[test]
fn typed_launch_future_hold_cycle_ignores_kin_sibling() {
    let plan = plan_typed_launch_units_with_flags(
            "%id(parent, family=root)\n%hold(future)\n%wait(unit=unit-2)\nFirst\n---\n%id(child, family=root)\nSecond",
            Some("multi_prompt"),
            Some("sase"),
            &[],
        )
        .unwrap();

    assert!(plan.diagnostics.is_empty(), "{:?}", plan.diagnostics);
}

#[test]
fn typed_launch_plan_validates_proc_project_policy() {
    let err = plan_typed_launch_units(
        "%proc(workspace=false)::\n```bash\njust check\n```",
        Some("auto"),
        None,
    )
    .unwrap_err();

    assert!(err.to_string().contains("requires an explicit cwd"));
}

#[test]
fn agent_unit_legacy_json_defaults_to_plain_identity() {
    let value = json!({
        "prompt": "Review",
        "identity": "reviewer",
        "identity_explicit": true,
    });
    let agent: AgentUnitWire = serde_json::from_value(value).unwrap();
    assert_eq!(agent.identity.as_deref(), Some("reviewer"));
    assert!(agent.identity_explicit);
    assert!(!agent.identity_force_reuse);
    assert!(agent.clan.is_none());
    assert!(!agent.clan_declared);
    assert!(agent.clan_tribe.is_none());
    assert!(agent.clan_summary.is_none());
    assert!(agent.clan_summary_script.is_none());
    assert!(agent.family_attach_parent.is_none());
    assert!(agent.family_attach_suffix.is_none());
    assert!(agent.tribe.is_none());
    let serialized = serde_json::to_value(&agent).unwrap();
    assert!(serialized.get("clan").is_none());
    assert!(serialized.get("clan_declared").is_none());
    assert!(serialized.get("tribe").is_none());
}

#[test]
fn agent_unit_identity_forms_round_trip_json() {
    let cases = [
        AgentUnitWire {
            prompt: "plain".to_string(),
            identity: Some("reviewer".to_string()),
            identity_explicit: true,
            ..Default::default()
        },
        AgentUnitWire {
            prompt: "join".to_string(),
            identity: Some("worker".to_string()),
            identity_explicit: true,
            clan: Some("research".to_string()),
            ..Default::default()
        },
        AgentUnitWire {
            prompt: "declare".to_string(),
            identity: Some("research.worker".to_string()),
            identity_explicit: true,
            clan: Some("research".to_string()),
            clan_declared: true,
            clan_tribe: Some("study".to_string()),
            clan_summary: Some("[bold]Research[/bold]".to_string()),
            clan_summary_script: None,
            ..Default::default()
        },
        AgentUnitWire {
            prompt: "family".to_string(),
            family_attach_parent: Some("parent".to_string()),
            family_attach_suffix: Some("reviewer".to_string()),
            ..Default::default()
        },
        AgentUnitWire {
            prompt: "tribe".to_string(),
            identity: Some("worker".to_string()),
            identity_explicit: true,
            tribe: Some("review".to_string()),
            ..Default::default()
        },
        AgentUnitWire {
            prompt: "auto-tribe".to_string(),
            tribe: Some("review".to_string()),
            ..Default::default()
        },
    ];
    for agent in cases {
        let value = serde_json::to_value(&agent).unwrap();
        let back: AgentUnitWire = serde_json::from_value(value).unwrap();
        assert_eq!(back, agent);
    }
}

#[test]
fn typed_launch_preserves_per_unit_workspace_and_dispatch() {
    let prompt = "%dispatch:apollo\n%id:observer\n#gh:sase\nWatch Apollo\n---\n%id:local-reviewer\n#git:dotfiles\nReview locally";
    let plan = plan_typed_launch_units(
        prompt,
        Some("multi_prompt"),
        Some("gh_sase-org__sase"),
    )
    .unwrap();

    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.dispatch_target.as_deref(), Some("apollo"));
            assert_eq!(agent.workspace_provider.as_deref(), Some("gh"));
            assert_eq!(agent.workspace_reference.as_deref(), Some("#gh:sase"));
            assert_eq!(agent.identity.as_deref(), Some("observer"));
            assert_eq!(agent.prompt, "Watch Apollo");
            assert!(!agent.prompt.contains("#gh:sase"));
            assert!(!agent.prompt.contains("%dispatch"));
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    match &plan.units[1].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert!(agent.dispatch_target.is_none());
            assert_eq!(agent.workspace_provider.as_deref(), Some("git"));
            assert_eq!(
                agent.workspace_reference.as_deref(),
                Some("#git:dotfiles")
            );
            assert_eq!(agent.prompt, "Review locally");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    assert_eq!(plan.selected_project.as_deref(), Some("gh_sase-org__sase"));
    let preview = plan.approval_preview.join("\n");
    assert!(preview.contains("workspace=#gh:sase"));
    assert!(preview.contains("machine=apollo"));
    assert!(preview.contains("workspace=#git:dotfiles"));
    assert!(preview.contains("machine=local"));

    let remote =
        crate::agent_unit_dispatch_prompt(match &plan.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => agent,
            other => panic!("expected agent payload, got {other:?}"),
        });
    assert!(remote.contains("%dispatch:apollo"));
    assert!(remote.contains("%id:observer"));
    assert!(remote.contains("#gh:sase"));
    assert!(remote.contains("Watch Apollo"));

    let local =
        crate::agent_unit_dispatch_prompt(match &plan.units[1].payload {
            LaunchUnitPayloadWire::Agent(agent) => agent,
            other => panic!("expected agent payload, got {other:?}"),
        });
    assert!(!local.contains("%dispatch"));
    assert!(local.contains("#git:dotfiles"));
    assert!(local.contains("%id:local-reviewer"));
}

#[test]
fn typed_launch_keeps_fenced_workspace_and_dispatch_inert() {
    let prompt =
        "```text\n%dispatch:apollo\n#gh:sase\n```\n%id:reviewer\nDo work";
    let plan =
        plan_typed_launch_units(prompt, Some("auto"), Some("sase")).unwrap();
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert!(agent.dispatch_target.is_none());
            assert!(agent.workspace_reference.is_none());
            assert!(agent.prompt.contains("%dispatch:apollo"));
            assert!(agent.prompt.contains("#gh:sase"));
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_does_not_replace_unit_workspace_with_plan_project() {
    let plan = plan_typed_launch_units(
        "%id:child\n#git:dotfiles\nReview",
        Some("auto"),
        Some("sase"),
    )
    .unwrap();
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(
                agent.workspace_reference.as_deref(),
                Some("#git:dotfiles")
            );
            assert_ne!(agent.workspace_reference.as_deref(), Some("sase"));
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    assert_eq!(plan.selected_project.as_deref(), Some("sase"));
}

#[test]
fn typed_launch_rejects_dispatch_combined_with_wait_or_family() {
    let wait = plan_typed_launch_units(
        "%dispatch:apollo\n%wait:builder\nDo remote",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();
    assert!(wait.to_string().contains("%wait"), "{wait}");
    let family = plan_typed_launch_units(
        "%dispatch:apollo\n%id(reviewer, family=parent)\nDo remote",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();
    assert!(family.to_string().contains("family"), "{family}");
    let local = plan_typed_launch_units(
        "%dispatch:local\nDo remote",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();
    assert!(local.to_string().contains("reserved"), "{local}");
}

#[test]
fn prompt_has_identity_directive_ignores_fenced_id() {
    assert!(prompt_has_identity_directive("%id:observer\nDo work"));
    assert!(prompt_has_identity_directive(
        "%id(reviewer, bead=sase-1)\nDo work"
    ));
    assert!(!prompt_has_identity_directive("Do work"));
    assert!(!prompt_has_identity_directive(
        "```text\n%id:observer\n```\nDo work"
    ));
}

#[test]
fn typed_launch_plan_preserves_clan_declaration_and_join() {
    let prompt = "%id:toobig-3j.foo.0\n%clan(toobig-3j, tribe=chop, summary=[[ [bold]Large[/bold]\n  Split safely. ]])\nLead\n---\n%id(bar.0, clan=toobig-3j)\n%wait:toobig-3j.foo.0\nJoin";

    let plan =
        plan_typed_launch_units(prompt, Some("multi_prompt"), Some("sase"))
            .unwrap();

    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.identity.as_deref(), Some("toobig-3j.foo.0"));
            assert!(agent.identity_explicit);
            assert_eq!(agent.clan.as_deref(), Some("toobig-3j"));
            assert!(agent.clan_declared);
            assert_eq!(agent.clan_tribe.as_deref(), Some("chop"));
            assert_eq!(
                agent.clan_summary.as_deref(),
                Some("[bold]Large[/bold]\nSplit safely.")
            );
            assert_eq!(agent.prompt, "Lead");
            assert_eq!(
                agent.effective_identity().as_deref(),
                Some("toobig-3j.foo.0")
            );
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    match &plan.units[1].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.identity.as_deref(), Some("bar.0"));
            assert_eq!(agent.clan.as_deref(), Some("toobig-3j"));
            assert!(!agent.clan_declared);
            assert_eq!(
                agent.effective_identity().as_deref(),
                Some("toobig-3j.bar.0")
            );
            assert_eq!(agent.prompt, "Join");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    assert_eq!(
        plan.units[1].waits,
        vec![WaitTargetWire::Logical {
            logical_id: "unit-1".to_string(),
            source: Some("%wait:toobig-3j.foo.0".to_string())
        }]
    );
    let reconstructed =
        agent_unit_dispatch_prompt(match &plan.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => agent,
            other => panic!("expected agent payload, got {other:?}"),
        });
    assert!(reconstructed.contains("%id:toobig-3j.foo.0"));
    assert!(reconstructed.contains("%clan(toobig-3j, tribe=chop"));
    assert!(!reconstructed.contains("%wait:"));
}

#[test]
fn typed_launch_clan_summary_ignores_inner_text_block_marker() {
    let summary = "Use `[<web>:<keyword> [...]]` for example, then continue.\n\
Keep this comma, and the rest of the prose in the summary.";
    let prompt =
        format!("%clan(research, tribe=study, summary=[[{summary}]])\nDo work");
    let plan =
        plan_typed_launch_units(&prompt, Some("multi_prompt"), Some("sase"))
            .unwrap();
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.clan.as_deref(), Some("research"));
            assert_eq!(agent.clan_tribe.as_deref(), Some("study"));
            assert_eq!(agent.clan_summary.as_deref(), Some(summary));
            assert_eq!(agent.prompt, "Do work");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_clan_summary_keeps_unbalanced_inner_closer() {
    let summary = "note: use ]] here, and more";
    let prompt =
        format!("%clan(research, tribe=study, summary=[[{summary}]])\nDo work");
    let plan =
        plan_typed_launch_units(&prompt, Some("multi_prompt"), Some("sase"))
            .unwrap();
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.clan.as_deref(), Some("research"));
            assert_eq!(agent.clan_tribe.as_deref(), Some("study"));
            assert_eq!(agent.clan_summary.as_deref(), Some(summary));
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
}

#[test]
fn parse_directive_args_text_block_corpus_matches_python() {
    use std::collections::BTreeMap;

    for case in crate::xprompt_text_block::xprompt_args_corpus() {
        let parsed = parse_directive_args_with_names(&case.source, ',');
        let mut positional = Vec::new();
        let mut named = BTreeMap::new();
        for arg in parsed {
            if let Some(name) = arg.name {
                named.insert(name, arg.value);
            } else {
                positional.push(arg.value);
            }
        }
        assert_eq!(positional, case.positional, "{}", case.id);
        assert_eq!(named, case.named, "{}", case.id);
    }
}

#[test]
fn typed_launch_plan_preserves_family_and_direct_tribe() {
    let family = plan_typed_launch_units(
        "%id(reviewer, family=parent)\nReview",
        Some("auto"),
        Some("sase"),
    )
    .unwrap();
    match &family.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.family_attach_parent.as_deref(), Some("parent"));
            assert_eq!(agent.family_attach_suffix.as_deref(), Some("reviewer"));
            assert!(agent.identity.is_none());
            assert_eq!(
                agent.effective_identity().as_deref(),
                Some("parent--reviewer")
            );
        }
        other => panic!("expected agent payload, got {other:?}"),
    }

    let named_tribe = plan_typed_launch_units(
        "%id(worker, tribe=review)\nReview",
        Some("auto"),
        Some("sase"),
    )
    .unwrap();
    match &named_tribe.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.identity.as_deref(), Some("worker"));
            assert_eq!(agent.tribe.as_deref(), Some("review"));
        }
        other => panic!("expected agent payload, got {other:?}"),
    }

    let auto_tribe = plan_typed_launch_units(
        "%id(tribe=review)\nReview",
        Some("auto"),
        Some("sase"),
    )
    .unwrap();
    match &auto_tribe.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert!(agent.identity.is_none());
            assert!(!agent.identity_explicit);
            assert_eq!(agent.tribe.as_deref(), Some("review"));
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_plan_rejects_conflicting_identity_forms() {
    let err = plan_typed_launch_units(
        "%clan:research\n%id(worker, clan=research)\nDo work",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("Cannot combine %clan with %id"));

    let err = plan_typed_launch_units(
        "%id(worker, clan=research, tribe=review)\nDo work",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"));

    let err = plan_typed_launch_units(
        "%clan(foo, color=blue)\nDo work",
        Some("auto"),
        Some("sase"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("Unsupported keyword on %clan"));
}

fn plan_queue(prompt: &str) -> LaunchPlanWire {
    plan_typed_launch_units_with_flags(prompt, Some("auto"), Some("sase"), &[])
        .unwrap()
}

fn plan_queue_err(prompt: &str) -> AgentLaunchFanoutPlanError {
    plan_typed_launch_units_with_flags(prompt, Some("auto"), Some("sase"), &[])
        .unwrap_err()
}

fn agent_fields(
    plan: &LaunchPlanWire,
) -> (Option<u32>, Option<i32>, Option<f64>, bool, String) {
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => (
            agent.authored_queue_capacity(),
            agent.wait_priority,
            agent.queue_weight,
            agent.queue_weight_explicit,
            agent.prompt.clone(),
        ),
        other => panic!("expected agent payload, got {other:?}"),
    }
}

fn proc_fields(
    plan: &LaunchPlanWire,
) -> (Option<u32>, Option<i32>, Option<f64>, bool) {
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Proc(proc_unit) => (
            proc_unit.queue_capacity,
            proc_unit.wait_priority,
            proc_unit.queue_weight,
            proc_unit.queue_weight_explicit,
        ),
        other => panic!("expected proc payload, got {other:?}"),
    }
}

#[test]
fn typed_launch_parses_queue_spellings_and_round_trips() {
    for prompt in [
        "%q:5\nDo work",
        "%queue:5\nDo work",
        "%q(5)\nDo work",
        "%queue(capacity=5)\nDo work",
    ] {
        let plan = plan_queue(prompt);
        let (runners, priority, weight, weight_explicit, cleaned) =
            agent_fields(&plan);
        assert_eq!(runners, Some(5), "{prompt}");
        assert_eq!(priority, None, "{prompt}");
        assert_eq!(weight, None, "{prompt}");
        assert!(!weight_explicit, "{prompt}");
        assert_eq!(cleaned, "Do work", "{prompt}");
        assert!(!cleaned.contains("%q"), "{prompt}");
    }
    let weight_only = plan_queue("%q(w=0.25)\nDo work");
    let (runners, priority, weight, weight_explicit, cleaned) =
        agent_fields(&weight_only);
    assert_eq!(runners, None);
    assert_eq!(priority, None);
    assert_eq!(weight, Some(0.25));
    assert!(weight_explicit);
    assert_eq!(cleaned, "Do work");

    let both =
        plan_queue("%w(builder, time=5m) %q(1, p=20, weight=2)\nDo work");
    match &both.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.authored_queue_capacity(), Some(1));
            assert_eq!(agent.wait_priority, Some(20));
            assert_eq!(agent.queue_weight, Some(2.0));
            assert!(agent.queue_weight_explicit);
            assert_eq!(agent.prompt, "Do work");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    assert_eq!(both.units[0].waits.len(), 2);
    let rebuilt = crate::agent_unit_dispatch_prompt_with_flags(
        match &both.units[0].payload {
            LaunchUnitPayloadWire::Agent(agent) => agent,
            other => panic!("expected agent payload, got {other:?}"),
        },
        &[],
    );
    assert!(rebuilt.contains("%queue(capacity=1, priority=20, weight=2)"));
    assert!(!rebuilt.contains("%wait(runners="));
    assert!(!rebuilt.contains("%queue(runners="));
    assert!(!rebuilt.contains("%wait(priority="));
}

#[test]
fn typed_launch_rejects_wait_queue_keywords() {
    let runners = plan_queue_err("%wait(runners=5)\nDo work");
    assert!(runners.to_string().contains("%queue(capacity="));
    let capacity = plan_queue_err("%wait(capacity=5)\nDo work");
    assert!(capacity.to_string().contains("%queue(capacity="));
    let obsolete = plan_queue_err("%queue(runners=5)\nDo work");
    assert!(obsolete.to_string().contains("capacity="));
    let priority = plan_queue_err("%wait(priority=10)\nDo work");
    assert!(priority.to_string().contains("%queue"));
    let plus = plan_queue_err("%wait(p=20)\nDo work");
    assert!(plus.to_string().contains("%queue"));
    let empty = plan_queue_err("%q\nDo work");
    assert!(empty.to_string().contains("bare %q"));
}

#[test]
fn typed_launch_proc_accepts_queue_spellings_and_authored_weight() {
    for prompt in [
        "%q:1\n%proc(\"just check\")",
        "%queue:1\n%proc(\"just check\")",
        "%q(1)\n%proc(\"just check\")",
        "%queue(capacity=1)\n%proc(\"just check\")",
    ] {
        let plan = plan_queue(prompt);
        let (capacity, priority, weight, weight_explicit) = proc_fields(&plan);
        assert_eq!(capacity, Some(1), "{prompt}");
        assert_eq!(priority, None, "{prompt}");
        assert_eq!(weight, Some(0.0), "{prompt}");
        assert!(!weight_explicit, "{prompt}");
        assert!(
            plan.approval_preview[1]
                .contains("queue=(capacity=1, weight=0 implicit)"),
            "{:?}",
            plan.approval_preview
        );
    }

    let plan =
        plan_queue("%q(priority=20, weight=0.25)\n%proc(\"just check\")");
    let (capacity, priority, weight, weight_explicit) = proc_fields(&plan);
    assert_eq!(capacity, None);
    assert_eq!(priority, Some(20));
    assert_eq!(weight, Some(0.25));
    assert!(weight_explicit);
    assert!(
        plan.approval_preview[1].contains("queue=(priority=20, weight=0.25)")
    );

    let value = serde_json::to_value(&plan.units[0].payload).unwrap();
    assert_eq!(value["queue_weight"], serde_json::json!(0.25));
    assert_eq!(value["queue_weight_explicit"], serde_json::json!(true));
}

#[test]
fn typed_launch_proc_queue_changes_content_digest() {
    let plain = plan_queue("%proc(\"just check\")");
    let queued = plan_queue("%q:1\n%proc(\"just check\")");
    let weighted = plan_queue("%q(1, weight=0.5)\n%proc(\"just check\")");

    assert_ne!(plain.content_digest, queued.content_digest);
    assert_ne!(queued.content_digest, weighted.content_digest);
}

#[test]
fn typed_launch_composes_disjoint_queue_and_fanout() {
    let plan = plan_typed_launch_units_with_flags(
        "%q:0 %queue(priority=10)\nFirst\n---\n%q(p=1, w=.25)\nSecond",
        Some("multi_prompt"),
        Some("sase"),
        &[],
    )
    .unwrap();
    match &plan.units[0].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.authored_queue_capacity(), Some(0));
            assert_eq!(agent.wait_priority, Some(10));
            assert_eq!(agent.prompt, "First");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
    match &plan.units[1].payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            assert_eq!(agent.authored_queue_capacity(), None);
            assert_eq!(agent.wait_priority, Some(1));
            assert_eq!(agent.queue_weight, Some(0.25));
            assert!(agent.queue_weight_explicit);
            assert_eq!(agent.prompt, "Second");
        }
        other => panic!("expected agent payload, got {other:?}"),
    }
}
