//! Fanout planning: multi-prompt, batch-predecessor, alternative,
//! model, repeat, and brace expansion (`fanout.rs`, `directive_scan.rs`).
use crate::agent_launch::directive_scan::{
    canonical_directive_name, launch_inline_literal_ranges,
};
use crate::agent_launch::fanout::{
    extract_first_model_value, render_alternative_prompt, AlternativeDirective,
    AlternativeReplacement, AlternativeVariant,
};
use crate::agent_launch::{
    bind_batch_predecessor_waits, plan_agent_launch_fanout,
    BatchPredecessorContextWire, BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION,
};

#[test]
fn fanout_planner_splits_multi_prompt_outside_fences() {
    let prompt = "one\n```\n---\n```\n---\n%wait\ntwo";

    let plan = plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

    assert_eq!(plan.launch_kind, "multi_prompt");
    assert_eq!(plan.slots.len(), 2);
    assert!(plan.slots[0].prompt.contains("---"));
    assert_eq!(plan.slots[1].prompt, "%wait\ntwo");
    assert!(plan.slots[1].wait_for_previous);
}

#[test]
fn fanout_planner_time_waits_defer_workspace() {
    let prompt = "%wait(time=5m)\ntwo";

    let plan = plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

    // `%time` is no longer an advertised directive. The time floor now
    // travels through `%wait(time=...)`, which still marks the slot as
    // deferred for workspace allocation.
    // `%tribe` and `%t` are removed identity directives and stay raw.
    assert_eq!(canonical_directive_name("tribe"), "tribe");
    assert_eq!(canonical_directive_name("t"), "t");
    assert_eq!(canonical_directive_name("time"), "time");
    assert_eq!(canonical_directive_name("c"), "clan");
    assert_eq!(canonical_directive_name("f"), "f");
    assert_eq!(canonical_directive_name("g"), "g");
    // `%edit` was removed and stays a non-special raw name, but `%e` is now
    // the `%effort` alias, so the launch planner canonicalizes it.
    assert_eq!(canonical_directive_name("edit"), "edit");
    assert_eq!(canonical_directive_name("e"), "effort");
    assert_eq!(plan.slots.len(), 1);
    assert!(plan.slots[0].wait_for_previous);
}

#[test]
fn fanout_planner_t_xprompt_defer_workspace() {
    let prompt = "#t:5m\ntwo";

    let plan = plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

    assert_eq!(plan.slots.len(), 1);
    assert!(plan.slots[0].wait_for_previous);
}

#[test]
fn fanout_planner_ignores_wait_forms_inside_adjacent_inline_code() {
    for prompt in [
        "keep `foo`/`%wait` and `#t:5m` literal",
        "prefix`%wait(time=5m)`suffix",
        "bare #t is not a time reference",
    ] {
        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();
        assert!(!plan.slots[0].wait_for_previous, "prompt was {prompt:?}");
    }

    for prompt in ["#t:`5m` active", "%wait(time=`5m`) active"] {
        let plan =
            plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();
        assert!(plan.slots[0].wait_for_previous, "prompt was {prompt:?}");
    }
}

fn predecessor_context() -> BatchPredecessorContextWire {
    BatchPredecessorContextWire {
        schema_version: BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION,
        project_name: "sase".to_string(),
        timestamp: "260501_120000".to_string(),
        artifact_dir: "/tmp/sase/artifacts/ace-run/202605/01/20260501120000"
            .to_string(),
        name: Some("builder".to_string()),
    }
}

#[test]
fn batch_predecessor_binding_consumes_zero_argument_wait_forms() {
    for source in [
        "%wait\nReview",
        "%w\nReview",
        "%wait()\nReview",
        "%w( \t )\nReview",
    ] {
        let binding =
            bind_batch_predecessor_waits(source, &predecessor_context())
                .unwrap();

        assert_eq!(binding.prompt, "Review");
        assert_eq!(binding.wait_names, vec!["builder"]);
        assert_eq!(binding.wait_for_artifacts, vec![predecessor_context()]);
        assert_eq!(binding.bound_wait_count, 1);
    }
}

#[test]
fn batch_predecessor_binding_preserves_explicit_and_non_wait_targets() {
    let binding = bind_batch_predecessor_waits(
        "%wait %wait:reviewer %wait(agent=ops) %queue:1\nReview",
        &predecessor_context(),
    )
    .unwrap();

    assert_eq!(
        binding.prompt,
        " %wait:reviewer %wait(agent=ops) %queue:1\nReview"
    );
    assert_eq!(binding.wait_names, vec!["builder"]);
    assert_eq!(binding.wait_for_artifacts, vec![predecessor_context()]);
    assert_eq!(binding.bound_wait_count, 1);
}

#[test]
fn batch_predecessor_binding_ignores_literal_regions() {
    let prompt = "```text\n%wait\n```\n`%w`\n%xprompts_enabled:false\n%wait\n%xprompts_enabled:true\nReview";

    let binding =
        bind_batch_predecessor_waits(prompt, &predecessor_context()).unwrap();

    assert_eq!(binding.prompt, prompt);
    assert!(binding.wait_names.is_empty());
    assert!(binding.wait_for_artifacts.is_empty());
    assert_eq!(binding.bound_wait_count, 0);
}

#[test]
fn batch_predecessor_binding_without_name_keeps_identity_only_dependency() {
    let mut context = predecessor_context();
    context.name = None;

    let binding =
        bind_batch_predecessor_waits("%wait\nReview", &context).unwrap();

    assert_eq!(binding.prompt, "Review");
    assert!(binding.wait_names.is_empty());
    assert_eq!(binding.wait_for_artifacts, vec![context]);
    assert_eq!(binding.bound_wait_count, 1);
}

#[test]
fn batch_predecessor_binding_validates_context() {
    let mut context = predecessor_context();
    context.timestamp = "20260501120000".to_string();

    let err =
        bind_batch_predecessor_waits("%wait\nReview", &context).unwrap_err();

    assert!(err.to_string().contains("expected YYmmdd_HHMMSS"));
}

#[test]
fn fanout_planner_deprecated_time_directive_is_not_special() {
    let prompt = "%time:5m\ntwo";

    let plan = plan_agent_launch_fanout(prompt, Some("multi_prompt")).unwrap();

    assert_eq!(plan.slots.len(), 1);
    assert!(!plan.slots[0].wait_for_previous);
}

#[test]
fn fanout_planner_preserves_named_alt_ids_and_values_only() {
    let prompt = "%alt(sec=[[security]],perf=[[performance]])\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(plan.launch_kind, "alternatives");
    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].alt_id.as_deref(), Some("sec"));
    assert_eq!(plan.slots[0].prompt, "security\nReview");
    assert_eq!(plan.slots[1].alt_id.as_deref(), Some("perf"));
    assert_eq!(plan.slots[1].prompt, "performance\nReview");
}

#[test]
fn fanout_planner_allocates_unnamed_alt_ids_after_named_ids() {
    let prompt = "%(fast=a,b,2=c,d)";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("fast"), Some("1"), Some("2"), Some("3")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c", "d"]
    );
}

#[test]
fn fanout_planner_composes_cartesian_alt_ids() {
    let prompt = "%alt(left=a,right=b) %alt(red=x,blue=y)";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("left.red"),
            Some("left.blue"),
            Some("right.red"),
            Some("right.blue")
        ]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["a x", "a y", "b x", "b y"]
    );
}

#[test]
fn fanout_planner_correlates_shared_named_alt_keys() {
    let prompt =
            "#gh:sase %{a=Describe | b=Explain} how this repo works %{a=in detail}.";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("b")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec![
            "#gh:sase Describe how this repo works in detail.",
            "#gh:sase Explain how this repo works."
        ]
    );
}

#[test]
fn fanout_planner_correlates_transitive_alt_keys() {
    let prompt = "%{a=1|b=2} x %{a=3} y %{a=4|b=5}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("b")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["1 x 3 y 4", "2 x y 5"]
    );
}

#[test]
fn fanout_planner_single_shared_key_collapses_to_one_slot() {
    let prompt = "%{a=X} %{a=Y}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(plan.slots.len(), 1);
    assert_eq!(plan.slots[0].alt_id.as_deref(), Some("a"));
    assert_eq!(plan.slots[0].prompt, "X Y");
}

#[test]
fn fanout_planner_cartesian_products_independent_correlated_groups() {
    let prompt = "%{a=A | b=B} %{x=X | y=Y} %{a=C} %{x=Z}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("a.x"), Some("a.y"), Some("b.x"), Some("b.y")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["A X C Z", "A Y C", "B X Z", "B Y"]
    );
}

#[test]
fn fanout_planner_correlated_group_mixes_named_and_unnamed_ids() {
    let prompt = "%{a=X | Y} %{a=Z}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("1")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["X Z", "Y"]
    );
}

#[test]
fn fanout_planner_rejects_repeated_top_level_models_with_alternatives() {
    let prompt = "%id:foo\n%model:opus\n%model:sonnet %alt(x,y)\nReview";

    let err = plan_agent_launch_fanout(prompt, Some("model")).unwrap_err();

    let message = err.to_string();
    assert!(
        message.contains("%model:opus ... %model:sonnet"),
        "message was {message:?}"
    );
    assert!(
        message.contains("use %{%m:opus | %m:sonnet} instead"),
        "message was {message:?}"
    );
}

#[test]
fn fanout_planner_splits_model_branches_and_alternatives() {
    let prompt = "%id:foo\n%{%m:opus | %m:sonnet} %alt(x,y)\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(plan.slots.len(), 4);
    assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
    assert_eq!(plan.slots[0].alt_id.as_deref(), Some("1.1"));
    assert!(plan.slots[0].prompt.contains("%m:opus x\nReview"));
    assert_eq!(plan.slots[3].model.as_deref(), Some("sonnet"));
    assert_eq!(plan.slots[3].alt_id.as_deref(), Some("2.2"));
    assert!(plan.slots[3].prompt.contains("%m:sonnet y\nReview"));
}

#[test]
fn extract_first_model_value_strips_known_effort_suffix() {
    // A trailing `@<known-effort>` is peeled off so the slot is named by
    // the clean model, mirroring the Python `split_model_effort` rule.
    assert_eq!(
        extract_first_model_value("%model:opus@xhigh do work"),
        Some("opus".to_string())
    );
    assert_eq!(
        extract_first_model_value("%m:codex/gpt-5.6-sol@low do work"),
        Some("codex/gpt-5.6-sol".to_string())
    );
    // No suffix → unchanged.
    assert_eq!(
        extract_first_model_value("%model:opus do work"),
        Some("opus".to_string())
    );
    // Unknown trailing token is not an effort level → left intact.
    assert_eq!(
        extract_first_model_value("%model:agy/flash@v2 do work"),
        Some("agy/flash@v2".to_string())
    );
    // Backtick-literal model values keep any `@` verbatim.
    assert_eq!(
        extract_first_model_value("%model:`agy/flash@xhigh` do work"),
        Some("agy/flash@xhigh".to_string())
    );
}

#[test]
fn fanout_planner_strips_branch_effort_for_slot_naming() {
    // Per-branch `@effort` fan-out: slots are named by the clean model
    // while each branch body retains its `@effort` token for the launched
    // agent's own directive parsing.
    let prompt = "%{%m:opus@xhigh | %m:sonnet@low} %alt(x,y)\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(plan.slots.len(), 4);
    assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
    assert!(plan.slots[0].prompt.contains("%m:opus@xhigh"));
    assert_eq!(plan.slots[3].model.as_deref(), Some("sonnet"));
    assert!(plan.slots[3].prompt.contains("%m:sonnet@low"));
}

#[test]
fn fanout_planner_model_alt_ids_preserve_named_model_branches() {
    let prompt = "%alt(opus=%model:opus,sonnet=%model:sonnet)\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
    assert_eq!(plan.slots[0].alt_id.as_deref(), Some("opus"));
    assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
    assert_eq!(plan.slots[1].alt_id.as_deref(), Some("sonnet"));
}

#[test]
fn fanout_planner_ignores_models_inside_adjacent_inline_code() {
    let prompt = "keep `foo`/`%m:wrong` then %{left=%m:opus | right=%m:sonnet}";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
    assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
    assert!(plan
        .slots
        .iter()
        .all(|slot| slot.prompt.contains("`%m:wrong`")));
}

#[test]
fn launch_inline_scanner_preserves_argument_parser_precedence() {
    let prompt = concat!(
        "#name:`arg with spaces` #research(compare `a` and `b`) ",
        "%model:`custom model` %wait(time=`5m`)"
    );

    assert!(launch_inline_literal_ranges(prompt).is_empty());
}

#[test]
fn fanout_planner_extracts_repeat_slots() {
    for prompt in [
        "%repeat:3 %id:task %model:opus do work",
        "%r:3 %i:task %model:opus do work",
    ] {
        let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

        assert_eq!(plan.launch_kind, "repeat");
        assert_eq!(plan.slots.len(), 3);
        assert_eq!(plan.slots[0].repeat_name.as_deref(), Some("task"));
        assert_eq!(plan.slots[0].prompt, "  %model:opus do work");
        assert!(!plan.slots[0].wait_for_previous);
        assert!(plan.slots[1].wait_for_previous);
    }
}

#[test]
fn fanout_planner_preserves_repeat_bead_association() {
    for prompt in [
        "%repeat:2 %id(task, bead=sase-8f.2) do work",
        "%r:2 %i(bead=sase-8f.2) do work",
    ] {
        let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

        assert_eq!(plan.slots.len(), 2);
        assert!(plan
            .slots
            .iter()
            .all(|slot| slot.bead_id.as_deref() == Some("sase-8f.2")));
        assert!(plan
            .slots
            .iter()
            .all(|slot| !slot.prompt.contains("bead=sase-8f.2")));
    }

    let named = plan_agent_launch_fanout(
        "%r:2 %id(task, clan=research, bead=`sase-8f.2`) do work",
        Some("repeat"),
    )
    .unwrap();
    assert_eq!(named.slots[0].repeat_name.as_deref(), Some("task"));
    assert_eq!(named.slots[0].bead_id.as_deref(), Some("sase-8f.2"));
}

#[test]
fn fanout_planner_preserves_repeat_and_id_inside_literal_zones() {
    let prompt = concat!(
        "%xprompts_enabled:false\n",
        "%r:9 %i:disabled\n",
        "%xprompts_enabled:true\n",
        "```text\n%repeat:8 %id:fenced\n```\n",
        "keep `foo`/`%r:7` and prefix`%i:inline`suffix ",
        "%r:2 %id:right work",
    );

    let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].repeat_name.as_deref(), Some("right"));
    assert!(plan.slots[0].prompt.contains("%r:9 %i:disabled"));
    assert!(plan.slots[0]
        .prompt
        .contains("```text\n%repeat:8 %id:fenced\n```"));
    assert!(plan.slots[0].prompt.contains("`%r:7`"));
    assert!(plan.slots[0].prompt.contains("`%i:inline`"));
}

#[test]
fn fanout_planner_does_not_support_removed_name_spellings() {
    let prompt = "%r:2 %name:legacy %n:short work";

    let plan = plan_agent_launch_fanout(prompt, Some("repeat")).unwrap();

    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].repeat_name, None);
    assert!(plan.slots[0].prompt.contains("%name:legacy"));
    assert!(plan.slots[0].prompt.contains("%n:short"));
    assert_eq!(canonical_directive_name("name"), "name");
    assert_eq!(canonical_directive_name("n"), "n");
}

#[test]
fn fanout_planner_brace_shorthand_splits_pipe_branches() {
    let prompt = "%{a | b | c}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(plan.launch_kind, "alternatives");
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["a\nReview", "b\nReview", "c\nReview"]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("1"), Some("2"), Some("3")]
    );
}

#[test]
fn fanout_planner_ignores_alternative_inside_adjacent_inline_code() {
    let prompt = "keep `foo`/`%{a | b}` then %{x | y}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].prompt, "keep `foo`/`%{a | b}` then x");
    assert_eq!(plan.slots[1].prompt, "keep `foo`/`%{a | b}` then y");
}

#[test]
fn fanout_planner_brace_branch_text_keeps_commas() {
    let prompt = "%{foo, bar | baz}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].prompt, "foo, bar");
    assert_eq!(plan.slots[1].prompt, "baz");
}

#[test]
fn fanout_planner_brace_named_and_numeric_branch_ids() {
    let prompt = "%{fast=a | b | 2=c | d}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("fast"), Some("1"), Some("2"), Some("3")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c", "d"]
    );
}

#[test]
fn fanout_planner_brace_named_text_blocks() {
    let prompt = "%{sec=[[security]] | perf=[[performance]]}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].alt_id.as_deref(), Some("sec"));
    assert_eq!(plan.slots[0].prompt, "security\nReview");
    assert_eq!(plan.slots[1].alt_id.as_deref(), Some("perf"));
    assert_eq!(plan.slots[1].prompt, "performance\nReview");
}

#[test]
fn fanout_planner_brace_single_branch_has_implicit_empty_variant() {
    let prompt = "before %{a} after";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].prompt, "before a after");
    assert_eq!(plan.slots[0].alt_id.as_deref(), Some("1"));
    assert_eq!(plan.slots[1].prompt, "before after");
    assert_eq!(plan.slots[1].alt_id.as_deref(), Some("2"));
}

#[test]
fn fanout_planner_empty_branch_removes_space_before_punctuation() {
    let prompt = "works %{extra}.";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["works extra.", "works."]
    );
}

#[test]
fn fanout_planner_empty_branch_collapses_between_words() {
    let prompt = "A %{extra} B";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["A extra B", "A B"]
    );
}

#[test]
fn fanout_planner_empty_branch_removes_leading_space() {
    let prompt = "%{extra} Review";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["extra Review", "Review"]
    );
}

#[test]
fn fanout_planner_empty_branch_removes_trailing_space() {
    let prompt = "Review %{extra}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["Review extra", "Review"]
    );
}

#[test]
fn render_alternative_prompt_empty_branch_does_not_invent_space() {
    let prompt = "A%B";
    let directives = vec![AlternativeDirective {
        start: 1,
        end: 2,
        args: Vec::new(),
    }];
    let combination = vec![AlternativeVariant {
        id: "empty".to_string(),
        replacements: vec![AlternativeReplacement {
            directive_index: 0,
            value: String::new(),
        }],
    }];

    assert_eq!(
        render_alternative_prompt(prompt, &directives, &combination),
        "AB"
    );
}

#[test]
fn fanout_planner_empty_branch_collapses_multiple_spaces() {
    let prompt = "A  %{extra}  B";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["A  extra  B", "A B"]
    );
}

#[test]
fn fanout_planner_empty_branch_preserves_newlines_and_indentation() {
    let prompt = "Header\n  %{extra}\n  Footer";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["Header\n  extra\n  Footer", "Header\n  \n  Footer"]
    );
}

#[test]
fn fanout_planner_empty_branch_preserves_following_directive_separator() {
    let prompt = "Do work. %{extra} %{%m:opus | %m:gpt-5.6-sol}";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.model.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("opus"),
            Some("gpt-5.6-sol"),
            Some("opus"),
            Some("gpt-5.6-sol")
        ]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec![
            "Do work. extra %m:opus",
            "Do work. extra %m:gpt-5.6-sol",
            "Do work. %m:opus",
            "Do work. %m:gpt-5.6-sol",
        ]
    );
}

#[test]
fn fanout_planner_brace_nested_pipes_do_not_split() {
    let prompt = "%{a (x | y) | b [c | d] | `e | f`}";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["a (x | y)", "b [c | d]", "e | f"]
    );
}

#[test]
fn fanout_planner_brace_composes_cartesian_with_paren_alt() {
    let prompt = "%{a | b} %alt(x,y)";

    let plan = plan_agent_launch_fanout(prompt, Some("alternatives")).unwrap();

    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["a x", "a y", "b x", "b y"]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("1.1"), Some("1.2"), Some("2.1"), Some("2.2")]
    );
}

#[test]
fn fanout_planner_brace_model_branches_match_paren_parity() {
    let prompt = "%{opus=%model:opus | sonnet=%model:sonnet}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(plan.slots.len(), 2);
    assert_eq!(plan.slots[0].model.as_deref(), Some("opus"));
    assert_eq!(plan.slots[0].alt_id.as_deref(), Some("opus"));
    assert_eq!(plan.slots[1].model.as_deref(), Some("sonnet"));
    assert_eq!(plan.slots[1].alt_id.as_deref(), Some("sonnet"));
}

#[test]
fn fanout_planner_brace_value_fanout_after_directive_colon() {
    let prompt = "%m:opus %effort:%{medium | high | xhigh}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.model.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("opus"), Some("opus"), Some("opus")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("1"), Some("2"), Some("3")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec![
            "%m:opus %effort:medium\nReview",
            "%m:opus %effort:high\nReview",
            "%m:opus %effort:xhigh\nReview",
        ]
    );
}

#[test]
fn fanout_planner_brace_value_fanout_after_effort_e_alias() {
    // `%e:%{...}` fans out exactly like `%effort:%{...}`; the alias prefix
    // is preserved verbatim in each slot body.
    let prompt = "%m:opus %e:%{medium | high | xhigh}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.model.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("opus"), Some("opus"), Some("opus")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("1"), Some("2"), Some("3")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec![
            "%m:opus %e:medium\nReview",
            "%m:opus %e:high\nReview",
            "%m:opus %e:xhigh\nReview",
        ]
    );
}

#[test]
fn fanout_planner_model_value_fanout_after_directive_colon() {
    let prompt = "%m:%{opus | sonnet}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.model.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("opus"), Some("sonnet")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec!["%m:opus\nReview", "%m:sonnet\nReview"]
    );
}

#[test]
fn fanout_planner_value_fanouts_compose_cartesian() {
    let prompt = "%m:%{opus | sonnet} %effort:%{medium | high}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.model.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("opus"), Some("opus"), Some("sonnet"), Some("sonnet")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("1.1"), Some("1.2"), Some("2.1"), Some("2.2")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.prompt.as_str())
            .collect::<Vec<_>>(),
        vec![
            "%m:opus %effort:medium\nReview",
            "%m:opus %effort:high\nReview",
            "%m:sonnet %effort:medium\nReview",
            "%m:sonnet %effort:high\nReview",
        ]
    );
}

#[test]
fn fanout_planner_rejects_repeated_models_with_brace_alternatives() {
    let prompt = "%model:opus\n%model:sonnet %{x | y}\nReview";

    let err = plan_agent_launch_fanout(prompt, Some("model")).unwrap_err();

    let message = err.to_string();
    assert!(
        message.contains("%model:opus ... %model:sonnet"),
        "message was {message:?}"
    );
    assert!(
        message.contains("use %{%m:opus | %m:sonnet} instead"),
        "message was {message:?}"
    );
}

#[test]
fn fanout_planner_rejects_paren_multi_model_directive() {
    let err = plan_agent_launch_fanout("%m(opus,sonnet) review", Some("model"))
        .unwrap_err();

    let message = err.to_string();
    assert!(
        message.contains("%m(opus,sonnet) is no longer supported"),
        "message was {message:?}"
    );
    assert!(
        message.contains("use %{%m:opus | %m:sonnet} instead"),
        "message was {message:?}"
    );
}

#[test]
fn fanout_planner_rejects_repeated_top_level_model_directives() {
    let err = plan_agent_launch_fanout(
        "%model:opus\n%model:sonnet\nreview",
        Some("model"),
    )
    .unwrap_err();

    let message = err.to_string();
    assert!(
        message.contains("%model:opus ... %model:sonnet"),
        "message was {message:?}"
    );
    assert!(
        message.contains("use %{%m:opus | %m:sonnet} instead"),
        "message was {message:?}"
    );
}

#[test]
fn fanout_planner_rejects_same_value_repeated_model_directives() {
    let err = plan_agent_launch_fanout(
        "%model:opus\n%model:opus\nreview",
        Some("model"),
    )
    .unwrap_err();

    let message = err.to_string();
    assert!(
        message.contains("use %{%m:opus | %m:opus} instead"),
        "message was {message:?}"
    );
}

#[test]
fn fanout_planner_single_top_level_model_is_single_launch() {
    for prompt in ["%m:opus review", "%model(opus) review"] {
        let plan = plan_agent_launch_fanout(prompt, Some("auto")).unwrap();

        assert_eq!(plan.launch_kind, "single");
        assert_eq!(plan.slots.len(), 1);
        assert_eq!(plan.slots[0].prompt, prompt);
    }
}

#[test]
fn fanout_planner_brace_model_branches_report_model_slots() {
    let prompt = "%{%m:opus | %m:sonnet}\nReview";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.model.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("opus"), Some("sonnet")]
    );
    assert_eq!(
        plan.slots
            .iter()
            .map(|slot| slot.alt_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("1"), Some("2")]
    );
}

#[test]
fn fanout_planner_unvalued_model_markers_do_not_count_as_repeated() {
    let prompt = "%model\n%model()\n%model+ %{x | y}";

    let plan = plan_agent_launch_fanout(prompt, Some("model")).unwrap();

    assert_eq!(plan.launch_kind, "model");
    assert_eq!(plan.slots.len(), 2);
}

#[test]
fn fanout_planner_unclosed_brace_reports_missing_close() {
    let err =
        plan_agent_launch_fanout("%{a | b", Some("alternatives")).unwrap_err();

    let message = err.to_string();
    assert!(message.contains("%{"), "message was {message:?}");
    assert!(message.contains('}'), "message was {message:?}");
}
