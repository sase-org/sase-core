//! Changeset and merged-feed tests over the shared fixture corpus.

use super::super::causes::attribute_instruction_causes;
use super::super::classify::classify_subjects;
use super::super::feed::{build_changesets, build_feed, FeedScope};
use super::super::subjects::{derive_subjects, memory_history_pathspecs};
use super::super::wire::{
    MemoryHistoryChangesetWire, MemoryHistoryClassWire,
    MemoryHistorySubjectWire, MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};
use super::corpus::{
    build_home_corpus, build_project_corpus, corpus_time, Corpus,
    HOME_SCOPE_KEY, PROJECT_SCOPE_KEY,
};
use crate::file_history::{build_index, FileHistoryBudgetWire};

fn pipeline(corpus: &Corpus) -> Vec<MemoryHistorySubjectWire> {
    let specs = memory_history_pathspecs(&corpus.scope).unwrap();
    let index =
        build_index(&corpus.repo, &specs, &FileHistoryBudgetWire::default())
            .unwrap();
    let mut subjects = derive_subjects(&index, &corpus.scope).unwrap();
    classify_subjects(&corpus.repo, &index, &mut subjects, &corpus.scope)
        .unwrap();
    attribute_instruction_causes(&corpus.repo, &mut subjects, &corpus.scope)
        .unwrap();
    subjects
}

fn key_id(kind: &str, name: &str) -> String {
    format!("{kind}:{PROJECT_SCOPE_KEY}/{name}")
}

fn changeset_at<'a>(
    changesets: &'a [MemoryHistoryChangesetWire],
    commit: &str,
) -> &'a MemoryHistoryChangesetWire {
    changesets
        .iter()
        .find(|changeset| changeset.commit == commit)
        .unwrap_or_else(|| panic!("no changeset at {commit}"))
}

fn project_feed_scopes<'a>(
    corpus: &'a Corpus,
    subjects: &'a [MemoryHistorySubjectWire],
) -> Vec<FeedScope<'a>> {
    vec![FeedScope {
        scope_key: corpus.scope.scope_key.as_str(),
        subjects,
    }]
}

#[test]
fn co_rendered_changeset_splits_authored_and_consequence() {
    let corpus = build_project_corpus();
    let subjects = pipeline(&corpus);
    let changesets = build_changesets(PROJECT_SCOPE_KEY, &subjects);

    let changeset = changeset_at(&changesets, &corpus.commits["co_render"]);
    assert!(!changeset.regen_only);
    assert!(!changeset.boilerplate);
    assert_eq!(changeset.authored.len(), 1);
    assert_eq!(
        changeset.authored[0].subject_id,
        key_id("note", "lint_and_test")
    );
    assert_eq!(
        changeset.authored[0].class,
        MemoryHistoryClassWire::Authored
    );
    assert_eq!(changeset.consequences.len(), 1);
    assert_eq!(
        changeset.consequences[0].subject_id,
        key_id("instructions", ".")
    );
    assert_eq!(
        changeset.consequences[0].class,
        MemoryHistoryClassWire::Rendered
    );
}

#[test]
fn roster_add_keeps_note_authored_and_roster_consequence() {
    let corpus = build_project_corpus();
    let subjects = pipeline(&corpus);
    let changesets = build_changesets(PROJECT_SCOPE_KEY, &subjects);

    let changeset = changeset_at(&changesets, &corpus.commits["roster_add"]);
    assert!(!changeset.regen_only);
    assert_eq!(changeset.authored.len(), 1);
    assert_eq!(
        changeset.authored[0].subject_id,
        key_id("note", "lint_and_test")
    );
    assert_eq!(
        changeset.authored[0].class,
        MemoryHistoryClassWire::Authored
    );
    assert_eq!(changeset.consequences.len(), 1);
    assert_eq!(
        changeset.consequences[0].subject_id,
        key_id("note", "roster")
    );
    // A created generated note folds by role, not by class.
    assert_eq!(
        changeset.consequences[0].class,
        MemoryHistoryClassWire::Created
    );
}

#[test]
fn roster_only_edit_folds_as_regen_only() {
    let corpus = build_project_corpus();
    let subjects = pipeline(&corpus);
    let changesets = build_changesets(PROJECT_SCOPE_KEY, &subjects);

    let changeset = changeset_at(&changesets, &corpus.commits["roster_edit"]);
    assert!(changeset.authored.is_empty());
    assert_eq!(changeset.consequences.len(), 1);
    assert_eq!(
        changeset.consequences[0].class,
        MemoryHistoryClassWire::Regenerated
    );
    assert!(changeset.regen_only);
}

#[test]
fn regen_only_agents_commit_is_regen_only() {
    let corpus = build_project_corpus();
    let subjects = pipeline(&corpus);
    let changesets = build_changesets(PROJECT_SCOPE_KEY, &subjects);

    let changeset = changeset_at(&changesets, &corpus.commits["regen_only"]);
    assert!(changeset.authored.is_empty());
    assert_eq!(changeset.consequences.len(), 1);
    assert_eq!(
        changeset.consequences[0].class,
        MemoryHistoryClassWire::RegenOnly
    );
    assert!(changeset.regen_only);

    // Omitted from the default feed, present with `include_hidden`.
    let scopes = project_feed_scopes(&corpus, &subjects);
    let default = build_feed(&scopes, None, None, false);
    assert!(
        default
            .changesets
            .iter()
            .all(|entry| entry.commit != corpus.commits["regen_only"]),
        "regen_only leaks into the default feed"
    );
    let with_hidden = build_feed(&scopes, None, None, true);
    let kept =
        changeset_at(&with_hidden.changesets, &corpus.commits["regen_only"]);
    assert!(kept.regen_only);
}

#[test]
fn init_boilerplate_is_flagged_and_kept_in_default_feed() {
    let corpus = build_project_corpus();
    let subjects = pipeline(&corpus);
    let scopes = project_feed_scopes(&corpus, &subjects);

    let default = build_feed(&scopes, None, None, false);
    let init = changeset_at(&default.changesets, &corpus.commits["init"]);
    assert!(init.boilerplate);
    assert!(!init.authored.is_empty());
    assert!(!init.regen_only);
}

#[test]
fn hidden_versions_leave_default_feed_and_return_with_hidden() {
    let corpus = build_project_corpus();
    let subjects = pipeline(&corpus);
    let scopes = project_feed_scopes(&corpus, &subjects);

    let default = build_feed(&scopes, None, None, false);
    assert!(default.hidden_changeset_count > 0);
    for label in ["reflow", "dirmove", "regen_only"] {
        assert!(
            default
                .changesets
                .iter()
                .all(|entry| entry.commit != corpus.commits[label]),
            "{label} leaks into the default feed"
        );
    }

    let with_hidden = build_feed(&scopes, None, None, true);
    assert_eq!(with_hidden.hidden_changeset_count, 0);
    for label in ["reflow", "dirmove", "regen_only"] {
        assert!(
            with_hidden
                .changesets
                .iter()
                .any(|entry| entry.commit == corpus.commits[label]),
            "{label} missing with include_hidden"
        );
    }
}

#[test]
fn merge_orders_home_between_project_commits() {
    let project = build_project_corpus();
    let project_subjects = pipeline(&project);
    // Between the co-render (index 12) and config (index 13) commits.
    let home = build_home_corpus(corpus_time(12) + 150);
    let home_subjects = pipeline(&home);
    let scopes = vec![
        FeedScope {
            scope_key: project.scope.scope_key.as_str(),
            subjects: &project_subjects,
        },
        FeedScope {
            scope_key: home.scope.scope_key.as_str(),
            subjects: &home_subjects,
        },
    ];

    let feed = build_feed(&scopes, None, None, true);
    assert_eq!(feed.schema_version, MEMORY_HISTORY_WIRE_SCHEMA_VERSION);
    let position = |commit: &str| {
        feed.changesets
            .iter()
            .position(|entry| entry.commit == commit)
            .unwrap_or_else(|| panic!("no changeset at {commit}"))
    };
    let config = position(&project.commits["config"]);
    let home_note = position(&home.commits["note"]);
    let co_render = position(&project.commits["co_render"]);
    assert!(
        config < home_note && home_note < co_render,
        "newest-first order is config, home, co_render"
    );
    assert_eq!(feed.changesets[home_note].scope_key, HOME_SCOPE_KEY);
}

#[test]
fn since_and_limit_cut_the_merged_list() {
    let project = build_project_corpus();
    let project_subjects = pipeline(&project);
    let home = build_home_corpus(corpus_time(12) + 150);
    let home_subjects = pipeline(&home);
    let scopes = vec![
        FeedScope {
            scope_key: project.scope.scope_key.as_str(),
            subjects: &project_subjects,
        },
        FeedScope {
            scope_key: home.scope.scope_key.as_str(),
            subjects: &home_subjects,
        },
    ];

    // `since` is an inclusive lower bound: config and newer survive,
    // the home note and the co-render do not.
    let cut = build_feed(&scopes, Some(corpus_time(13)), None, true);
    assert!(cut
        .changesets
        .iter()
        .any(|entry| entry.commit == project.commits["config"]));
    assert!(cut
        .changesets
        .iter()
        .all(|entry| entry.commit != home.commits["note"]));
    assert!(cut
        .changesets
        .iter()
        .all(|entry| entry.commit != project.commits["co_render"]));

    // `limit` applies after filtering: the single newest changeset.
    let one = build_feed(&scopes, None, Some(1), true);
    assert_eq!(one.changesets.len(), 1);
    assert_eq!(one.changesets[0].commit, project.commits["gone_delete"]);
}
