//! Subject-identity and classification tests over the shared corpus.

use super::subjects::{
    derive_subjects, memory_history_pathspecs, subject_path_aliases,
};
use super::wire::{MemoryHistorySubjectKindWire, MemoryHistorySubjectWire};
use crate::file_history::wire::FileChangeKindWire;
use crate::file_history::{build_index, FileHistoryBudgetWire};
use corpus::{build_project_corpus, Corpus, PROJECT_SCOPE_KEY};

mod cache_queries;
mod causes;
mod classify;
mod corpus;
mod feed;
mod review;

fn budget() -> FileHistoryBudgetWire {
    FileHistoryBudgetWire::default()
}

fn derive(corpus: &Corpus) -> Vec<MemoryHistorySubjectWire> {
    let specs = memory_history_pathspecs(&corpus.scope).unwrap();
    let index = build_index(&corpus.repo, &specs, &budget()).unwrap();
    derive_subjects(&index, &corpus.scope).unwrap()
}

fn subject_by_id<'a>(
    subjects: &'a [MemoryHistorySubjectWire],
    id: &str,
) -> &'a MemoryHistorySubjectWire {
    subjects
        .iter()
        .find(|subject| subject.id == id)
        .unwrap_or_else(|| {
            panic!("missing subject {id}; have {:?}", ids(subjects))
        })
}

fn ids(subjects: &[MemoryHistorySubjectWire]) -> Vec<&str> {
    subjects.iter().map(|subject| subject.id.as_str()).collect()
}

#[test]
fn subject_ids_after_full_history() {
    let corpus = build_project_corpus();
    let subjects = derive(&corpus);
    let key = PROJECT_SCOPE_KEY;
    subject_by_id(&subjects, &format!("note:{key}/lint_and_test"));
    subject_by_id(&subjects, &format!("web:{key}/glossary"));
    subject_by_id(&subjects, &format!("strand:{key}/glossary/bead"));
    subject_by_id(&subjects, &format!("instructions:{key}/."));
    subject_by_id(&subjects, &format!("asset:{key}/sase/memory/diagram.png"));

    // Ordinals run oldest-first over newest-first rows.
    for subject in &subjects {
        let total = subject.versions.len() as u64;
        for (index, version) in subject.versions.iter().enumerate() {
            assert_eq!(version.ordinal, total - index as u64, "{}", subject.id);
        }
    }
}

#[test]
fn historical_paths_alias_to_subjects() {
    let corpus = build_project_corpus();
    let subjects = derive(&corpus);
    let aliases = subject_path_aliases(&subjects);
    let key = PROJECT_SCOPE_KEY;
    let lint = format!("note:{key}/lint_and_test");
    assert_eq!(aliases.get("memory/build_and_run.md"), Some(&lint));
    assert_eq!(aliases.get("sase/memory/build_and_run.md"), Some(&lint));
    assert_eq!(aliases.get("sase/memory/lint_and_test.md"), Some(&lint));
    let bead = format!("strand:{key}/glossary/bead");
    assert_eq!(aliases.get("sase/memory/glossary/stitch.md"), Some(&bead));
    assert_eq!(aliases.get("sase/memory/glossary/bead.md"), Some(&bead));

    // The directory move does not split the subject.
    let lint_subject = subject_by_id(&subjects, &lint);
    assert!(lint_subject
        .paths
        .contains(&"memory/build_and_run.md".to_string()));
    assert!(lint_subject
        .paths
        .contains(&"sase/memory/build_and_run.md".to_string()));
    assert!(lint_subject
        .paths
        .contains(&"sase/memory/lint_and_test.md".to_string()));

    // The content rename keeps rename similarity in range.
    let renamed = lint_subject
        .versions
        .iter()
        .find(|version| version.commit == corpus.commits["content_rename"])
        .unwrap();
    assert_eq!(renamed.kind, FileChangeKindWire::Moved);
    let similarity = renamed.similarity.unwrap();
    assert!(
        (55..=75).contains(&similarity),
        "similarity around 63, got {similarity}"
    );
}

#[test]
fn delete_and_recreate_keeps_subject_with_gap() {
    let corpus = build_project_corpus();
    let subjects = derive(&corpus);
    let key = PROJECT_SCOPE_KEY;
    let scratch = subject_by_id(&subjects, &format!("note:{key}/scratch"));
    assert_eq!(scratch.kind, MemoryHistorySubjectKindWire::Note);
    let newest = &scratch.versions[0];
    assert_eq!(newest.kind, FileChangeKindWire::Created);
    assert!(newest.gap_before);
    assert_eq!(newest.commit, corpus.commits["recreate_scratch"]);

    // The stays-deleted note keeps its subject with a deleted tip.
    let gone = subject_by_id(&subjects, &format!("note:{key}/gone"));
    assert_eq!(gone.versions[0].kind, FileChangeKindWire::Deleted);
    assert_eq!(gone.versions[0].commit, corpus.commits["gone_delete"]);
}

#[test]
fn shim_convergence_aliases_without_a_row() {
    let corpus = build_project_corpus();
    let subjects = derive(&corpus);
    let key = PROJECT_SCOPE_KEY;
    let instructions =
        subject_by_id(&subjects, &format!("instructions:{key}/."));

    // Five commits touch AGENTS.md; the convergence commit aliases
    // instead of adding a row, and two diverged rows remain.
    assert_eq!(instructions.diverged_count, 2);
    assert_eq!(instructions.versions.len(), 7);
    let diverged: Vec<_> = instructions
        .versions
        .iter()
        .filter(|version| version.diverged)
        .collect();
    assert_eq!(diverged.len(), 2);
    assert_eq!(diverged[0].source_path, "CLAUDE.md");
    assert_eq!(diverged[0].commit, corpus.commits["shim_diverge"]);
    assert_eq!(diverged[1].commit, corpus.commits["init"]);

    let aliased: Vec<_> = instructions
        .versions
        .iter()
        .filter(|version| {
            version.aliased_paths.contains(&"CLAUDE.md".to_string())
        })
        .collect();
    assert_eq!(aliased.len(), 1);
    assert_eq!(aliased[0].commit, corpus.commits["regen_only"]);
    assert!(!aliased[0].diverged);

    // The shim path is not its own subject.
    assert!(
        subjects
            .iter()
            .all(|subject| subject.id != format!("note:{key}/CLAUDE")),
        "have {:?}",
        ids(&subjects)
    );
}

#[test]
fn asset_generated_and_managed_flags() {
    let corpus = build_project_corpus();
    let subjects = derive(&corpus);
    let key = PROJECT_SCOPE_KEY;

    let asset = subject_by_id(
        &subjects,
        &format!("asset:{key}/sase/memory/diagram.png"),
    );
    assert_eq!(asset.kind, MemoryHistorySubjectKindWire::Asset);
    assert_eq!(asset.display_name, "diagram.png");

    let roster = subject_by_id(&subjects, &format!("note:{key}/roster"));
    assert!(roster.generated);
    for subject in &subjects {
        if subject.id != roster.id {
            assert!(!subject.generated, "{}", subject.id);
        }
        if subject.kind == MemoryHistorySubjectKindWire::Instructions {
            assert!(subject.managed, "{}", subject.id);
        } else {
            assert!(!subject.managed, "{}", subject.id);
        }
        assert!(!subject.template, "{}", subject.id);
    }

    let instructions =
        subject_by_id(&subjects, &format!("instructions:{key}/."));
    assert_eq!(instructions.display_name, "AGENTS.md");
}

#[test]
fn pathspecs_are_sorted_deduped_and_validated() {
    let corpus = build_project_corpus();
    let specs = memory_history_pathspecs(&corpus.scope).unwrap();
    assert_eq!(
        specs,
        vec!["AGENTS.md", "CLAUDE.md", "memory", "sase/memory"]
    );

    let mut bad = corpus.scope.clone();
    bad.memory_roots = vec!["a/../b".to_string()];
    assert!(memory_history_pathspecs(&bad).is_err());
}
