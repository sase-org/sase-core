//! Cross-module prediction flows: compile once, predict, rank, continue.

use std::sync::Arc;

use crate::prompt_prediction::corpus::compile_prompt_prediction_corpus;
use crate::prompt_prediction::model::PromptPredictionModel;
use crate::prompt_prediction::wire::{
    PromptPredictionCorpusOptionsWire, PromptPredictionModelConfigWire,
    PromptPredictionRequestWire, PromptPredictionRowWire,
    PromptPredictionSourceRole, PromptPrefixRankRequestWire,
    PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
};

fn typed(text: &str, epoch: i64) -> PromptPredictionRowWire {
    PromptPredictionRowWire {
        text: text.to_string(),
        epoch_seconds: epoch,
        project: None,
        origin: Some("typed".to_string()),
        cancelled: false,
    }
}

fn history_model(texts: &[&str]) -> PromptPredictionModel {
    let rows: Vec<PromptPredictionRowWire> = texts
        .iter()
        .enumerate()
        .map(|(i, text)| typed(text, 100 + i as i64))
        .collect();
    let corpus = compile_prompt_prediction_corpus(
        &rows,
        &PromptPredictionCorpusOptionsWire {
            now_epoch: 1_000,
            ..Default::default()
        },
    );
    PromptPredictionModel::new(
        vec![(Arc::new(corpus), PromptPredictionSourceRole::History, 1.0)],
        PromptPredictionModelConfigWire::default(),
    )
}

fn request(text: &str) -> PromptPredictionRequestWire {
    PromptPredictionRequestWire {
        schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
        text_before_cursor: text.to_string(),
        project: None,
        limit: 5,
        max_words: 4,
        confidence: "balanced".to_string(),
        include_draft: false,
    }
}

#[test]
fn chain_runs_on_formulaic_prompts() {
    let model = history_model(&[
        "can you help me implement it now",
        "can you help me implement it today",
        "can you help me implement it fast",
        "can you help me implement it well",
    ]);
    let result = model.predict(&request("can you help me"));
    assert!(result.confident, "candidates={:?}", result.candidates);
    assert!(
        result.ghost.len() >= 2,
        "formulaic prompts chain: {:?}",
        result.ghost
    );
    assert_eq!(result.ghost[0], "implement");
}

#[test]
fn ghost_words_come_from_gated_continuation() {
    let model = history_model(&[
        "deploy the release to production now",
        "deploy the release to production today",
        "deploy the release to production fast",
        "deploy the release to production soon",
    ]);
    let result = model.predict(&request("deploy the release"));
    assert!(result.confident);
    assert_eq!(result.ghost[0], "to");
    // Continuation previews ride along on menu candidates.
    assert!(result
        .candidates
        .iter()
        .all(|cand| cand.continuation.len() <= 3));
}

#[test]
fn eager_covers_more_than_cautious() {
    let texts = [
        "help me implement it",
        "help me implement it now",
        "help me review it",
    ];
    let model = history_model(&texts);
    let mut cautious = request("help me");
    cautious.confidence = "cautious".to_string();
    let mut eager = request("help me");
    eager.confidence = "eager".to_string();
    let cautious_result = model.predict(&cautious);
    let eager_result = model.predict(&eager);
    // Eager gates at least as often as cautious.
    assert!(
        eager_result.confident || !cautious_result.confident,
        "cautious={} eager={}",
        cautious_result.confident,
        eager_result.confident
    );
}

#[test]
fn rank_prefix_serves_current_word_completion() {
    let model = history_model(&[
        "help me implement it",
        "help me implement it now",
        "help me implement it today",
        "help me implement it fast",
    ]);
    let result = model.rank_prefix(&PromptPrefixRankRequestWire {
        schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
        text_before_word: "help me ".to_string(),
        prefix: "impl".to_string(),
        project: None,
        limit: 5,
    });
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].key, "implement");
    assert_eq!(result.context_words, vec!["help", "me"]);
}

#[test]
fn structured_contexts_never_offer() {
    let model =
        history_model(&["help me implement it", "help me implement it now"]);
    let result = model.predict(&request("#gh:sase"));
    assert!(result.blocked_reason.is_some());
    assert!(!result.confident);
}

/// Performance budgets (§5.4): local compile ≤ 50 ms per 1k prompts,
/// local-only predict p95 ≤ 0.5 ms, local corpus ≤ 5 MB.
///
/// Budgets are release budgets (the shipped wheel never runs debug
/// codegen): run with
/// `cargo test --release -p sase_core prompt_prediction -- --ignored`.
#[test]
#[ignore]
fn performance_budgets_on_synthetic_10k_corpus() {
    let subjects = [
        "help",
        "review",
        "fix",
        "implement",
        "test",
        "deploy",
        "refactor",
        "document",
    ];
    let objects = [
        "parser", "cache", "pipeline", "config", "runner", "index", "snapshot",
        "gateway",
    ];
    let mut rows = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        let subject = subjects[i % subjects.len()];
        let object = objects[(i / subjects.len()) % objects.len()];
        rows.push(PromptPredictionRowWire {
            text: format!("can you {subject} the {object} now please {i}",),
            epoch_seconds: 1_000 + i as i64,
            project: None,
            origin: Some("typed".to_string()),
            cancelled: false,
        });
    }
    let started = std::time::Instant::now();
    let corpus = compile_prompt_prediction_corpus(
        &rows,
        &PromptPredictionCorpusOptionsWire {
            now_epoch: 20_000,
            ..Default::default()
        },
    );
    let compile_ms = started.elapsed().as_secs_f64() * 1000.0;
    let stats = corpus.stats();
    let model = PromptPredictionModel::new(
        vec![(Arc::new(corpus), PromptPredictionSourceRole::History, 1.0)],
        PromptPredictionModelConfigWire::default(),
    );
    let queries = [
        "can you help",
        "can you review the",
        "please fix the cache",
        "can you deploy",
        "refactor the",
    ];
    let mut latencies: Vec<f64> = Vec::new();
    for round in 0..40 {
        for query in &queries {
            let request = PromptPredictionRequestWire {
                schema_version: PROMPT_PREDICTION_WIRE_SCHEMA_VERSION,
                text_before_cursor: query.to_string(),
                project: None,
                limit: 5,
                max_words: 4,
                confidence: "balanced".to_string(),
                include_draft: false,
            };
            let query_started = std::time::Instant::now();
            let _ = model.predict(&request);
            latencies.push(query_started.elapsed().as_secs_f64() * 1000.0);
            let _ = round;
        }
    }
    latencies
        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p95 = latencies[(latencies.len() * 95 / 100).min(latencies.len() - 1)];
    println!(
        "prompt_prediction perf: compile_ms={compile_ms:.1} \
         (budget 500) predict_p95_ms={p95:.3} (budget 0.5) \
         approx_bytes={} (budget 5242880) rows_used={}",
        stats.approx_bytes, stats.rows_used,
    );
    assert!(
        compile_ms <= 500.0,
        "compile {compile_ms:.1} ms over 500 ms budget"
    );
    assert!(p95 <= 0.5, "predict p95 {p95:.3} ms over 0.5 ms budget");
    assert!(
        stats.approx_bytes <= 5 * 1024 * 1024,
        "corpus {} bytes over 5 MB budget",
        stats.approx_bytes
    );
}

#[test]
fn empty_model_is_silent() {
    let model = history_model(&[]);
    let result = model.predict(&request("help me"));
    assert!(!result.confident);
    assert!(result.ghost.is_empty());
    assert!(result.candidates.is_empty());
    assert!(result.blocked_reason.is_none());
}
