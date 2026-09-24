use std::time::Instant;

use sase_core::command_line::{CommandLineGrammar, DynamicCandidateWire};

fn real_grammar() -> CommandLineGrammar {
    let text = include_str!("fixtures/command_line/sase_spec.json");
    CommandLineGrammar::from_json(text).expect("real spec loads")
}

fn representative_lines() -> Vec<String> {
    let bases = [
        "bead cl",
        "bead close sase-1 --reason ",
        "bead close --res",
        "bead close --resolution=d",
        "bead list -s ",
        "bead list -s bogus",
        "bead note sase-1 --edit x --",
        "proc run -c /tmp -- ls -la",
        "sase proc run ls -la",
        "agent",
        "agent ",
        "run",
        "run .",
        "run \"fix it\"",
        "tui",
        "bead close \"sase-1",
        "nosuch cmd",
        "bead close ",
        "bead close sase-1 extra",
        "mini --req x a b",
    ];
    let mut lines = Vec::new();
    for i in 0..200 {
        let base = bases[i % bases.len()];
        if i % 3 == 0 {
            lines.push(format!("{base} extra-{i}"));
        } else {
            lines.push((*base).to_string());
        }
    }
    lines
}

fn percentile(mut values: Vec<f64>, pct: f64) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if values.is_empty() {
        return 0.0;
    }
    let index = ((pct / 100.0) * (values.len() as f64 - 1.0)).round() as usize;
    values[index.min(values.len() - 1)]
}

#[test]
fn command_line_perf_budgets() {
    let grammar = real_grammar();
    let lines = representative_lines();

    let mut resolve_samples = Vec::new();
    for _ in 0..5 {
        for line in &lines {
            let cursor = line.chars().count();
            let start = Instant::now();
            let _ = grammar.resolve(line, cursor);
            resolve_samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
    }
    let resolve_p95 = percentile(resolve_samples, 95.0);

    let dynamic: Vec<DynamicCandidateWire> = (0..1000)
        .map(|i| DynamicCandidateWire {
            value: format!("candidate-{i:04}"),
            display: None,
            description: None,
            badge: None,
            source: None,
            partial: None,
        })
        .collect();
    let mut complete_samples = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let _ = grammar.complete(
            "bead list -s ",
            "bead list -s ".chars().count(),
            &dynamic,
            &[],
            100,
        );
        complete_samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let complete_p95 = percentile(complete_samples, 95.0);

    eprintln!("command_line resolve p95: {resolve_p95:.3} ms");
    eprintln!("command_line complete p95: {complete_p95:.3} ms");

    #[cfg(not(debug_assertions))]
    {
        assert!(
            resolve_p95 < 1.0,
            "resolve p95 {resolve_p95:.3} ms exceeds 1 ms"
        );
        assert!(
            complete_p95 < 2.0,
            "complete p95 {complete_p95:.3} ms exceeds 2 ms"
        );
    }
    #[cfg(debug_assertions)]
    {
        assert!(
            resolve_p95 < 25.0,
            "resolve p95 {resolve_p95:.3} ms exceeds 25 ms debug ceiling"
        );
        assert!(
            complete_p95 < 50.0,
            "complete p95 {complete_p95:.3} ms exceeds 50 ms debug ceiling"
        );
    }
}
