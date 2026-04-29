//! Direct-parser benchmark for `sase_core::parse_project_bytes`.
//!
//! Phase 1E (sase-16.5) of `sase_100/plans/202604/rust_backend_phase1.md`.
//!
//! This is intentionally a `cargo run --example` rather than a
//! `criterion` bench so the workspace stays free of heavyweight bench
//! deps. The harness measures only the pure Rust parser — the
//! `bench_core_parse.py` benchmark in `sase_100/tests/perf/` covers the
//! end-to-end PyO3 conversion cost on the Python side.
//!
//! Workloads:
//!
//! - `golden_myproj`: the committed
//!   `crates/sase_core/tests/fixtures/myproj.gp` (mirrors the Python
//!   golden corpus).
//! - `synthetic_<N>`: a generated multi-spec file (defaults to N=200)
//!   that stretches the parser past the noise floor.
//!
//! Usage:
//!
//! ```bash
//! cargo run --release --example bench_parse
//! cargo run --release --example bench_parse -- --num-specs 1000 --runs 30
//! cargo run --release --example bench_parse -- --json
//! ```

use std::time::Instant;

const GOLDEN_PRIMARY: &str = include_str!("../tests/fixtures/myproj.gp");

fn build_synthetic(num_specs: usize) -> String {
    let mut out = String::with_capacity(num_specs * 600);
    for i in 0..num_specs {
        if i > 0 {
            out.push_str("\n\n");
        }
        out.push_str(&format!(
            "## ChangeSpec\n\
             NAME: spec_{i}\n\
             DESCRIPTION:\n  Synthetic spec number {i}.\n  Generated for the Phase 1E benchmark.\n\
             PARENT:\n\
             PR: https://example.test/repo/pull/{i}\n\
             BUG: BUG-{i}\n\
             STATUS: WIP\n\
             TEST TARGETS: tests/test_spec_{i}.py\n\
             KICKSTART:\n  Kick this off carefully.\n\
             COMMITS:\n  (1) [run] Initial Commit {i}\n      | CHAT: ~/.sase/chats/spec_{i}.md (0s)\n      | DIFF: ~/.sase/diffs/spec_{i}.diff\n\
             HOOKS:\n  just lint\n      | (1) [260101_120000] PASSED (3s)\n\
             COMMENTS:\n  [critique] ~/.sase/comments/spec_{i}.json - (1)\n\
             MENTORS:\n  (1) profileA[1/1]\n      | [260101_130000] profileA:mentor1 - PASSED - (1m0s)\n\
             TIMESTAMPS:\n  260101_120000 STATUS WIP -> Ready\n\
             DELTAS:\n  + src/spec_{i}.rs\n",
        ));
    }
    out
}

#[derive(Debug)]
struct Summary {
    label: String,
    size_bytes: usize,
    runs: usize,
    min_us: f64,
    median_us: f64,
    p95_us: f64,
    max_us: f64,
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let idx = (((n - 1) as f64) * pct).round() as usize;
    sorted[idx.min(n - 1)]
}

fn time_bytes(label: &str, data: &[u8], runs: usize, warmup: usize) -> Summary {
    for _ in 0..warmup {
        let _ = sase_core::parse_project_bytes("bench.gp", data)
            .expect("parser failure during warmup");
    }
    let mut samples_us: Vec<f64> = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        let _ = sase_core::parse_project_bytes("bench.gp", data)
            .expect("parser failure during measured run");
        samples_us.push(start.elapsed().as_secs_f64() * 1_000_000.0);
    }
    samples_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = if samples_us.is_empty() {
        0.0
    } else {
        let mid = samples_us.len() / 2;
        if samples_us.len() % 2 == 0 {
            (samples_us[mid - 1] + samples_us[mid]) / 2.0
        } else {
            samples_us[mid]
        }
    };
    Summary {
        label: label.to_string(),
        size_bytes: data.len(),
        runs,
        min_us: *samples_us.first().unwrap_or(&0.0),
        median_us: median,
        p95_us: percentile(&samples_us, 0.95),
        max_us: *samples_us.last().unwrap_or(&0.0),
    }
}

fn print_human(rows: &[Summary]) {
    println!();
    println!(
        "{:<22} {:>10} {:>6} {:>10} {:>12} {:>10} {:>10}",
        "label", "bytes", "runs", "min_us", "median_us", "p95_us", "max_us"
    );
    println!("{}", "-".repeat(90));
    for r in rows {
        println!(
            "{:<22} {:>10} {:>6} {:>10.2} {:>12.2} {:>10.2} {:>10.2}",
            r.label,
            r.size_bytes,
            r.runs,
            r.min_us,
            r.median_us,
            r.p95_us,
            r.max_us
        );
    }
}

fn print_json(rows: &[Summary]) {
    print!("[");
    for (i, r) in rows.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!(
            "{{\"label\":\"{}\",\"size_bytes\":{},\"runs\":{},\"min_us\":{:.3},\"median_us\":{:.3},\"p95_us\":{:.3},\"max_us\":{:.3}}}",
            r.label, r.size_bytes, r.runs, r.min_us, r.median_us, r.p95_us, r.max_us
        );
    }
    println!("]");
}

struct Args {
    runs: usize,
    warmup: usize,
    num_specs: usize,
    json: bool,
}

fn parse_args() -> Args {
    let mut runs = 50_usize;
    let mut warmup = 5_usize;
    let mut num_specs = 200_usize;
    let mut json = false;
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--runs" => {
                runs = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .expect("--runs needs an integer");
            }
            "--warmup" => {
                warmup = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .expect("--warmup needs an integer");
            }
            "--num-specs" => {
                num_specs = iter
                    .next()
                    .and_then(|v| v.parse().ok())
                    .expect("--num-specs needs an integer");
            }
            "--json" => {
                json = true;
            }
            "-h" | "--help" => {
                println!(
                    "Usage: cargo run --release --example bench_parse -- \
                     [--runs N] [--warmup N] [--num-specs N] [--json]"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    Args {
        runs,
        warmup,
        num_specs,
        json,
    }
}

fn main() {
    let args = parse_args();
    let synthetic = build_synthetic(args.num_specs);
    let rows = vec![
        time_bytes(
            "golden_myproj",
            GOLDEN_PRIMARY.as_bytes(),
            args.runs,
            args.warmup,
        ),
        time_bytes(
            &format!("synthetic_{}_specs", args.num_specs),
            synthetic.as_bytes(),
            args.runs,
            args.warmup,
        ),
    ];
    if args.json {
        print_json(&rows);
    } else {
        print_human(&rows);
    }
}
