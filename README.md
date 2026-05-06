# sase-core

Rust core for the [sase](https://github.com/sase-org/sase) ChangeSpec backend.

This repo is the eventual home of the Rust ChangeSpec parser, query engine, graph index, and bead data backend. The
Python `sase_100` repo remains the product shell; this crate owns deterministic core data operations as they are ported.

## Layout

```
crates/
  sase_core/      # pure-Rust core: wire types + full-file parser
  sase_core_py/   # PyO3 binding placeholder (filled in in Phase 1D)
  sase_gateway/   # local host HTTP gateway for SASE mobile clients
```

The pure crate has no PyO3 dependency. This is deliberate — later UniFFI, WASM, or server crates need to consume
`sase_core` without dragging a Python toolchain into their build.

`sase_gateway` is also pure Rust. It owns the mobile gateway HTTP wire contract and server skeleton without depending on
the Python binding crate.

## Build & test

```bash
cargo fmt --all
cargo fmt --all -- --check   # CI gate
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run --release --example bench_parse   # direct-parser benchmark
```

`rust-toolchain.toml` pins the `stable` channel and installs `rustfmt` and `clippy`. `Cargo.lock` is committed so the
workspace builds reproducibly.

The companion `sase_100` repo ships matching `just` targets so a contributor can drive both repos from one tree:

```bash
just rust-install     # maturin develop --release into .venv
just rust-test        # cargo test --workspace
just rust-fmt-check   # cargo fmt --all -- --check
just rust-clippy      # warnings-as-errors
just rust-bench       # cargo run --release --example bench_parse
just rust-check       # fmt-check + clippy + tests
just bench-core       # Python-side end-to-end benchmark
```

All `rust-*` targets short-circuit with a friendly message when `../sase-core` is not present, so a pure-Python
`just install`/`just check` flow is unaffected.

## Wire contract

`crates/sase_core/src/wire.rs` mirrors `sase_100/src/sase/core/wire.py`:

| Rust type              | Python dataclass       |
| ---------------------- | ---------------------- |
| `SourceSpanWire`       | `SourceSpanWire`       |
| `CommitWire`           | `CommitWire`           |
| `HookStatusLineWire`   | `HookStatusLineWire`   |
| `HookWire`             | `HookWire`             |
| `CommentWire`          | `CommentWire`          |
| `MentorStatusLineWire` | `MentorStatusLineWire` |
| `MentorWire`           | `MentorWire`           |
| `TimestampWire`        | `TimestampWire`        |
| `DeltaWire`            | `DeltaWire`            |
| `ChangeSpecWire`       | `ChangeSpecWire`       |
| `ParseErrorWire`       | `ParseErrorWire`       |

`crates/sase_core/src/agent_scan/wire.rs` mirrors
`sase_100/src/sase/core/agent_scan_wire.py` (Phase 3B):

| Rust type                       | Python dataclass                |
| ------------------------------- | ------------------------------- |
| `AgentArtifactScanOptionsWire`  | `AgentArtifactScanOptionsWire`  |
| `AgentArtifactScanStatsWire`    | `AgentArtifactScanStatsWire`    |
| `DoneMarkerWire`                | `DoneMarkerWire`                |
| `AgentMetaWire`                 | `AgentMetaWire`                 |
| `RunningMarkerWire`             | `RunningMarkerWire`             |
| `WaitingMarkerWire`             | `WaitingMarkerWire`             |
| `WorkflowStateWire`             | `WorkflowStateWire`             |
| `WorkflowStepStateWire`         | `WorkflowStepStateWire`         |
| `PromptStepMarkerWire`          | `PromptStepMarkerWire`          |
| `PlanPathMarkerWire`            | `PlanPathMarkerWire`            |
| `AgentArtifactRecordWire`       | `AgentArtifactRecordWire`       |
| `AgentArtifactScanWire`         | `AgentArtifactScanWire`         |

JSON shape rules (enforced by tests):

- `Option<T>::None` → JSON `null` (never omitted).
- Empty list fields → JSON `[]` (never `null`).
- `schema_version` is the first field of `ChangeSpecWire` so a Rust parser can refuse to deserialize newer records.
- Field declaration order matches the Python dataclasses, so byte-for-byte parity is reachable when both sides preserve
  declaration order.

`crates/sase_core/tests/python_wire_parity.rs` checks Rust JSON against a captured Python fixture in both directions.

## Bead storage contract

`crates/sase_core/src/bead/` mirrors the portable pieces of `sase_100/src/sase/bead/` for the bead backend migration:

- `wire.rs` defines `IssueWire`, `DependencyWire`, `StatusWire`, `IssueTypeWire`, validation errors, and operation
  outcomes.
- `config.rs` loads and saves `sdd/beads/config.json` using the same pretty JSON shape as Python.
- `jsonl.rs` imports and exports `sdd/beads/issues.jsonl`, skips corrupt lines, applies legacy defaults, validates
  records, sorts import rows as Python does for parent-before-child loading, and exports compact JSON sorted by issue ID.
- `schema.rs` pins the current SQLite schema plus migration fragments for legacy issue type names, `is_ready_to_work`,
  and ChangeSpec metadata columns.

`crates/sase_core/tests/bead_storage_parity.rs` carries the Phase A bead fixtures forward into Rust and checks the
current JSONL/config shape, legacy defaults, tolerant corrupt-line handling, missing-file behavior, and byte-compatible
JSONL export. No production Python code routes through these bead APIs yet; read bindings and store operations land in
later phases.

## Parser status

`crates/sase_core/src/parser.rs` exposes:

```rust
pub fn parse_project_bytes(
    path: &str,
    data: &[u8],
) -> Result<Vec<ChangeSpecWire>, ParseErrorWire>;
```

Phase 1C handles ChangeSpec boundaries (`## ChangeSpec` headers, direct `NAME:` starts, two-blank-line / new-NAME
terminators), the scalar fields `NAME`, `DESCRIPTION`, `PARENT`, `CL`/`PR`, `BUG`, `STATUS`, `TEST TARGETS`, and
`KICKSTART`, **and** structured section parsing for `COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`, and
`DELTAS`. Suffix-prefix parsing matches `sase.ace.changespec.suffix_utils` (including `~!:`, `~@:`, `~$:`, `?$:`, `!:`,
`@:`, `$:`, `%:`, `^:`, the legacy `~:` plain form, the standalone `@`/`%`/`^` markers, and the `!: metahook | ...` →
`metahook_complete` promotion).

`source_span.start_line` / `end_line` are inclusive 1-based and reflect the real last non-blank line of the spec, which
improves on Phase 0's Python placeholder (`end_line == start_line`).

### Documented incompatibility: `source_span.end_line`

Python's `changespec_to_wire` writes `end_line == start_line` because the Python parser does not track end positions.
Rust tracks real end lines (a deliberate Phase 1 improvement, per `sase_100/plans/202604/rust_backend_phase1.md`).

`crates/sase_core/tests/golden_corpus_parity.rs` normalizes Rust's `end_line` down to `start_line` before comparing
against the Python golden snapshot, so the rest of the wire is checked byte-for-byte. The real end-line behavior is
exercised by parser unit tests instead. Phase 1F decides whether to backfill end-line tracking in Python or keep this
normalization at the parity boundary.

## Phase status

Currently complete: **Phase 1A** (workspace + wire types), **Phase 1B** (scalar parser skeleton), **Phase 1C** (section
parser parity), **Phase 1D** (PyO3 binding + Python adapter in `sase_100`), and **Phase 1E** (dev workflow, benchmarks,
packaging decision) of `sase_100/plans/202604/rust_backend_phase1.md`. Remaining work:

- **1F** — cross-repo parity gate and handoff.

For Phase 3 (`rust_backend_phase3_agent_scan.md`): **Phase 3B** added the
pure-Rust artifact filesystem snapshot scanner under
`crates/sase_core/src/agent_scan/` and parity tests in
`crates/sase_core/tests/agent_scan_parity.rs`. The PyO3 binding for
`scan_agent_artifacts` lands in Phase 3C.

## Agent artifact scanner (Phase 3B)

`crates/sase_core/src/agent_scan/scanner.rs` exposes:

```rust
pub fn scan_agent_artifacts(
    projects_root: &Path,
    options: AgentArtifactScanOptionsWire,
) -> AgentArtifactScanWire;
```

It walks `projects_root/<project>/artifacts/<workflow>/<timestamp>/` for
the workflow folder families pinned in `agent_scan_wire.py`
(`ace-run`, `run`, `fix-hook`, `crs`, `summarize-hook`, plus `mentor-*`
and `workflow-*` prefixes), and parses the marker files `agent_meta.json`,
`done.json`, `running.json`, `waiting.json`, `workflow_state.json`,
`plan_path.json`, and `prompt_step_*.json`. Soft errors (unreadable
directories, malformed marker JSON, marker JSON whose top level is not a
JSON object) are absorbed silently and counted on
`AgentArtifactScanStatsWire`. Records are sorted by
`(project_name, workflow_dir_name, timestamp)` before returning, matching
`scan_agent_artifacts_python` in `sase_100`.

## Python binding (`sase_core_rs`)

`crates/sase_core_py` is a `cdylib` that builds the Python extension module `sase_core_rs`. It exposes one function:

```python
sase_core_rs.parse_project_bytes(path: str, data: bytes) -> list[dict]
```

The result is plain Python `dict`/`list`/`str`/`int`/`bool`/`None` mirroring the `ChangeSpecWire` JSON shape — no PyO3
classes leak across the boundary in Phase 1. A Rust `ParseErrorWire` is surfaced as a Python `ValueError` whose message
is the wire error's `Display` form (`"kind: message (file_path)"`).

Building the wheel requires a Python interpreter on the host (`maturin develop` or `maturin build` from
`crates/sase_core_py`). It is opt-in: the Python `sase` install does not require Rust, and `SASE_CORE_BACKEND=python`
(the default) ignores the binding entirely. The `is_rust_available()` probe in `sase.core.backend` lazy-imports
`sase_core_rs`, so a missing module never breaks startup.

## Benchmarks

Phase 1E adds two benchmark harnesses so future phases can decide whether to default `SASE_CORE_BACKEND` to `rust`:

- **Rust direct** — `cargo run --release --example bench_parse` from this repo (or `just rust-bench` from `sase_100`).
  Times only the pure Rust parser over the golden corpus and a synthetic multi-spec file. No Python in the loop.
- **End-to-end Python** — `python tests/perf/bench_core_parse.py` in `sase_100` (or `just bench-core`). Times Python
  direct, Python facade, `sase_core_rs.parse_project_bytes` direct, the Rust facade (PyO3 + dict→`ChangeSpecWire`
  rehydration), and the `SASE_CORE_DUAL_RUN=1` overhead. The facade number is the one to compare against Python — the
  direct number isolates how much of the cost is PyO3/dict marshaling vs. parsing.

Both harnesses accept `--num-specs` and `--runs` flags so they can be tuned for noise floor vs. wall-clock budget.

## Packaging decision (Phase 1E)

`sase` does **not** depend on a built `sase_core_rs` wheel. The extension is detected opportunistically at import time:

- Pure-Python install: `pip install sase` (or `just install`) succeeds with no Rust toolchain. `is_rust_available()`
  returns `False`, `parse_project_bytes` runs through the Python implementation.
- Rust-enabled install: a contributor runs `just rust-install` (which uses `maturin develop --release` from
  `crates/sase_core_py`). After that, `is_rust_available()` returns `True` and `SASE_CORE_BACKEND=rust` (or dual-run)
  routes through the binding.
- CI: `just check` does not invoke any Rust target, so Python-only jobs cannot be broken by Rust packaging churn. A
  separate `just rust-check` (fmt-check + clippy + tests) runs on demand.

This keeps the rollout reversible: Phase 1F can flip the default to `rust` without touching the install path, and a
future hard dependency on the wheel is a separate decision that can be staged behind an extra (`pip install sase[rust]`)
if it ever becomes desirable.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
