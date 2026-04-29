# sase-core

Rust core for the [sase](https://github.com/sase-org/sase) ChangeSpec backend.

This repo is the eventual home of the Rust ChangeSpec parser, query engine,
and graph index. Phase 1A only ships the Cargo workspace skeleton and the
typed wire contract — no parsing logic yet. The Python backend in `sase_100`
remains the default; the Rust backend is opt-in via `SASE_CORE_BACKEND=rust`
once Phase 1D wires up the PyO3 binding.

## Layout

```
crates/
  sase_core/      # pure-Rust core: wire types + full-file parser
  sase_core_py/   # PyO3 binding placeholder (filled in in Phase 1D)
```

The pure crate has no PyO3 dependency. This is deliberate — later UniFFI,
WASM, or server crates need to consume `sase_core` without dragging a Python
toolchain into their build.

## Build & test

```bash
cargo fmt --all
cargo fmt --all -- --check   # CI gate
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

`rust-toolchain.toml` pins the `stable` channel and installs `rustfmt` and
`clippy`. `Cargo.lock` is committed so the workspace builds reproducibly.

## Wire contract

`crates/sase_core/src/wire.rs` mirrors `sase_100/src/sase/core/wire.py`:

| Rust type              | Python dataclass        |
|------------------------|-------------------------|
| `SourceSpanWire`       | `SourceSpanWire`        |
| `CommitWire`           | `CommitWire`            |
| `HookStatusLineWire`   | `HookStatusLineWire`    |
| `HookWire`             | `HookWire`              |
| `CommentWire`          | `CommentWire`           |
| `MentorStatusLineWire` | `MentorStatusLineWire`  |
| `MentorWire`           | `MentorWire`            |
| `TimestampWire`        | `TimestampWire`         |
| `DeltaWire`            | `DeltaWire`             |
| `ChangeSpecWire`       | `ChangeSpecWire`        |
| `ParseErrorWire`       | `ParseErrorWire`        |

JSON shape rules (enforced by tests):

- `Option<T>::None` → JSON `null` (never omitted).
- Empty list fields → JSON `[]` (never `null`).
- `schema_version` is the first field of `ChangeSpecWire` so a Rust parser
  can refuse to deserialize newer records.
- Field declaration order matches the Python dataclasses, so byte-for-byte
  parity is reachable when both sides preserve declaration order.

`crates/sase_core/tests/python_wire_parity.rs` checks Rust JSON against a
captured Python fixture in both directions.

## Parser status

`crates/sase_core/src/parser.rs` exposes:

```rust
pub fn parse_project_bytes(
    path: &str,
    data: &[u8],
) -> Result<Vec<ChangeSpecWire>, ParseErrorWire>;
```

Phase 1C handles ChangeSpec boundaries (`## ChangeSpec` headers, direct
`NAME:` starts, two-blank-line / new-NAME terminators), the scalar
fields `NAME`, `DESCRIPTION`, `PARENT`, `CL`/`PR`, `BUG`, `STATUS`,
`TEST TARGETS`, and `KICKSTART`, **and** structured section parsing for
`COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`, and `DELTAS`.
Suffix-prefix parsing matches `sase.ace.changespec.suffix_utils`
(including `~!:`, `~@:`, `~$:`, `?$:`, `!:`, `@:`, `$:`, `%:`, `^:`,
the legacy `~:` plain form, the standalone `@`/`%`/`^` markers, and the
`!: metahook | ...` → `metahook_complete` promotion).

`source_span.start_line` / `end_line` are inclusive 1-based and reflect
the real last non-blank line of the spec, which improves on Phase 0's
Python placeholder (`end_line == start_line`).

### Documented incompatibility: `source_span.end_line`

Python's `changespec_to_wire` writes `end_line == start_line` because
the Python parser does not track end positions. Rust tracks real end
lines (a deliberate Phase 1 improvement, per
`sase_100/plans/202604/rust_backend_phase1.md`).

`crates/sase_core/tests/golden_corpus_parity.rs` normalizes Rust's
`end_line` down to `start_line` before comparing against the Python
golden snapshot, so the rest of the wire is checked byte-for-byte. The
real end-line behavior is exercised by parser unit tests instead.
Phase 1F decides whether to backfill end-line tracking in Python or
keep this normalization at the parity boundary.

## Phase status

Currently complete: **Phase 1A** (workspace + wire types),
**Phase 1B** (scalar parser skeleton), and **Phase 1C** (section parser
parity) of `sase_100/plans/202604/rust_backend_phase1.md`. Subsequent
phases:

- **1D** — PyO3 binding + Python adapter in `sase_100`.
- **1E** — dev workflow, benchmarks, packaging decision.
- **1F** — cross-repo parity gate and handoff.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
