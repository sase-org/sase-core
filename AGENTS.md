# sase-core

Rust core for [sase](https://github.com/sase-org/sase). Domain logic and its serde
`*Wire` contracts live in `sase_core`. sase (Python) calls them through the
`sase_core_rs` extension, looking each binding up by its Python name with
`require_rust_binding("<name>")`.

| Crate                     | Owns                                                                                                                                              |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/sase_core`        | All domain logic, with no PyO3. Flat top-level modules, one per domain; `just modules` prints a one-line summary of each                          |
| `crates/sase_core_py`     | The `sase_core_rs` extension (PyPI `sase-core-rs`). One binding domain per `src/<domain>/`, often named differently from the core module it binds |
| `crates/sase_gateway`     | The mobile and fleet HTTP gateway plus the `sase_sudo_runner` and `sase_federation_worker` binaries. They ship inside the wheel                   |
| `crates/sase_xprompt_lsp` | The `sase-xprompt-lsp` language server                                                                                                            |

## Build and verify

Never run bare `cargo`. `sase_core_py` needs a Python >= 3.12 interpreter to build and
libpython to test, and `scripts/check.sh`, which every `just` recipe calls, resolves
both.

- `just fast` is the inner loop (`cargo check --workspace --all-targets`). Any
  `sase_core` edit recompiles the whole crate, so batch your edits between runs.
- `just test -p <crate> [<filter>]` runs targeted tests while you iterate.
- `just fmt` applies formatting.
- `just check` is the gate and runs the same steps as CI: fmt-check, features
  (`./scripts/check.sh features`), clippy with `-D warnings`, every test, and the
  script tests. A features failure is fixed by
  `cargo hakari generate && cargo hakari manage-deps`. Pass it before you finish.
  It takes about 5 minutes, so give it an explicit tool timeout of 10 minutes or more.

## sase tool runs

Agents run `sase tool run check` here, not bare `just check`: `check` is
guarded and a raw agent invocation is refused with the wrapped and bypass
forms. To run raw on purpose: `SASE_TOOL_BYPASS='<why>' just check`.

A targeted run never replaces `just check`. For example, `-p sase_core` alone skips the
`sase_core_py` binding tests, and that gap let stale schema fixtures reach master in
`a509dcc`. `master` is unprotected, and a red commit also fails every release-plz run.

A test that fails under `just check` but passes when rerun alone is probably a load
flake. Before you treat it as yours, look for its `sase-core flake:` bead in
`sase bead list -T flake`. Never weaken an assertion to get a green run.

## Conventions

- Write free functions over `*Wire` serde structs, with errors as `thiserror` enums.
- Do not use `macro_rules!`. A generated item is invisible to grep and to the binding
  checks, so use a generic helper instead.
- A multi-file module's `mod.rs` is a facade that holds only `mod` and `pub use` lines.
  Tests sit beside the code in `tests.rs` or `tests/`. Keep new files at or under 1,500
  lines.
- Import core items by module path (`sase_core::<module>::Item`). Do not add names to
  the root `pub use` list in `crates/sase_core/src/lib.rs` or new `core_*` aliases to
  `crates/sase_core_py/src/prelude.rs`. Both are being retired.
- release-plz owns versions and changelogs:
  - Never edit a `version` field, a path-dependency version pin, or a `CHANGELOG.md`. A
    deliberate release recovery needs user approval and the `manual-version` PR label.
  - Commit subjects are Conventional Commits.
  - Mark a breaking change with `feat!:`/`fix!:` or a `BREAKING CHANGE:` footer. That
    covers removing or renaming a Python binding, and any wire change that old readers
    reject, because released sase accepts every core in its `sase-core-rs` minor window.
- Canonicalize both sides of a path comparison, or neither side. On macOS, `/tmp` and
  `/var` are symlinks into `/private`, so a symlinked ancestor is not evidence of an
  attack. CI also runs on macOS.

## Recipes

**Add a core function and expose it to Python.** This is the most common change.

1. Implement the function and its request/response `*Wire` types in the owning
   `sase_core` module, with tests. Export them from the module facade.
2. Put the binding in the domain that already binds that module:
   `rg -l '<sibling fn>' crates/sase_core_py/src/*/`.
3. Model the binding on a neighbour:
   - Use `#[pyfunction]`, `#[pyo3(name = "<python name>")]` and `fn py_<python name>`.
   - Parse the input dict into the request `*Wire`.
   - Map the core error to a Python exception.
   - Return `serialize_to_py(py, &out)`.
4. Register it in that domain's `register_<domain>` with
   `m.add_function(wrap_pyfunction!(py_<name>, m)?)?`. The compiler does not check this,
   so a missed registration only surfaces as an `AttributeError` in sase.
5. Add a round-trip test in the domain's `tests.rs`.
6. sase CI builds the core at the commit named in sase's `sase-core-revision.txt`. Any
   sase code that calls the new binding stays red until that pin moves past your commit.
   Move it with `just ratchet-core-revision` in sase, or wait for the six-hourly ratchet
   PR. See sase's `docs/rust_backend.md`.

**Change a wire schema version.** `*_WIRE_SCHEMA_VERSION` values are copied by hand.

1. Search both repos for the constant, its `*_wire_schema_version` getter and the old
   number.
2. Update every copy together:
   - in this repo, the Rust tests and fixtures
   - in sase, the Python mirror constant, `tools/validate_sase_core_rs`, and their tests

**Add a gateway route.**

1. Write the handler in `crates/sase_gateway/src/routes/`.
2. Register it in `routes/router.rs` and declare it in `contract.rs`. No test ties those
   two together, so do both.
3. Regenerate the snapshot with
   `UPDATE_MOBILE_CONTRACT=1 just test -p sase_gateway committed_`. For fleet routes,
   use `UPDATE_FLEET_CONTRACT=1` instead.
4. List the route in `crates/sase_gateway/README.md`.
