# sase-core

[![PyPI](https://img.shields.io/pypi/v/sase-core-rs?logo=pypi&logoColor=white)](https://pypi.org/project/sase-core-rs/)

Rust core for the [sase](https://github.com/sase-org/sase) backend. sase consumes it as
the PyPI `sase-core-rs` extension: `pyproject.toml` pins a minor window and
`sase-core-revision.txt` pins the exact revision its CI builds.

## Layout

| Crate                     | Owns                                                                                                                                              |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/sase_core`        | All domain logic, with no PyO3. Flat top-level modules, one per domain; `just modules` prints a one-line summary of each                          |
| `crates/sase_core_py`     | The `sase_core_rs` extension (PyPI `sase-core-rs`). One binding domain per `src/<domain>/`, often named differently from the core module it binds |
| `crates/sase_gateway`     | The mobile and fleet HTTP gateway plus the `sase_sudo_runner` and `sase_federation_worker` binaries. They ship inside the wheel                   |
| `crates/sase_xprompt_lsp` | The `sase-xprompt-lsp` language server                                                                                                            |

## Development

See `AGENTS.md`. The short version:

```bash
just fast    # inner loop: cargo check --workspace --all-targets
just check   # the gate (same steps as CI): fmt-check, features, clippy, tests, script-test
```

```bash
cargo run --release --example bench_parse   # direct-parser benchmark
```

`rust-toolchain.toml` pins the `stable` channel and installs `rustfmt` and `clippy`.
`Cargo.lock` is committed so the workspace builds reproducibly.

## Releasing / versioning

release-plz owns the workspace and crate release versions. Normal feature and fix PRs
must not edit `[workspace.package].version`, crate `[package].version`, or local
path-dependency version pins in `Cargo.toml`; use Conventional Commits metadata instead
and let release-plz calculate the next version from the merged commits. For a breaking
change on the `0.x` line, mark the commit or squash-merge title with `!` (for example,
`feat(core)!: remove legacy API`) or include a `BREAKING CHANGE:` footer so release-plz
computes the minor bump.

The `Cargo version guard` PR check blocks release-owned Cargo version edits outside
release-plz branches. The `manual-version` PR label is reserved for deliberate release
recovery or other explicitly approved version overrides.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
