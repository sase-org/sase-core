# Agent Instructions for sase-core

release-plz owns the workspace and crate release versions. Do not manually edit `[workspace.package].version`, crate
`[package].version`, or local path-dependency version pins in `Cargo.toml` during normal feature or fix work.

For breaking changes, use Conventional Commits metadata (`feat!:` or a `BREAKING CHANGE:` footer) so release-plz
computes the correct version. Deliberate release recovery version edits require explicit user approval and the
`manual-version` PR label.

## Verification

Run `just check` (or `./scripts/check.sh`) from the repo root before every commit; it runs the same gates as CI.

`crates/sase_core_py` builds PyO3 with the `abi3-py312` feature, so the workspace only builds when a Python >= 3.12
interpreter is reachable. The script finds one and exports `PYO3_PYTHON`, and fails loudly when it cannot.

Never verify with `cargo test -p sase_core` alone: it excludes the `sase_core_py` binding tests, which is how three
stale schema-version fixtures reached master in `a509dcc`.

`master` is unprotected and a red commit there also fails every `Release-plz` run until it is fixed, which is why
the pre-commit gate matters more here than in a PR-gated repo.
