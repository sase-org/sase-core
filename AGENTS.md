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

## Platform paths

Canonicalize both sides of a path comparison, or neither. When production must canonicalize (for a security or
identity check), the value it compares against — denylist entry, expected argument, allowed root, asserted
expectation — must be canonicalized the same way. When production must preserve a caller-supplied path (because it
is echoed back to the caller), it must not silently compare it against a canonicalized one. A symlinked ancestor is
not by itself evidence of an attack: on macOS `/tmp` resolves to `/private/tmp` and `/var` resolves to
`/private/var`, so platform aliases trip any "any ancestor is a symlink" check.

## macOS verification

CI runs `rust-checks` on both `ubuntu-latest` and `macos-latest`, and both legs block. `./scripts/check.sh` (or
`just check`) is the entry point on either platform: it resolves a Python >= 3.12 interpreter for `PYO3_PYTHON`
itself on macOS just as it does on Linux, so never verify with bare `cargo` invocations.
