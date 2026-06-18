# Agent Instructions for sase-core

release-plz owns the workspace and crate release versions. Do not manually edit `[workspace.package].version`, crate
`[package].version`, or local path-dependency version pins in `Cargo.toml` during normal feature or fix work.

For breaking changes, use Conventional Commits metadata (`feat!:` or a `BREAKING CHANGE:` footer) so release-plz
computes the correct version. Deliberate release recovery version edits require explicit user approval and the
`manual-version` PR label.
