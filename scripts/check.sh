#!/usr/bin/env bash
# Single source of truth for the gates CI runs. Agents and CI must both call
# this script (never `cargo` directly for clippy/test) so local verification
# cannot silently drift from what CI checks.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# crates/sase_core_py pins pyo3's abi3-py312 feature, which hard-errors at
# build time against any interpreter older than 3.12. On this machine
# python3 resolves through a pyenv shim to 3.11, so `cargo build/test/clippy
# --workspace` fail with an opaque pyo3-build-config error unless a newer
# interpreter is selected explicitly. That opacity is exactly what causes
# agents to fall back to `cargo test -p sase_core`, which silently skips the
# sase_core_py binding tests - the fallback that let stale schema-version
# assertions reach master in a509dcc. Resolving a qualifying interpreter here
# keeps that fallback from ever being necessary.
resolve_pyo3_python() {
    if [[ -n "${PYO3_PYTHON:-}" ]]; then
        return 0
    fi

    local candidate
    for candidate in python3.14 python3.13 python3.12 python3; do
        if command -v "$candidate" >/dev/null 2>&1; then
            if "$candidate" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' 2>/dev/null; then
                PYO3_PYTHON="$(command -v "$candidate")"
                export PYO3_PYTHON
                return 0
            fi
        fi
    done

    echo "error: no Python >= 3.12 interpreter found for crates/sase_core_py's abi3-py312 pyo3 feature." >&2
    echo "Install one (e.g. python3.12+) or set PYO3_PYTHON to an explicit interpreter path." >&2
    return 1
}

configure_pyo3_python() {
    resolve_pyo3_python

    local libdir
    libdir="$("$PYO3_PYTHON" - <<'PY'
import pathlib
import sysconfig

if sysconfig.get_config_var("Py_ENABLE_SHARED"):
    libdir = sysconfig.get_config_var("LIBDIR") or sysconfig.get_config_var("LIBPL")
    if libdir and pathlib.Path(libdir).is_dir():
        print(libdir)
PY
)"

    if [[ -z "$libdir" ]]; then
        return 0
    fi

    case ":${LD_LIBRARY_PATH:-}:" in
        *":$libdir:"*) ;;
        *) export LD_LIBRARY_PATH="$libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ;;
    esac

    if [[ "$(uname -s)" == "Darwin" ]]; then
        case ":${DYLD_LIBRARY_PATH:-}:" in
            *":$libdir:"*) ;;
            *) export DYLD_LIBRARY_PATH="$libdir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" ;;
        esac
    fi
}

usage() {
    cat >&2 <<EOF
usage: $(basename "${BASH_SOURCE[0]}") [fmt-check|fmt|features|check [args...]|clippy [args...]|test [args...]|script-test|modules|all]

  fmt-check   cargo fmt --all -- --check
  fmt         cargo fmt --all
  features    verify unified dependency features (cargo-hakari workspace-hack)
  check       cargo check --workspace --all-targets [args...] (inner loop)
  clippy      cargo clippy --workspace --all-targets [args...] -- -D warnings
  test        cargo test --workspace [args...]
  script-test unittest the release-workflow helper scripts in .github/scripts
  modules     list each sase_core top-level module with its one-line //! summary
  all         fmt-check, features, clippy, test, then script-test (default)

Trailing arguments to the check, clippy and test subcommands are forwarded to
the underlying cargo invocation, e.g. 'check.sh test -p sase_gateway' or
'check.sh test -- --skip foo'. An explicit package selection (-p, --package,
--workspace) replaces the default --workspace scope.
EOF
}

cmd_fmt_check() {
    cargo fmt --all -- --check
}

cmd_fmt() {
    cargo fmt --all
}

# cargo unions --workspace with -p/--package instead of intersecting them, so a
# bare forwarded selection would still run the whole workspace. When the caller
# passes its own package selection, drop the default --workspace scope so a
# single-crate run stays cheap. That cheapness holds only because the
# cargo-hakari workspace-hack unifies dependency features: without it, a `-p`
# run resolves fewer features and the next workspace build recompiles.
default_scope() {
    local arg
    for arg in "$@"; do
        case "$arg" in
            --) break ;;
            -p*|--package*|--workspace) return 1 ;;
        esac
    done
    return 0
}

cmd_check() {
    configure_pyo3_python
    if default_scope "$@"; then
        cargo check --workspace --all-targets "$@"
    else
        cargo check --all-targets "$@"
    fi
}

# Tool-free workspace-hack drift gate: for each workspace member, every package
# in its normal+build+dev closure must resolve the same features as the
# workspace-wide resolution. Uses only `cargo tree` and `cargo metadata`
# (no PYO3_PYTHON, no compile, no cargo-hakari install). The edge set matches
# what `check`/`clippy` (`--all-targets`) and `test` actually build.
cmd_features() {
    python3 - <<'PY'
import json
import subprocess
import sys

HACK_CRATE = "sase_workspace_hack"
EDGES = "normal,build,dev"


def run_cargo(args):
    proc = subprocess.run(
        ["cargo", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(proc.returncode)
    return proc.stdout


def parse_tree(output):
    packages = {}
    for line in output.splitlines():
        line = line.strip().replace("(*)", "").strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        key = f"{parts[0]} {parts[1]}"
        feats = set()
        if len(parts) > 2:
            for feat in " ".join(parts[2:]).split(","):
                feat = feat.strip()
                if feat:
                    feats.add(feat)
        packages[key] = packages.get(key, set()) | feats
    return packages


try:
    meta = json.loads(run_cargo(["metadata", "--no-deps", "--format-version", "1"]))
except SystemExit:
    raise
members = sorted(
    pkg["name"]
    for pkg in meta["packages"]
    if pkg["id"] in meta["workspace_members"] and pkg["name"] != HACK_CRATE
)

workspace = parse_tree(
    run_cargo(["tree", "--workspace", "-e", EDGES, "--prefix", "none", "-f", "{p} {f}"])
)

failures = []
for member in members:
    member_pkgs = parse_tree(
        run_cargo(["tree", "-p", member, "-e", EDGES, "--prefix", "none", "-f", "{p} {f}"])
    )
    for pkg in sorted(member_pkgs):
        if pkg not in workspace:
            continue
        if member_pkgs[pkg] != workspace[pkg]:
            member_only = sorted(member_pkgs[pkg] - workspace[pkg])
            workspace_only = sorted(workspace[pkg] - member_pkgs[pkg])
            failures.append(
                f"error: feature drift for {pkg} in -p {member}:\n"
                f"  member features:    {sorted(member_pkgs[pkg])}\n"
                f"  workspace features: {sorted(workspace[pkg])}\n"
                f"  member-only:   {member_only}\n"
                f"  workspace-only: {workspace_only}"
            )

if failures:
    sys.stderr.write("\n".join(failures) + "\n")
    sys.stderr.write(
        "run `cargo hakari generate && cargo hakari manage-deps` to re-unify "
        "features (cargo-hakari install needed only for regeneration).\n"
    )
    raise SystemExit(1)
PY
}

cmd_clippy() {
    configure_pyo3_python
    if default_scope "$@"; then
        cargo clippy --workspace --all-targets "$@" -- -D warnings
    else
        cargo clippy --all-targets "$@" -- -D warnings
    fi
}

cmd_test() {
    configure_pyo3_python
    if default_scope "$@"; then
        cargo test --workspace "$@"
    else
        cargo test "$@"
    fi
}

cmd_script_test() {
    python3 -m unittest discover -s .github/scripts -t .github/scripts
}

# Fresh-by-construction module map: every sase_core top-level module with the
# first line of its `//!` summary. The module root is `<m>.rs` or `<m>/mod.rs`;
# `lib.rs` is skipped. Not a gate: P1 decides whether to commit a generated map.
cmd_modules() {
    local src_dir="$repo_root/crates/sase_core/src"
    local path mod root line
    for path in "$src_dir"/*; do
        mod="$(basename "$path")"
        if [[ -f "$path" ]]; then
            [[ "$mod" == *.rs ]] || continue
            mod="${mod%.rs}"
            root="$path"
        elif [[ -f "$path/mod.rs" ]]; then
            mod="$(basename "$path")"
            root="$path/mod.rs"
        else
            continue
        fi
        [[ "$mod" == "lib" ]] && continue
        line="$(grep -m1 '^//!' "$root" | sed -e 's|^//! \?||')" || line=""
        printf '%s: %s\n' "$mod" "$line"
    done | sort
}

cmd_all() {
    cmd_fmt_check
    cmd_features
    cmd_clippy
    cmd_test
    cmd_script_test
}

subcommand="${1:-all}"
if [[ $# -gt 0 ]]; then
    shift
fi

case "$subcommand" in
    fmt-check) cmd_fmt_check ;;
    fmt) cmd_fmt ;;
    features) cmd_features ;;
    check) cmd_check "$@" ;;
    clippy) cmd_clippy "$@" ;;
    test) cmd_test "$@" ;;
    script-test) cmd_script_test ;;
    modules) cmd_modules ;;
    all) cmd_all ;;
    *)
        usage
        exit 2
        ;;
esac
