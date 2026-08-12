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

usage() {
    cat >&2 <<EOF
usage: $(basename "${BASH_SOURCE[0]}") [fmt-check|fmt|clippy|test|all]

  fmt-check   cargo fmt --all -- --check
  fmt         cargo fmt --all
  clippy      cargo clippy --workspace --all-targets -- -D warnings
  test        cargo test --workspace
  all         fmt-check, then clippy, then test (default)
EOF
}

cmd_fmt_check() {
    cargo fmt --all -- --check
}

cmd_fmt() {
    cargo fmt --all
}

cmd_clippy() {
    resolve_pyo3_python
    cargo clippy --workspace --all-targets -- -D warnings
}

cmd_test() {
    resolve_pyo3_python
    cargo test --workspace
}

cmd_all() {
    cmd_fmt_check
    cmd_clippy
    cmd_test
}

subcommand="${1:-all}"

case "$subcommand" in
    fmt-check) cmd_fmt_check ;;
    fmt) cmd_fmt ;;
    clippy) cmd_clippy ;;
    test) cmd_test ;;
    all) cmd_all ;;
    *)
        usage
        exit 2
        ;;
esac
