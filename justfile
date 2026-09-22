default: check

# Agents must run guarded tools through `sase tool run` (sase docs/tool.md).
# The guard is the first dependency, ahead of the check script, so a refusal
# costs milliseconds rather than a full gate run.
_require-tool-run name:
    @scripts/require_tool_run {{ name }}

check: (_require-tool-run "check")
    ./scripts/check.sh all

# Inner loop: workspace check without formatting, lint, or tests.
fast *args:
    ./scripts/check.sh check {{args}}

fmt:
    ./scripts/check.sh fmt

clippy:
    ./scripts/check.sh clippy

test *args:
    ./scripts/check.sh test {{args}}

# Fresh-by-construction module map: each sase_core top-level module with its one-line //! summary.
modules:
    ./scripts/check.sh modules
