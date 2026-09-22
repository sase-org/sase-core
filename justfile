default: check

check:
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
