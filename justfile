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
