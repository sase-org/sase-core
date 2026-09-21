default: check

check:
    ./scripts/check.sh all

fmt:
    ./scripts/check.sh fmt

clippy:
    ./scripts/check.sh clippy

test *args:
    ./scripts/check.sh test {{args}}
