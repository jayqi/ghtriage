# Print this help documentation
help:
    just --list

# Sync requirements
sync:
    uv sync

# Run linting (variadic)
lint *args:
    uv run -- ruff format --check {{args}}
    uv run -- ruff check {{args}}

# Run formatting (variadic)
format *args:
    uv run -- ruff format {{args}}
    uv run -- ruff check --fix --extend-fixable=F {{args}}

# Run test suite (variadic)
test *args:
    uv run --isolated --no-editable -- \
        python -I -m pytest {{args}}
