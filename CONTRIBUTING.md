# Contributing

This is a portfolio/PoC project — contributions are not expected. That said, if you've found a bug or have a suggestion, feel free to open an issue.

## Running locally

```bash
# Python virtual environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# Backend tests
pytest backend/ -v

# Frontend
cd frontend && npm install && npm test
```

## Quality gates (mirrors CI)

```bash
make lint              # ruff check
make test-backend      # pytest
make test-frontend     # vitest
make typecheck-frontend
make build-frontend
make synth             # CDK synthesis (requires CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION)
```

## Code style

- Python: formatted and linted with `ruff`. Run `make lint` before committing.
- TypeScript: type-checked with `tsc`. Run `make typecheck-frontend` before committing.
- No new dependencies without a clear reason.
- Backend business logic belongs in dedicated modules under `backend/`; `handler.py` files are thin entry points only.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and [docs/DECISIONS.md](docs/DECISIONS.md) before making structural changes.
