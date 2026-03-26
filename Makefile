PYTHON ?= python

.PHONY: test test-backend test-frontend test-target lint lint-backend typecheck-frontend build-frontend synth

test:
	$(PYTHON) -m pytest -q

test-backend:
	$(PYTHON) -m pytest backend/ -q

test-frontend:
	npm --prefix frontend test

test-target:
	$(PYTHON) -m pytest backend/ai/hypothesis_evaluator/tests/test_evaluator.py backend/orchestration/final_report_builder/tests/test_report_builder.py -q

lint:
	$(PYTHON) -m ruff check backend infra app.py

lint-backend: lint

typecheck-frontend:
	npm --prefix frontend run typecheck

build-frontend:
	npm --prefix frontend run build

synth:
	cdk synth
