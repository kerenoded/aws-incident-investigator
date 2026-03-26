"""Schema example contract validation tests.

Ensures documentation examples under schemas/examples/ remain valid against
their canonical JSON Schemas. This is a broad, low-maintenance guardrail
that protects API/docs contract integrity.
"""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest
from referencing import Registry, Resource


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMAS_DIR = REPO_ROOT / "schemas"
EXAMPLES_DIR = SCHEMAS_DIR / "examples"


EXAMPLE_TO_SCHEMA = {
    "final-report.example.json": "final-report.schema.json",
    "hypothesis.example.json": "hypothesis.schema.json",
    "incident-context.example.json": "incident-context.schema.json",
    "incident-manual-triggered.example.json": "incident.schema.json",
    "scope.example.json": "scope.schema.json",
}


def _load_json(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def _build_schema_registry() -> Registry:
    """Load all top-level schemas into a registry for local $ref resolution."""
    registry = Registry()

    for schema_path in SCHEMAS_DIR.glob("*.schema.json"):
        schema = _load_json(schema_path)
        resource = Resource.from_contents(schema)

        # Common local reference forms used in this repository.
        registry = registry.with_resource(schema_path.name, resource)
        registry = registry.with_resource(f"./{schema_path.name}", resource)

    return registry


@pytest.mark.parametrize(
    ("example_name", "schema_name"),
    EXAMPLE_TO_SCHEMA.items(),
)
def test_schema_example_matches_contract(example_name: str, schema_name: str):
    registry = _build_schema_registry()

    example = _load_json(EXAMPLES_DIR / example_name)
    schema = _load_json(SCHEMAS_DIR / schema_name)

    validator = jsonschema.Draft202012Validator(schema, registry=registry)
    validator.validate(example)
