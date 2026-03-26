"""
Backend test suite root conftest.

Adds the component directories that use bare module imports (e.g.
``from logs_worker import collect_logs``) to sys.path once for the
whole suite. This replaces per-test-file sys.path boilerplate.

The repo root is already on sys.path via pytest.ini (pythonpath = .).
pytest also adds ``backend/`` to sys.path automatically for tests in
package-structured directories (those with __init__.py).
"""

import sys
from pathlib import Path

_BACKEND = Path(__file__).parent

# Component directories whose modules are imported by bare name in tests
# (e.g. ``from hypotheses_builder import build_hypotheses``).
_COMPONENT_DIRS = [
    _BACKEND / "workers" / "logs",
    _BACKEND / "workers" / "metrics",
    _BACKEND / "workers" / "traces",
    _BACKEND / "ai" / "hypothesis_evaluator",
    _BACKEND / "orchestration" / "scope_builder",
    _BACKEND / "orchestration" / "candidate_hypotheses_builder",
    _BACKEND / "orchestration" / "final_report_builder",
]

for _dir in _COMPONENT_DIRS:
    _dir_str = str(_dir)
    if _dir_str not in sys.path:
        sys.path.insert(0, _dir_str)
