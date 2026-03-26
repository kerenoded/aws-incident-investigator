"""
Scope Builder — core logic (pure functions, no AWS calls).

Implements:
  - ADR-003: scope before retrieval
  - ADR-004: default baseline = immediately preceding window of equal duration
  - Context-snapshot-only runtime scope derivation
"""

from datetime import datetime


def build_scope_from_context_snapshot(incident: dict) -> dict:
    """Derive investigation scope from a context snapshot in workflow input.

    Uses immutable snapshot fields captured at trigger time.
    """
    context_snapshot = incident.get("contextSnapshot")
    if not isinstance(context_snapshot, dict):
        raise ValueError("Incident payload missing required object field: 'contextSnapshot'.")

    window_start = _parse_iso(incident["windowStart"])
    window_end = _parse_iso(incident["windowEnd"])
    duration = window_end - window_start

    baseline_start = window_start - duration
    baseline_end = window_start

    service = context_snapshot.get("service")
    region = context_snapshot.get("region")
    if not isinstance(service, str) or service.strip() == "":
        raise ValueError("contextSnapshot.service must be a non-empty string.")
    if not isinstance(region, str) or region.strip() == "":
        raise ValueError("contextSnapshot.region must be a non-empty string.")

    trace_services = _clean_string_list(context_snapshot.get("xrayServices"))

    return {
        "incidentId": incident["incidentId"],
        "signalType": incident.get("signalType"),
        "service": service.strip(),
        "region": region.strip(),
        "incidentWindow": {
            "start": _fmt_utc(window_start),
            "end": _fmt_utc(window_end),
        },
        "baselineWindow": {
            "start": _fmt_utc(baseline_start),
            "end": _fmt_utc(baseline_end),
        },
        "logGroups": _clean_string_list(context_snapshot.get("logGroups")),
        "metrics": _clean_object_list(context_snapshot.get("metricDescriptors")),
        "traceServices": trace_services,
        "dependencies": [],
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_iso(ts: str) -> datetime:
    """Parse an ISO 8601 UTC timestamp string into a timezone-aware datetime."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _fmt_utc(dt: datetime) -> str:
    """Format a datetime as a UTC ISO 8601 string with a literal Z suffix."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean_string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []
    return [v.strip() for v in values if isinstance(v, str) and v.strip() != ""]


def _clean_object_list(values) -> list[dict]:
    if not isinstance(values, list):
        return []
    return [v for v in values if isinstance(v, dict)]
