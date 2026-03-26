"""
Input validation helpers for the trigger Lambda.

Pure functions — no I/O, no AWS calls.
"""

import os
from datetime import datetime, timedelta

_CONTEXT_REQUIRED_FIELDS = {"contextId", "signalType", "windowStart", "windowEnd"}
_OPTIONAL_FIELDS = {"environment", "severity"}


def is_non_empty_string(value) -> bool:
    return isinstance(value, str) and value.strip() != ""


def parse_iso_utc(ts: str) -> datetime:
    parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("Timestamp must be UTC with timezone offset.")
    return parsed


def validate_context_body(body: dict) -> str | None:
    """Validate context-first trigger request shape. Returns error message or None."""
    unknown_fields = set(body.keys()) - _CONTEXT_REQUIRED_FIELDS - _OPTIONAL_FIELDS
    if unknown_fields:
        return f"Unknown fields are not allowed: {sorted(unknown_fields)}"

    for field in _CONTEXT_REQUIRED_FIELDS:
        if not is_non_empty_string(body.get(field)):
            return f"Field '{field}' must be a non-empty string."

    for field in _OPTIONAL_FIELDS:
        if field in body and not is_non_empty_string(body.get(field)):
            return f"Field '{field}' must be a non-empty string when provided."

    try:
        window_start = parse_iso_utc(body["windowStart"])
        window_end = parse_iso_utc(body["windowEnd"])
    except ValueError:
        return "windowStart and windowEnd must be valid ISO 8601 timestamps."

    if window_end <= window_start:
        return "windowEnd must be after windowStart."

    return None


def validate_normalized_payload(payload: dict) -> str | None:
    """Validate the normalized incident payload before StartExecution. Returns error or None."""
    missing = [f for f in ("incidentId", "service", "signalType", "windowStart", "windowEnd", "region") if f not in payload]
    if missing:
        return f"Normalized payload missing required fields: {sorted(missing)}"

    for field in ("incidentId", "service", "signalType", "windowStart", "windowEnd", "region", "triggerSource"):
        if not is_non_empty_string(payload.get(field)):
            return f"Normalized payload field '{field}' must be a non-empty string."

    if payload.get("triggerSource") != "manual":
        return "Normalized payload triggerSource must be 'manual' for API-triggered investigations."

    if payload.get("triggerModel") != "context":
        return "Normalized payload triggerModel must be 'context'."
    if not is_non_empty_string(payload.get("contextId")):
        return "Normalized payload contextId must be a non-empty string for context-triggered investigations."
    if not isinstance(payload.get("contextSnapshot"), dict):
        return "Normalized payload contextSnapshot must be an object for context-triggered investigations."

    try:
        window_start = parse_iso_utc(payload["windowStart"])
        window_end = parse_iso_utc(payload["windowEnd"])
    except ValueError:
        return "Normalized payload windowStart and windowEnd must be valid ISO 8601 timestamps."

    if window_end <= window_start:
        return "Normalized payload windowEnd must be after windowStart."

    return None


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value
