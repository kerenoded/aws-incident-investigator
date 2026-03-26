"""Authorization helpers for incident context ownership checks."""

from __future__ import annotations


def extract_cognito_sub(event: dict) -> str | None:
    claims = (
        event.get("requestContext", {})
        .get("authorizer", {})
        .get("claims", {})
    )
    value = claims.get("sub")
    if isinstance(value, str) and value.strip() != "":
        return value.strip()
    return None


def is_owner(*, owner_sub: str | None, caller_sub: str | None) -> bool:
    if not isinstance(owner_sub, str) or owner_sub.strip() == "":
        return False
    if not isinstance(caller_sub, str) or caller_sub.strip() == "":
        return False
    return owner_sub.strip() == caller_sub.strip()
