"""
Context reading and snapshot building for the trigger Lambda.

Reads an incident context from DynamoDB and builds an immutable snapshot
that travels with the investigation payload through Step Functions.
"""

from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from .validation import is_non_empty_string, require_env


def get_context_meta(context_id: str) -> tuple[dict | None, bool]:
    """Read incident context item from DynamoDB.

    Returns:
        (item, had_error) — item is None when not found or on error.
    """
    try:
        contexts_table = boto3.resource("dynamodb").Table(require_env("CONTEXTS_TABLE_NAME"))
        response = contexts_table.get_item(Key={"PK": f"CONTEXT#{context_id}", "SK": "META"})
    except (ClientError, EnvironmentError):
        return None, True
    return response.get("Item"), False


def build_context_snapshot(context_item: dict) -> dict | str:
    """Build an immutable context snapshot from a DynamoDB context item.

    Returns:
        Snapshot dict on success, or an error string describing the validation failure.
    """
    region = context_item.get("region")
    if not is_non_empty_string(region):
        return "Incident context region must be a non-empty string."

    log_groups = _clean_string_list(context_item.get("logGroups"))
    metric_descriptors = _clean_metric_descriptors(context_item.get("metricDescriptors"))
    xray_services = _clean_string_list(context_item.get("xrayServices"))

    if not any((log_groups, metric_descriptors, xray_services)):
        return (
            "Incident context must include at least one evidence source: "
            "logGroups, metricDescriptors, or xrayServices."
        )

    service = _derive_service_from_context(xray_services=xray_services)
    if not is_non_empty_string(service):
        return "Incident context must include xrayServices to resolve runtime service."

    return {
        "contextId": context_item.get("contextId"),
        "capturedAt": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "region": region.strip(),
        "logGroups": log_groups,
        "metricDescriptors": metric_descriptors,
        "xrayServices": xray_services,
    }


def _derive_service_from_context(*, xray_services: list[str]) -> str | None:
    for service in xray_services:
        if is_non_empty_string(service):
            return service.strip()
    return None


def _clean_string_list(values) -> list[str]:
    if not isinstance(values, list):
        return []
    out = {str(v).strip() for v in values if isinstance(v, str) and str(v).strip() != ""}
    return sorted(out)


def _clean_metric_descriptors(values) -> list[dict]:
    if not isinstance(values, list):
        return []

    cleaned = []
    for item in values:
        if not isinstance(item, dict):
            continue
        namespace = item.get("namespace")
        metric_name = item.get("metricName")
        if not is_non_empty_string(namespace) or not is_non_empty_string(metric_name):
            continue

        normalized = {
            "namespace": namespace.strip(),
            "metricName": metric_name.strip(),
            "dimensions": _normalize_dimensions(item.get("dimensions")),
            "stat": item.get("stat") if is_non_empty_string(item.get("stat")) else "Average",
        }
        cleaned.append(normalized)

    return cleaned


def _normalize_dimensions(value) -> dict:
    if isinstance(value, dict):
        return {
            str(k).strip(): str(v).strip()
            for k, v in value.items()
            if is_non_empty_string(k) and is_non_empty_string(v)
        }

    if isinstance(value, list):
        out = {}
        for dim in value:
            if not isinstance(dim, dict):
                continue
            name = dim.get("name")
            dim_value = dim.get("value")
            if is_non_empty_string(name) and is_non_empty_string(dim_value):
                out[name.strip()] = dim_value.strip()
        return out

    return {}
