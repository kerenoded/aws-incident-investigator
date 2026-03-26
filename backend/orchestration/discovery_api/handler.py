"""Dynamic discovery API for incident context resource search.

Endpoints:
  GET /incident-contexts/discovery/log-groups
  GET /incident-contexts/discovery/metrics
  GET /incident-contexts/discovery/metrics/namespaces
  GET /incident-contexts/discovery/xray/services
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from shared.authz.context_access import extract_cognito_sub

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 50
# Maximum pages of list_metrics to scan for namespace discovery.
# 3 pages × 500 metrics/page = at most 1500 raw metric records scanned per call.
_MAX_NAMESPACE_SCAN_PAGES = 3


def lambda_handler(event: dict, context) -> dict:
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    if method == "OPTIONS":
        return _ok_response(200, {"ok": True})

    if method != "GET":
        return _error_response(405, "METHOD_NOT_ALLOWED", f"Method {method} not supported.")

    caller_sub = extract_cognito_sub(event)
    if caller_sub is None:
        return _error_response(403, "AUTHZ_FORBIDDEN", "Missing caller identity.")

    query = event.get("queryStringParameters") or {}
    region, error = _require_non_empty(query, "region")
    if error:
        return _error_response(400, "VALIDATION_ERROR", error)

    page_size, error = _parse_page_size(query.get("pageSize"))
    if error:
        return _error_response(400, "VALIDATION_ERROR", error)

    q = _clean_optional_str(query.get("q"))
    next_token = _clean_optional_str(query.get("nextToken"))

    try:
        if path.endswith("/incident-contexts/discovery/log-groups"):
            return _handle_log_groups(region, q, page_size, next_token)
        if path.endswith("/incident-contexts/discovery/metrics/namespaces"):
            return _handle_metric_namespaces(region, q, page_size)
        if path.endswith("/incident-contexts/discovery/metrics"):
            return _handle_metrics(region, q, page_size, next_token, query)
        if path.endswith("/incident-contexts/discovery/xray/services"):
            return _handle_xray_services(region, q, page_size, next_token, query)
    except (ClientError, BotoCoreError):
        return _error_response(500, "INTERNAL_ERROR", "Failed to discover AWS resources.")

    return _error_response(404, "NOT_FOUND", "Discovery endpoint not found.")


def _handle_log_groups(region: str, q: str | None, page_size: int, next_token: str | None) -> dict:
    client = boto3.client("logs", region_name=region)
    req: dict = {"limit": page_size}
    if q:
        req["logGroupNamePattern"] = q
    if next_token:
        req["nextToken"] = next_token

    response = client.describe_log_groups(**req)
    items = []
    for lg in response.get("logGroups", []):
        name = str(lg.get("logGroupName") or "").strip()
        if not name:
            continue
        items.append({"logGroupName": name, "arn": lg.get("arn")})

    return _ok_response(200, {"items": items[:page_size], "nextToken": response.get("nextToken")})


def _handle_metrics(
    region: str,
    q: str | None,
    page_size: int,
    next_token: str | None,
    query: dict,
) -> dict:
    namespace, error = _require_non_empty(query, "namespace")
    if error:
        return _error_response(400, "VALIDATION_ERROR", error)

    client = boto3.client("cloudwatch", region_name=region)
    req: dict = {"Namespace": namespace}
    if next_token:
        req["NextToken"] = next_token

    response = client.list_metrics(**req)
    items = []
    for metric in response.get("Metrics", []):
        metric_name = str(metric.get("MetricName") or "").strip()
        if not metric_name:
            continue
        dimensions = metric.get("Dimensions", []) if isinstance(metric.get("Dimensions"), list) else []
        if q:
            q_lower = q.lower()
            dim_values = [str(d.get("Value") or "").lower() for d in dimensions if isinstance(d, dict)]
            if q_lower not in metric_name.lower() and not any(q_lower in v for v in dim_values):
                continue
        items.append(
            {
                "namespace": namespace,
                "metricName": metric_name,
                "dimensions": {
                    d["Name"]: d["Value"]
                    for d in dimensions
                    if isinstance(d, dict) and d.get("Name") and d.get("Value")
                },
            }
        )

    return _ok_response(200, {"items": items[:page_size], "nextToken": response.get("NextToken")})


def _handle_xray_services(
    region: str,
    q: str | None,
    page_size: int,
    next_token: str | None,
    query: dict,
) -> dict:
    lookback_minutes, error = _parse_lookback_minutes(query.get("lookbackMinutes"))
    if error:
        return _error_response(400, "VALIDATION_ERROR", error)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=lookback_minutes)

    client = boto3.client("xray", region_name=region)
    req: dict = {
        "StartTime": start_time,
        "EndTime": end_time,
        "TimeRangeType": "Event",
    }
    if next_token:
        req["NextToken"] = next_token

    response = client.get_trace_summaries(**req)
    found: set[str] = set()
    for summary in response.get("TraceSummaries", []):
        for service in summary.get("ServiceIds", []):
            name = str(service.get("Name") or "").strip()
            if not name:
                continue
            if q and q.lower() not in name.lower():
                continue
            found.add(name)

    items = [{"serviceName": name} for name in sorted(found)[:page_size]]
    return _ok_response(200, {"items": items, "nextToken": response.get("NextToken")})


def _handle_metric_namespaces(region: str, q: str | None, page_size: int) -> dict:
    """Return unique CloudWatch namespaces matching the optional q filter.

    Bounded to _MAX_NAMESPACE_SCAN_PAGES pages of list_metrics results.
    Response nextToken is always null — results are fully deduplicated and
    filtered before returning.
    """
    client = boto3.client("cloudwatch", region_name=region)
    namespaces: set[str] = set()
    next_token: str | None = None

    for _ in range(_MAX_NAMESPACE_SCAN_PAGES):
        req: dict = {}
        if next_token:
            req["NextToken"] = next_token
        response = client.list_metrics(**req)
        for metric in response.get("Metrics", []):
            ns = str(metric.get("Namespace") or "").strip()
            if ns:
                namespaces.add(ns)
        next_token = response.get("NextToken")
        if not next_token:
            break

    q_lower = q.lower() if q else None
    filtered = sorted(ns for ns in namespaces if not q_lower or q_lower in ns.lower())
    items = [{"namespace": ns} for ns in filtered[:page_size]]
    return _ok_response(200, {"items": items, "nextToken": None})


def _require_non_empty(query: dict, key: str) -> tuple[str | None, str | None]:
    raw = query.get(key)
    if not isinstance(raw, str) or raw.strip() == "":
        return None, f"Query parameter '{key}' is required."
    return raw.strip(), None


def _clean_optional_str(value) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None


def _parse_page_size(value) -> tuple[int, str | None]:
    if value is None:
        return DEFAULT_PAGE_SIZE, None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return 0, "Query parameter 'pageSize' must be an integer."
    if parsed <= 0:
        return 0, "Query parameter 'pageSize' must be greater than 0."
    if parsed > MAX_PAGE_SIZE:
        return MAX_PAGE_SIZE, None
    return parsed, None


def _parse_lookback_minutes(value) -> tuple[int, str | None]:
    if value is None:
        return 180, None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return 0, "Query parameter 'lookbackMinutes' must be an integer."
    if parsed < 5 or parsed > 1440:
        return 0, "Query parameter 'lookbackMinutes' must be between 5 and 1440."
    return parsed, None


def _ok_response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps(body, default=str),
    }


def _error_response(status_code: int, error_code: str, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps({"error": error_code, "message": message}),
    }


def _cors_headers() -> dict:
    allowed_origin = os.environ.get("ALLOWED_ORIGIN", "http://localhost:5173")
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": allowed_origin,
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
    }
