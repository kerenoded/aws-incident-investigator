"""
Investigation API Lambda handler.

Handles read-only investigation endpoints:

  GET /investigations/{incidentId}
      Returns investigation metadata and status from DynamoDB.
      200 — metadata dict
      202 — investigation exists but is still RUNNING
      404 — investigation not found

  GET /investigations/{incidentId}/report
      Returns the full investigation report from S3.
      200 — full report JSON
      202 — investigation exists but report is not yet ready
      404 — investigation not found

See docs/API_CONTRACT.md for the full contract.

Environment variables:
    TABLE_NAME  DynamoDB table name (required).
    S3_BUCKET   S3 bucket name (required for report endpoint).
"""

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def lambda_handler(event: dict, context) -> dict:
    """Route GET requests to the appropriate handler."""
    method = event.get("httpMethod", "")
    path = event.get("path", "")
    path_params = event.get("pathParameters") or {}

    incident_id = path_params.get("incidentId")
    if not incident_id:
        return _error_response(400, "VALIDATION_ERROR", "Missing incidentId path parameter.")

    if method == "GET" and path.endswith("/report"):
        return _handle_get_report(event, incident_id)

    if method == "GET":
        return _handle_get_status(event, incident_id)

    return _error_response(405, "METHOD_NOT_ALLOWED", f"Method {method} not supported.")


# ---------------------------------------------------------------------------
# GET /investigations/{incidentId}
# ---------------------------------------------------------------------------

def _handle_get_status(event: dict, incident_id: str) -> dict:
    try:
        item = _get_dynamo_meta(incident_id)
    except ClientError as exc:
        logger.error(
            "DynamoDB read failed in get_status",
            extra={"action": "get_status", "incidentId": incident_id, "error": str(exc)},
        )
        return _error_response(500, "INTERNAL_ERROR", "Failed to read investigation.")
    if item is None:
        return _not_found_response(incident_id)

    status = item.get("status", "UNKNOWN")
    body = {
        "incidentId": incident_id,
        "status": status,
        "service": item.get("service"),
        "region": item.get("region"),
        "windowStart": item.get("windowStart"),
        "windowEnd": item.get("windowEnd"),
        "createdAt": item.get("createdAt"),
        "updatedAt": item.get("updatedAt"),
    }
    http_status = 202 if status == "RUNNING" else 200
    return _ok_response(http_status, body)


# ---------------------------------------------------------------------------
# GET /investigations/{incidentId}/report
# ---------------------------------------------------------------------------

def _handle_get_report(event: dict, incident_id: str) -> dict:
    try:
        item = _get_dynamo_meta(incident_id)
    except ClientError as exc:
        logger.error(
            "DynamoDB read failed in get_report",
            extra={"action": "get_report", "incidentId": incident_id, "error": str(exc)},
        )
        return _error_response(500, "INTERNAL_ERROR", "Failed to read investigation.")
    if item is None:
        return _not_found_response(incident_id)

    status = item.get("status", "UNKNOWN")
    if status != "COMPLETED":
        return _ok_response(202, {"incidentId": incident_id, "status": status})

    s3_key = item.get("s3ReportKey")
    if not s3_key:
        return _error_response(
            500, "INTERNAL_ERROR", "Report key not found in investigation record."
        )

    bucket = _require_env("S3_BUCKET")
    report = _fetch_s3_json(bucket, s3_key)
    if report is None:
        return _error_response(500, "INTERNAL_ERROR", "Failed to fetch report from storage.")

    validation_error = _validate_report_for_response(report)
    if validation_error:
        logger.error(
            "Stored report failed contract validation",
            extra={
                "step": "api",
                "action": "validate_report",
                "incidentId": incident_id,
                "error": validation_error,
            },
        )
        return _error_response(500, "INTERNAL_ERROR", "Stored report failed contract validation.")

    return _ok_response(200, report)


# ---------------------------------------------------------------------------
# DynamoDB + S3 helpers
# ---------------------------------------------------------------------------

def _get_dynamo_meta(incident_id: str) -> dict | None:
    """Fetch the META item for an investigation. Returns None if not found.

    Raises ClientError on DynamoDB read failures (e.g. throttling, permissions).
    Callers are responsible for distinguishing not-found (None) from read errors.
    """
    table_name = _require_env("TABLE_NAME")
    table = boto3.resource("dynamodb").Table(table_name)
    try:
        response = table.get_item(
            Key={"PK": f"INCIDENT#{incident_id}", "SK": "META"}
        )
    except ClientError as exc:
        logger.warning(
            "DynamoDB GetItem failed",
            extra={"action": "get_dynamo_meta", "incidentId": incident_id, "error": str(exc)},
        )
        raise
    return response.get("Item")


def _fetch_s3_json(bucket: str, key: str) -> dict | None:
    """Fetch and parse a JSON object from S3. Returns None on any error."""
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "S3 GetObject failed",
            extra={"action": "fetch_s3_json", "bucket": bucket, "key": key, "error": str(exc)},
        )
        return None


def _validate_report_for_response(report: dict) -> str | None:
    """Lightweight validation at API response boundary for stored reports."""
    if not isinstance(report, dict):
        return "Report must be a JSON object."

    for field in ("incidentId", "summary", "topHypotheses", "evidenceHighlights"):
        if field not in report:
            return f"Report missing required field: {field}"

    if not isinstance(report["incidentId"], str) or report["incidentId"].strip() == "":
        return "Report incidentId must be a non-empty string."
    if not isinstance(report["summary"], str) or report["summary"].strip() == "":
        return "Report summary must be a non-empty string."
    if not isinstance(report["topHypotheses"], list):
        return "Report topHypotheses must be a list."
    if not isinstance(report["evidenceHighlights"], list):
        return "Report evidenceHighlights must be a list."

    return None


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


def _ok_response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps(body),
    }


def _error_response(status_code: int, error_code: str, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps({"error": error_code, "message": message}),
    }


def _not_found_response(incident_id: str) -> dict:
    return _error_response(404, "NOT_FOUND", f"Investigation {incident_id} not found.")


def _cors_headers() -> dict:
    allowed_origin = os.environ.get("ALLOWED_ORIGIN", "http://localhost:5173")
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": allowed_origin,
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    }
