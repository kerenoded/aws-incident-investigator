"""
Trigger Lambda handler.

Entry point for manual investigation requests via POST /investigations
(API Gateway Lambda proxy integration).

Responsibility:
  1. Validate required fields from the request body.
  2. Generate a stable incidentId.
  3. Write a RUNNING record to the DynamoDB Investigations table.
  4. Start the Step Functions execution with the incident payload.
  5. Return HTTP 202 with { incidentId, status: "RUNNING" }.

Alarm-driven path (EventBridge → Lambda) is not yet implemented.
A comment block below marks where that normalizer would be added.
See docs/API_CONTRACT.md for the full API contract.

Environment variables:
    TABLE_NAME  DynamoDB table name (required).
    SFN_ARN     Step Functions state machine ARN (required).
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from shared.authz.context_access import extract_cognito_sub, is_owner

from .context_snapshot import build_context_snapshot, get_context_meta
from .idempotency import (
    extract_idempotency_key,
    get_incident_status,
    reserve_or_get_existing_incident,
)
from .validation import (
    is_non_empty_string,
    require_env,
    validate_context_body,
    validate_normalized_payload,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_DEFAULT_TTL_RETENTION_DAYS = 30
_DEFAULT_PAYLOAD_WARNING_THRESHOLD_BYTES = 180 * 1024
_IDEMPOTENCY_TTL_DAYS = 7


def lambda_handler(event: dict, context) -> dict:
    """Route to the appropriate trigger handler based on event source.

    Handles:
      - API Gateway proxy events (manual trigger via POST /investigations)

    Not yet implemented:
      - EventBridge CloudWatch Alarm state-change events (alarm-driven trigger)
        Would be added here: detect event["source"] == "aws.cloudwatch" and
        normalise the alarm payload into an incident payload before proceeding
        with the same DynamoDB write + Step Functions start flow.
    """
    if "httpMethod" in event or "requestContext" in event:
        return _handle_api_gateway(event)

    # Alarm-driven path placeholder — not yet implemented
    logger.warning(
        "Received unsupported trigger event shape",
        extra={"step": "trigger", "action": "route", "eventKeys": list(event.keys())},
    )
    return {
        "statusCode": 400,
        "headers": _cors_headers(),
        "body": json.dumps({
            "error": "UNSUPPORTED_TRIGGER",
            "message": "Only API Gateway triggers (POST /investigations) are supported in this version.",
        }),
    }


# ---------------------------------------------------------------------------
# Manual trigger via API Gateway
# ---------------------------------------------------------------------------

def _handle_api_gateway(event: dict) -> dict:
    body_str = event.get("body") or "{}"
    try:
        body = json.loads(body_str)
    except (json.JSONDecodeError, TypeError):
        return _error_response(400, "VALIDATION_ERROR", "Request body must be valid JSON.")

    if not isinstance(body, dict):
        return _error_response(400, "VALIDATION_ERROR", "Request body must be a JSON object.")

    has_context_id = is_non_empty_string(body.get("contextId"))
    has_service = is_non_empty_string(body.get("service"))
    if has_service:
        return _error_response(
            400,
            "VALIDATION_ERROR",
            "Legacy service-based trigger payloads are no longer supported. Use 'contextId'.",
        )

    if not has_context_id:
        return _error_response(
            400,
            "VALIDATION_ERROR",
            "Request must include 'contextId'.",
        )

    validation_error = validate_context_body(body)
    if validation_error:
        return _error_response(400, "VALIDATION_ERROR", validation_error)
    trigger_mode = "context"

    caller_sub = extract_cognito_sub(event)
    if caller_sub is None:
        return _error_response(403, "AUTHZ_FORBIDDEN", "Missing caller identity.")

    context_item, read_error = get_context_meta(body["contextId"])
    if read_error:
        return _error_response(500, "INTERNAL_ERROR", "Failed to read incident context.")
    if context_item is None:
        return _error_response(404, "NOT_FOUND", f"Incident context {body['contextId']} not found.")
    if not is_owner(owner_sub=context_item.get("ownerSub"), caller_sub=caller_sub):
        return _error_response(404, "NOT_FOUND", f"Incident context {body['contextId']} not found.")

    snapshot_or_error = build_context_snapshot(context_item)
    if isinstance(snapshot_or_error, str):
        return _error_response(400, "VALIDATION_ERROR", snapshot_or_error)
    context_snapshot = snapshot_or_error

    service = context_snapshot["service"]
    region = context_snapshot["region"]

    now = datetime.now(timezone.utc)
    incident_id = _generate_incident_id(now)

    payload = {
        "incidentId": incident_id,
        "service": service,
        "signalType": body["signalType"],
        "windowStart": body["windowStart"],
        "windowEnd": body["windowEnd"],
        "region": region,
        "triggerSource": "manual",
        "triggerModel": trigger_mode,
    }
    payload["contextId"] = body["contextId"]
    payload["contextSnapshot"] = context_snapshot

    # Pass through optional fields if provided
    for optional in ("environment", "severity"):
        if optional in body:
            payload[optional] = body[optional]

    normalized_payload_error = validate_normalized_payload(payload)
    if normalized_payload_error:
        return _error_response(400, "VALIDATION_ERROR", normalized_payload_error)

    _warn_if_payload_large(
        payload=payload,
        incident_id=incident_id,
        step="trigger",
    )

    table_name = require_env("TABLE_NAME")
    sfn_arn = require_env("SFN_ARN")
    created_at = now.isoformat()
    ttl = _compute_ttl_epoch(now)
    idempotency_ttl = int(now.timestamp()) + (_IDEMPOTENCY_TTL_DAYS * 24 * 60 * 60)
    try:
        idempotency_key = extract_idempotency_key(event)
    except ValueError as exc:
        return _error_response(400, "VALIDATION_ERROR", str(exc))

    idempotency_scope = f"context:{body['contextId']}"

    logger.info(
        "Starting investigation",
        extra={
            "step": "trigger",
            "action": "start",
            "incidentId": incident_id,
            "service": service,
            "triggerModel": trigger_mode,
        },
    )

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    if idempotency_key:
        try:
            existing_incident_id = reserve_or_get_existing_incident(
                table=table,
                idempotency_scope=idempotency_scope,
                idempotency_key=idempotency_key,
                incident_id=incident_id,
                created_at=created_at,
                ttl=idempotency_ttl,
            )
        except ClientError as exc:
            logger.error(
                "Failed processing idempotency key",
                extra={
                    "step": "trigger",
                    "action": "idempotency",
                    "incidentId": incident_id,
                    "error": str(exc),
                },
            )
            return _error_response(500, "INTERNAL_ERROR", "Failed to process idempotency key.")

        if existing_incident_id:
            status = get_incident_status(table, existing_incident_id) or "RUNNING"
            return {
                "statusCode": 202,
                "headers": _cors_headers(),
                "body": json.dumps(
                    {
                        "incidentId": existing_incident_id,
                        "status": status,
                        "duplicateRequest": True,
                    }
                ),
            }

    try:
        table.put_item(
            Item={
                "PK": f"INCIDENT#{incident_id}",
                "SK": "META",
                "incidentId": incident_id,
                "status": "RUNNING",
                "service": service,
                "signalType": body["signalType"],
                "region": region,
                "windowStart": body["windowStart"],
                "windowEnd": body["windowEnd"],
                "triggerSource": "manual",
                "triggerModel": trigger_mode,
                "createdAt": created_at,
                "updatedAt": created_at,
                "ttl": ttl,
                "contextId": body["contextId"],
            }
        )
    except ClientError as exc:
        logger.error(
            "DynamoDB put_item failed in trigger",
            extra={
                "step": "trigger",
                "action": "put_meta",
                "incidentId": incident_id,
                "error": str(exc),
            },
        )
        return _error_response(500, "INTERNAL_ERROR", "Failed to create investigation metadata.")

    sfn_client = boto3.client("stepfunctions")
    try:
        sfn_client.start_execution(
            stateMachineArn=sfn_arn,
            name=incident_id,
            input=json.dumps(payload),
        )
    except ClientError as exc:
        logger.error(
            "Step Functions start_execution failed; marking investigation FAILED",
            extra={
                "step": "trigger",
                "action": "start_execution",
                "incidentId": incident_id,
                "error": str(exc),
            },
        )
        _mark_investigation_failed(table, incident_id, now)
        return _error_response(500, "INTERNAL_ERROR", "Failed to start investigation workflow.")

    return {
        "statusCode": 202,
        "headers": _cors_headers(),
        "body": json.dumps({"incidentId": incident_id, "status": "RUNNING"}),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mark_investigation_failed(table, incident_id: str, now: datetime) -> None:
    """Update the DynamoDB item to FAILED after a workflow start failure.

    Retries once on ClientError. If both attempts fail, logs and returns
    without raising — accepting the stuck-RUNNING record as a rare edge case.
    """
    update_kwargs = {
        "Key": {"PK": f"INCIDENT#{incident_id}", "SK": "META"},
        "UpdateExpression": "SET #s = :status, updatedAt = :updatedAt, errorReason = :reason",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {
            ":status": "FAILED",
            ":updatedAt": now.isoformat(),
            ":reason": "workflow_start_failed",
        },
    }
    for attempt in range(2):
        try:
            table.update_item(**update_kwargs)
            return
        except ClientError as exc:
            if attempt == 0:
                logger.warning(
                    "DynamoDB update-to-FAILED failed; retrying once",
                    extra={"step": "trigger", "action": "mark_failed", "incidentId": incident_id, "error": str(exc)},
                )
            else:
                logger.error(
                    "DynamoDB update-to-FAILED failed after retry; investigation may be stuck in RUNNING",
                    extra={"step": "trigger", "action": "mark_failed", "incidentId": incident_id, "error": str(exc)},
                )


def _compute_ttl_epoch(now: datetime) -> int:
    """Compute DynamoDB TTL epoch from retention-days configuration."""
    retention_days_raw = os.environ.get("TTL_RETENTION_DAYS", str(_DEFAULT_TTL_RETENTION_DAYS))
    try:
        retention_days = int(retention_days_raw)
    except ValueError:
        retention_days = _DEFAULT_TTL_RETENTION_DAYS

    if retention_days <= 0:
        retention_days = _DEFAULT_TTL_RETENTION_DAYS

    return int(now.timestamp()) + (retention_days * 24 * 60 * 60)


def _payload_warning_threshold_bytes() -> int:
    raw = os.environ.get("PAYLOAD_WARNING_THRESHOLD_BYTES", str(_DEFAULT_PAYLOAD_WARNING_THRESHOLD_BYTES))
    try:
        value = int(raw)
    except ValueError:
        value = _DEFAULT_PAYLOAD_WARNING_THRESHOLD_BYTES
    return max(1, value)


def _warn_if_payload_large(*, payload: dict, incident_id: str, step: str) -> None:
    payload_bytes = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    warning_threshold_bytes = _payload_warning_threshold_bytes()
    if payload_bytes >= warning_threshold_bytes:
        logger.warning(
            "Payload size warning threshold reached",
            extra={
                "incidentId": incident_id,
                "step": step,
                "action": "payload_size_warning",
                "payloadBytes": payload_bytes,
                "warningThresholdBytes": warning_threshold_bytes,
            },
        )


def _generate_incident_id(now: datetime) -> str:
    date_str = now.strftime("%Y%m%d")
    short_id = uuid.uuid4().hex[:8]
    return f"inc-{date_str}-{short_id}"


def _cors_headers() -> dict:
    allowed_origin = os.environ.get("ALLOWED_ORIGIN", "http://localhost:5173")
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": allowed_origin,
    }


def _error_response(status_code: int, error_code: str, message: str) -> dict:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps({"error": error_code, "message": message}),
    }
