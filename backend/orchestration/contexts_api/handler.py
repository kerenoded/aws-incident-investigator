"""Incident context CRUD API Lambda handler.

Endpoints:
  POST /incident-contexts
  GET  /incident-contexts
  GET  /incident-contexts/{contextId}
  PATCH /incident-contexts/{contextId}
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from shared.authz.context_access import extract_cognito_sub, is_owner

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def lambda_handler(event: dict, context) -> dict:
    method = event.get("httpMethod", "")
    path_params = event.get("pathParameters") or {}
    context_id = path_params.get("contextId")

    if method == "POST":
        return _handle_create(event)
    if method == "GET" and context_id:
        return _handle_get_by_id(event, context_id)
    if method == "GET":
        return _handle_list(event)
    if method == "PATCH" and context_id:
        return _handle_patch(event, context_id)
    if method == "DELETE" and context_id:
        return _handle_delete(event, context_id)

    return _error_response(405, "METHOD_NOT_ALLOWED", f"Method {method} not supported.")


def _handle_create(event: dict) -> dict:
    caller_sub = extract_cognito_sub(event)
    if caller_sub is None:
        return _error_response(403, "AUTHZ_FORBIDDEN", "Missing caller identity.")

    body, error = _parse_body(event)
    if error:
        return _error_response(400, "VALIDATION_ERROR", error)

    validation_error = _validate_create_payload(body)
    if validation_error:
        return _error_response(400, "VALIDATION_ERROR", validation_error)

    now = _now_iso()
    context_id = f"ctx-{uuid.uuid4().hex[:12]}"
    item = {
        "PK": f"CONTEXT#{context_id}",
        "SK": "META",
        "entityType": "incidentContext",
        "contextId": context_id,
        "name": body["name"].strip(),
        "description": body.get("description", "").strip(),
        "region": body["region"].strip(),
        "logGroups": _clean_string_list(body.get("logGroups", [])),
        "metricDescriptors": _clean_object_list(body.get("metricDescriptors", [])),
        "xrayServices": _clean_string_list(body.get("xrayServices", [])),
        "ownerSub": caller_sub,
        "createdBy": caller_sub,
        "updatedBy": caller_sub,
        "createdAt": now,
        "updatedAt": now,
    }

    try:
        _table().put_item(Item=item)
    except ClientError:
        logger.exception("DynamoDB put_item failed for contextId=%s", context_id)
        return _error_response(500, "INTERNAL_ERROR", "Failed to create incident context.")

    return _ok_response(201, _to_response_item(item))


def _handle_list(event: dict) -> dict:
    caller_sub = extract_cognito_sub(event)
    if caller_sub is None:
        return _error_response(403, "AUTHZ_FORBIDDEN", "Missing caller identity.")

    try:
        response = _table().query(
            IndexName="OwnerUpdatedAtIndex",
            KeyConditionExpression=Key("ownerSub").eq(caller_sub),
            ProjectionExpression=(
                "contextId, #nm, #desc, #region, logGroups, metricDescriptors, "
                "xrayServices, createdAt, updatedAt, createdBy, updatedBy, ownerSub"
            ),
            ExpressionAttributeNames={
                "#nm": "name",
                "#desc": "description",
                "#region": "region",
            },
            ScanIndexForward=False,
            Limit=100,
        )
    except ClientError:
        logger.exception("DynamoDB query failed for OwnerUpdatedAtIndex caller=%s", caller_sub)
        return _error_response(500, "INTERNAL_ERROR", "Failed to list incident contexts.")

    items = response.get("Items", []) if isinstance(response, dict) else []
    return _ok_response(200, {"items": [_to_response_item(i) for i in items if isinstance(i, dict)]})


def _handle_get_by_id(event: dict, context_id: str) -> dict:
    caller_sub = extract_cognito_sub(event)
    if caller_sub is None:
        return _error_response(403, "AUTHZ_FORBIDDEN", "Missing caller identity.")

    item, read_error = _get_context_meta(context_id)
    if read_error:
        return _error_response(500, "INTERNAL_ERROR", "Failed to read incident context.")
    if item is None:
        return _error_response(404, "NOT_FOUND", f"Incident context {context_id} not found.")

    if not is_owner(owner_sub=item.get("ownerSub"), caller_sub=caller_sub):
        return _error_response(404, "NOT_FOUND", f"Incident context {context_id} not found.")

    return _ok_response(200, _to_response_item(item))


def _handle_patch(event: dict, context_id: str) -> dict:
    caller_sub = extract_cognito_sub(event)
    if caller_sub is None:
        return _error_response(403, "AUTHZ_FORBIDDEN", "Missing caller identity.")

    body, error = _parse_body(event)
    if error:
        return _error_response(400, "VALIDATION_ERROR", error)

    validation_error = _validate_patch_payload(body)
    if validation_error:
        return _error_response(400, "VALIDATION_ERROR", validation_error)

    item, read_error = _get_context_meta(context_id)
    if read_error:
        return _error_response(500, "INTERNAL_ERROR", "Failed to read incident context.")
    if item is None:
        return _error_response(404, "NOT_FOUND", f"Incident context {context_id} not found.")

    if not is_owner(owner_sub=item.get("ownerSub"), caller_sub=caller_sub):
        return _error_response(404, "NOT_FOUND", f"Incident context {context_id} not found.")

    updated_item = dict(item)
    for key in ("name", "description", "region"):
        if key in body:
            updated_item[key] = body[key].strip() if isinstance(body[key], str) else body[key]

    if "logGroups" in body:
        updated_item["logGroups"] = _clean_string_list(body.get("logGroups", []))
    if "metricDescriptors" in body:
        updated_item["metricDescriptors"] = _clean_object_list(body.get("metricDescriptors", []))
    if "xrayServices" in body:
        updated_item["xrayServices"] = _clean_string_list(body.get("xrayServices", []))

    updated_item["updatedAt"] = _now_iso()
    updated_item["updatedBy"] = caller_sub

    try:
        _table().put_item(Item=updated_item)
    except ClientError:
        logger.exception("DynamoDB put_item failed on patch for contextId=%s", context_id)
        return _error_response(500, "INTERNAL_ERROR", "Failed to update incident context.")

    return _ok_response(200, _to_response_item(updated_item))


def _handle_delete(event: dict, context_id: str) -> dict:
    caller_sub = extract_cognito_sub(event)
    if caller_sub is None:
        return _error_response(403, "AUTHZ_FORBIDDEN", "Missing caller identity.")

    item, read_error = _get_context_meta(context_id)
    if read_error:
        return _error_response(500, "INTERNAL_ERROR", "Failed to read incident context.")
    if item is None:
        return _error_response(404, "NOT_FOUND", f"Incident context {context_id} not found.")

    if not is_owner(owner_sub=item.get("ownerSub"), caller_sub=caller_sub):
        return _error_response(404, "NOT_FOUND", f"Incident context {context_id} not found.")

    try:
        _table().delete_item(Key={"PK": f"CONTEXT#{context_id}", "SK": "META"})
    except ClientError:
        logger.exception("DynamoDB delete_item failed for contextId=%s", context_id)
        return _error_response(500, "INTERNAL_ERROR", "Failed to delete incident context.")

    return {"statusCode": 204, "headers": _cors_headers(), "body": ""}


def _get_context_meta(context_id: str) -> tuple[dict | None, bool]:
    try:
        response = _table().get_item(Key={"PK": f"CONTEXT#{context_id}", "SK": "META"})
    except ClientError:
        logger.exception("DynamoDB get_item failed for contextId=%s", context_id)
        return None, True
    return response.get("Item"), False


def _table():
    return boto3.resource("dynamodb").Table(_require_env("CONTEXTS_TABLE_NAME"))


def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


def _parse_body(event: dict) -> tuple[dict, str | None]:
    body_str = event.get("body") or "{}"
    try:
        body = json.loads(body_str)
    except (json.JSONDecodeError, TypeError):
        return {}, "Request body must be valid JSON."
    if not isinstance(body, dict):
        return {}, "Request body must be a JSON object."
    return body, None


def _validate_create_payload(body: dict) -> str | None:
    for field in ("name", "region"):
        if not _is_non_empty_string(body.get(field)):
            return f"Field '{field}' must be a non-empty string."

    if "description" in body and not isinstance(body.get("description"), str):
        return "Field 'description' must be a string when provided."

    return _validate_collection_fields(body)


def _validate_patch_payload(body: dict) -> str | None:
    allowed = {
        "name",
        "description",
        "region",
        "logGroups",
        "metricDescriptors",
        "xrayServices",
    }
    unknown = [k for k in body.keys() if k not in allowed]
    if unknown:
        return f"Unknown fields are not allowed in patch: {sorted(unknown)}"
    if not body:
        return "Patch body must include at least one updatable field."

    for field in ("name", "region"):
        if field in body and not _is_non_empty_string(body.get(field)):
            return f"Field '{field}' must be a non-empty string when provided."
    if "description" in body and not isinstance(body.get("description"), str):
        return "Field 'description' must be a string when provided."

    return _validate_collection_fields(body)


def _validate_collection_fields(body: dict) -> str | None:
    list_string_fields = ("logGroups", "xrayServices")
    for field in list_string_fields:
        if field in body and not _is_list_of_non_empty_strings(body[field]):
            return f"Field '{field}' must be an array of non-empty strings when provided."

    if "metricDescriptors" in body and not _is_list_of_objects(body["metricDescriptors"]):
        return "Field 'metricDescriptors' must be an array of objects when provided."

    return None


def _is_non_empty_string(value) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _is_list_of_non_empty_strings(value) -> bool:
    if not isinstance(value, list):
        return False
    return all(isinstance(v, str) and v.strip() != "" for v in value)


def _is_list_of_objects(value) -> bool:
    if not isinstance(value, list):
        return False
    return all(isinstance(v, dict) for v in value)


def _clean_string_list(values: list) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(v).strip() for v in values if isinstance(v, str) and v.strip() != ""]


def _clean_object_list(values: list) -> list[dict]:
    if not isinstance(values, list):
        return []
    return [v for v in values if isinstance(v, dict)]


def _to_response_item(item: dict) -> dict:
    return {
        "contextId": item.get("contextId"),
        "name": item.get("name"),
        "description": item.get("description", ""),
        "region": item.get("region"),
        "logGroups": item.get("logGroups", []),
        "metricDescriptors": item.get("metricDescriptors", []),
        "xrayServices": item.get("xrayServices", []),
        "createdAt": item.get("createdAt"),
        "updatedAt": item.get("updatedAt"),
        "createdBy": item.get("createdBy"),
        "updatedBy": item.get("updatedBy"),
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _cors_headers() -> dict:
    allowed_origin = os.environ.get("ALLOWED_ORIGIN", "http://localhost:5173")
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": allowed_origin,
        "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key",
        "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS",
    }
