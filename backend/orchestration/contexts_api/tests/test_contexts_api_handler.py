import json
import os
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

import backend.orchestration.contexts_api.handler as contexts_handler


def _event(method: str, *, body: dict | None = None, context_id: str | None = None, sub: str = "user-1") -> dict:
    return {
        "httpMethod": method,
        "pathParameters": {"contextId": context_id} if context_id else None,
        "body": json.dumps(body) if body is not None else None,
        "requestContext": {
            "authorizer": {
                "claims": {
                    "sub": sub,
                }
            }
        },
    }


def _context_item(context_id: str = "ctx-123") -> dict:
    return {
        "PK": f"CONTEXT#{context_id}",
        "SK": "META",
        "contextId": context_id,
        "name": "Payments",
        "description": "Payment incident context",
        "region": "eu-west-1",
        "logGroups": ["/aws/lambda/payments"],
        "metricDescriptors": [{"namespace": "AWS/Lambda", "metricName": "Duration"}],
        "xrayServices": ["payments"],
        "ownerSub": "user-1",
        "createdBy": "user-1",
        "updatedBy": "user-1",
        "createdAt": "2026-01-01T00:00:00+00:00",
        "updatedAt": "2026-01-01T00:00:00+00:00",
    }


def _client_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "injected"}},
        "DynamoDB",
    )


@patch.dict(os.environ, {"CONTEXTS_TABLE_NAME": "IncidentContexts"})
@patch("backend.orchestration.contexts_api.handler.boto3")
def test_create_context_happy_path(mock_boto3):
    table = MagicMock()
    mock_boto3.resource.return_value.Table.return_value = table

    response = contexts_handler.lambda_handler(
        _event(
            "POST",
            body={
                "name": "Payments",
                "description": "desc",
                "region": "eu-west-1",
                "logGroups": ["/aws/lambda/payments"],
                "metricDescriptors": [{"namespace": "AWS/Lambda", "metricName": "Duration"}],
                "xrayServices": ["payments"],
            },
        ),
        None,
    )

    assert response["statusCode"] == 201
    body = json.loads(response["body"])
    assert body["contextId"].startswith("ctx-")
    assert body["name"] == "Payments"
    table.put_item.assert_called_once()


@patch.dict(os.environ, {"CONTEXTS_TABLE_NAME": "IncidentContexts"})
@patch("backend.orchestration.contexts_api.handler.boto3")
def test_list_contexts_owner_only(mock_boto3):
    table = MagicMock()
    table.query.return_value = {"Items": [_context_item()]}
    mock_boto3.resource.return_value.Table.return_value = table

    response = contexts_handler.lambda_handler(_event("GET", body=None), None)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert len(body["items"]) == 1
    assert body["items"][0]["contextId"] == "ctx-123"
    kwargs = table.query.call_args.kwargs
    assert kwargs["IndexName"] == "OwnerUpdatedAtIndex"


@patch.dict(os.environ, {"CONTEXTS_TABLE_NAME": "IncidentContexts"})
@patch("backend.orchestration.contexts_api.handler.boto3")
def test_get_by_id_unauthorized_owner_returns_404(mock_boto3):
    table = MagicMock()
    table.get_item.return_value = {"Item": _context_item()}
    mock_boto3.resource.return_value.Table.return_value = table

    response = contexts_handler.lambda_handler(_event("GET", context_id="ctx-123", sub="user-2"), None)

    assert response["statusCode"] == 404
    assert json.loads(response["body"])["error"] == "NOT_FOUND"


@patch.dict(os.environ, {"CONTEXTS_TABLE_NAME": "IncidentContexts"})
@patch("backend.orchestration.contexts_api.handler.boto3")
def test_patch_updates_context_for_owner(mock_boto3):
    table = MagicMock()
    table.get_item.return_value = {"Item": _context_item()}
    mock_boto3.resource.return_value.Table.return_value = table

    response = contexts_handler.lambda_handler(
        _event("PATCH", context_id="ctx-123", body={"name": "Payments v2", "region": "us-east-1"}),
        None,
    )

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["name"] == "Payments v2"
    assert body["region"] == "us-east-1"
    table.put_item.assert_called_once()


@patch.dict(os.environ, {"CONTEXTS_TABLE_NAME": "IncidentContexts"})
@patch("backend.orchestration.contexts_api.handler.boto3")
def test_patch_rejects_unknown_fields(mock_boto3):
    table = MagicMock()
    mock_boto3.resource.return_value.Table.return_value = table

    response = contexts_handler.lambda_handler(
        _event("PATCH", context_id="ctx-123", body={"unknown": "x"}),
        None,
    )

    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "VALIDATION_ERROR"
    table.get_item.assert_not_called()


@patch.dict(os.environ, {"CONTEXTS_TABLE_NAME": "IncidentContexts"})
@patch("backend.orchestration.contexts_api.handler.boto3")
def test_db_client_error_returns_500(mock_boto3):
    table = MagicMock()
    table.query.side_effect = _client_error()
    mock_boto3.resource.return_value.Table.return_value = table

    response = contexts_handler.lambda_handler(_event("GET", body=None), None)

    assert response["statusCode"] == 500
    assert json.loads(response["body"])["error"] == "INTERNAL_ERROR"
