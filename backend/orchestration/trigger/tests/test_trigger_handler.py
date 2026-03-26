"""Unit tests for context-only trigger handler behavior."""

import json
import os
from unittest.mock import MagicMock, patch

import jsonschema
import backend.orchestration.trigger.handler as trigger_handler
from referencing import Registry, Resource

_SCHEMAS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "schemas")
)


_CONTEXT_BODY = {
    "contextId": "ctx-123",
    "signalType": "latency_spike",
    "windowStart": "2026-03-12T10:00:00Z",
    "windowEnd": "2026-03-12T10:30:00Z",
}

_CONTEXT_EVENT = {
    "httpMethod": "POST",
    "path": "/investigations",
    "body": json.dumps(_CONTEXT_BODY),
    "requestContext": {
        "authorizer": {
            "claims": {
                "sub": "user-1",
            }
        }
    },
}

_CONTEXT_ITEM = {
    "PK": "CONTEXT#ctx-123",
    "SK": "META",
    "contextId": "ctx-123",
    "region": "eu-west-1",
    "logGroups": ["/aws/lambda/payment-service"],
    "metricDescriptors": [{"namespace": "AWS/Lambda", "metricName": "Duration"}],
    "xrayServices": ["payment-service"],
    "ownerSub": "user-1",
}


def _mock_table():
    table = MagicMock()
    table.put_item.return_value = {}
    table.update_item.return_value = {}
    return table


@patch.dict(
    os.environ,
    {
        "TABLE_NAME": "test-table",
        "CONTEXTS_TABLE_NAME": "contexts-table",
        "SFN_ARN": "arn:aws:states:eu-west-1:123:stateMachine:test",
    },
)
@patch("backend.orchestration.trigger.context_snapshot.boto3")
@patch("backend.orchestration.trigger.handler.boto3")
def test_context_only_request_starts_execution(mock_boto3, mock_snapshot_boto3):
    contexts_table = MagicMock()
    contexts_table.get_item.return_value = {"Item": _CONTEXT_ITEM}
    mock_snapshot_boto3.resource.return_value.Table.return_value = contexts_table

    incidents_table = _mock_table()
    mock_boto3.resource.return_value.Table.return_value = incidents_table
    mock_sfn = MagicMock()
    mock_boto3.client.return_value = mock_sfn

    response = trigger_handler.lambda_handler(_CONTEXT_EVENT, None)

    assert response["statusCode"] == 202
    payload = json.loads(mock_sfn.start_execution.call_args.kwargs["input"])
    assert payload["triggerModel"] == "context"
    assert payload["contextId"] == "ctx-123"
    assert isinstance(payload["contextSnapshot"], dict)


def test_service_only_request_rejected():
    event = {
        **_CONTEXT_EVENT,
        "body": json.dumps(
            {
                "service": "payment-service",
                "region": "eu-west-1",
                "signalType": "latency_spike",
                "windowStart": "2026-03-12T10:00:00Z",
                "windowEnd": "2026-03-12T10:30:00Z",
            }
        ),
    }

    response = trigger_handler.lambda_handler(event, None)
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "VALIDATION_ERROR"


def test_service_and_context_request_rejected():
    event = {
        **_CONTEXT_EVENT,
        "body": json.dumps({**_CONTEXT_BODY, "service": "payment-service", "region": "eu-west-1"}),
    }

    response = trigger_handler.lambda_handler(event, None)
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "VALIDATION_ERROR"


@patch.dict(
    os.environ,
    {
        "TABLE_NAME": "test-table",
        "CONTEXTS_TABLE_NAME": "contexts-table",
        "SFN_ARN": "arn:aws:states:eu-west-1:123:stateMachine:test",
    },
)
@patch("backend.orchestration.trigger.context_snapshot.boto3")
@patch("backend.orchestration.trigger.handler.boto3")
def test_sfn_payload_conforms_to_incident_schema(mock_boto3, mock_snapshot_boto3):
    """Step Functions execution input must conform to schemas/incident.schema.json."""
    contexts_table = MagicMock()
    contexts_table.get_item.return_value = {"Item": _CONTEXT_ITEM}
    mock_snapshot_boto3.resource.return_value.Table.return_value = contexts_table

    incidents_table = _mock_table()
    mock_boto3.resource.return_value.Table.return_value = incidents_table
    mock_sfn = MagicMock()
    mock_boto3.client.return_value = mock_sfn

    trigger_handler.lambda_handler(_CONTEXT_EVENT, None)

    payload = json.loads(mock_sfn.start_execution.call_args.kwargs["input"])

    with open(os.path.join(_SCHEMAS_DIR, "incident.schema.json")) as f:
        incident_schema = json.load(f)
    with open(os.path.join(_SCHEMAS_DIR, "context-snapshot.schema.json")) as f:
        snapshot_schema = json.load(f)

    registry = Registry().with_resource(
        uri="./context-snapshot.schema.json",
        resource=Resource.from_contents(snapshot_schema),
    )
    validator = jsonschema.Draft202012Validator(incident_schema, registry=registry)
    validator.validate(payload)
