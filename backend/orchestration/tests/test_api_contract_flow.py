"""Lightweight in-process API flow contract test.

Validates the critical request/response sequence without deployed infrastructure:
  POST /investigations -> 202 with incidentId
  GET /investigations/{id} -> 202 RUNNING
  GET /investigations/{id}/report -> 200 final report contract fields

All AWS calls are mocked.
"""

import json
import os
from unittest.mock import MagicMock, patch

import backend.orchestration.trigger.handler as trigger_handler
import backend.orchestration.api.handler as api_handler


@patch.dict(
    os.environ,
    {
        "TABLE_NAME": "test-table",
        "CONTEXTS_TABLE_NAME": "contexts-table",
        "SFN_ARN": "arn:aws:states:eu-west-1:123:stateMachine:test",
        "S3_BUCKET": "test-bucket",
    },
)
@patch("backend.orchestration.trigger.context_snapshot.boto3")
@patch("backend.orchestration.trigger.handler.boto3")
@patch("backend.orchestration.api.handler.boto3")
def test_api_flow_trigger_status_report_contract(mock_api_boto3, mock_trigger_boto3, mock_snapshot_boto3):
    contexts_table = MagicMock()
    contexts_table.get_item.return_value = {
        "Item": {
            "PK": "CONTEXT#ctx-abc123",
            "SK": "META",
            "contextId": "ctx-abc123",
            "region": "eu-west-1",
            "logGroups": ["/aws/lambda/payment-service"],
            "metricDescriptors": [
                {"namespace": "AWS/Lambda", "metricName": "Duration", "dimensions": {"FunctionName": "payment-service"}}
            ],
            "xrayServices": ["payment-service"],
            "ownerSub": "user-1",
        }
    }
    mock_snapshot_boto3.resource.return_value.Table.return_value = contexts_table

    trigger_table = MagicMock()
    trigger_sfn = MagicMock()
    mock_trigger_boto3.resource.return_value.Table.return_value = trigger_table
    mock_trigger_boto3.client.return_value = trigger_sfn

    trigger_event = {
        "httpMethod": "POST",
        "path": "/investigations",
        "body": json.dumps(
            {
                "contextId": "ctx-abc123",
                "signalType": "latency_spike",
                "windowStart": "2026-03-12T10:00:00Z",
                "windowEnd": "2026-03-12T10:30:00Z",
            }
        ),
        "requestContext": {
            "authorizer": {
                "claims": {
                    "sub": "user-1",
                }
            }
        },
    }

    trigger_response = trigger_handler.lambda_handler(trigger_event, None)
    assert trigger_response["statusCode"] == 202
    trigger_body = json.loads(trigger_response["body"])
    incident_id = trigger_body["incidentId"]
    assert incident_id.startswith("inc-")
    assert trigger_body["status"] == "RUNNING"

    # API status read (RUNNING)
    running_meta = {
        "PK": f"INCIDENT#{incident_id}",
        "SK": "META",
        "incidentId": incident_id,
        "status": "RUNNING",
        "service": "payment-service",
        "region": "eu-west-1",
        "windowStart": "2026-03-12T10:00:00Z",
        "windowEnd": "2026-03-12T10:30:00Z",
        "createdAt": "2026-03-12T10:00:00Z",
        "updatedAt": "2026-03-12T10:00:00Z",
    }

    completed_meta = {
        **running_meta,
        "status": "COMPLETED",
        "s3ReportKey": f"investigations/{incident_id}/final-report.json",
        "summary": "Most likely cause: runtime latency regression",
        "signalType": "latency_spike",
    }

    api_table = MagicMock()
    api_table.get_item.side_effect = [
        {"Item": running_meta},
        {"Item": completed_meta},
    ]
    api_table.query.return_value = {"Items": []}
    mock_api_boto3.resource.return_value.Table.return_value = api_table

    report_payload = {
        "incidentId": incident_id,
        "summary": "Most likely cause: runtime latency regression",
        "topHypotheses": [
            {
                "cause": "runtime latency regression",
                "confidence": 0.66,
                "supportingEvidenceIds": [],
            }
        ],
        "evidenceHighlights": ["latency rose 4x"],
    }
    api_s3 = MagicMock()
    api_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=json.dumps(report_payload).encode()))
    }
    mock_api_boto3.client.return_value = api_s3

    status_event = {
        "httpMethod": "GET",
        "path": f"/investigations/{incident_id}",
        "pathParameters": {"incidentId": incident_id},
    }
    status_response = api_handler.lambda_handler(status_event, None)
    assert status_response["statusCode"] == 202
    assert json.loads(status_response["body"])["status"] == "RUNNING"

    report_event = {
        "httpMethod": "GET",
        "path": f"/investigations/{incident_id}/report",
        "pathParameters": {"incidentId": incident_id},
    }
    report_response = api_handler.lambda_handler(report_event, None)
    assert report_response["statusCode"] == 200
    report_body = json.loads(report_response["body"])
    assert report_body["incidentId"] == incident_id
    assert isinstance(report_body.get("summary"), str) and report_body["summary"].strip()
    assert isinstance(report_body.get("topHypotheses"), list)
    assert isinstance(report_body.get("evidenceHighlights"), list)
