"""
Unit tests for backend/orchestration/api/handler.py.

Covers:
  GET /investigations/{incidentId}
    - RUNNING investigation → 202
    - COMPLETED investigation → 200
    - Investigation not found → 404 NOT_FOUND
    - DynamoDB ClientError → 500 INTERNAL_ERROR

  GET /investigations/{incidentId}/report
    - Investigation not found → 404 NOT_FOUND
    - Investigation still RUNNING → 202 REPORT_NOT_READY body
    - COMPLETED but missing s3ReportKey → 500 INTERNAL_ERROR
    - COMPLETED but S3 fetch fails → 500 INTERNAL_ERROR
    - COMPLETED but DynamoDB raises ClientError → 500 INTERNAL_ERROR
    - Happy path: COMPLETED with valid report → 200
"""

import json
import os

from botocore.exceptions import ClientError
from unittest.mock import MagicMock, patch

import backend.orchestration.api.handler as api_handler


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_INCIDENT_ID = "inc-20260312-abcd1234"

_STATUS_EVENT = {
    "httpMethod": "GET",
    "path": f"/investigations/{_INCIDENT_ID}",
    "pathParameters": {"incidentId": _INCIDENT_ID},
    "queryStringParameters": None,
    "requestContext": {
        "authorizer": {
            "claims": {}
        }
    },
}

_REPORT_EVENT = {
    "httpMethod": "GET",
    "path": f"/investigations/{_INCIDENT_ID}/report",
    "pathParameters": {"incidentId": _INCIDENT_ID},
    "queryStringParameters": None,
    "requestContext": {
        "authorizer": {
            "claims": {}
        }
    },
}

_META_RUNNING = {
    "PK": f"INCIDENT#{_INCIDENT_ID}",
    "SK": "META",
    "incidentId": _INCIDENT_ID,
    "status": "RUNNING",
    "service": "payment-service",
    "region": "eu-west-1",
    "windowStart": "2026-03-12T10:00:00Z",
    "windowEnd": "2026-03-12T10:30:00Z",
    "createdAt": "2026-03-12T10:00:00Z",
    "updatedAt": "2026-03-12T10:00:00Z",
}

_META_COMPLETED = {
    **_META_RUNNING,
    "status": "COMPLETED",
    "s3ReportKey": f"investigations/{_INCIDENT_ID}/final-report.json",
    "summary": "Most likely cause: deployment regression",
}

_SAMPLE_REPORT = {
    "incidentId": _INCIDENT_ID,
    "summary": "Most likely cause: deployment regression — payment-service (confidence 65%).",
    "topHypotheses": [{"cause": "deployment regression", "confidence": 0.65, "supportingEvidenceIds": []}],
    "evidenceHighlights": ["p95 latency increased 4.8x vs baseline"],
    "incidentWindow": {"start": "2026-03-12T10:00:00Z", "end": "2026-03-12T10:30:00Z"},
    "workerErrors": [],
    "aiAssessments": None,
    "confidenceExplanation": {
        "topHypothesisCause": "deployment regression",
        "whyRankedHighest": "Deterministic confidence gap is strongest.",
        "strongestEvidence": [],
        "missingEvidence": {"aiIdentified": [], "collectionGaps": []},
        "contribution": {
            "rankingDriver": "deterministic",
            "deterministic": {
                "topConfidence": 0.65,
                "runnerUpConfidence": None,
                "confidenceDelta": None,
            },
            "ai": {
                "assessmentAvailable": False,
                "topHypothesisMatch": None,
                "plausibility": None,
                "reason": None,
            },
        },
    },
}


def _client_error(code: str = "ThrottlingException") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "injected error"}},
        "GetItem",
    )


def _patch_env():
    return patch.dict(os.environ, {"TABLE_NAME": "test-table", "S3_BUCKET": "test-bucket"})


# ---------------------------------------------------------------------------
# GET /investigations/{incidentId}  — status endpoint
# ---------------------------------------------------------------------------


class TestGetStatus:
    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_running_investigation_returns_202(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": _META_RUNNING}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_STATUS_EVENT, None)

        assert response["statusCode"] == 202
        body = json.loads(response["body"])
        assert body["status"] == "RUNNING"
        assert body["incidentId"] == _INCIDENT_ID

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_completed_investigation_returns_200(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": _META_COMPLETED}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_STATUS_EVENT, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["status"] == "COMPLETED"

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_not_found_returns_404(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}  # no "Item" key → not found
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_STATUS_EVENT, None)

        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert body["error"] == "NOT_FOUND"

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_dynamodb_client_error_returns_500(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.side_effect = _client_error()
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_STATUS_EVENT, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["error"] == "INTERNAL_ERROR"
        assert "message" in body

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_status_response_contains_expected_fields(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": _META_COMPLETED}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_STATUS_EVENT, None)
        body = json.loads(response["body"])

        for field in ("incidentId", "status", "service", "region", "windowStart", "windowEnd", "createdAt", "updatedAt"):
            assert field in body


# ---------------------------------------------------------------------------
# GET /investigations/{incidentId}/report  — report endpoint
# ---------------------------------------------------------------------------


class TestGetReport:
    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_not_found_returns_404(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_REPORT_EVENT, None)

        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert body["error"] == "NOT_FOUND"

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_running_returns_202(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": _META_RUNNING}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_REPORT_EVENT, None)

        assert response["statusCode"] == 202
        body = json.loads(response["body"])
        assert body["status"] == "RUNNING"

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_completed_missing_s3_key_returns_500(self, mock_boto3):
        meta = {**_META_COMPLETED}
        del meta["s3ReportKey"]
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": meta}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_REPORT_EVENT, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["error"] == "INTERNAL_ERROR"

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_s3_fetch_failure_returns_500(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": _META_COMPLETED}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        # S3 client raises on get_object
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = Exception("S3 unavailable")
        mock_boto3.client.return_value = mock_s3

        response = api_handler.lambda_handler(_REPORT_EVENT, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["error"] == "INTERNAL_ERROR"
        assert "message" in body

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_dynamodb_client_error_returns_500(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.side_effect = _client_error()
        mock_boto3.resource.return_value.Table.return_value = mock_table

        response = api_handler.lambda_handler(_REPORT_EVENT, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["error"] == "INTERNAL_ERROR"
        assert "message" in body

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_happy_path_returns_200_with_report(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": _META_COMPLETED}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(_SAMPLE_REPORT).encode()))
        }
        mock_boto3.client.return_value = mock_s3

        response = api_handler.lambda_handler(_REPORT_EVENT, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["incidentId"] == _INCIDENT_ID
        assert "topHypotheses" in body
        assert "summary" in body
        assert "confidenceExplanation" in body

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_invalid_stored_report_contract_returns_500(self, mock_boto3):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": _META_COMPLETED}
        mock_boto3.resource.return_value.Table.return_value = mock_table

        invalid_report = {"incidentId": _INCIDENT_ID, "summary": "ok"}
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps(invalid_report).encode()))
        }
        mock_boto3.client.return_value = mock_s3

        response = api_handler.lambda_handler(_REPORT_EVENT, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert body["error"] == "INTERNAL_ERROR"
        assert "contract validation" in body["message"]

    @_patch_env()
    @patch("backend.orchestration.api.handler.boto3")
    def test_missing_incident_id_returns_400(self, mock_boto3):
        event = {**_REPORT_EVENT, "pathParameters": {}}
        response = api_handler.lambda_handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert body["error"] == "VALIDATION_ERROR"

