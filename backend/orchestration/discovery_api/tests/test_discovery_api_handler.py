import json
import os
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

import backend.orchestration.discovery_api.handler as discovery_handler


def _event(path: str, *, query: dict | None = None, sub: str | None = "user-1") -> dict:
    claims = {"sub": sub} if sub is not None else {}
    return {
        "httpMethod": "GET",
        "path": path,
        "queryStringParameters": query,
        "requestContext": {
            "authorizer": {
                "claims": claims,
            }
        },
    }


def _client_error() -> ClientError:
    return ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "injected"}},
        "AWS",
    )


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
def test_requires_authentication():
    response = discovery_handler.lambda_handler(
        _event("/incident-contexts/discovery/log-groups", query={"region": "eu-west-1"}, sub=None),
        None,
    )
    assert response["statusCode"] == 403
    assert json.loads(response["body"])["error"] == "AUTHZ_FORBIDDEN"


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
def test_requires_region():
    response = discovery_handler.lambda_handler(
        _event("/incident-contexts/discovery/log-groups", query={}),
        None,
    )
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "VALIDATION_ERROR"


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_log_groups_discovery_happy_path(mock_boto3):
    logs_client = MagicMock()
    logs_client.describe_log_groups.return_value = {
        "logGroups": [
            {"logGroupName": "/aws/lambda/payment-service", "arn": "arn:aws:logs:1"},
        ],
        "nextToken": "next-a",
    }
    mock_boto3.client.return_value = logs_client

    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/log-groups",
            query={"region": "eu-west-1", "q": "payment", "pageSize": "10", "nextToken": "abc"},
        ),
        None,
    )

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert len(body["items"]) == 1
    assert body["items"][0]["logGroupName"] == "/aws/lambda/payment-service"
    assert body["nextToken"] == "next-a"
    logs_client.describe_log_groups.assert_called_once_with(limit=10, logGroupNamePattern="payment", nextToken="abc")


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_metrics_requires_namespace(mock_boto3):
    response = discovery_handler.lambda_handler(
        _event("/incident-contexts/discovery/metrics", query={"region": "eu-west-1"}),
        None,
    )
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "VALIDATION_ERROR"
    mock_boto3.client.assert_not_called()


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_metrics_discovery_happy_path(mock_boto3):
    cw_client = MagicMock()
    cw_client.list_metrics.return_value = {
        "Metrics": [
            {
                "Namespace": "AWS/Lambda",
                "MetricName": "Duration",
                "Dimensions": [{"Name": "FunctionName", "Value": "payment-service"}],
            },
            {
                "Namespace": "AWS/Lambda",
                "MetricName": "Errors",
                "Dimensions": [],
            },
        ],
        "NextToken": "next-m",
    }
    mock_boto3.client.return_value = cw_client

    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/metrics",
            query={
                "region": "eu-west-1",
                "namespace": "AWS/Lambda",
                "q": "Duration",
                "pageSize": "5",
                "nextToken": "m0",
            },
        ),
        None,
    )

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert len(body["items"]) == 1
    assert body["items"][0]["metricName"] == "Duration"
    assert body["nextToken"] == "next-m"
    cw_client.list_metrics.assert_called_once_with(Namespace="AWS/Lambda", NextToken="m0")


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_xray_discovery_happy_path(mock_boto3):
    xray_client = MagicMock()
    xray_client.get_trace_summaries.return_value = {
        "TraceSummaries": [
            {
                "ServiceIds": [
                    {"Name": "payment-service"},
                    {"Name": "auth-service"},
                ]
            }
        ],
        "NextToken": "next-x",
    }
    mock_boto3.client.return_value = xray_client

    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/xray/services",
            query={"region": "eu-west-1", "q": "payment", "lookbackMinutes": "60"},
        ),
        None,
    )

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["items"] == [{"serviceName": "payment-service"}]
    assert body["nextToken"] == "next-x"


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_invalid_lookback_returns_400(mock_boto3):
    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/xray/services",
            query={"region": "eu-west-1", "lookbackMinutes": "2"},
        ),
        None,
    )
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "VALIDATION_ERROR"
    mock_boto3.client.assert_not_called()


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_page_size_is_capped(mock_boto3):
    logs_client = MagicMock()
    logs_client.describe_log_groups.return_value = {"logGroups": [], "nextToken": None}
    mock_boto3.client.return_value = logs_client

    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/log-groups",
            query={"region": "eu-west-1", "pageSize": "999"},
        ),
        None,
    )

    assert response["statusCode"] == 200
    logs_client.describe_log_groups.assert_called_once_with(limit=50)


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_aws_error_returns_500(mock_boto3):
    logs_client = MagicMock()
    logs_client.describe_log_groups.side_effect = _client_error()
    mock_boto3.client.return_value = logs_client

    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/log-groups",
            query={"region": "eu-west-1"},
        ),
        None,
    )

    assert response["statusCode"] == 500
    assert json.loads(response["body"])["error"] == "INTERNAL_ERROR"


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_metric_namespaces_happy_path(mock_boto3):
    cw_client = MagicMock()
    cw_client.list_metrics.return_value = {
        "Metrics": [
            {"Namespace": "AWS/Lambda"},
            {"Namespace": "AWS/ApplicationELB"},
            {"Namespace": "Custom/MyService"},
        ],
        "NextToken": None,
    }
    mock_boto3.client.return_value = cw_client

    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/metrics/namespaces",
            query={"region": "eu-west-1", "q": "AWS"},
        ),
        None,
    )

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    namespaces = [item["namespace"] for item in body["items"]]
    assert namespaces == ["AWS/ApplicationELB", "AWS/Lambda"]  # sorted, filtered; Custom excluded
    assert body["nextToken"] is None
    cw_client.list_metrics.assert_called_once_with()


@patch.dict(os.environ, {"ALLOWED_ORIGIN": "http://localhost:5173"})
@patch("backend.orchestration.discovery_api.handler.boto3")
def test_metric_namespaces_bounded_at_3_pages(mock_boto3):
    cw_client = MagicMock()
    # All 3 pages return a NextToken — scan must stop after page 3, not fetch page 4.
    cw_client.list_metrics.side_effect = [
        {"Metrics": [{"Namespace": "AWS/EC2"}], "NextToken": "tok-1"},
        {"Metrics": [{"Namespace": "AWS/DynamoDB"}], "NextToken": "tok-2"},
        {"Metrics": [{"Namespace": "AWS/Lambda"}], "NextToken": "tok-3"},
    ]
    mock_boto3.client.return_value = cw_client

    response = discovery_handler.lambda_handler(
        _event(
            "/incident-contexts/discovery/metrics/namespaces",
            query={"region": "eu-west-1", "q": "AWS"},
        ),
        None,
    )

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert len(body["items"]) == 3
    assert cw_client.list_metrics.call_count == 3  # bounded — did not fetch page 4

