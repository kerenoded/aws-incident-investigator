"""
Unit tests for the Metrics Worker.

Uses unittest.mock to patch the CloudWatch client — no real AWS calls are made.
Schema validation uses jsonschema against schemas/worker-output.schema.json.

Run from repo root:
    python -m pytest backend/workers/metrics/tests/ -v
"""

import json
import os

import jsonschema
import pytest
from botocore.exceptions import ClientError

from metrics_worker import collect_metrics

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SCHEMA_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..", "schemas", "worker-output.schema.json",
    )
)

# Scope matching schemas/examples/scope.example.json (payment-service, 15-min window).
SCOPE = {
    "incidentId": "inc-test-001",
    "service": "payment-service",
    "region": "eu-west-1",
    "incidentWindow": {"start": "2026-03-20T10:00:00Z", "end": "2026-03-20T10:15:00Z"},
    "baselineWindow": {"start": "2026-03-20T09:45:00Z", "end": "2026-03-20T10:00:00Z"},
    "metrics": [
        {
            "namespace": "AWS/ApplicationELB",
            "metricName": "TargetResponseTime",
            "dimensions": {"TargetGroup": "payment-service-tg"},
            "stat": "p95",
        },
        {
            "namespace": "AWS/ECS",
            "metricName": "CPUUtilization",
            "dimensions": {"ServiceName": "payment-service", "ClusterName": "main-cluster"},
            "stat": "Average",
        },
    ],
}


def _cw_error(code: str = "InvalidParameterValue") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}},
        "GetMetricStatistics",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCollectMetrics:

    def test_happy_path_one_anomalous_one_normal(self, mocker):
        """Anomalous metric (TargetResponseTime 4.8x spike) produces a finding;
        normal metric (CPUUtilization 1.1x) is below threshold and is skipped."""
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            name = kwargs["MetricName"]
            start = kwargs["StartTime"]
            is_incident = start.hour == 10  # incident window starts 10:00

            if name == "TargetResponseTime":
                stat = kwargs["ExtendedStatistics"][0]
                value = 3.2 if is_incident else 0.67
                return {"Datapoints": [{"ExtendedStatistics": {stat: value}, "Unit": "Seconds"}]}

            # CPUUtilization — small change, below threshold
            value = 55.0 if is_incident else 50.0
            return {"Datapoints": [{"Average": value, "Unit": "Percent"}]}

        cw.get_metric_statistics.side_effect = _side_effect
        result = collect_metrics(SCOPE, cw)

        assert result["incidentId"] == "inc-test-001"
        assert result["source"] == "metrics"
        assert result["errors"] == []
        assert len(result["findings"]) == 1

        finding = result["findings"][0]
        # Position 0 in scope.metrics → ev-metrics-001
        assert finding["evidenceId"] == "ev-metrics-001"
        assert finding["findingType"] == "latency_spike"
        assert finding["source"] == "metrics"
        assert finding["details"]["metricName"] == "TargetResponseTime"
        assert finding["details"]["currentValue"] == pytest.approx(3.2)
        assert finding["details"]["baselineValue"] == pytest.approx(0.67)
        assert finding["details"]["unit"] == "Seconds"
        assert "changeRatio" in finding["details"]
        assert 0.0 <= finding["score"] <= 1.0

    def test_no_datapoints_returns_empty_findings(self, mocker):
        """CloudWatch returning no datapoints yields findings=[] and errors=[]."""
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}

        result = collect_metrics(SCOPE, cw)

        assert result["findings"] == []
        assert result["errors"] == []

    def test_cloudwatch_error_per_metric_continues(self, mocker):
        """A ClientError on one metric populates errors[] but the other metric is
        still attempted (per-metric handling, not all-or-nothing)."""
        cw = mocker.MagicMock()
        call_count = {"n": 0}

        def _side_effect(**kwargs):
            call_count["n"] += 1
            if kwargs["MetricName"] == "TargetResponseTime":
                raise _cw_error("InvalidParameterValue")
            # CPUUtilization: empty — below threshold, no finding
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _side_effect
        result = collect_metrics(SCOPE, cw)

        assert len(result["errors"]) == 1
        assert "cloudwatch_error" in result["errors"][0]["reason"]
        assert "TargetResponseTime" in result["errors"][0]["source"]
        # CPUUtilization was still attempted after the first metric failed
        assert call_count["n"] > 1
        assert result["findings"] == []

    def test_baseline_zero_current_nonzero_generates_finding(self, mocker):
        """Metric absent from baseline but present in incident window → finding produced."""
        cw = mocker.MagicMock()
        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApplicationELB",
                    "metricName": "HTTPCode_Target_5XX_Count",
                    "dimensions": {"TargetGroup": "payment-service-tg"},
                    "stat": "Sum",
                }
            ],
        }

        def _side_effect(**kwargs):
            start = kwargs["StartTime"]
            if start.hour == 10:  # incident window
                return {"Datapoints": [{"Sum": 42.0, "Unit": "Count"}]}
            return {"Datapoints": []}  # baseline: no data

        cw.get_metric_statistics.side_effect = _side_effect
        result = collect_metrics(scope, cw)

        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["evidenceId"] == "ev-metrics-001"
        assert finding["findingType"] == "error_rate_spike"
        assert finding["details"]["baselineValue"] is None

    def test_evidence_id_stable_by_metric_position_not_finding_order(self, mocker):
        """evidenceId is based on position in scope.metrics, not finding order.
        Metric at index 0 produces no finding; metric at index 1 does.
        The finding must carry ev-metrics-002, not ev-metrics-001."""
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            name = kwargs["MetricName"]
            start = kwargs["StartTime"]
            if name == "TargetResponseTime":
                return {"Datapoints": []}  # no signal
            # CPUUtilization: 3x spike
            value = 90.0 if start.hour == 10 else 30.0
            return {"Datapoints": [{"Average": value, "Unit": "Percent"}]}

        cw.get_metric_statistics.side_effect = _side_effect
        result = collect_metrics(SCOPE, cw)

        assert len(result["findings"]) == 1
        assert result["findings"][0]["evidenceId"] == "ev-metrics-002"
        assert result["findings"][0]["findingType"] == "cpu_spike"

    def test_output_schema_valid(self, mocker):
        """Output envelope must conform to schemas/worker-output.schema.json."""
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}

        result = collect_metrics(SCOPE, cw)

        with open(SCHEMA_PATH) as f:
            schema = json.load(f)

        # Raises jsonschema.ValidationError if the output does not conform.
        jsonschema.validate(result, schema)


class TestApiGatewayFindingTypes:
    """Verify that API Gateway metric names map to the correct finding types."""

    def _scope_with_metric(self, namespace: str, metric_name: str, stat: str) -> dict:
        return {
            **SCOPE,
            "metrics": [
                {
                    "namespace": namespace,
                    "metricName": metric_name,
                    "dimensions": {"ApiName": "my-api", "Stage": "prod"},
                    "stat": stat,
                }
            ],
        }

    def _spike_side_effect(self, stat_key: str, incident_value: float, baseline_value: float):
        """Return a CloudWatch side-effect that fakes an incident spike."""
        def _side_effect(**kwargs):
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            value = incident_value if is_incident else baseline_value
            # Percentile stats are returned under ExtendedStatistics.
            if stat_key.startswith("p"):
                return {"Datapoints": [{"ExtendedStatistics": {stat_key: value}, "Unit": "Milliseconds"}]}
            return {"Datapoints": [{stat_key: value, "Unit": "Count"}]}
        return _side_effect

    def test_apigw_4xxerror_maps_to_error_rate_spike(self, mocker):
        """4XXError metric must produce findingType=error_rate_spike, not metric_spike."""
        cw = mocker.MagicMock()
        cw.get_metric_statistics.side_effect = self._spike_side_effect("Sum", 320.0, 20.0)
        scope = self._scope_with_metric("AWS/ApiGateway", "4XXError", "Sum")

        result = collect_metrics(scope, cw)

        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "error_rate_spike"
        assert finding["resourceType"] == "api-gateway"
        assert finding["details"]["metricName"] == "4XXError"

    def test_apigw_5xxerror_maps_to_error_rate_spike(self, mocker):
        """5XXError metric must produce findingType=error_rate_spike, not metric_spike."""
        cw = mocker.MagicMock()
        cw.get_metric_statistics.side_effect = self._spike_side_effect("Sum", 85.0, 2.0)
        scope = self._scope_with_metric("AWS/ApiGateway", "5XXError", "Sum")

        result = collect_metrics(scope, cw)

        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "error_rate_spike"
        assert finding["resourceType"] == "api-gateway"
        assert finding["details"]["metricName"] == "5XXError"

    def test_apigw_5xx_lowercase_maps_to_error_rate_spike(self, mocker):
        """REST API ApiId/Stage series uses metric name `5xx` in CloudWatch."""
        cw = mocker.MagicMock()
        cw.get_metric_statistics.side_effect = self._spike_side_effect("Sum", 75.0, 1.0)
        scope = self._scope_with_metric("AWS/ApiGateway", "5xx", "Sum")

        result = collect_metrics(scope, cw)

        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "error_rate_spike"
        assert finding["resourceType"] == "api-gateway"
        assert finding["details"]["metricName"] == "5xx"

    def test_apigw_integration_latency_maps_to_latency_spike(self, mocker):
        """IntegrationLatency must produce findingType=latency_spike (now explicit in FINDING_TYPE_MAP)."""
        cw = mocker.MagicMock()
        cw.get_metric_statistics.side_effect = self._spike_side_effect("p95", 4200.0, 180.0)
        scope = self._scope_with_metric("AWS/ApiGateway", "IntegrationLatency", "p95")

        result = collect_metrics(scope, cw)

        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "latency_spike"
        assert finding["resourceType"] == "api-gateway"
        assert finding["details"]["metricName"] == "IntegrationLatency"

    def test_apigw_latency_uses_more_sensitive_threshold_than_generic_metrics(self, mocker):
        """API Gateway latency should alert at 1.3x, while generic metric_spike remains below 1.5x."""
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            if metric_name == "Latency":
                value = 1300.0 if is_incident else 1000.0  # 1.3x
                return {"Datapoints": [{"Average": value, "Unit": "Milliseconds"}]}
            if metric_name == "ReturnedItemCount":
                value = 130.0 if is_incident else 100.0  # 1.3x, should stay below generic 1.5 threshold
                return {"Datapoints": [{"Average": value, "Unit": "Count"}]}
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _side_effect
        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "Latency",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Average",
                },
                {
                    "namespace": "AWS/DynamoDB",
                    "metricName": "ReturnedItemCount",
                    "dimensions": {"TableName": "items", "Operation": "Scan"},
                    "stat": "Average",
                },
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        assert result["findings"][0]["details"]["metricName"] == "Latency"


class TestApiGatewayStageFallback:
    """Verify API Gateway Stage dimension fallback behavior for case mismatch."""

    def test_apigw_stage_case_fallback_finds_datapoints(self, mocker):
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            stage = next((d["Value"] for d in kwargs.get("Dimensions", []) if d.get("Name") == "Stage"), None)
            start = kwargs["StartTime"]
            is_incident = start.hour == 10

            # Simulate CloudWatch data existing only for lowercase stage.
            if stage == "prod":
                return {"Datapoints": [{"Sum": (100.0 if is_incident else 10.0), "Unit": "Count"}]}
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _side_effect

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert len(result["errors"]) == 0
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "error_rate_spike"
        assert finding["details"]["metricName"] == "5XXError"

    def test_apigw_stage_fallback_attempts_multiple_stage_variants(self, mocker):
        cw = mocker.MagicMock()
        seen_stages = []

        def _side_effect(**kwargs):
            stage = next((d["Value"] for d in kwargs.get("Dimensions", []) if d.get("Name") == "Stage"), None)
            if stage is not None:
                seen_stages.append(stage)
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _side_effect

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "Count",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["findings"] == []
        assert result["errors"] == []
        # Incident + baseline windows each try Stage case variants.
        assert "Prod" in seen_stages
        assert "prod" in seen_stages


class TestApiGatewayDescriptorReconciliation:
    """Verify API Gateway list_metrics-based dimension reconciliation."""

    def test_apigw_reconciliation_retries_with_discovered_dimensions(self, mocker):
        cw = mocker.MagicMock()

        def _get_metric_statistics_side_effect(**kwargs):
            dims = {d["Name"]: d["Value"] for d in kwargs.get("Dimensions", [])}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10

            # Data exists only for the dimensions discovered via list_metrics.
            if dims.get("ApiName") == "my-api-correct" and dims.get("Stage") == "Prod":
                return {"Datapoints": [{"Sum": (120.0 if is_incident else 20.0), "Unit": "Count"}]}
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _get_metric_statistics_side_effect
        cw.list_metrics.return_value = {
            "Metrics": [
                {
                    "Namespace": "AWS/ApiGateway",
                    "MetricName": "5XXError",
                    "Dimensions": [
                        {"Name": "ApiName", "Value": "my-api-correct"},
                        {"Name": "Stage", "Value": "Prod"},
                    ],
                }
            ]
        }

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api-wrong", "Stage": "prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        assert result["findings"][0]["findingType"] == "error_rate_spike"
        assert cw.list_metrics.call_count >= 1

    def test_no_datapoints_emits_structured_debug_payload(self, mocker, caplog):
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "Count",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        with caplog.at_level("DEBUG"):
            result = collect_metrics(scope, cw)

        assert result["findings"] == []
        assert result["errors"] == []
        messages = [record.getMessage() for record in caplog.records]
        structured = [m for m in messages if m.startswith("metrics_no_datapoints ")]
        assert structured, "Expected structured no-datapoints debug log"
        assert any("attemptedDimensions" in m for m in structured)
        assert any("discoveredDimensionCandidates" in m for m in structured)

    def test_apigw_discovery_failure_emits_explicit_warning(self, mocker, caplog):
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        cw.list_metrics.side_effect = _cw_error("AccessDenied")

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        with caplog.at_level("WARNING"):
            result = collect_metrics(scope, cw)

        assert result["errors"] == []
        messages = [record.getMessage() for record in caplog.records]
        assert any(m.startswith("metrics_apigw_dimension_discovery_failed ") for m in messages)

    def test_apigw_reconciliation_supports_api_id_dimensions(self, mocker):
        cw = mocker.MagicMock()

        def _get_metric_statistics_side_effect(**kwargs):
            dims = {d["Name"]: d["Value"] for d in kwargs.get("Dimensions", [])}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10

            # Data exists only when ApiId-based dimensions are used.
            if dims.get("ApiId") == "a1b2c3d4" and dims.get("Stage") == "$default":
                return {"Datapoints": [{"Sum": (80.0 if is_incident else 10.0), "Unit": "Count"}]}
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _get_metric_statistics_side_effect
        cw.list_metrics.return_value = {
            "Metrics": [
                {
                    "Namespace": "AWS/ApiGateway",
                    "MetricName": "5XXError",
                    "Dimensions": [
                        {"Name": "ApiId", "Value": "a1b2c3d4"},
                        {"Name": "Stage", "Value": "$default"},
                    ],
                }
            ]
        }

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        assert result["findings"][0]["findingType"] == "error_rate_spike"

    def test_apigw_latency_reconciliation_uses_unfiltered_api_id_candidates_even_when_filtered_candidates_exist(self, mocker):
        """Latency metrics should still try ApiId candidates from unfiltered discovery."""
        cw = mocker.MagicMock()

        def _get_metric_statistics_side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            if metric_name != "Latency":
                return {"Datapoints": []}
            dims = {d["Name"]: d["Value"] for d in kwargs.get("Dimensions", [])}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10

            # No data for ApiName-only candidates.
            if dims.get("ApiName") == "my-api":
                return {"Datapoints": []}

            # Data exists for ApiId candidate discovered via unfiltered list_metrics.
            if dims.get("ApiId") == "a1b2c3d4" and dims.get("Stage") == "Prod":
                return {"Datapoints": [{"Average": (1300.0 if is_incident else 1000.0), "Unit": "Milliseconds"}]}

            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _get_metric_statistics_side_effect

        # First discovery call: filtered by ApiName returns only ApiName candidates.
        # Second discovery call: unfiltered returns ApiId candidates.
        cw.list_metrics.side_effect = [
            {
                "Metrics": [
                    {
                        "Namespace": "AWS/ApiGateway",
                        "MetricName": "Latency",
                        "Dimensions": [
                            {"Name": "ApiName", "Value": "my-api"},
                            {"Name": "Stage", "Value": "Prod"},
                        ],
                    }
                ]
            },
            {
                "Metrics": [
                    {
                        "Namespace": "AWS/ApiGateway",
                        "MetricName": "Latency",
                        "Dimensions": [
                            {"Name": "ApiId", "Value": "a1b2c3d4"},
                            {"Name": "Stage", "Value": "Prod"},
                        ],
                    }
                ]
            },
        ]

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "Latency",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Average",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "latency_spike"
        assert finding["details"]["currentValue"] == pytest.approx(1300.0)
        assert finding["details"]["baselineValue"] == pytest.approx(1000.0)

    def test_apigw_metric_name_alias_fallback_uses_5xx_when_5xxerror_has_no_data(self, mocker):
        cw = mocker.MagicMock()

        def _get_metric_statistics_side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            dims = {d["Name"]: d["Value"] for d in kwargs.get("Dimensions", [])}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10

            if metric_name == "5xx" and dims.get("ApiId") == "a1b2c3d4" and dims.get("Stage") == "Prod":
                return {"Datapoints": [{"Sum": (75.0 if is_incident else 1.0), "Unit": "Count"}]}

            # Simulate no datapoints for 5XXError name.
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _get_metric_statistics_side_effect
        cw.list_metrics.return_value = {
            "Metrics": [
                {
                    "Namespace": "AWS/ApiGateway",
                    "MetricName": "5XXError",
                    "Dimensions": [
                        {"Name": "ApiId", "Value": "a1b2c3d4"},
                        {"Name": "Stage", "Value": "Prod"},
                    ],
                }
            ]
        }

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiId": "a1b2c3d4", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        assert result["findings"][0]["findingType"] == "error_rate_spike"

    def test_apigw_sparse_error_metric_imputes_zero_instead_of_no_data(self, mocker, caplog):
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        with caplog.at_level("DEBUG"):
            result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert result["findings"] == []
        messages = [record.getMessage() for record in caplog.records]
        assert any(m.startswith("metrics_zero_imputed ") for m in messages)
        assert any(m.startswith("metrics_zero_both_windows ") for m in messages)
        assert not any(m.startswith("metrics_no_datapoints ") for m in messages)

    def test_apigw_sparse_error_metric_uses_60s_and_aggregates_datapoints(self, mocker, caplog):
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            if metric_name != "5XXError":
                return {"Datapoints": []}
            # Worker should force 60s period for sparse API GW error counters.
            assert kwargs["Period"] == 60
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            if is_incident:
                return {
                    "Datapoints": [
                        {"Sum": 1.0, "Unit": "Count", "Timestamp": "2026-03-20T10:04:00+00:00"},
                        {"Sum": 2.0, "Unit": "Count", "Timestamp": "2026-03-20T10:05:00+00:00"},
                    ]
                }
            return {
                "Datapoints": [
                    {"Sum": 1.0, "Unit": "Count", "Timestamp": "2026-03-20T09:45:00+00:00"}
                ]
            }

        cw.get_metric_statistics.side_effect = _side_effect

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        with caplog.at_level("DEBUG"):
            result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "error_rate_spike"
        # Incident is aggregated to 3.0, baseline to 1.0 -> ratio 3.0
        assert finding["details"]["currentValue"] == pytest.approx(3.0)
        assert finding["details"]["baselineValue"] == pytest.approx(1.0)
        messages = [record.getMessage() for record in caplog.records]
        assert any(m.startswith("metrics_sparse_counter_window ") for m in messages)

    def test_apigw_baseline_zero_summary_uses_non_ratio_wording(self, mocker):
        """When baseline is zero/missing, summary should avoid synthetic ratio wording."""
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            if metric_name != "5XXError":
                return {"Datapoints": []}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            if is_incident:
                return {
                    "Datapoints": [
                        {"Sum": 3.0, "Unit": "Count", "Timestamp": "2026-03-20T10:04:00+00:00"}
                    ]
                }
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _side_effect
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert "present in incident window with baseline at zero" in finding["summary"]
        assert finding["details"]["baselineAbsent"] is True
        assert finding["details"]["changeRatio"] is None
        # Baseline-absent sparse counter scoring should scale above the legacy floor.
        assert finding["score"] == pytest.approx(0.3, rel=1e-4)

    def test_apigw_baseline_zero_high_magnitude_scales_score(self, mocker):
        """Large baseline-zero APIGW counters should score above the legacy 0.30 floor."""
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            if metric_name != "5XXError":
                return {"Datapoints": []}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            if is_incident:
                # 38 errors in-window with no baseline datapoints.
                return {
                    "Datapoints": [
                        {"Sum": 38.0, "Timestamp": "2026-03-20T10:05:00+00:00"}
                    ]
                }
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _side_effect
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["details"]["unit"] == "Count"  # default fallback when CW omits unit.
        assert finding["details"]["baselineAbsent"] is True
        # 38 / 50 = 0.76 from sparse baseline-absent scoring.
        assert finding["score"] == pytest.approx(0.76, rel=1e-4)

    def test_apigw_sparse_counter_normalizes_literal_none_unit_to_count(self, mocker):
        """CloudWatch may return Unit='None'; sparse counters should normalize to Count."""
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            if metric_name != "5XXError":
                return {"Datapoints": []}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            if is_incident:
                return {
                    "Datapoints": [
                        {"Sum": 12.0, "Unit": "None", "Timestamp": "2026-03-20T10:05:00+00:00"}
                    ]
                }
            return {"Datapoints": []}

        cw.get_metric_statistics.side_effect = _side_effect
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["details"]["unit"] == "Count"

    def test_apigw_duplicate_equivalent_metrics_are_deduplicated(self, mocker):
        """Equivalent APIGW descriptors should produce a single finding."""
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            metric_name = kwargs["MetricName"]
            if metric_name != "5XXError":
                return {"Datapoints": []}
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            if is_incident:
                return {
                    "Datapoints": [
                        {"Sum": 38.0, "Unit": "Count", "Timestamp": "2026-03-20T10:05:00+00:00"}
                    ]
                }
            return {
                "Datapoints": [
                    {"Sum": 0.0, "Unit": "Count", "Timestamp": "2026-03-20T09:50:00+00:00"}
                ]
            }

        cw.get_metric_statistics.side_effect = _side_effect

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                },
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api"},
                    "stat": "Sum",
                },
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1

    def test_apigw_discovery_is_cached_per_metric_descriptor_within_invocation(self, mocker):
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "Latency",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Average",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        assert result["findings"][0]["findingType"] == "metric_collection_gap"
        # Discovery is cached across incident+baseline fetches; with the
        # new filtered-first strategy it may make up to two calls once
        # (filtered + unfiltered fallback), but not per window.
        assert cw.list_metrics.call_count <= 2


class TestErrorSpikeFallbackFinding:
    def test_error_spike_adds_fallback_finding_when_no_error_rate_spike_detected(self, mocker):
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "signalType": "error_spike",
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "error_signal_not_confirmed_by_metrics"
        assert finding["details"]["signalType"] == "error_spike"
        assert isinstance(finding["details"]["checkedErrorMetrics"], list)
        assert len(finding["details"]["checkedErrorMetrics"]) >= 1

    def test_non_error_spike_does_not_add_fallback_finding(self, mocker):
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        cw.list_metrics.return_value = {"Metrics": [], "NextToken": None}

        scope = {
            **SCOPE,
            "signalType": "latency_spike",
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert result["findings"] == []

    def test_error_spike_does_not_add_fallback_when_error_rate_spike_exists(self, mocker):
        cw = mocker.MagicMock()

        def _side_effect(**kwargs):
            start = kwargs["StartTime"]
            is_incident = start.hour == 10
            return {"Datapoints": [{"Sum": (50.0 if is_incident else 5.0), "Unit": "Count"}]}

        cw.get_metric_statistics.side_effect = _side_effect

        scope = {
            **SCOPE,
            "signalType": "error_spike",
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "5XXError",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Sum",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        assert result["findings"][0]["findingType"] == "error_rate_spike"


class TestApiGatewayLatencyCollectionGap:
    def test_apigw_latency_no_datapoints_adds_metric_collection_gap_finding(self, mocker):
        cw = mocker.MagicMock()
        cw.get_metric_statistics.return_value = {"Datapoints": []}
        cw.list_metrics.return_value = {
            "Metrics": [
                {
                    "Namespace": "AWS/ApiGateway",
                    "MetricName": "Latency",
                    "Dimensions": [
                        {"Name": "ApiName", "Value": "my-api"},
                        {"Name": "Stage", "Value": "Prod"},
                    ],
                }
            ]
        }

        scope = {
            **SCOPE,
            "metrics": [
                {
                    "namespace": "AWS/ApiGateway",
                    "metricName": "Latency",
                    "dimensions": {"ApiName": "my-api", "Stage": "Prod"},
                    "stat": "Average",
                }
            ],
        }

        result = collect_metrics(scope, cw)

        assert result["errors"] == []
        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["findingType"] == "metric_collection_gap"
        assert "no datapoints" in finding["summary"]
        assert finding["details"]["diagnosticType"] == "collection_gap"
        assert finding["details"]["collectionGapReason"] in {
            "no_matching_datapoints_after_apigw_reconciliation",
            "no_datapoints_for_exact_metric_dimensions",
        }
