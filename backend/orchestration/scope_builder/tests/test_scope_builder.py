"""Unit tests for snapshot-only scope derivation logic."""

import pytest

from scope import build_scope_from_context_snapshot


_INCIDENT = {
    "incidentId": "inc-test-ctx-001",
    "signalType": "latency_spike",
    "windowStart": "2026-03-20T10:00:00Z",
    "windowEnd": "2026-03-20T10:15:00Z",
    "contextSnapshot": {
        "contextId": "ctx-123",
        "service": "payment-service",
        "region": "eu-west-1",
        "logGroups": ["/aws/lambda/payment-service"],
        "metricDescriptors": [
            {
                "namespace": "AWS/Lambda",
                "metricName": "Duration",
                "dimensions": {"FunctionName": "payment-service"},
                "stat": "Average",
            }
        ],
        "xrayServices": ["payment-service"],
    },
}


def test_builds_scope_from_context_snapshot():
    scope = build_scope_from_context_snapshot(_INCIDENT)

    assert scope["incidentId"] == "inc-test-ctx-001"
    assert scope["signalType"] == "latency_spike"
    assert scope["service"] == "payment-service"
    assert scope["region"] == "eu-west-1"
    assert scope["logGroups"] == ["/aws/lambda/payment-service"]
    assert scope["metrics"][0]["metricName"] == "Duration"
    assert scope["traceServices"] == ["payment-service"]


def test_baseline_window_is_preceding_equal_duration():
    scope = build_scope_from_context_snapshot(_INCIDENT)

    assert scope["incidentWindow"]["start"] == "2026-03-20T10:00:00Z"
    assert scope["incidentWindow"]["end"] == "2026-03-20T10:15:00Z"
    assert scope["baselineWindow"]["start"] == "2026-03-20T09:45:00Z"
    assert scope["baselineWindow"]["end"] == "2026-03-20T10:00:00Z"


def test_missing_snapshot_raises_value_error():
    with pytest.raises(ValueError, match="contextSnapshot"):
        build_scope_from_context_snapshot({"incidentId": "inc-1"})
