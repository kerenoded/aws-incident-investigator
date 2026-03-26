"""
Unit tests for the Candidate Hypotheses Builder.

No AWS calls are made — all logic is pure Python over fixture dicts.
Schema validation uses jsonschema against schemas/hypothesis.schema.json
(validates each hypothesis item in the returned envelope).

Run from repo root:
    python -m pytest backend/orchestration/candidate_hypotheses_builder/tests/ -v
"""

import json
import os

import jsonschema
import pytest

from hypotheses_builder import build_hypotheses, _extract_exception_class

# ---------------------------------------------------------------------------
# Schema path
# ---------------------------------------------------------------------------

HYPOTHESIS_SCHEMA_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..", "schemas", "hypothesis.schema.json",
    )
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SCOPE = {
    "incidentId": "inc-test-001",
    "service": "payment-service",
    "region": "eu-west-1",
    "incidentWindow": {"start": "2026-03-20T10:00:00Z", "end": "2026-03-20T10:15:00Z"},
    "baselineWindow": {"start": "2026-03-20T09:45:00Z", "end": "2026-03-20T10:00:00Z"},
}

_LATENCY_FINDING = {
    "evidenceId": "ev-metrics-001",
    "source": "metrics",
    "resourceType": "alb",
    "resourceName": "payment-service-tg",
    "findingType": "latency_spike",
    "summary": "p95 latency increased 4.8x vs baseline",
    "score": 0.91,
    "details": {},
}

_ERROR_RATE_FINDING = {
    "evidenceId": "ev-metrics-002",
    "source": "metrics",
    "resourceType": "alb",
    "resourceName": "payment-service-tg",
    "findingType": "error_rate_spike",
    "summary": "5xx error rate spike vs baseline",
    "score": 0.78,
    "details": {},
}

# Non-timeout error log finding — does NOT trigger downstream_dependency_latency.
_ERROR_LOG_FINDING = {
    "evidenceId": "ev-logs-001",
    "source": "logs",
    "resourceType": "lambda-function",
    "resourceName": "payment-service",
    "findingType": "error_log_spike",
    "summary": '"Exception" log count changed 6.0x vs baseline [error_log_spike]',
    "score": 0.72,
    "details": {"pattern": "Exception"},
}

# Timeout error log finding — triggers downstream_dependency_latency.
_TIMEOUT_FINDING = {
    "evidenceId": "ev-logs-002",
    "source": "logs",
    "resourceType": "lambda-function",
    "resourceName": "payment-service",
    "findingType": "error_log_spike",
    "summary": '"timeout" log count changed 8.0x vs baseline [error_log_spike]',
    "score": 0.84,
    "details": {"pattern": "timeout"},
}

_CPU_FINDING = {
    "evidenceId": "ev-metrics-003",
    "source": "metrics",
    "resourceType": "lambda",
    "resourceName": "payment-service",
    "findingType": "cpu_spike",
    "summary": "CPU utilisation spike vs baseline",
    "score": 0.70,
    "details": {},
}

_GENERIC_LATENCY_METRIC_SPIKE_FINDING = {
    "evidenceId": "ev-metrics-010",
    "source": "metrics",
    "resourceType": "dynamodb-table",
    "resourceName": "items",
    "findingType": "metric_spike",
    "summary": "SuccessfulRequestLatency (Average) changed 1.6x vs baseline [metric_spike]",
    "score": 0.32,
    "details": {"metricName": "SuccessfulRequestLatency"},
}

_TRACE_DOMINANT_SEGMENT_FINDING = {
    "evidenceId": "ev-traces-002",
    "source": "traces",
    "resourceType": "xray-service",
    "resourceName": "simple-crud-api-dev-items",
    "findingType": "latency_dominant_segment",
    "summary": (
        "Dominant segment in sampled simple-crud-api-dev-items traces is "
        "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI (100% of sampled segment time)"
    ),
    "score": 1.0,
    "details": {"dominantSegment": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI"},
}

_NEW_ERROR_PATTERN_FINDING = {
    "evidenceId": "ev-logs-010",
    "source": "logs",
    "resourceType": "lambda-function",
    "resourceName": "payment-service",
    "findingType": "new_error_pattern",
    "summary": '"ERROR" appears in incident window with no baseline occurrences [new_error_pattern]',
    "score": 0.3,
    "details": {
        "pattern": "ERROR",
        "incidentCount": 29,
        "baselineCount": 0,
        "changeRatio": None,
    },
}


def _empty_outputs() -> dict:
    return {
        source: {"incidentId": "inc-test-001", "source": source, "findings": [], "errors": []}
        for source in ("metrics", "logs", "traces")
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBuildHypotheses:

    def test_downstream_dependency_latency_from_timeout_finding(self):
        """error_log_spike with timeout pattern alone does NOT trigger downstream hypothesis.

        Without trace_dependency_fault corroboration, a timeout log is treated as a
        Lambda self-timeout (which is covered by the runtime latency regression rule),
        not a downstream dependency failure.
        """
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_TIMEOUT_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        causes = [h["cause"] for h in result["hypotheses"]]
        assert not any("downstream" in c for c in causes)

    def test_downstream_dependency_latency_requires_trace_fault_when_only_timeout_log(self):
        """timeout log alone (no trace_dependency_fault) does NOT trigger downstream hypothesis.

        A Lambda self-timeout produces a 'timeout' log pattern but no faulted downstream
        subsegment in X-Ray. The downstream dependency rule must not fire in this case.
        """
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_TIMEOUT_FINDING]
        # No trace findings at all — simulates Lambda self-timeout scenario.

        result = build_hypotheses(SCOPE, outputs)

        causes = [h["cause"] for h in result["hypotheses"]]
        assert not any("downstream" in c for c in causes), (
            "downstream hypothesis should not fire on timeout log alone without trace fault"
        )

    def test_downstream_dependency_latency_fires_with_timeout_log_and_trace_fault(self):
        """timeout log + trace_dependency_fault together DO trigger downstream hypothesis."""
        trace_fault_finding = {
            "evidenceId": "ev-traces-005",
            "source": "traces",
            "resourceType": "xray-service",
            "resourceName": "my-db",
            "findingType": "trace_dependency_fault",
            "summary": "Faulted downstream subsegment detected",
            "score": 0.9,
            "details": {
                "faultedSubsegments": [{"name": "my-db"}],
                "incidentWindow": {
                    "start": "2026-03-20T10:00:00Z",
                    "end": "2026-03-20T10:15:00Z",
                },
            },
        }
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_TIMEOUT_FINDING]
        outputs["traces"]["findings"] = [trace_fault_finding]

        result = build_hypotheses(SCOPE, outputs)

        causes = [h["cause"] for h in result["hypotheses"]]
        assert any("downstream" in c for c in causes), (
            "downstream hypothesis should fire when timeout log is corroborated by trace fault"
        )

    def test_downstream_dependency_latency_fires_on_trace_fault_alone(self):
        """trace_dependency_fault alone (no timeout log) triggers downstream hypothesis."""
        trace_fault_finding = {
            "evidenceId": "ev-traces-005",
            "source": "traces",
            "resourceType": "xray-service",
            "resourceName": "my-db",
            "findingType": "trace_dependency_fault",
            "summary": "Faulted downstream subsegment detected",
            "score": 0.9,
            "details": {
                "faultedSubsegments": [{"name": "my-db"}],
                "incidentWindow": {
                    "start": "2026-03-20T10:00:00Z",
                    "end": "2026-03-20T10:15:00Z",
                },
            },
        }
        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [trace_fault_finding]

        result = build_hypotheses(SCOPE, outputs)

        causes = [h["cause"] for h in result["hypotheses"]]
        assert any("downstream" in c for c in causes)

    def test_downstream_dependency_latency_not_triggered_by_non_timeout(self):
        """error_log_spike with non-timeout pattern does NOT trigger downstream rule."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_ERROR_LOG_FINDING]  # pattern="Exception"

        result = build_hypotheses(SCOPE, outputs)

        causes = [h["cause"] for h in result["hypotheses"]]
        assert not any("downstream" in c for c in causes)

    def test_resource_exhaustion_from_cpu_spike(self):
        """cpu_spike → resource_exhaustion hypothesis at base confidence 0.20."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [_CPU_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        h1 = result["hypotheses"][0]
        assert "resource" in h1["cause"]
        assert h1["confidence"] == pytest.approx(0.20, rel=1e-4)

    def test_empty_worker_outputs_falls_back_to_unknown_cause(self):
        """All workers return empty findings → single unknown_cause hypothesis."""
        outputs = _empty_outputs()

        result = build_hypotheses(SCOPE, outputs)

        assert len(result["hypotheses"]) == 1
        h = result["hypotheses"][0]
        assert h["id"] == "h1"
        assert h["cause"] == "unknown_cause"
        assert h["confidence"] == pytest.approx(0.05, rel=1e-4)
        assert h["supportingEvidenceIds"] == []

    def test_empty_dict_falls_back_to_unknown_cause(self):
        """Completely empty workerOutputs dict → unknown_cause fallback."""
        result = build_hypotheses(SCOPE, {})

        assert len(result["hypotheses"]) == 1
        assert result["hypotheses"][0]["cause"] == "unknown_cause"

    def test_hypotheses_sorted_by_confidence_descending(self):
        """Multiple hypotheses are sorted highest confidence first."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [_LATENCY_FINDING, _CPU_FINDING]
        outputs["logs"]["findings"] = [_ERROR_LOG_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        confidences = [h["confidence"] for h in result["hypotheses"]]
        assert confidences == sorted(confidences, reverse=True)

    def test_at_most_three_hypotheses_returned(self):
        """Multiple rules firing returns at most 3 hypotheses.

        Note: _TIMEOUT_FINDING without trace_dependency_fault no longer triggers
        the downstream dependency rule (see _rule_downstream_dependency_latency).
        Three rules fire here: runtime_latency_regression, app_error_surge,
        resource_exhaustion.
        """
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [_LATENCY_FINDING, _CPU_FINDING]
        outputs["logs"]["findings"] = [_TIMEOUT_FINDING, _ERROR_LOG_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        assert len(result["hypotheses"]) <= 3

    def test_hypothesis_ids_are_sequential(self):
        """Multiple hypotheses have IDs h1, h2 in sequential order."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [_LATENCY_FINDING, _CPU_FINDING]
        outputs["logs"]["findings"] = [_ERROR_LOG_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        ids = [h["id"] for h in result["hypotheses"]]
        assert ids == [f"h{i + 1}" for i in range(len(ids))]

    def test_each_hypothesis_conforms_to_schema(self):
        """Each hypothesis in the output conforms to schemas/hypothesis.schema.json."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [_LATENCY_FINDING]
        outputs["logs"]["findings"] = [_ERROR_LOG_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        with open(HYPOTHESIS_SCHEMA_PATH) as f:
            schema = json.load(f)
        for hypothesis in result["hypotheses"]:
            jsonschema.validate(hypothesis, schema)

    def test_unknown_cause_conforms_to_schema(self):
        """The fallback unknown_cause hypothesis also conforms to the schema."""
        result = build_hypotheses(SCOPE, {})

        with open(HYPOTHESIS_SCHEMA_PATH) as f:
            schema = json.load(f)
        jsonschema.validate(result["hypotheses"][0], schema)

    def test_runtime_latency_regression_from_metric_spike_and_error_and_trace(self):
        """Incident-like cross-source latency/error/trace evidence should not fall back to unknown."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [_GENERIC_LATENCY_METRIC_SPIKE_FINDING]
        outputs["logs"]["findings"] = [_ERROR_LOG_FINDING]
        outputs["traces"]["findings"] = [_TRACE_DOMINANT_SEGMENT_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        h1 = result["hypotheses"][0]
        assert h1["cause"] == "runtime latency regression"
        assert h1["confidence"] == pytest.approx(0.65, rel=1e-4)
        assert "ev-metrics-010" in h1["supportingEvidenceIds"]
        assert "ev-logs-001" in h1["supportingEvidenceIds"]
        assert "ev-traces-002" in h1["supportingEvidenceIds"]
        assert not any(h["cause"] == "unknown_cause" for h in result["hypotheses"])

    def test_runtime_latency_cross_evidence_breadth_boost_applies_without_error_signal(self):
        """Multiple latency/trace findings across sources should increase confidence even without error signals."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [
            {
                "evidenceId": "ev-metrics-003",
                "source": "metrics",
                "resourceType": "lambda",
                "resourceName": "fn-a",
                "findingType": "latency_spike",
                "summary": "Duration (Average) changed 34.7x vs baseline [latency_spike]",
                "score": 1.0,
                "details": {},
            },
            {
                "evidenceId": "ev-metrics-016",
                "source": "metrics",
                "resourceType": "api-gateway",
                "resourceName": "api-a",
                "findingType": "latency_spike",
                "summary": "IntegrationLatency (Average) changed 1.3x vs baseline [latency_spike]",
                "score": 0.26,
                "details": {},
            },
        ]
        outputs["traces"]["findings"] = [
            {
                "evidenceId": "ev-traces-001",
                "source": "traces",
                "resourceType": "xray-service",
                "resourceName": "service-a",
                "findingType": "trace_latency_spike",
                "summary": "X-Ray response time for service-a changed 48.3x vs baseline [trace_latency_spike]",
                "score": 1.0,
                "details": {
                    "incidentAvgResponseTimeMs": 1234.9,
                    "baselineAvgResponseTimeMs": 25.55,
                    "changeRatio": 48.3327,
                    "incidentWindow": {"start": "2026-03-23T10:19:00Z", "end": "2026-03-23T10:22:00Z"},
                    "baselineWindow": {"start": "2026-03-23T10:16:00Z", "end": "2026-03-23T10:19:00Z"},
                },
            },
            {
                "evidenceId": "ev-traces-002",
                "source": "traces",
                "resourceType": "xray-service",
                "resourceName": "service-a",
                "findingType": "latency_dominant_segment",
                "summary": "Dominant segment in sampled service-a traces is fn-a (100% of sampled segment time)",
                "score": 1.0,
                "details": {
                    "dominantSegment": "fn-a",
                    "dominantSegmentPct": 100.0,
                    "dominantSegmentMs": 12226.0,
                    "sampledTraceCount": 10,
                    "incidentWindow": {"start": "2026-03-23T10:19:00Z", "end": "2026-03-23T10:22:00Z"},
                },
            },
        ]

        result = build_hypotheses(SCOPE, outputs)

        h1 = result["hypotheses"][0]
        assert h1["cause"] == "runtime latency regression"
        # 0.35 base + 0.15 trace_bottleneck + 0.10 multi_latency + 0.10 cross_evidence_breadth
        assert h1["confidence"] == pytest.approx(0.70, rel=1e-4)
        boosts = {b["name"]: b for b in h1["confidenceBreakdown"]["boosts"]}
        assert boosts["error_signal"]["applied"] is False
        assert boosts["trace_bottleneck_signal"]["applied"] is True
        assert boosts["multiple_latency_signals"]["applied"] is True
        assert boosts["cross_evidence_breadth"]["applied"] is True

    def test_runtime_latency_supporting_evidence_deduplicates_equivalent_trace_findings(self):
        """Mirrored trace variants should collapse to one representative evidence ID per symptom."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [
            {
                "evidenceId": "ev-metrics-003",
                "source": "metrics",
                "resourceType": "lambda",
                "resourceName": "fn-a",
                "findingType": "latency_spike",
                "summary": "Duration (Average) changed 34.7x vs baseline [latency_spike]",
                "score": 1.0,
                "details": {},
            }
        ]
        outputs["traces"]["findings"] = [
            {
                "evidenceId": "ev-traces-001",
                "source": "traces",
                "resourceType": "xray-service",
                "resourceName": "service-a",
                "findingType": "trace_latency_spike",
                "summary": "X-Ray response time for service-a changed 48.3x vs baseline [trace_latency_spike]",
                "score": 1.0,
                "details": {
                    "incidentAvgResponseTimeMs": 1234.9,
                    "baselineAvgResponseTimeMs": 25.55,
                    "changeRatio": 48.3327,
                    "incidentWindow": {"start": "2026-03-23T10:19:00Z", "end": "2026-03-23T10:22:00Z"},
                    "baselineWindow": {"start": "2026-03-23T10:16:00Z", "end": "2026-03-23T10:19:00Z"},
                },
            },
            {
                "evidenceId": "ev-traces-004",
                "source": "traces",
                "resourceType": "xray-service",
                "resourceName": "service-b",
                "findingType": "trace_latency_spike",
                "summary": "X-Ray response time for service-b changed 48.3x vs baseline [trace_latency_spike]",
                "score": 1.0,
                "details": {
                    "incidentAvgResponseTimeMs": 1234.9,
                    "baselineAvgResponseTimeMs": 25.55,
                    "changeRatio": 48.3327,
                    "incidentWindow": {"start": "2026-03-23T10:19:00Z", "end": "2026-03-23T10:22:00Z"},
                    "baselineWindow": {"start": "2026-03-23T10:16:00Z", "end": "2026-03-23T10:19:00Z"},
                },
            },
            {
                "evidenceId": "ev-traces-002",
                "source": "traces",
                "resourceType": "xray-service",
                "resourceName": "service-a",
                "findingType": "latency_dominant_segment",
                "summary": "Dominant segment in sampled service-a traces is fn-a (100% of sampled segment time)",
                "score": 1.0,
                "details": {
                    "dominantSegment": "fn-a",
                    "dominantSegmentPct": 100.0,
                    "dominantSegmentMs": 12226.0,
                    "sampledTraceCount": 10,
                    "incidentWindow": {"start": "2026-03-23T10:19:00Z", "end": "2026-03-23T10:22:00Z"},
                },
            },
            {
                "evidenceId": "ev-traces-005",
                "source": "traces",
                "resourceType": "xray-service",
                "resourceName": "service-b",
                "findingType": "latency_dominant_segment",
                "summary": "Dominant segment in sampled service-b traces is fn-a (100% of sampled segment time)",
                "score": 1.0,
                "details": {
                    "dominantSegment": "fn-a",
                    "dominantSegmentPct": 100.0,
                    "dominantSegmentMs": 12226.0,
                    "sampledTraceCount": 10,
                    "incidentWindow": {"start": "2026-03-23T10:19:00Z", "end": "2026-03-23T10:22:00Z"},
                },
            },
        ]

        result = build_hypotheses(SCOPE, outputs)
        top = result["hypotheses"][0]

        assert top["cause"] == "runtime latency regression"
        assert top["supportingEvidenceIds"] == ["ev-metrics-003", "ev-traces-001", "ev-traces-002"]

    def test_logs_only_new_error_pattern_produces_application_error_surge(self):
        """Logs-only baseline-zero error pattern should not fall back to unknown_cause."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_NEW_ERROR_PATTERN_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        h1 = result["hypotheses"][0]
        assert h1["cause"] == "application error surge"
        assert h1["confidence"] == pytest.approx(0.45, rel=1e-4)
        assert "ev-logs-010" in h1["supportingEvidenceIds"]
        assert not any(h["cause"] == "unknown_cause" for h in result["hypotheses"])

    def test_application_error_surge_includes_error_rate_evidence_as_corroboration(self):
        """App-error hypothesis should carry error-rate evidence IDs for operator context."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_NEW_ERROR_PATTERN_FINDING]
        outputs["metrics"]["findings"] = [_ERROR_RATE_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        h1 = result["hypotheses"][0]
        assert h1["cause"] == "application error surge"
        assert "ev-logs-010" in h1["supportingEvidenceIds"]
        assert "ev-metrics-002" in h1["supportingEvidenceIds"]
        assert h1["confidence"] == pytest.approx(0.50, rel=1e-4)

    def test_application_error_surge_cross_source_corroboration_boost_visible_in_breakdown(self):
        """When logs+metrics support app-error, apply a small deterministic cross-source boost."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_NEW_ERROR_PATTERN_FINDING]
        outputs["metrics"]["findings"] = [_ERROR_RATE_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        h1 = result["hypotheses"][0]
        boosts = {b["name"]: b for b in h1["confidenceBreakdown"]["boosts"]}
        assert boosts["cross_source_corroboration"]["applied"] is True
        assert boosts["cross_source_corroboration"]["value"] == pytest.approx(0.05, rel=1e-4)

    def test_application_error_surge_runtime_error_exact_incident_like_outcome(self):
        """Incident-like logs+API-gateway metrics produce the expected RuntimeError hypothesis shape."""
        outputs = _empty_outputs()
        outputs["metrics"]["findings"] = [
            {
                "evidenceId": "ev-metrics-023",
                "source": "metrics",
                "resourceType": "api-gateway",
                "resourceName": "simple-crud-api-dev",
                "findingType": "error_rate_spike",
                "summary": "5XXError (Sum) present in incident window with baseline at zero [error_rate_spike]",
                "score": 0.76,
                "details": {
                    "metricName": "5XXError",
                    "namespace": "AWS/ApiGateway",
                    "stat": "Sum",
                    "currentValue": 38.0,
                    "baselineValue": 0.0,
                    "baselineAbsent": True,
                },
            }
        ]
        outputs["logs"]["findings"] = [
            {
                "evidenceId": "ev-logs-001",
                "source": "logs",
                "resourceType": "lambda-function",
                "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                "findingType": "new_error_pattern",
                "summary": '"ERROR" appears in incident window with no baseline occurrences [new_error_pattern]',
                "score": 0.3,
                "details": {
                    "pattern": "ERROR",
                    "incidentCount": 76,
                    "baselineCount": 0,
                    "exceptionSamples": [
                        "[ERROR] RuntimeError: Simulated error for testing (param1=1)",
                        "[ERROR] RuntimeError: Simulated error for testing (param1=1)",
                    ],
                },
            },
            {
                "evidenceId": "ev-logs-012",
                "source": "logs",
                "resourceType": "api-gateway",
                "resourceName": "simple-crud-api-dev-access",
                "findingType": "new_error_pattern",
                "summary": '"HTTP 5XX" appears in incident window with no baseline occurrences [new_error_pattern]',
                "score": 0.3,
                "details": {
                    "pattern": "HTTP 5XX",
                    "incidentCount": 76,
                    "baselineCount": 0,
                },
            },
        ]

        result = build_hypotheses(SCOPE, outputs)

        h1 = result["hypotheses"][0]
        assert h1["cause"] == "application error surge — RuntimeError"
        assert h1["confidence"] == pytest.approx(0.6, rel=1e-4)
        assert h1["supportingEvidenceIds"] == ["ev-logs-001", "ev-logs-012", "ev-metrics-023"]

        breakdown = h1["confidenceBreakdown"]
        assert breakdown["base"] == pytest.approx(0.25, rel=1e-4)
        assert breakdown["totalBeforeCap"] == pytest.approx(0.6, rel=1e-4)
        assert breakdown["cap"] == pytest.approx(0.7, rel=1e-4)
        assert breakdown["final"] == pytest.approx(0.6, rel=1e-4)
        boosts = {b["name"]: b for b in breakdown["boosts"]}
        assert boosts["new_error_pattern_present"]["applied"] is True
        assert boosts["incident_error_count_ge_20"]["applied"] is True
        assert boosts["multiple_error_log_findings"]["applied"] is True
        assert boosts["cross_source_corroboration"]["applied"] is True

        assert "Dominant exception class in log samples: RuntimeError." in h1["rationale"]
        assert "Observed aggregated error-pattern count in sampled findings: 152." in h1["rationale"]
        assert "Patterns: ERROR, HTTP 5XX." in h1["rationale"]


# ---------------------------------------------------------------------------
# Exception-class label enrichment tests
# ---------------------------------------------------------------------------

def _make_error_log_finding(evidenceId: str, samples: list | None = None) -> dict:
    """Helper: build a minimal error_log_spike finding with optional exceptionSamples."""
    details: dict = {"pattern": "ERROR", "incidentCount": 5}
    if samples is not None:
        details["exceptionSamples"] = samples
    return {
        "evidenceId": evidenceId,
        "source": "logs",
        "resourceType": "lambda-function",
        "resourceName": "my-function",
        "findingType": "error_log_spike",
        "summary": "ERROR count spike",
        "score": 0.75,
        "details": details,
    }


class TestExtractExceptionClass:
    """Unit tests for the _extract_exception_class helper directly.

    The function now accepts ``list[str]`` (plain sample strings), mirroring
    the ``NormalizedSignals.app_error_exception_samples`` field.
    """

    def test_aws_sdk_parenthesised_format(self):
        """AWS SDK format '(ValidationException)' is extracted correctly."""
        samples = [
            "ClientError: An error occurred (ValidationException) when calling the GetItem operation: schema mismatch"
        ]
        assert _extract_exception_class(samples) == "ValidationException"

    def test_access_denied_exception(self):
        """(AccessDeniedException) is extracted."""
        samples = ["ClientError: An error occurred (AccessDeniedException) when calling PutItem"]
        assert _extract_exception_class(samples) == "AccessDeniedException"

    def test_resource_not_found_exception(self):
        """(ResourceNotFoundException) is extracted."""
        samples = ["ClientError: An error occurred (ResourceNotFoundException) when calling GetFunction"]
        assert _extract_exception_class(samples) == "ResourceNotFoundException"

    def test_conditional_check_failed_exception(self):
        """(ConditionalCheckFailedException) is extracted."""
        samples = ["ClientError: An error occurred (ConditionalCheckFailedException) when calling PutItem"]
        assert _extract_exception_class(samples) == "ConditionalCheckFailedException"

    def test_bare_prefix_fallback(self):
        """Bare prefix 'SomeException: message' is matched by the fallback pattern."""
        samples = ["NullPointerException: cannot be null"]
        assert _extract_exception_class(samples) == "NullPointerException"

    def test_no_samples_returns_none(self):
        """Empty list → None."""
        assert _extract_exception_class([]) is None

    def test_non_matching_sample_text_returns_none(self):
        """Generic lowercase error message with no exception class → None."""
        samples = ["error: connection reset by peer"]
        assert _extract_exception_class(samples) is None

    def test_most_frequent_class_wins_across_multiple_samples(self):
        """When two classes appear, the more frequent one is returned."""
        samples = [
            "ClientError: An error occurred (ValidationException) when calling GetItem",
            "ClientError: An error occurred (ValidationException) when calling PutItem",
            "ClientError: An error occurred (AccessDeniedException) when calling GetItem",
        ]
        assert _extract_exception_class(samples) == "ValidationException"

    def test_most_frequent_class_across_combined_samples(self):
        """Most-frequent exception class is counted across all sample strings."""
        samples = [
            "ClientError: An error occurred (ValidationException) when calling GetItem",
            "ClientError: An error occurred (ValidationException) when calling PutItem",
            "ClientError: An error occurred (AccessDeniedException) when calling GetItem",
        ]
        assert _extract_exception_class(samples) == "ValidationException"

    def test_non_string_items_are_skipped(self):
        """Non-string entries in the list do not crash the helper."""
        samples = [None, 42, "ClientError: An error occurred (ThrottlingException) when calling GetItem"]
        assert _extract_exception_class(samples) == "ThrottlingException"

    def test_empty_list_returns_none(self):
        """Empty input returns None."""
        assert _extract_exception_class([]) is None


class TestApplicationErrorSurgeExceptionLabel:
    """Integration tests: exception class label enrichment through build_hypotheses."""

    def test_validation_exception_enriches_cause_label(self):
        """ValidationException in samples → cause includes 'ValidationException'."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=[
                "ClientError: An error occurred (ValidationException) when calling the GetItem operation: "
                "The provided key element does not match the schema"
            ],
        )]

        result = build_hypotheses(SCOPE, outputs)

        h = result["hypotheses"][0]
        assert "ValidationException" in h["cause"]
        assert h["cause"].startswith("application error surge")

    def test_access_denied_enriches_cause_label(self):
        """AccessDeniedException in samples → cause includes 'AccessDeniedException'."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["ClientError: An error occurred (AccessDeniedException) when calling PutItem"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        h = result["hypotheses"][0]
        assert "AccessDeniedException" in h["cause"]

    def test_resource_not_found_enriches_cause_label(self):
        """ResourceNotFoundException in samples → cause includes 'ResourceNotFoundException'."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["ClientError: An error occurred (ResourceNotFoundException) when calling GetFunction"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        h = result["hypotheses"][0]
        assert "ResourceNotFoundException" in h["cause"]

    def test_conditional_check_failed_enriches_cause_label(self):
        """ConditionalCheckFailedException in samples → cause includes it."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["ClientError: An error occurred (ConditionalCheckFailedException) when calling PutItem"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        h = result["hypotheses"][0]
        assert "ConditionalCheckFailedException" in h["cause"]

    def test_no_samples_falls_back_to_plain_label(self):
        """Findings with no exceptionSamples → cause is plain 'application error surge'."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding("ev-logs-001", samples=None)]

        result = build_hypotheses(SCOPE, outputs)

        h = result["hypotheses"][0]
        assert h["cause"] == "application error surge"

    def test_non_matching_samples_falls_back_to_plain_label(self):
        """Samples with no extractable exception class → plain 'application error surge'."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["error: connection reset by peer", "failed to connect"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        h = result["hypotheses"][0]
        assert h["cause"] == "application error surge"

    def test_enriched_cause_also_appears_in_rationale(self):
        """Exception class is mentioned in the rationale when it enriches the cause."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["ClientError: An error occurred (ThrottlingException) when calling GetItem"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        h = result["hypotheses"][0]
        assert "ThrottlingException" in h["rationale"]

    def test_enrichment_does_not_change_confidence(self):
        """Enriching the cause label does not alter confidence scores."""
        # Finding without samples
        outputs_plain = _empty_outputs()
        outputs_plain["logs"]["findings"] = [_make_error_log_finding("ev-logs-001", samples=None)]

        # Same finding with a matching sample
        outputs_enriched = _empty_outputs()
        outputs_enriched["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["ClientError: An error occurred (ValidationException) when calling GetItem"],
        )]

        plain = build_hypotheses(SCOPE, outputs_plain)
        enriched = build_hypotheses(SCOPE, outputs_enriched)

        assert plain["hypotheses"][0]["confidence"] == pytest.approx(
            enriched["hypotheses"][0]["confidence"], rel=1e-4
        )


# ---------------------------------------------------------------------------
# Exception qualifier label tests
# ---------------------------------------------------------------------------

class TestExceptionQualifierLabel:
    """Cause label includes a short qualifier for well-understood exception classes."""

    # sample_suffix provides per-class context; for guarded qualifiers the
    # text must contain a guard keyword or the qualifier won't be applied.
    @pytest.mark.parametrize("exception_class,sample_suffix,expected_qualifier", [
        (
            "ValidationException",
            "when calling GetItem: The provided key element does not match the schema",
            "key/schema mismatch",
        ),
        ("AccessDeniedException", "when calling SomeOp", "access denied"),
        ("AccessDenied", "when calling SomeOp", "access denied"),
        ("ResourceNotFoundException", "when calling DescribeTable", "resource not found"),
        ("ConditionalCheckFailedException", "when calling PutItem", "write condition failed"),
        ("ThrottlingException", "when calling GetItem", "request throttled"),
        ("ServiceUnavailableException", "when calling SomeOp", "service unavailable"),
    ])
    def test_known_exception_class_includes_qualifier(
        self, exception_class: str, sample_suffix: str, expected_qualifier: str
    ):
        """Known exception classes produce 'application error surge — Class (qualifier)'."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=[f"ClientError: An error occurred ({exception_class}) {sample_suffix}"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        cause = result["hypotheses"][0]["cause"]
        assert exception_class in cause
        assert f"({expected_qualifier})" in cause

    def test_unknown_exception_class_no_qualifier(self):
        """An exception class not in the qualifier table produces no parenthesised qualifier."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["ClientError: An error occurred (SomeObscureException) when calling Op"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        cause = result["hypotheses"][0]["cause"]
        assert "SomeObscureException" in cause
        # No qualifier in parentheses after the class name
        assert "()" not in cause
        assert cause == "application error surge \u2014 SomeObscureException"

    def test_validation_exception_without_guard_keyword_no_qualifier(self):
        """ValidationException without schema/key text falls back to the plain class name."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_make_error_log_finding(
            "ev-logs-001",
            samples=["ClientError: An error occurred (ValidationException) when calling PutItem: Expression must not be empty"],
        )]

        result = build_hypotheses(SCOPE, outputs)

        cause = result["hypotheses"][0]["cause"]
        assert "ValidationException" in cause
        assert "key/schema mismatch" not in cause
        assert cause == "application error surge \u2014 ValidationException"


# ---------------------------------------------------------------------------
# trace_dependency_fault finding in hypothesis rules
# ---------------------------------------------------------------------------

_TRACE_FAULT_FINDING_STRONG = {
    "evidenceId": "ev-traces-003",
    "source": "traces",
    "resourceType": "xray-subsegment",
    "resourceName": "payment-service",
    "findingType": "trace_dependency_fault",
    "summary": "Faulted downstream call in sampled payment-service traces: s3.PutObject (fault, HTTP 500, 3/10 traces)",
    "score": 0.90,
    "details": {
        "traceService": "payment-service",
        "sampledTraceCount": 10,
        "faultedSubsegments": [{"name": "s3.PutObject", "namespace": "aws", "fault": True, "error": False, "throttle": False, "httpStatus": 500, "occurrences": 3}],
    },
}

_TRACE_FAULT_FINDING_WEAK = {
    "evidenceId": "ev-traces-003",
    "source": "traces",
    "resourceType": "xray-subsegment",
    "resourceName": "payment-service",
    "findingType": "trace_dependency_fault",
    "summary": "Faulted downstream call in sampled payment-service traces: dynamodb.get_item (error, HTTP 400, 3/10 traces)",
    "score": 0.70,
    "details": {
        "traceService": "payment-service",
        "sampledTraceCount": 10,
        "faultedSubsegments": [{"name": "dynamodb.get_item", "namespace": "aws", "fault": False, "error": True, "throttle": False, "httpStatus": 400, "occurrences": 3}],
    },
}


# ---------------------------------------------------------------------------
# Application error surge — trace fault corroboration boost
# ---------------------------------------------------------------------------

_NEW_ERROR_PATTERN_WITH_VALIDATION = {
    "evidenceId": "ev-logs-001",
    "source": "logs",
    "resourceType": "lambda-function",
    "resourceName": "my-function",
    "findingType": "new_error_pattern",
    "summary": "\"ERROR\" appears in incident window",
    "score": 0.3,
    "details": {
        "logGroup": "/aws/lambda/my-function",
        "pattern": "ERROR",
        "incidentCount": 30,
        "baselineCount": 0,
        "changeRatio": None,
        "exceptionSamples": [
            "[ERROR] ClientError: An error occurred (ValidationException) when calling the GetItem operation: The provided key element does not match the schema"
        ],
    },
}

_NEW_ERROR_PATTERN_SECOND = {
    "evidenceId": "ev-logs-002",
    "source": "logs",
    "resourceType": "lambda-function",
    "resourceName": "my-function",
    "findingType": "new_error_pattern",
    "summary": "\"Exception\" appears in incident window",
    "score": 0.3,
    "details": {
        "logGroup": "/aws/lambda/my-function",
        "pattern": "Exception",
        "incidentCount": 30,
        "baselineCount": 0,
        "changeRatio": None,
        "exceptionSamples": [],
    },
}

_TRACE_FAULT_WEAK_WITH_WINDOW = {
    **_TRACE_FAULT_FINDING_WEAK,
    "details": {
        **_TRACE_FAULT_FINDING_WEAK["details"],
        "incidentWindow": {"start": "2026-03-22T18:21:00Z", "end": "2026-03-22T18:24:00Z"},
    },
}


class TestApplicationErrorSurgeTraceCorroboration:
    """Trace fault (4xx) findings corroborate the application error surge hypothesis."""

    def test_weak_trace_fault_boosts_error_surge_confidence(self):
        """When error logs and a weak trace fault (4xx) are both present, the trace fault
        evidence corroborates h1 with a +0.05 boost and its evidence ID appears in h1.
        """
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_NEW_ERROR_PATTERN_WITH_VALIDATION, _NEW_ERROR_PATTERN_SECOND]
        outputs["traces"]["findings"] = [_TRACE_FAULT_WEAK_WITH_WINDOW]

        result = build_hypotheses(SCOPE, outputs)

        h1 = next(h for h in result["hypotheses"] if "application error surge" in h["cause"])

        # trace fault evidence ID must be in h1's supporting evidence
        assert "ev-traces-003" in h1["supportingEvidenceIds"], (
            "Trace fault evidence ID must appear in h1 supporting evidence"
        )

        # trace_fault_corroboration boost must be present and applied
        boost_names = [b["name"] for b in h1["confidenceBreakdown"]["boosts"] if b["applied"]]
        assert "trace_fault_corroboration" in boost_names, (
            f"Expected trace_fault_corroboration boost to be applied. Applied boosts: {boost_names}"
        )

    def test_trace_corroboration_raises_confidence_vs_logs_only(self):
        """h1 confidence with trace corroboration must exceed h1 confidence without it."""
        outputs_with_trace = _empty_outputs()
        outputs_with_trace["logs"]["findings"] = [_NEW_ERROR_PATTERN_WITH_VALIDATION, _NEW_ERROR_PATTERN_SECOND]
        outputs_with_trace["traces"]["findings"] = [_TRACE_FAULT_WEAK_WITH_WINDOW]

        outputs_without_trace = _empty_outputs()
        outputs_without_trace["logs"]["findings"] = [_NEW_ERROR_PATTERN_WITH_VALIDATION, _NEW_ERROR_PATTERN_SECOND]

        result_with = build_hypotheses(SCOPE, outputs_with_trace)
        result_without = build_hypotheses(SCOPE, outputs_without_trace)

        h1_with = next(h for h in result_with["hypotheses"] if "application error surge" in h["cause"])
        h1_without = next(h for h in result_without["hypotheses"] if "application error surge" in h["cause"])

        assert h1_with["confidence"] > h1_without["confidence"], (
            f"Trace corroboration must increase h1 confidence. "
            f"With: {h1_with['confidence']}, without: {h1_without['confidence']}"
        )
        # Adding trace evidence contributes +0.05 (trace_fault_corroboration) and also
        # unlocks +0.05 (cross_source_corroboration, since logs + traces = 2 sources),
        # so the net delta when no trace was present before is +0.10.
        assert h1_with["confidence"] == pytest.approx(
            h1_without["confidence"] + 0.10, rel=1e-4
        )

    def test_no_trace_fault_means_no_corroboration_boost(self):
        """When only error logs are present (no trace faults), the boost must not be applied."""
        outputs = _empty_outputs()
        outputs["logs"]["findings"] = [_NEW_ERROR_PATTERN_WITH_VALIDATION]

        result = build_hypotheses(SCOPE, outputs)

        h1 = next(h for h in result["hypotheses"] if "application error surge" in h["cause"])
        boost_names = [b["name"] for b in h1["confidenceBreakdown"]["boosts"] if b["applied"]]
        assert "trace_fault_corroboration" not in boost_names


class TestTraceDependencyFaultHypotheses:

    def test_strong_trace_fault_triggers_downstream_failure_hypothesis(self):
        """trace_dependency_fault with score>=0.75 (5xx/fault) fires downstream dependency failure."""
        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [_TRACE_FAULT_FINDING_STRONG]

        result = build_hypotheses(SCOPE, outputs)

        causes = [h["cause"] for h in result["hypotheses"]]
        assert any("downstream dependency failure" in c for c in causes)

        h = next(h for h in result["hypotheses"] if "downstream" in h["cause"])
        # confidence = 0.15 (base) + 0.20 (strong trace fault) = 0.35
        assert h["confidence"] == pytest.approx(0.35, rel=1e-4)
        assert "ev-traces-003" in h["supportingEvidenceIds"]

    def test_weak_trace_fault_4xx_triggers_downstream_client_error_hypothesis(self):
        """trace_dependency_fault with score<0.75 (4xx error-only) fires 'downstream dependency
        client error' — NOT 'downstream dependency failure'. Confidence boost is smaller.
        """
        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [_TRACE_FAULT_FINDING_WEAK]

        result = build_hypotheses(SCOPE, outputs)

        causes = [h["cause"] for h in result["hypotheses"]]
        assert any("downstream dependency client error" in c for c in causes), (
            f"Expected 'downstream dependency client error' in causes but got: {causes}"
        )
        assert not any("downstream dependency failure" in c for c in causes), (
            "4xx-only trace fault must NOT produce 'downstream dependency failure'"
        )

        h = next(h for h in result["hypotheses"] if "downstream dependency client error" in h["cause"])
        # confidence = 0.15 (base) + 0.10 (weak 4xx trace fault) = 0.25
        assert h["confidence"] == pytest.approx(0.25, rel=1e-4)
        assert "ev-traces-003" in h["supportingEvidenceIds"]

    def test_weak_trace_fault_rationale_clarifies_xray_4xx_vs_api_5xx_layers(self):
        """Weak trace fault rationale should explicitly clarify X-Ray 4xx vs API-layer 5xx.

        This avoids operator confusion in incidents where dependency request
        errors (400) propagate to API Gateway 5xx responses.
        """
        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [_TRACE_FAULT_FINDING_WEAK]

        result = build_hypotheses(SCOPE, outputs)

        h = next(h for h in result["hypotheses"] if "downstream dependency client error" in h["cause"])
        rationale = h["rationale"]
        assert "X-Ray dependency subsegment" in rationale
        assert "client-side 4xx" in rationale
        assert "API edge metrics may still show 5xx" in rationale

    def test_strong_trace_fault_cause_is_downstream_dependency_failure(self):
        """trace_dependency_fault with score>=0.75 (5xx/fault) produces cause
        'downstream dependency failure' — strong signal, not client error.
        """
        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [_TRACE_FAULT_FINDING_STRONG]

        result = build_hypotheses(SCOPE, outputs)

        h = next(h for h in result["hypotheses"] if "downstream" in h["cause"])
        assert h["cause"] == "downstream dependency failure"

    def test_trace_fault_evidence_id_in_supporting_evidence(self):
        """trace_dependency_fault evidenceId appears in downstream hypothesis supportingEvidenceIds."""
        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [_TRACE_FAULT_FINDING_STRONG]

        result = build_hypotheses(SCOPE, outputs)

        h = next(h for h in result["hypotheses"] if "downstream" in h["cause"])
        assert "ev-traces-003" in h["supportingEvidenceIds"]

    def test_trace_fault_plus_log_timeout_both_contribute(self):
        """trace_dependency_fault + log timeout → combined boost in downstream rule."""
        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [_TRACE_FAULT_FINDING_STRONG]
        outputs["logs"]["findings"] = [_TIMEOUT_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        h = next(h for h in result["hypotheses"] if "downstream" in h["cause"])
        # 0.15 (base) + 0.20 (timeout log) + 0.20 (strong trace fault) = 0.55
        assert h["confidence"] == pytest.approx(0.55, rel=1e-4)
        assert "ev-traces-003" in h["supportingEvidenceIds"]
        assert "ev-logs-002" in h["supportingEvidenceIds"]

    def test_mirror_trace_fault_findings_are_deduplicated(self):
        """Two trace_dependency_fault findings with the same faulted subsegment names and
        incident window from different X-Ray service nodes (e.g. Lambda node + DynamoDB node)
        must collapse to one. Confidence must not be double-counted.
        """
        primary = {
            **_TRACE_FAULT_FINDING_WEAK,
            "evidenceId": "ev-traces-003",
            "resourceName": "my-lambda-function",
            "details": {
                **_TRACE_FAULT_FINDING_WEAK["details"],
                "traceService": "my-lambda-function",
                "incidentWindow": {"start": "2026-03-22T18:21:00Z", "end": "2026-03-22T18:24:00Z"},
            },
        }
        mirror = {
            **_TRACE_FAULT_FINDING_WEAK,
            "evidenceId": "ev-traces-007",
            "resourceName": "my-dynamodb-table",        # different X-Ray service node
            "details": {
                **_TRACE_FAULT_FINDING_WEAK["details"],
                "traceService": "my-dynamodb-table",
                "incidentWindow": {"start": "2026-03-22T18:21:00Z", "end": "2026-03-22T18:24:00Z"},
            },
        }

        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [primary, mirror]

        result = build_hypotheses(SCOPE, outputs)

        h = next(h for h in result["hypotheses"] if "downstream dependency client error" in h["cause"])
        # confidence = 0.15 (base) + 0.10 * 1 (single weak fault after dedup) = 0.25
        assert h["confidence"] == pytest.approx(0.25, rel=1e-4), (
            f"Mirror findings were double-counted. Got {h['confidence']}, expected 0.25"
        )

    def test_distinct_trace_fault_findings_are_not_deduplicated(self):
        """Two trace_dependency_fault findings with different faulted subsegment names
        represent independent faults and must NOT be merged.
        """
        first = {
            **_TRACE_FAULT_FINDING_WEAK,
            "evidenceId": "ev-traces-003",
            "details": {
                **_TRACE_FAULT_FINDING_WEAK["details"],
                "faultedSubsegments": [{"name": "dynamodb.get_item", "namespace": "aws", "fault": False, "error": True, "throttle": False, "httpStatus": 400, "occurrences": 3}],
                "incidentWindow": {"start": "2026-03-22T18:21:00Z", "end": "2026-03-22T18:24:00Z"},
            },
        }
        second = {
            **_TRACE_FAULT_FINDING_WEAK,
            "evidenceId": "ev-traces-008",
            "resourceName": "other-service",
            "details": {
                **_TRACE_FAULT_FINDING_WEAK["details"],
                "traceService": "other-service",
                "faultedSubsegments": [{"name": "s3.GetObject", "namespace": "aws", "fault": False, "error": True, "throttle": False, "httpStatus": 400, "occurrences": 3}],
                "incidentWindow": {"start": "2026-03-22T18:21:00Z", "end": "2026-03-22T18:24:00Z"},
            },
        }

        outputs = _empty_outputs()
        outputs["traces"]["findings"] = [first, second]

        result = build_hypotheses(SCOPE, outputs)

        h = next(h for h in result["hypotheses"] if "downstream" in h["cause"])
        # confidence = 0.15 (base) + 0.10 * 2 (two distinct faults) = 0.35
        assert h["confidence"] == pytest.approx(0.35, rel=1e-4), (
            f"Distinct faults were unexpectedly merged. Got {h['confidence']}, expected 0.35"
        )


# ---------------------------------------------------------------------------
# Metric finding deduplication
# ---------------------------------------------------------------------------

class TestMetricDeduplication:

    def test_duplicate_metric_findings_collapsed_to_one(self):
        """Two metric findings identical in signal (same namespace/metric/stat/values)
        but different evidenceIds are deduplicated — only the first is kept."""
        outputs = _empty_outputs()
        # Simulate Lambda Duration appearing twice with different dimension keys
        # but the same observed values (as happens with FunctionName vs Resource dims).
        finding_a = {
            "evidenceId": "ev-metrics-003",
            "source": "metrics",
            "resourceType": "lambda",
            "resourceName": "my-fn",
            "findingType": "latency_spike",
            "summary": "Duration (Average) changed 150.6x vs baseline [latency_spike]",
            "score": 1.0,
            "details": {
                "metricName": "Duration",
                "namespace": "AWS/Lambda",
                "stat": "Average",
                "currentValue": 6000.0,
                "baselineValue": 39.85,
            },
        }
        finding_b = {
            "evidenceId": "ev-metrics-008",
            "source": "metrics",
            "resourceType": "lambda",
            "resourceName": "my-fn",
            "findingType": "latency_spike",
            "summary": "Duration (Average) changed 150.6x vs baseline [latency_spike]",
            "score": 1.0,
            "details": {
                "metricName": "Duration",
                "namespace": "AWS/Lambda",
                "stat": "Average",
                "currentValue": 6000.0,
                "baselineValue": 39.85,
            },
        }
        outputs["metrics"]["findings"] = [finding_a, finding_b]
        # Add error log so the runtime latency rule fires and both IDs reach supportingEvidenceIds.
        outputs["logs"]["findings"] = [_ERROR_LOG_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        # Both findings are latency signals, but after dedup only one should appear
        # in any hypothesis's supportingEvidenceIds.
        all_ids = [eid for h in result["hypotheses"] for eid in h["supportingEvidenceIds"]]
        assert all_ids.count("ev-metrics-003") + all_ids.count("ev-metrics-008") <= 1, (
            "Duplicate metric finding leaked into hypothesis evidence"
        )

    def test_distinct_metric_findings_not_collapsed(self):
        """Two metric findings with the same metric name but different observed values
        are NOT deduplicated — they are independent signals."""
        outputs = _empty_outputs()
        finding_a = {
            "evidenceId": "ev-metrics-001",
            "source": "metrics",
            "resourceType": "lambda",
            "resourceName": "my-fn",
            "findingType": "latency_spike",
            "summary": "Duration (Average) changed 4x",
            "score": 0.8,
            "details": {
                "metricName": "Duration",
                "namespace": "AWS/Lambda",
                "stat": "Average",
                "currentValue": 400.0,
                "baselineValue": 100.0,
            },
        }
        finding_b = {
            "evidenceId": "ev-metrics-002",
            "source": "metrics",
            "resourceType": "api-gateway",
            "resourceName": "my-api",
            "findingType": "latency_spike",
            "summary": "IntegrationLatency (Average) changed 5x",
            "score": 0.9,
            "details": {
                "metricName": "IntegrationLatency",
                "namespace": "AWS/ApiGateway",
                "stat": "Average",
                "currentValue": 500.0,
                "baselineValue": 100.0,
            },
        }
        outputs["metrics"]["findings"] = [finding_a, finding_b]
        # Add error signal so runtime latency rule fires and includes both
        outputs["logs"]["findings"] = [_ERROR_LOG_FINDING]

        result = build_hypotheses(SCOPE, outputs)

        all_ids = [eid for h in result["hypotheses"] for eid in h["supportingEvidenceIds"]]
        assert "ev-metrics-001" in all_ids
        assert "ev-metrics-002" in all_ids
