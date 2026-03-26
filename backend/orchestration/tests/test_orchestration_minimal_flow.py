"""Minimal orchestration integration test (in-process, no Step Functions simulation).

Covers the practical happy-path chain:
worker outputs -> deterministic hypotheses -> final report assembly.
"""

from hypotheses_builder import build_hypotheses
from report_builder import build_report


def test_minimal_orchestration_chain_worker_outputs_to_final_report():
    scope = {
        "incidentId": "inc-integ-001",
        "service": "payment-service",
        "region": "eu-west-1",
        "incidentWindow": {"start": "2026-03-20T10:00:00Z", "end": "2026-03-20T10:15:00Z"},
        "baselineWindow": {"start": "2026-03-20T09:45:00Z", "end": "2026-03-20T10:00:00Z"},
    }
    worker_outputs = {
        "metrics": {
            "incidentId": "inc-integ-001",
            "source": "metrics",
            "findings": [
                {
                    "evidenceId": "ev-metrics-001",
                    "source": "metrics",
                    "resourceType": "alb",
                    "resourceName": "payment-service",
                    "findingType": "latency_spike",
                    "summary": "p95 latency increased 4.0x vs baseline",
                    "score": 0.9,
                    "details": {},
                }
            ],
            "errors": [],
        },
        "logs": {
            "incidentId": "inc-integ-001",
            "source": "logs",
            "findings": [
                {
                    "evidenceId": "ev-logs-001",
                    "source": "logs",
                    "resourceType": "lambda-function",
                    "resourceName": "payment-service",
                    "findingType": "error_log_spike",
                    "summary": "timeout log count changed 6.0x vs baseline",
                    "score": 0.8,
                    "details": {"pattern": "timeout"},
                }
            ],
            "errors": [],
        },
        "traces": {
            "incidentId": "inc-integ-001",
            "source": "traces",
            "findings": [],
            "errors": [],
        },
    }

    hypotheses = build_hypotheses(scope, worker_outputs)
    report = build_report(
        scope,
        worker_outputs,
        hypotheses,
        ai_evaluation=None,
    )

    assert report["incidentId"] == "inc-integ-001"
    assert isinstance(report["summary"], str) and report["summary"].strip() != ""
    assert isinstance(report["topHypotheses"], list)
    assert isinstance(report["evidenceHighlights"], list)
    assert "confidenceExplanation" in report
    assert report["confidenceExplanation"]["contribution"]["rankingDriver"] == "deterministic"
    assert "collectionGaps" in report["confidenceExplanation"]["missingEvidence"]
    assert isinstance(report["confidenceExplanation"]["missingEvidence"]["collectionGaps"], list)
    assert "operatorFocus" in report
    assert isinstance(report["operatorFocus"]["whereToLookFirst"], str)

    all_input_evidence_ids = {
        finding["evidenceId"]
        for source in ("metrics", "logs", "traces")
        for finding in worker_outputs[source]["findings"]
    }
    referenced_evidence_ids = {
        evidence_id
        for h in report["topHypotheses"]
        for evidence_id in h.get("supportingEvidenceIds", [])
    }
    assert referenced_evidence_ids.issubset(all_input_evidence_ids)


def test_incident_like_latency_error_trace_signals_produce_concrete_hypothesis():
    """Regression: incident-like evidence should not end as unknown_cause."""
    scope = {
        "incidentId": "inc-integ-incident-like-001",
        "service": "simple-crud-api-dev-items",
        "region": "eu-west-1",
        "incidentWindow": {"start": "2026-03-20T13:40:00Z", "end": "2026-03-20T13:54:00Z"},
        "baselineWindow": {"start": "2026-03-20T13:26:00Z", "end": "2026-03-20T13:40:00Z"},
    }
    worker_outputs = {
        "metrics": {
            "incidentId": "inc-integ-incident-like-001",
            "source": "metrics",
            "findings": [
                {
                    "evidenceId": "ev-metrics-010",
                    "source": "metrics",
                    "resourceType": "dynamodb-table",
                    "resourceName": "items",
                    "findingType": "metric_spike",
                    "summary": "SuccessfulRequestLatency (Average) changed 1.6x vs baseline [metric_spike]",
                    "score": 0.32,
                    "details": {"metricName": "SuccessfulRequestLatency"},
                }
            ],
            "errors": [],
        },
        "logs": {
            "incidentId": "inc-integ-incident-like-001",
            "source": "logs",
            "findings": [
                {
                    "evidenceId": "ev-logs-001",
                    "source": "logs",
                    "resourceType": "lambda-function",
                    "resourceName": "simple-crud-api-dev-items",
                    "findingType": "error_log_spike",
                    "summary": '"ERROR" log count changed 10.8x vs baseline [error_log_spike]',
                    "score": 1.0,
                    "details": {"pattern": "ERROR"},
                }
            ],
            "errors": [],
        },
        "traces": {
            "incidentId": "inc-integ-incident-like-001",
            "source": "traces",
            "findings": [
                {
                    "evidenceId": "ev-traces-001",
                    "source": "traces",
                    "resourceType": "xray-service",
                    "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                    "findingType": "latency_dominant_segment",
                    "summary": (
                        "Dominant segment in sampled simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI traces "
                        "is simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI "
                        "(100% of sampled segment time)"
                    ),
                    "score": 1.0,
                    "details": {"dominantSegment": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI"},
                }
            ],
            "errors": [],
        },
    }

    hypotheses = build_hypotheses(scope, worker_outputs)
    report = build_report(
        scope,
        worker_outputs,
        hypotheses,
        ai_evaluation=None,
    )

    assert report["topHypotheses"]
    assert report["topHypotheses"][0]["cause"] == "runtime latency regression"
    assert report["topHypotheses"][0]["cause"] != "unknown_cause"
    assert report["topHypotheses"][0]["supportingEvidenceIds"]


def test_logs_only_new_error_pattern_produces_non_unknown_report_hypothesis():
    """Regression: clear logs-only error surge should not become unknown_cause."""
    scope = {
        "incidentId": "inc-integ-logs-only-001",
        "service": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
        "region": "eu-west-1",
        "incidentWindow": {"start": "2026-03-21T08:16:00Z", "end": "2026-03-21T08:20:00Z"},
        "baselineWindow": {"start": "2026-03-21T08:12:00Z", "end": "2026-03-21T08:16:00Z"},
    }
    worker_outputs = {
        "metrics": {
            "incidentId": "inc-integ-logs-only-001",
            "source": "metrics",
            "findings": [],
            "errors": [],
        },
        "logs": {
            "incidentId": "inc-integ-logs-only-001",
            "source": "logs",
            "findings": [
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
                        "incidentCount": 29,
                        "baselineCount": 0,
                        "changeRatio": None,
                    },
                }
            ],
            "errors": [],
        },
        "traces": {
            "incidentId": "inc-integ-logs-only-001",
            "source": "traces",
            "findings": [],
            "errors": [],
        },
    }

    hypotheses = build_hypotheses(scope, worker_outputs)
    report = build_report(
        scope,
        worker_outputs,
        hypotheses,
        ai_evaluation=None,
    )

    assert report["topHypotheses"]
    assert report["topHypotheses"][0]["cause"] == "application error surge"
    assert report["topHypotheses"][0]["cause"] != "unknown_cause"
    assert report["topHypotheses"][0]["supportingEvidenceIds"] == ["ev-logs-001"]
    assert report["operatorFocus"]["topErrorPattern"] is not None
    assert report["operatorFocus"]["topErrorPattern"]["pattern"] == "ERROR"
