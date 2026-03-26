"""
Unit tests for the Final Report Builder.

No AWS calls are made — all logic is pure Python over fixture dicts.
Schema validation uses jsonschema against schemas/final-report.schema.json.

Run from repo root:
    python -m pytest backend/orchestration/final_report_builder/tests/ -v
"""

import json
import os

import jsonschema
import pytest

from report_builder import build_report

# ---------------------------------------------------------------------------
# Schema path
# ---------------------------------------------------------------------------

FINAL_REPORT_SCHEMA_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..", "schemas", "final-report.schema.json",
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

HYPOTHESES = {
    "incidentId": "inc-test-001",
    "hypotheses": [
        {
            "id": "h1",
            "cause": "runtime latency regression",
            "rationale": "Latency spike and log errors correlate.",
            "supportingEvidenceIds": ["ev-metrics-001", "ev-logs-001"],
            "confidence": 0.65,
        },
        {
            "id": "h2",
            "cause": "resource exhaustion",
            "rationale": "CPU spike detected.",
            "supportingEvidenceIds": ["ev-metrics-002"],
            "confidence": 0.20,
        },
    ],
}

WORKER_OUTPUTS = {
    "metrics": {
        "incidentId": "inc-test-001",
        "source": "metrics",
        "findings": [
            {
                "evidenceId": "ev-metrics-001",
                "source": "metrics",
                "findingType": "latency_spike",
                "summary": "p95 latency increased 4.8x vs baseline",
                "score": 0.91,
            },
            {
                "evidenceId": "ev-metrics-002",
                "source": "metrics",
                "findingType": "cpu_spike",
                "summary": "CPU utilisation spike vs baseline",
                "score": 0.70,
            },
        ],
        "errors": [],
    },
    "logs": {
        "incidentId": "inc-test-001",
        "source": "logs",
        "findings": [
            {
                "evidenceId": "ev-logs-001",
                "source": "logs",
                "findingType": "error_log_spike",
                "summary": '"timeout" log count changed 8.0x vs baseline [error_log_spike]',
                "score": 0.84,
            }
        ],
        "errors": [],
    },
    "traces": {
        "incidentId": "inc-test-001",
        "source": "traces",
        "findings": [],
        "errors": [],
    },
}

EMPTY_HYPOTHESES = {"incidentId": "inc-test-001", "hypotheses": []}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBuildReport:

    def test_full_inputs_returns_required_fields(self):
        """Full inputs produce a report with all required fields present."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        assert result["incidentId"] == "inc-test-001"
        assert "summary" in result
        assert "topHypotheses" in result
        assert "evidenceHighlights" in result
        assert "operatorFocus" in result

    def test_top_hypotheses_derived_from_deterministic_hypotheses(self):
        """topHypotheses reflects the deterministic hypothesis order and confidence."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        assert len(result["topHypotheses"]) == 2
        assert result["topHypotheses"][0]["cause"] == "runtime latency regression"
        assert result["topHypotheses"][0]["confidence"] == pytest.approx(0.65, rel=1e-4)
        assert result["topHypotheses"][1]["cause"] == "resource exhaustion"

    def test_top_hypotheses_include_confidence_breakdown_when_available(self):
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "rationale": "Errors increased",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.45,
                    "confidenceBreakdown": {
                        "base": 0.25,
                        "boosts": [
                            {"name": "new_error_pattern_present", "value": 0.1, "applied": True}
                        ],
                        "totalBeforeCap": 0.35,
                        "cap": 0.7,
                        "final": 0.45,
                    },
                }
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, hypotheses)

        top = result["topHypotheses"][0]
        assert "confidenceBreakdown" in top
        assert top["confidenceBreakdown"]["base"] == pytest.approx(0.25, rel=1e-4)

    def test_summary_references_top_hypothesis_cause(self):
        """Summary mentions the cause of the top hypothesis."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        assert "runtime latency regression" in result["summary"]

    def test_summary_fallback_when_no_hypotheses(self):
        """Empty hypotheses list yields a fallback summary."""
        result = build_report(SCOPE, {}, EMPTY_HYPOTHESES)

        assert "No strong root-cause hypothesis" in result["summary"]

    def test_evidence_highlights_sorted_by_score_descending(self):
        """evidenceHighlights lists finding summaries highest score first."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        highlights = result["evidenceHighlights"]
        # Scores: ev-metrics-001 (0.91), ev-logs-001 (0.84) — only supporting evidence shown
        assert highlights[0] == "p95 latency increased 4.8x vs baseline"
        assert highlights[1] == '"timeout" log count changed 8.0x vs baseline [error_log_spike]'

    def test_evidence_highlights_prioritize_top_hypothesis_supporting_evidence(self):
        """Highlights should align with top hypothesis support, excluding unrelated high-score findings."""
        worker_outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-001",
                        "source": "metrics",
                        "findingType": "latency_spike",
                        "summary": "Supported metric finding",
                        "score": 0.4,
                    }
                ],
                "errors": [],
            },
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "findingType": "error_log_spike",
                        "summary": "Top-supported log finding",
                        "score": 0.5,
                    }
                ],
                "errors": [],
            },
            "changes": {
                "incidentId": "inc-test-001",
                "source": "changes",
                "findings": [],
                "errors": [],
            },
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [
                    {
                        "evidenceId": "ev-traces-001",
                        "source": "traces",
                        "findingType": "latency_dominant_segment",
                        "summary": "Unrelated but high-score trace segment",
                        "score": 0.99,
                    }
                ],
                "errors": [],
            },
        }

        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "rationale": "Log errors increased.",
                    "supportingEvidenceIds": ["ev-logs-001", "ev-metrics-001"],
                    "confidence": 0.45,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)

        # Only top-hypothesis supporting evidence should appear.
        assert result["evidenceHighlights"] == [
            "Top-supported log finding",
            "Supported metric finding",
        ]
        assert "Unrelated but high-score trace segment" not in result["evidenceHighlights"]

    def test_evidence_highlights_deduplicates_equivalent_trace_latency_spikes(self):
        """Equivalent mirrored trace latency findings should appear once in highlights."""
        worker_outputs = {
            "metrics": {"incidentId": "inc-test-001", "source": "metrics", "findings": [], "errors": []},
            "logs": {"incidentId": "inc-test-001", "source": "logs", "findings": [], "errors": []},
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [
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
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "runtime latency regression",
                    "supportingEvidenceIds": ["ev-traces-001", "ev-traces-004"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)

        assert len(result["evidenceHighlights"]) == 1
        assert "changed 48.3x vs baseline" in result["evidenceHighlights"][0]

    def test_evidence_highlights_capped_at_five(self):
        """At most 5 evidence highlights are returned even when more findings exist."""
        many_findings = [
            {
                "evidenceId": f"ev-metrics-{i:03d}",
                "source": "metrics",
                "findingType": "latency_spike",
                "summary": f"finding {i}",
                "score": float(i) / 10,
            }
            for i in range(8)
        ]
        outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": many_findings,
                "errors": [],
            },
        }

        result = build_report(SCOPE, outputs, EMPTY_HYPOTHESES)

        assert len(result["evidenceHighlights"]) <= 5

    def test_evidence_highlights_deduplicates_overlapping_log_patterns(self):
        """Two error_log_spike findings from the same resource keep only the highest-scored one."""
        worker_outputs = {
            "metrics": {"incidentId": "inc-test-001", "source": "metrics", "findings": [], "errors": []},
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "resourceName": "payment-service",
                        "findingType": "error_log_spike",
                        "summary": '"ERROR" log count changed 12.0x vs baseline',
                        "score": 1.0,
                        "details": {"pattern": "ERROR"},
                    },
                    {
                        "evidenceId": "ev-logs-002",
                        "source": "logs",
                        "resourceName": "payment-service",
                        "findingType": "error_log_spike",
                        "summary": '"Exception" log count changed 9.0x vs baseline',
                        "score": 0.80,
                        "details": {"pattern": "Exception"},
                    },
                ],
                "errors": [],
            },
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {"incidentId": "inc-test-001", "source": "traces", "findings": [], "errors": []},
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001", "ev-logs-002"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        highlights = result["evidenceHighlights"]

        # Only the higher-scored finding should appear.
        assert '"ERROR" log count changed 12.0x vs baseline' in highlights
        assert '"Exception" log count changed 9.0x vs baseline' not in highlights

    def test_worker_errors_aggregated_from_all_sources(self):
        """workerErrors aggregates errors from all worker envelopes."""
        outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [],
                "errors": [{"source": "AWS/ApplicationELB", "reason": "cloudwatch_error: ThrottlingException"}],
            },
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [],
                "errors": [{"source": "xray", "reason": "X-Ray tracing not enabled"}],
            },
        }

        result = build_report(SCOPE, outputs, EMPTY_HYPOTHESES)

        assert len(result["workerErrors"]) == 2
        sources = [e["source"] for e in result["workerErrors"]]
        assert "AWS/ApplicationELB" in sources
        assert "xray" in sources

    def test_empty_worker_outputs_no_highlights_no_errors(self):
        """Empty worker outputs produce empty evidenceHighlights and workerErrors."""
        result = build_report(SCOPE, {}, EMPTY_HYPOTHESES)

        assert result["evidenceHighlights"] == []
        assert result["workerErrors"] == []

    def test_null_ai_evaluation_uses_deterministic_hypotheses(self):
        """aiEvaluation=None → topHypotheses still comes from deterministic hypotheses."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=None)

        assert result["topHypotheses"][0]["cause"] == "runtime latency regression"

    def test_incident_window_copied_from_scope(self):
        """incidentWindow in the report matches scope.incidentWindow."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        assert result["incidentWindow"] == SCOPE["incidentWindow"]

    def test_baseline_window_copied_from_scope(self):
        """baselineWindow in the report matches scope.baselineWindow."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        assert result["baselineWindow"] == SCOPE["baselineWindow"]

    def test_supporting_evidence_ids_in_top_hypotheses(self):
        """topHypotheses carry supportingEvidenceIds from the deterministic hypotheses."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        h1 = result["topHypotheses"][0]
        assert "ev-metrics-001" in h1["supportingEvidenceIds"]
        assert "ev-logs-001" in h1["supportingEvidenceIds"]

    def test_confidence_explanation_contains_expected_core_fields(self):
        """confidenceExplanation is present and machine-readable."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        explanation = result["confidenceExplanation"]
        assert explanation["topHypothesisCause"] == "runtime latency regression"
        assert "whyRankedHighest" in explanation
        assert "strongestEvidence" in explanation
        assert "missingEvidence" in explanation
        assert "contribution" in explanation
        assert explanation["contribution"]["rankingDriver"] == "deterministic"

    def test_confidence_explanation_strongest_evidence_top_three_sorted(self):
        """strongestEvidence is capped at top 3 and sorted by score desc."""
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "runtime latency regression",
                    "supportingEvidenceIds": [
                        "ev-metrics-001",
                        "ev-metrics-002",
                        "ev-logs-001",
                    ],
                    "confidence": 0.65,
                }
            ],
        }
        result = build_report(SCOPE, WORKER_OUTPUTS, hypotheses)

        strongest = result["confidenceExplanation"]["strongestEvidence"]
        assert len(strongest) == 3
        assert [item["evidenceId"] for item in strongest] == [
            "ev-metrics-001",  # 0.91
            "ev-logs-001",     # 0.84
            "ev-metrics-002",  # 0.70
        ]

    def test_confidence_explanation_strongest_evidence_deduplicates_equivalent_trace_latency(self):
        worker_outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-003",
                        "source": "metrics",
                        "resourceType": "lambda",
                        "resourceName": "fn-a",
                        "findingType": "latency_spike",
                        "summary": "Duration (Average) changed 34.7x vs baseline [latency_spike]",
                        "score": 1.0,
                    }
                ],
                "errors": [],
            },
            "logs": {"incidentId": "inc-test-001", "source": "logs", "findings": [], "errors": []},
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [
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
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "runtime latency regression",
                    "supportingEvidenceIds": ["ev-metrics-003", "ev-traces-001", "ev-traces-004"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        strongest = result["confidenceExplanation"]["strongestEvidence"]

        assert [item["evidenceId"] for item in strongest] == ["ev-metrics-003", "ev-traces-001"]

    def test_evidence_highlights_deduplicates_equivalent_dominant_segment_trace_findings(self):
        worker_outputs = {
            "metrics": {"incidentId": "inc-test-001", "source": "metrics", "findings": [], "errors": []},
            "logs": {"incidentId": "inc-test-001", "source": "logs", "findings": [], "errors": []},
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [
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
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "runtime latency regression",
                    "supportingEvidenceIds": ["ev-traces-002", "ev-traces-005"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)

        assert result["evidenceHighlights"] == [
            "Dominant segment in sampled service-a traces is fn-a (100% of sampled segment time)"
        ]

    def test_confidence_explanation_missing_evidence_always_includes_collection_gaps(self):
        """collectionGaps is always present, even when empty."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        missing = result["confidenceExplanation"]["missingEvidence"]
        assert "collectionGaps" in missing
        assert isinstance(missing["collectionGaps"], list)
        assert missing["collectionGaps"] == []

    def test_confidence_explanation_collection_gaps_populated_from_worker_errors(self):
        """collectionGaps mirrors aggregated worker errors."""
        outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [],
                "errors": [{"source": "metrics", "reason": "cloudwatch_error: ThrottlingException"}],
            }
        }
        result = build_report(SCOPE, outputs, EMPTY_HYPOTHESES)

        gaps = result["confidenceExplanation"]["missingEvidence"]["collectionGaps"]
        assert gaps == [{"source": "metrics", "reason": "cloudwatch_error: ThrottlingException"}]

    def test_confidence_explanation_uses_ai_missing_evidence_when_available(self):
        """AI missingEvidence is copied into confidenceExplanation.missingEvidence.aiIdentified."""
        ai_evaluation = {
            "topHypothesis": "runtime latency regression — payment-service",
            "assessments": [
                {
                    "cause": "runtime latency regression — payment-service",
                    "plausibility": 0.72,
                    "reason": "Deployment timing and logs align.",
                }
            ],
            "missingEvidence": ["api gateway access logs by route"],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "api gateway access logs by route"
        ]

    def test_confidence_explanation_filters_generic_ai_missing_evidence(self):
        """Generic boilerplate missingEvidence entries are filtered out."""
        ai_evaluation = {
            "topHypothesis": "runtime latency regression — payment-service",
            "assessments": [
                {
                    "cause": "runtime latency regression — payment-service",
                    "plausibility": 0.72,
                    "reason": "Deployment timing and logs align.",
                }
            ],
            "missingEvidence": [
                "container restart count",
                "database connection pool metrics",
                "api gateway access logs by route",
                "api gateway access logs by route",
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "api gateway access logs by route"
        ]

    def test_confidence_explanation_filters_additional_generic_ai_missing_evidence_substrings(self):
        ai_evaluation = {
            "topHypothesis": "application error surge",
            "assessments": [
                {
                    "cause": "application error surge",
                    "plausibility": 0.7,
                    "reason": "Runtime errors increased.",
                }
            ],
            "missingEvidence": [
                "Detailed stack traces for RuntimeError to identify the exact code path",
                "User requests during the incident window to correlate with error spikes",
                "X-Ray traces with detailed downstream call failures",
                "API Gateway route-level status code counts",
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "API Gateway route-level status code counts"
        ]

    def test_confidence_explanation_filters_more_generic_traffic_and_window_phrases(self):
        ai_evaluation = {
            "topHypothesis": "application error surge",
            "assessments": [
                {
                    "cause": "application error surge",
                    "plausibility": 0.7,
                    "reason": "Runtime errors increased.",
                }
            ],
            "missingEvidence": [
                "More detailed logs from the lambda function",
                "User traffic patterns during the incident window",
                "CloudWatch Insights query grouped by route",
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "CloudWatch Insights query grouped by route"
        ]

    def test_confidence_explanation_filters_singular_logs_phrase_variant(self):
        ai_evaluation = {
            "topHypothesis": "application error surge",
            "assessments": [
                {
                    "cause": "application error surge",
                    "plausibility": 0.7,
                    "reason": "Runtime errors increased.",
                }
            ],
            "missingEvidence": [
                "Detailed error messages from downstream services",
                "API Gateway route-level status code counts",
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "API Gateway route-level status code counts"
        ]

    def test_confidence_explanation_keeps_exception_scoped_stack_trace_request(self):
        ai_evaluation = {
            "topHypothesis": "application error surge",
            "assessments": [
                {
                    "cause": "application error surge",
                    "plausibility": 0.7,
                    "reason": "Runtime errors increased.",
                }
            ],
            "missingEvidence": [
                "Detailed stack traces for RuntimeError occurrences",
                "X-Ray traces with more detailed downstream request information",
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "Detailed stack traces for RuntimeError occurrences"
        ]

    def test_confidence_explanation_ai_contribution_match_and_plausibility(self):
        """AI contribution reflects matching top hypothesis and plausibility when available."""
        ai_evaluation = {
            "topHypothesis": "runtime latency regression",
            "assessments": [
                {
                    "cause": "runtime latency regression",
                    "plausibility": 0.72,
                    "reason": "Latency spike and log errors align.",
                }
            ],
            "missingEvidence": [],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)
        ai = result["confidenceExplanation"]["contribution"]["ai"]
        assert ai["assessmentAvailable"] is True
        assert ai["topHypothesisMatch"] is True
        assert ai["plausibility"] == pytest.approx(0.72, rel=1e-4)
        assert ai["reason"] == "Latency spike and log errors align."

    def test_ai_assessments_deduplicates_same_cause_keeps_highest_plausibility(self):
        ai_evaluation = {
            "topHypothesis": "application error surge — RuntimeError",
            "assessments": [
                {
                    "cause": "application error surge — RuntimeError",
                    "plausibility": 0.85,
                    "reason": "High confidence explanation.",
                },
                {
                    "cause": "application error surge — RuntimeError",
                    "plausibility": 0.15,
                    "reason": "Low confidence duplicate explanation.",
                },
            ],
            "missingEvidence": [],
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge — RuntimeError",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, hypotheses, ai_evaluation=ai_evaluation)

        assert result["aiAssessments"] == [
            {
                "cause": "application error surge — RuntimeError",
                "plausibility": 0.85,
                "reason": "High confidence explanation.",
            }
        ]
        ai = result["confidenceExplanation"]["contribution"]["ai"]
        assert ai["assessmentAvailable"] is True
        assert ai["plausibility"] == pytest.approx(0.85, rel=1e-4)
        assert ai["reason"] == "High confidence explanation."

    def test_confidence_explanation_ai_contribution_empty_when_no_assessments(self):
        """Without AI assessments, AI contribution fields are explicit null/false."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=None)
        ai = result["confidenceExplanation"]["contribution"]["ai"]
        assert ai["assessmentAvailable"] is False
        assert ai["topHypothesisMatch"] is None
        assert ai["plausibility"] is None
        assert ai["reason"] is None
        assert ai["unavailableReason"] == "ai_evaluation_not_returned"

    def test_confidence_explanation_ai_unavailable_reason_from_evaluation_payload(self):
        result = build_report(
            SCOPE,
            WORKER_OUTPUTS,
            HYPOTHESES,
            ai_evaluation={"unavailableReason": "bedrock_call_failed"},
        )
        ai = result["confidenceExplanation"]["contribution"]["ai"]
        assert ai["assessmentAvailable"] is False
        assert ai["unavailableReason"] == "bedrock_call_failed"

    def test_schema_valid_full_report(self):
        """Full report output conforms to schemas/final-report.schema.json."""
        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES)

        with open(FINAL_REPORT_SCHEMA_PATH) as f:
            schema = json.load(f)
        jsonschema.validate(result, schema)

    def test_schema_valid_minimal_report(self):
        """Minimal report (empty workers, no hypotheses) also conforms to schema."""
        result = build_report(SCOPE, {}, EMPTY_HYPOTHESES)

        with open(FINAL_REPORT_SCHEMA_PATH) as f:
            schema = json.load(f)
        jsonschema.validate(result, schema)

    def test_operator_focus_uses_strongest_supporting_resource(self):
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001", "ev-metrics-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, hypotheses)
        focus = result["operatorFocus"]

        assert focus["primaryImplicatedResource"] is not None
        assert focus["primaryImplicatedResource"]["evidenceId"] == "ev-metrics-001"
        assert isinstance(focus["whereToLookFirst"], str) and focus["whereToLookFirst"].strip() != ""

    def test_operator_focus_top_error_pattern_comes_from_supporting_logs_details(self):
        worker_outputs = {
            **WORKER_OUTPUTS,
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "resourceType": "lambda-function",
                        "resourceName": "payment-service",
                        "findingType": "new_error_pattern",
                        "summary": '"timeout" appears in incident window with no baseline occurrences',
                        "score": 0.84,
                        "details": {
                            "pattern": "timeout",
                            "incidentCount": 42,
                            "baselineCount": 0,
                            "changeRatio": None,
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        top_error = result["operatorFocus"]["topErrorPattern"]

        assert top_error is not None
        assert top_error["pattern"] == "timeout"
        assert top_error["incidentCount"] == 42
        assert top_error["baselineCount"] == 0

    def test_operator_focus_top_error_pattern_prefers_logs_over_metrics(self):
        worker_outputs = {
            **WORKER_OUTPUTS,
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-001",
                        "source": "metrics",
                        "resourceType": "api-gateway",
                        "resourceName": "my-api",
                        "findingType": "error_rate_spike",
                        "summary": "5XXError (Sum) present in incident window with baseline at zero",
                        "score": 0.9,
                        "details": {"metricName": "5XXError"},
                    }
                ],
                "errors": [],
            },
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "resourceType": "lambda-function",
                        "resourceName": "my-func",
                        "findingType": "new_error_pattern",
                        "summary": '"RuntimeError" appears in incident window with no baseline occurrences',
                        "score": 0.3,
                        "details": {
                            "pattern": "RuntimeError",
                            "incidentCount": 20,
                            "baselineCount": 0,
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-metrics-001", "ev-logs-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        top_error = result["operatorFocus"]["topErrorPattern"]

        assert top_error is not None
        assert top_error["source"] == "logs"
        assert top_error["pattern"] == "RuntimeError"

    def test_where_to_look_first_uses_metrics_wording_for_non_log_pattern(self):
        worker_outputs = {
            **WORKER_OUTPUTS,
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-001",
                        "source": "metrics",
                        "resourceType": "api-gateway",
                        "resourceName": "my-api",
                        "findingType": "error_rate_spike",
                        "summary": "5XXError (Sum) present in incident window with baseline at zero",
                        "score": 0.9,
                        "details": {"metricName": "5XXError"},
                    }
                ],
                "errors": [],
            },
            "logs": {"incidentId": "inc-test-001", "source": "logs", "findings": [], "errors": []},
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-metrics-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        where_to_look = result["operatorFocus"]["whereToLookFirst"]

        assert "inspect metrics" in where_to_look

    def test_operator_focus_top_error_pattern_passes_through_exception_samples(self):
        """exceptionSamples from logs finding details are carried into topErrorPattern."""
        worker_outputs = {
            **WORKER_OUTPUTS,
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "findingType": "error_log_spike",
                        "summary": '"ERROR" log count changed 8.0x vs baseline',
                        "score": 0.84,
                        "details": {
                            "pattern": "ERROR",
                            "incidentCount": 80,
                            "baselineCount": 10,
                            "changeRatio": 8.0,
                            "exceptionSamples": [
                                "ValidationException: The provided key element does not match the schema",
                                "Simulated error for testing (param1=1)",
                            ],
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        top_error = result["operatorFocus"]["topErrorPattern"]

        assert top_error is not None
        assert "exceptionSamples" in top_error
        assert top_error["exceptionSamples"] == [
            "ValidationException: The provided key element does not match the schema",
            "Simulated error for testing (param1=1)",
        ]

    def test_operator_focus_top_error_pattern_no_exception_samples_when_absent(self):
        """topErrorPattern does not include exceptionSamples when details has none."""
        worker_outputs = {
            **WORKER_OUTPUTS,
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "findingType": "error_log_spike",
                        "summary": '"ERROR" log count changed 3.0x vs baseline',
                        "score": 0.60,
                        "details": {
                            "pattern": "ERROR",
                            "incidentCount": 30,
                            "baselineCount": 10,
                            "changeRatio": 3.0,
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.5,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        top_error = result["operatorFocus"]["topErrorPattern"]

        assert top_error is not None
        assert "exceptionSamples" not in top_error

    def test_operator_focus_top_error_pattern_caps_exception_samples_at_three(self):
        """exceptionSamples in topErrorPattern is capped at 3 even if details has more."""
        worker_outputs = {
            **WORKER_OUTPUTS,
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "findingType": "error_log_spike",
                        "summary": '"ERROR" log count changed 5.0x vs baseline',
                        "score": 0.75,
                        "details": {
                            "pattern": "ERROR",
                            "incidentCount": 50,
                            "baselineCount": 10,
                            "changeRatio": 5.0,
                            "exceptionSamples": [
                                "Error A",
                                "Error B",
                                "Error C",
                                "Error D",  # this fourth entry must be dropped
                            ],
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.5,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        top_error = result["operatorFocus"]["topErrorPattern"]

        assert top_error is not None
        assert len(top_error["exceptionSamples"]) == 3

    def test_where_to_look_first_exception_samples_produces_short_sentence(self):
        """whereToLookFirst is a short fixed sentence when exception samples exist.

        The full exception text belongs to topErrorPattern.exceptionSamples;
        whereToLookFirst must not embed it.
        """
        long_sample = (
            "ClientError: An error occurred (ValidationException) when calling the GetItem operation: "
            "The provided key element does not match the schema"
        )  # 150 chars — longer than the 120-char limit
        assert len(long_sample) > 120

        worker_outputs = {
            **WORKER_OUTPUTS,
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "findingType": "error_log_spike",
                        "summary": '"ERROR" log count spike',
                        "score": 0.84,
                        "details": {
                            "pattern": "ERROR",
                            "incidentCount": 80,
                            "baselineCount": 10,
                            "changeRatio": 8.0,
                            "exceptionSamples": [long_sample],
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        where_to_look = result["operatorFocus"]["whereToLookFirst"]

        assert "exception errors present" in where_to_look
        assert "see sample messages below" in where_to_look
        # Exception text must not be embedded here — it belongs to topErrorPattern
        assert "ValidationException" not in where_to_look
        assert "\u2026" not in where_to_look
        assert "look for:" not in where_to_look

    def test_where_to_look_first_short_sample_same_short_sentence(self):
        """Short exception samples also produce the concise fixed sentence (no embedding)."""
        short_sample = "ValidationException: The provided key element does not match the schema"
        assert len(short_sample) <= 120

        worker_outputs = {
            **WORKER_OUTPUTS,
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "findingType": "error_log_spike",
                        "summary": '"ERROR" log count spike',
                        "score": 0.84,
                        "details": {
                            "pattern": "ERROR",
                            "incidentCount": 10,
                            "baselineCount": 0,
                            "changeRatio": None,
                            "exceptionSamples": [short_sample],
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.5,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        where_to_look = result["operatorFocus"]["whereToLookFirst"]

        assert "exception errors present" in where_to_look
        assert short_sample not in where_to_look
        assert "…" not in where_to_look

    def test_where_to_look_first_cross_component_prefers_log_component_then_metrics(self):
        worker_outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-001",
                        "source": "metrics",
                        "resourceType": "api-gateway",
                        "resourceName": "simple-crud-api-dev",
                        "findingType": "error_rate_spike",
                        "summary": "5XXError (Sum) present in incident window with baseline at zero",
                        "score": 0.90,
                    }
                ],
                "errors": [],
            },
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "resourceType": "lambda-function",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "new_error_pattern",
                        "summary": '"ERROR" appears in incident window with no baseline occurrences',
                        "score": 0.30,
                        "details": {
                            "pattern": "ERROR",
                            "incidentCount": 76,
                            "baselineCount": 0,
                            "exceptionSamples": ["[ERROR] RuntimeError: Simulated error"],
                        },
                    }
                ],
                "errors": [],
            },
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {"incidentId": "inc-test-001", "source": "traces", "findings": [], "errors": []},
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-metrics-001", "ev-logs-001"],
                    "confidence": 0.6,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        where_to_look = result["operatorFocus"]["whereToLookFirst"]

        assert "Start with simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI logs" in where_to_look
        assert "then correlate with simple-crud-api-dev metrics" in where_to_look

    def test_top_error_pattern_exception_samples_are_deduplicated(self):
        worker_outputs = {
            **WORKER_OUTPUTS,
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-001",
                        "source": "logs",
                        "findingType": "new_error_pattern",
                        "summary": '"ERROR" appears in incident window with no baseline occurrences',
                        "score": 0.84,
                        "details": {
                            "pattern": "ERROR",
                            "incidentCount": 76,
                            "baselineCount": 0,
                            "changeRatio": None,
                            "exceptionSamples": [
                                "[ERROR] RuntimeError: Simulated error for testing (param1=1)",
                                "[ERROR] RuntimeError: Simulated error for testing (param1=1)",
                                "[ERROR] RuntimeError: Simulated error for testing (param1=1)",
                            ],
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001"],
                    "confidence": 0.55,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)

        assert result["operatorFocus"]["topErrorPattern"]["exceptionSamples"] == [
            "[ERROR] RuntimeError: Simulated error for testing (param1=1)"
        ]

    def test_why_ranked_highest_single_hypothesis_includes_supporting_evidence_context(self):
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge",
                    "supportingEvidenceIds": ["ev-logs-001", "ev-metrics-001"],
                    "confidence": 0.55,
                }
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, hypotheses)
        msg = result["confidenceExplanation"]["whyRankedHighest"]

        assert "Only one deterministic hypothesis was generated with confidence 55%" in msg
        assert "supported by 2 strongest evidence item(s)" in msg
        assert "across 2 source(s)" in msg

    def test_why_ranked_highest_source_count_uses_all_supporting_evidence_not_only_top3(self):
        """Source count should reflect all supporting evidence IDs, even if top-3 strongest are same source."""
        worker_outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-003",
                        "source": "metrics",
                        "resourceType": "lambda",
                        "resourceName": "fn-a",
                        "findingType": "latency_spike",
                        "summary": "Duration changed 34.7x",
                        "score": 1.0,
                    },
                    {
                        "evidenceId": "ev-metrics-016",
                        "source": "metrics",
                        "resourceType": "api-gateway",
                        "resourceName": "my-api",
                        "findingType": "latency_spike",
                        "summary": "IntegrationLatency changed 39.9x",
                        "score": 1.0,
                    },
                    {
                        "evidenceId": "ev-metrics-018",
                        "source": "metrics",
                        "resourceType": "api-gateway",
                        "resourceName": "my-api",
                        "findingType": "latency_spike",
                        "summary": "Latency changed 36.1x",
                        "score": 1.0,
                    },
                ],
                "errors": [],
            },
            "logs": {"incidentId": "inc-test-001", "source": "logs", "findings": [], "errors": []},
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [
                    {
                        "evidenceId": "ev-traces-001",
                        "source": "traces",
                        "resourceType": "xray-service",
                        "resourceName": "fn-a",
                        "findingType": "trace_latency_spike",
                        "summary": "X-Ray response time changed 48.3x",
                        "score": 1.0,
                        "details": {
                            "incidentAvgResponseTimeMs": 1234.9,
                            "baselineAvgResponseTimeMs": 25.55,
                            "changeRatio": 48.3327,
                            "incidentWindow": {"start": "2026-03-23T10:19:00Z", "end": "2026-03-23T10:22:00Z"},
                            "baselineWindow": {"start": "2026-03-23T10:16:00Z", "end": "2026-03-23T10:19:00Z"},
                        },
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "runtime latency regression",
                    "supportingEvidenceIds": [
                        "ev-metrics-003",
                        "ev-metrics-016",
                        "ev-metrics-018",
                        "ev-traces-001",
                    ],
                    "confidence": 0.70,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        msg = result["confidenceExplanation"]["whyRankedHighest"]

        # Strongest top-3 may all be metrics, but source count must include traces support.
        assert "supported by 3 strongest evidence item(s)" in msg
        assert "across 2 source(s)" in msg

    def test_full_incident_like_report_matches_expected_operator_and_ai_outcomes(self):
        """Locks expected output behavior for the incident-like RuntimeError + API GW 5XX scenario."""
        scope = {
            "incidentId": "inc-20260322-3e109c71",
            "incidentWindow": {"start": "2026-03-22T10:04:00Z", "end": "2026-03-22T10:07:00Z"},
            "baselineWindow": {"start": "2026-03-22T10:01:00Z", "end": "2026-03-22T10:04:00Z"},
        }
        worker_outputs = {
            "metrics": {
                "incidentId": "inc-20260322-3e109c71",
                "source": "metrics",
                "findings": [
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
                            "incidentWindow": {"start": "2026-03-22T10:04:00Z", "end": "2026-03-22T10:07:00Z"},
                            "baselineWindow": {"start": "2026-03-22T10:01:00Z", "end": "2026-03-22T10:04:00Z"},
                        },
                    }
                ],
                "errors": [],
            },
            "logs": {
                "incidentId": "inc-20260322-3e109c71",
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
                ],
                "errors": [],
            },
            "changes": {"incidentId": "inc-20260322-3e109c71", "source": "changes", "findings": [], "errors": []},
            "traces": {
                "incidentId": "inc-20260322-3e109c71",
                "source": "traces",
                "findings": [
                    {
                        "evidenceId": "ev-traces-002",
                        "source": "traces",
                        "resourceType": "xray-service",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "latency_dominant_segment",
                        "summary": "Dominant segment in sampled traces is lambda",
                        "score": 1.0,
                    }
                ],
                "errors": [],
            },
        }
        hypotheses = {
            "incidentId": "inc-20260322-3e109c71",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "application error surge — RuntimeError",
                    "rationale": "App/runtime errors increased.",
                    "supportingEvidenceIds": ["ev-logs-001", "ev-logs-012", "ev-metrics-023"],
                    "confidence": 0.6,
                    "confidenceBreakdown": {
                        "base": 0.25,
                        "boosts": [
                            {"name": "new_error_pattern_present", "value": 0.1, "applied": True},
                            {"name": "incident_error_count_ge_20", "value": 0.1, "applied": True},
                            {"name": "multiple_error_log_findings", "value": 0.1, "applied": True},
                            {"name": "cross_source_corroboration", "value": 0.05, "applied": True},
                        ],
                        "totalBeforeCap": 0.6,
                        "cap": 0.7,
                        "final": 0.6,
                    },
                }
            ],
        }
        ai_evaluation = {
            "topHypothesis": "application error surge — RuntimeError",
            "assessments": [
                {
                    "cause": "application error surge — RuntimeError",
                    "plausibility": 0.85,
                    "reason": "Application/runtime error logs increased in the incident window versus baseline.",
                }
            ],
            "missingEvidence": [
                "Detailed stack traces for RuntimeError occurrences",
                "X-Ray traces with more detailed downstream request information",
                "User traffic patterns during the incident window",
            ],
        }

        result = build_report(scope, worker_outputs, hypotheses, ai_evaluation=ai_evaluation)

        assert result["incidentId"] == "inc-20260322-3e109c71"
        assert result["evidenceHighlights"] == [
            "5XXError (Sum) present in incident window with baseline at zero [error_rate_spike]",
            '"ERROR" appears in incident window with no baseline occurrences [new_error_pattern]',
            '"HTTP 5XX" appears in incident window with no baseline occurrences [new_error_pattern]',
        ]

        confidence = result["confidenceExplanation"]
        assert confidence["strongestEvidence"][0]["evidenceId"] == "ev-metrics-023"
        assert confidence["strongestEvidence"][1]["evidenceId"] == "ev-logs-001"
        assert confidence["strongestEvidence"][2]["evidenceId"] == "ev-logs-012"
        assert confidence["missingEvidence"]["aiIdentified"] == [
            "Detailed stack traces for RuntimeError occurrences"
        ]
        assert confidence["contribution"]["ai"]["assessmentAvailable"] is True
        assert confidence["contribution"]["ai"]["topHypothesisMatch"] is True
        assert confidence["contribution"]["ai"]["plausibility"] == pytest.approx(0.85, rel=1e-4)

        focus = result["operatorFocus"]
        assert focus["mostLikelyAffectedComponent"] == "simple-crud-api-dev"
        assert focus["primaryImplicatedResource"]["evidenceId"] == "ev-metrics-023"
        assert focus["topErrorPattern"]["evidenceId"] == "ev-logs-001"
        assert focus["topErrorPattern"]["exceptionSamples"] == [
            "[ERROR] RuntimeError: Simulated error for testing (param1=1)"
        ]
        assert focus["whereToLookFirst"] == (
            "Start with simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI logs — "
            "exception errors present; then correlate with simple-crud-api-dev metrics in the incident window."
        )

    def test_operator_focus_top_error_pattern_populated_from_lower_scored_log_evidence(self):
        """topErrorPattern is populated even when log findings are not in the top-3 strongest.

        Reproduces the timeout/latency scenario: top-3 strongest are all latency findings
        (score 1.0), but the top hypothesis also supports log error findings (score 0.3).
        The fallback must find the log finding and populate topErrorPattern.
        """
        worker_outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-003",
                        "source": "metrics",
                        "resourceType": "lambda",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "latency_spike",
                        "summary": "Duration (Average) changed 150.6x vs baseline [latency_spike]",
                        "score": 1.0,
                    },
                    {
                        "evidenceId": "ev-metrics-002",
                        "source": "metrics",
                        "resourceType": "lambda",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "error_rate_spike",
                        "summary": "Errors (Average) present in incident window with baseline at zero [error_rate_spike]",
                        "score": 0.3,
                        "details": {"metricName": "Errors"},
                    },
                ],
                "errors": [],
            },
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-003",
                        "source": "logs",
                        "resourceType": "lambda-function",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "new_error_pattern",
                        "summary": '"timeout" appears in incident window with no baseline occurrences [new_error_pattern]',
                        "score": 0.3,
                        "details": {
                            "pattern": "timeout",
                            "incidentCount": 31,
                            "baselineCount": 0,
                            "changeRatio": None,
                        },
                    },
                ],
                "errors": [],
            },
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [
                    {
                        "evidenceId": "ev-traces-001",
                        "source": "traces",
                        "resourceType": "xray-service",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "trace_latency_spike",
                        "summary": "X-Ray response time changed 246.8x vs baseline [trace_latency_spike]",
                        "score": 1.0,
                        "details": {
                            "incidentAvgResponseTimeMs": 6860.45,
                            "baselineAvgResponseTimeMs": 27.8,
                            "changeRatio": 246.7788,
                            "incidentWindow": {"start": "2026-03-23T12:52:00Z", "end": "2026-03-23T12:55:00Z"},
                            "baselineWindow": {"start": "2026-03-23T12:49:00Z", "end": "2026-03-23T12:52:00Z"},
                        },
                    },
                    {
                        "evidenceId": "ev-traces-002",
                        "source": "traces",
                        "resourceType": "xray-service",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "latency_dominant_segment",
                        "summary": "Dominant segment in sampled traces is simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI (100%)",
                        "score": 1.0,
                        "details": {
                            "dominantSegment": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                            "dominantSegmentPct": 100.0,
                            "dominantSegmentMs": 69085.0,
                            "sampledTraceCount": 10,
                            "incidentWindow": {"start": "2026-03-23T12:52:00Z", "end": "2026-03-23T12:55:00Z"},
                        },
                    },
                ],
                "errors": [],
            },
        }
        # Top hypothesis has 9 supporting IDs — top-3 strongest are all latency (score 1.0)
        hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "runtime latency regression",
                    "supportingEvidenceIds": [
                        "ev-metrics-003",
                        "ev-traces-001",
                        "ev-metrics-002",
                        "ev-logs-003",
                        "ev-traces-002",
                    ],
                    "confidence": 0.85,
                }
            ],
        }

        result = build_report(SCOPE, worker_outputs, hypotheses)
        top_error = result["operatorFocus"]["topErrorPattern"]

        assert top_error is not None, (
            "topErrorPattern must be populated from lower-scored log evidence "
            "when top-3 strongest are all latency findings"
        )
        assert top_error["source"] == "logs"
        assert top_error["pattern"] == "timeout"
        assert top_error["incidentCount"] == 31
        assert top_error["baselineCount"] == 0

    def test_operator_focus_includes_trace_dependency_hint_when_trace_fault_exists(self):
        worker_outputs = {
            **WORKER_OUTPUTS,
            "traces": {
                "incidentId": "inc-test-001",
                "source": "traces",
                "findings": [
                    {
                        "evidenceId": "ev-traces-003",
                        "source": "traces",
                        "resourceType": "xray-subsegment",
                        "resourceName": "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                        "findingType": "trace_dependency_fault",
                        "summary": "Faulted downstream call in sampled traces: dynamodb.get_item (fault, 10/10 traces)",
                        "score": 0.7,
                        "details": {
                            "sampledTraceCount": 10,
                            "faultedSubsegments": [
                                {
                                    "name": "dynamodb.get_item",
                                    "namespace": "local",
                                    "httpStatus": None,
                                    "occurrences": 10,
                                },
                                {
                                    "name": "DynamoDB",
                                    "namespace": "aws",
                                    "httpStatus": 400,
                                    "occurrences": 10,
                                }
                            ],
                        },
                    }
                ],
                "errors": [],
            },
        }

        result = build_report(SCOPE, worker_outputs, HYPOTHESES)
        hint = result["operatorFocus"].get("traceDependencyHint")

        assert hint is not None
        assert hint["evidenceId"] == "ev-traces-003"
        # Prefer AWS namespace + HTTP status-bearing subsegment over local wrapper.
        assert hint["subsegmentName"] == "DynamoDB"
        assert hint["namespace"] == "aws"
        assert hint["httpStatus"] == 400
        assert hint["sampledTraceCount"] == 10

    def test_confidence_explanation_filters_generic_ai_missing_evidence_new_variants(self):
        ai_evaluation = {
            "topHypothesis": "application error surge",
            "assessments": [
                {
                    "cause": "application error surge",
                    "plausibility": 0.7,
                    "reason": "Runtime errors increased.",
                }
            ],
            "missingEvidence": [
                "Detailed error logs from the application with specific stack traces",
                "X-Ray traces with detailed downstream call failures",
                "API Gateway route-level status code counts",
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "API Gateway route-level status code counts"
        ]

    def test_confidence_explanation_filters_generic_xray_payload_phrase_keeps_specific_schema_gap(self):
        """Filter broad X-Ray payload requests while keeping actionable schema/change gaps."""
        ai_evaluation = {
            "topHypothesis": "application error surge — ValidationException (key/schema mismatch)",
            "assessments": [
                {
                    "cause": "application error surge — ValidationException (key/schema mismatch)",
                    "plausibility": 0.6,
                    "reason": "ValidationException errors increased in the incident window.",
                }
            ],
            "missingEvidence": [
                "Detailed error logs from the Lambda function simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                "X-Ray traces with detailed downstream request and response payloads",
                "DynamoDB item schema details",
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert result["confidenceExplanation"]["missingEvidence"]["aiIdentified"] == [
            "DynamoDB item schema details"
        ]

    def test_ai_next_best_actions_is_included_when_valid(self):
        ai_evaluation = {
            "topHypothesis": "runtime latency regression — payment-service",
            "assessments": [
                {
                    "cause": "runtime latency regression — payment-service",
                    "plausibility": 0.72,
                    "reason": "Deployment timing and logs align.",
                }
            ],
            "missingEvidence": [],
            "nextBestActions": [
                {
                    "action": "Validate runtime behavior health",
                    "why": "Runtime latency indicators align with the incident.",
                    "evidenceIds": ["ev-logs-001", "ev-metrics-001"],
                    "expectedSignal": "Rollout errors or restart spikes in same window",
                    "confidence": 0.74,
                }
            ],
        }

        result = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation)

        assert "aiNextBestActions" in result
        assert result["aiNextBestActions"] == [
            {
                "action": "Validate runtime behavior health",
                "why": "Runtime latency indicators align with the incident.",
                "evidenceIds": ["ev-logs-001", "ev-metrics-001"],
                "expectedSignal": "Rollout errors or restart spikes in same window",
                "confidence": pytest.approx(0.74, rel=1e-4),
            }
        ]

    def test_ai_next_best_actions_is_omitted_when_missing_or_invalid(self):
        result_missing = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation={})
        assert "aiNextBestActions" not in result_missing

        ai_evaluation_invalid = {
            "topHypothesis": "runtime latency regression — payment-service",
            "assessments": [],
            "missingEvidence": [],
            "nextBestActions": [
                {
                    "action": "Check DB key schema",
                    "why": "Potential schema mismatch",
                    "evidenceIds": ["invalid-id-format"],
                    "expectedSignal": "ValidationException in logs",
                    "confidence": 0.7,
                }
            ],
        }
        result_invalid = build_report(SCOPE, WORKER_OUTPUTS, HYPOTHESES, ai_evaluation=ai_evaluation_invalid)
        assert "aiNextBestActions" not in result_invalid

    def test_ai_next_best_actions_capped_at_three_and_counterexample_generic(self):
        """Use-case-shaped hardening: keep behavior generic and bounded across services."""
        other_scope = {
            **SCOPE,
            "service": "orders-api",
        }
        other_outputs = {
            "metrics": {
                "incidentId": "inc-test-001",
                "source": "metrics",
                "findings": [
                    {
                        "evidenceId": "ev-metrics-101",
                        "source": "metrics",
                        "findingType": "latency_spike",
                        "summary": "p99 latency increased 3.2x",
                        "score": 0.9,
                    }
                ],
                "errors": [],
            },
            "logs": {
                "incidentId": "inc-test-001",
                "source": "logs",
                "findings": [
                    {
                        "evidenceId": "ev-logs-101",
                        "source": "logs",
                        "findingType": "new_error_pattern",
                        "summary": "SerializationException burst",
                        "score": 0.6,
                    }
                ],
                "errors": [],
            },
            "changes": {"incidentId": "inc-test-001", "source": "changes", "findings": [], "errors": []},
            "traces": {"incidentId": "inc-test-001", "source": "traces", "findings": [], "errors": []},
        }
        other_hypotheses = {
            "incidentId": "inc-test-001",
            "hypotheses": [
                {
                    "id": "h1",
                    "cause": "serialization mismatch",
                    "supportingEvidenceIds": ["ev-logs-101", "ev-metrics-101"],
                    "confidence": 0.55,
                }
            ],
        }
        ai_evaluation = {
            "topHypothesis": "serialization mismatch",
            "assessments": [
                {
                    "cause": "serialization mismatch",
                    "plausibility": 0.7,
                    "reason": "Error burst and latency align.",
                }
            ],
            "missingEvidence": [],
            "nextBestActions": [
                {"action": "A1", "why": "W1", "evidenceIds": ["ev-logs-101"], "expectedSignal": "S1", "confidence": 0.9},
                {"action": "A2", "why": "W2", "evidenceIds": ["ev-metrics-101"], "expectedSignal": "S2", "confidence": 0.8},
                {"action": "A3", "why": "W3", "evidenceIds": ["ev-logs-102"], "expectedSignal": "S3", "confidence": 0.7},
                {"action": "A4", "why": "W4", "evidenceIds": ["ev-metrics-102"], "expectedSignal": "S4", "confidence": 0.6},
            ],
        }

        result = build_report(other_scope, other_outputs, other_hypotheses, ai_evaluation=ai_evaluation)

        assert "aiNextBestActions" in result
        assert len(result["aiNextBestActions"]) == 3
        assert [a["action"] for a in result["aiNextBestActions"]] == ["A1", "A2", "A3"]
