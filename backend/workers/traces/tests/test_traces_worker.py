"""
Unit tests for the Trace Worker.

Uses unittest.mock to patch the X-Ray client — no real AWS calls are made.
Schema validation uses jsonschema + referencing.Registry against
schemas/worker-output.schema.json.
"""

import json
import os
from pathlib import Path

import jsonschema
import pytest
from botocore.exceptions import ClientError
from referencing import Registry, Resource

from traces_worker import (
    collect_traces,
    _aggregate_segment_durations_ms,
    _extract_faulted_subsegments,
    _build_dependency_fault_finding,
    _build_trace_error_status_finding,
    _is_root_service,
)


SCHEMA_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "schemas",
        "worker-output.schema.json",
    )
)


SCOPE = {
    "incidentId": "inc-test-001",
    "service": "payment-service",
    "region": "eu-west-1",
    "incidentWindow": {"start": "2026-03-20T10:00:00Z", "end": "2026-03-20T10:15:00Z"},
    "baselineWindow": {"start": "2026-03-20T09:45:00Z", "end": "2026-03-20T10:00:00Z"},
    "traceServices": ["payment-service"],
}


def _summary(
    trace_id: str,
    response_seconds: float,
    *,
    has_fault: bool = False,
    has_error: bool = False,
    has_throttle: bool = False,
) -> dict:
    return {
        "Id": trace_id,
        "ResponseTime": response_seconds,
        "HasFault": has_fault,
        "HasError": has_error,
        "HasThrottle": has_throttle,
    }


def _root_segment_doc(name: str, start: float, end: float) -> str:
    return json.dumps({"name": name, "start_time": start, "end_time": end})


def _root_doc_with_subsegment(
    root_name: str,
    root_start: float,
    root_end: float,
    sub_name: str,
    *,
    fault: bool = False,
    error: bool = False,
    throttle: bool = False,
    http_status: int | None = None,
    namespace: str | None = None,
    nested_subsegments: list | None = None,
) -> str:
    """Return a JSON root segment document with one embedded subsegment."""
    sub: dict = {"name": sub_name}
    if fault:
        sub["fault"] = True
    if error:
        sub["error"] = True
    if throttle:
        sub["throttle"] = True
    if namespace is not None:
        sub["namespace"] = namespace
    if http_status is not None:
        sub["http"] = {"response": {"status": http_status}}
    if nested_subsegments is not None:
        sub["subsegments"] = nested_subsegments
    return json.dumps({
        "name": root_name,
        "start_time": root_start,
        "end_time": root_end,
        "subsegments": [sub],
    })


def _xray_error(code: str = "AccessDeniedException") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}},
        "GetTraceSummaries",
    )


class TestCollectTraces:

    def test_empty_trace_services_returns_silent_empty_output(self, mocker):
        scope = {**SCOPE, "traceServices": []}
        xray = mocker.MagicMock()

        result = collect_traces(scope, xray)

        assert result["incidentId"] == "inc-test-001"
        assert result["source"] == "traces"
        assert result["findings"] == []
        assert result["errors"] == []
        xray.get_trace_summaries.assert_not_called()

    def test_happy_path_latency_and_dominant_segment_findings(self, mocker):
        xray = mocker.MagicMock()

        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 3.0), _summary("t2", 2.8)]},  # incident
            {"TraceSummaries": [_summary("b1", 0.6), _summary("b2", 0.7)]},  # baseline
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [
                {
                    "Segments": [
                        {"Document": _root_segment_doc("payment-service", 10.0, 12.0)},
                        {"Document": _root_segment_doc("auth-service", 20.0, 20.2)},
                    ]
                }
            ]
        }

        result = collect_traces(SCOPE, xray)

        assert result["errors"] == []
        assert len(result["findings"]) == 2
        assert result["findings"][0]["evidenceId"] == "ev-traces-001"
        assert result["findings"][0]["findingType"] == "trace_latency_spike"
        assert result["findings"][1]["evidenceId"] == "ev-traces-002"
        assert result["findings"][1]["findingType"] == "latency_dominant_segment"

    def test_at_most_two_findings_per_trace_service_without_fault(self, mocker):
        """When sampled traces have no faulted subsegments or error status, each root service
        produces at most latency_spike + dominant_segment findings."""
        scope = {**SCOPE, "traceServices": ["svc-a", "svc-b"]}
        xray = mocker.MagicMock()

        # 2 calls per service for summaries: incident, baseline — no HasFault/HasError flags
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("a1", 3.0)]},
            {"TraceSummaries": [_summary("a0", 0.5)]},
            {"TraceSummaries": [_summary("b1", 2.5)]},
            {"TraceSummaries": [_summary("b0", 0.5)]},
        ]
        # Each service's trace documents use that service's own name as root segment
        # so both qualify as root services and can emit latency + dominant findings.
        xray.batch_get_traces.side_effect = [
            {"Traces": [{"Segments": [{"Document": _root_segment_doc("svc-a", 1.0, 2.0)}]}]},
            {"Traces": [{"Segments": [{"Document": _root_segment_doc("svc-b", 1.0, 2.0)}]}]},
        ]

        result = collect_traces(scope, xray)

        # With no faulted subsegments and no error status, findings are latency + dominant per service.
        assert len(result["findings"]) <= 4
        # With 4 slots per service: svc-a slots 0-3 → 001-004; svc-b slots 0-3 → 005-008
        assert {f["evidenceId"] for f in result["findings"]}.issubset(
            {"ev-traces-001", "ev-traces-002", "ev-traces-003", "ev-traces-004",
             "ev-traces-005", "ev-traces-006", "ev-traces-007", "ev-traces-008"}
        )

    def test_xray_error_in_summaries_populates_errors(self, mocker):
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = _xray_error("AccessDeniedException")

        result = collect_traces(SCOPE, xray)

        assert result["findings"] == []
        assert len(result["errors"]) == 1
        assert result["errors"][0]["source"] == "xray:payment-service"
        assert "xray_error: AccessDeniedException" in result["errors"][0]["reason"]

    def test_no_incident_traces_yields_no_findings(self, mocker):
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": []},
            {"TraceSummaries": [_summary("b1", 0.6)]},
        ]

        result = collect_traces(SCOPE, xray)

        assert result["findings"] == []
        assert result["errors"] == []

    def test_output_schema_valid(self, mocker):
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 2.0)]},
            {"TraceSummaries": [_summary("b1", 0.5)]},
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [{"Document": _root_segment_doc("payment-service", 1.0, 2.0)}]}]
        }

        result = collect_traces(SCOPE, xray)

        schemas_dir = Path(SCHEMA_PATH).parent
        worker_schema = json.loads(Path(SCHEMA_PATH).read_text())
        evidence_schema = json.loads((schemas_dir / "evidence.schema.json").read_text())

        registry = Registry().with_resource(
            uri="evidence.schema.json",
            resource=Resource.from_contents(evidence_schema),
        )
        validator = jsonschema.Draft202012Validator(worker_schema, registry=registry)
        validator.validate(result)

    def test_latency_finding_includes_incident_and_baseline_trace_status(self, mocker):
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {
                "TraceSummaries": [
                    _summary("t1", 2.5, has_error=True),
                    _summary("t2", 2.0, has_fault=True),
                ]
            },
            {
                "TraceSummaries": [
                    _summary("b1", 0.5),
                    _summary("b2", 0.6, has_throttle=True),
                ]
            },
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [{"Document": _root_segment_doc("payment-service", 1.0, 2.0)}]}]
        }

        result = collect_traces(SCOPE, xray)

        latency = [f for f in result["findings"] if f["findingType"] == "trace_latency_spike"]
        assert len(latency) == 1
        details = latency[0]["details"]
        assert details["incidentTraceStatus"] == {
            "traceCount": 2,
            "faultCount": 1,
            "errorCount": 1,
            "throttleCount": 0,
            "faultRate": 0.5,
            "errorRate": 0.5,
            "throttleRate": 0.0,
        }
        assert details["baselineTraceStatus"] == {
            "traceCount": 2,
            "faultCount": 0,
            "errorCount": 0,
            "throttleCount": 1,
            "faultRate": 0.0,
            "errorRate": 0.0,
            "throttleRate": 0.5,
        }

    def test_logs_status_without_dependency_fault_when_trace_status_flags_exist(self, mocker, caplog):
        """If summaries have HasError/HasFault but no faulted subsegments, emit debug breadcrumb."""
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 3.0, has_error=True)]},
            {"TraceSummaries": [_summary("b1", 0.5)]},
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [{"Document": _root_segment_doc("payment-service", 1.0, 2.0)}]}]
        }

        with caplog.at_level("DEBUG"):
            result = collect_traces(SCOPE, xray)

        assert result["errors"] == []
        assert not any(f["findingType"] == "trace_dependency_fault" for f in result["findings"])
        assert any("traces_status_without_dependency_fault" in rec.message for rec in caplog.records)


class TestAggregateSegmentDurations:

    def test_malformed_json_segment_is_skipped(self):
        traces = [{"Segments": [{"Document": "not-valid-json"}]}]
        result = _aggregate_segment_durations_ms(traces)
        assert result == {}

    def test_subsegment_with_parent_id_is_excluded(self):
        parent_doc = json.dumps({"name": "root-svc", "start_time": 0.0, "end_time": 1.0})
        child_doc = json.dumps(
            {"name": "child-svc", "parent_id": "abc123", "start_time": 0.0, "end_time": 0.5}
        )
        traces = [{"Segments": [{"Document": parent_doc}, {"Document": child_doc}]}]
        result = _aggregate_segment_durations_ms(traces)
        assert "root-svc" in result
        assert "child-svc" not in result
        assert result["root-svc"] == pytest.approx(1000.0)

    def test_segment_missing_times_is_skipped(self):
        doc_no_end = json.dumps({"name": "svc-a", "start_time": 1.0})
        doc_no_start = json.dumps({"name": "svc-b", "end_time": 2.0})
        traces = [{"Segments": [{"Document": doc_no_end}, {"Document": doc_no_start}]}]
        result = _aggregate_segment_durations_ms(traces)
        assert result == {}


class TestLatencyBaselineEdgeCases:

    def test_no_baseline_traces_assigns_threshold_ratio(self, mocker):
        """When baseline has no traces, ratio defaults to LATENCY_SPIKE_THRESHOLD (1.5)."""
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 2.0)]},  # incident
            {"TraceSummaries": []},                      # baseline: empty
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [{"Document": _root_segment_doc("payment-service", 1.0, 2.0)}]}]
        }

        result = collect_traces(SCOPE, xray)

        latency = [f for f in result["findings"] if f["findingType"] == "trace_latency_spike"]
        assert len(latency) == 1
        assert latency[0]["details"]["changeRatio"] == pytest.approx(1.5)

    def test_pagination_fetches_second_page(self, mocker):
        """NextToken in first response causes a second get_trace_summaries call."""
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 2.5)], "NextToken": "page-2-token"},  # page 1
            {"TraceSummaries": [_summary("t2", 3.0)]},                               # page 2
            {"TraceSummaries": [_summary("b1", 0.5)]},                               # baseline
        ]
        xray.batch_get_traces.return_value = {"Traces": []}

        result = collect_traces(SCOPE, xray)

        # Three summary calls: incident page 1, incident page 2, baseline
        assert xray.get_trace_summaries.call_count == 3
        assert result["errors"] == []


# ---------------------------------------------------------------------------
# Tests for the new trace_dependency_fault finding
# ---------------------------------------------------------------------------

_INCIDENT_WINDOW = {"start": "2026-03-20T10:00:00Z", "end": "2026-03-20T10:15:00Z"}


def _traces_from_docs(*docs: str) -> list:
    """Wrap document strings into the Traces list shape returned by batch_get_traces."""
    return [{"Segments": [{"Document": d}]} for d in docs]


class TestDependencyFaultFinding:

    def test_dynamodb_error_subsegment_emits_finding(self):
        """DynamoDB error=true with HTTP 400 → trace_dependency_fault finding."""
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "dynamodb.get_item",
            error=True,
            http_status=400,
            namespace="aws",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["findingType"] == "trace_dependency_fault"
        assert result["evidenceId"] == "ev-traces-003"
        assert result["source"] == "traces"
        assert result["resourceType"] == "xray-subsegment"
        faulted = result["details"]["faultedSubsegments"]
        assert len(faulted) == 1
        assert faulted[0]["name"] == "dynamodb.get_item"
        assert faulted[0]["error"] is True
        assert faulted[0]["fault"] is False
        assert faulted[0]["httpStatus"] == 400
        assert faulted[0]["occurrences"] == 1

    def test_healthy_subsegments_yield_no_finding(self):
        """Subsegments with all flags false → no trace_dependency_fault finding."""
        doc = json.dumps({
            "name": "payment-service",
            "start_time": 0.0,
            "end_time": 1.0,
            "subsegments": [
                {"name": "dynamodb.get_item", "start_time": 0.0, "end_time": 0.1},
            ],
        })
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is None

    def test_no_subsegments_yields_no_finding(self):
        """Root segment with no subsegments → no fault finding."""
        doc = _root_segment_doc("payment-service", 0.0, 1.0)
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is None

    def test_fault_flag_yields_score_0_90(self):
        """fault=true → score 0.90 (5xx server error)."""
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "s3.PutObject",
            fault=True,
            http_status=500,
            namespace="aws",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.90

    def test_throttle_flag_yields_score_0_75(self):
        """throttle=true (no fault) → score 0.75."""
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "dynamodb.put_item",
            throttle=True,
            namespace="aws",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.75

    def test_error_only_4xx_yields_score_0_70(self):
        """error=true only (4xx, no fault/throttle) → score 0.70."""
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "dynamodb.get_item",
            error=True,
            http_status=400,
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.70

    def test_local_namespace_fault_yields_score_0_70(self):
        """namespace=local, fault=true → score 0.70.

        The local wrapper segment propagates fault=true from the inner AWS call
        but does not itself represent a downstream service failure.
        Without a real aws/remote-namespace fault, the score stays at 0.70.
        """
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "dynamodb.get_item",
            fault=True,
            namespace="local",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.70, (
            "namespace=local fault must NOT drive score to 0.90 — "
            "local wrapper propagation is not a downstream service failure"
        )

    def test_local_wrapper_plus_aws_error_400_yields_score_0_70(self):
        """Real Lambda/SDK X-Ray pattern for a DynamoDB ValidationException.

        The SDK produces two subsegments:
        - dynamodb.get_item (namespace=local, fault=true, no httpStatus)
        - DynamoDB (namespace=aws, error=true, httpStatus=400)

        Only the aws subsegment has error=true, not fault=true.
        Score should be 0.70 (error/4xx only — no real downstream fault).
        """
        local_wrapper = {"name": "dynamodb.get_item", "fault": True, "namespace": "local"}
        aws_subsegment = {
            "name": "DynamoDB",
            "error": True,
            "namespace": "aws",
            "http": {"response": {"status": 400}},
        }
        doc = json.dumps({
            "name": "payment-service",
            "start_time": 0.0,
            "end_time": 1.0,
            # local_wrapper contains aws_subsegment as a nested child
            "subsegments": [
                {**local_wrapper, "subsegments": [aws_subsegment]}
            ],
        })
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.70, (
            "ValidationException (400) pattern must yield score 0.70, not 0.90 — "
            "the local wrapper fault must not override the real aws error=400 signal"
        )

    def test_aws_namespace_fault_5xx_yields_score_0_90(self):
        """namespace=aws, fault=true, httpStatus=500 → score 0.90 (genuine downstream 5xx)."""
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "DynamoDB",
            fault=True,
            http_status=500,
            namespace="aws",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.90

    def test_aws_namespace_fault_no_http_status_yields_score_0_90(self):
        """namespace=aws, fault=true, httpStatus=None → score 0.90.

        No HTTP status means connectivity failure / timeout — a genuine
        downstream outage signal that should drive the strong score.
        """
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "DynamoDB",
            fault=True,
            namespace="aws",
            # no http_status — simulates connectivity / timeout
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.90

    def test_aws_namespace_fault_4xx_yields_score_0_70(self):
        """namespace=aws, fault=true, httpStatus=400 → score 0.70.

        The AWS SDK sets fault=true on 4xx errors (e.g. ValidationException).
        A 400 is a client/request error, not a downstream service failure.
        The fault flag must not promote it to the strong 0.90 score.
        """
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "DynamoDB",
            fault=True,
            error=True,
            http_status=400,
            namespace="aws",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.70, (
            "aws namespace fault=true with httpStatus=400 (e.g. ValidationException) "
            "must yield score 0.70, not 0.90 — 4xx is a client error, not downstream failure"
        )

    def test_remote_namespace_throttle_yields_score_0_75(self):
        """namespace=remote, throttle=true → score 0.75 (genuine outbound throttle unchanged)."""
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "https://api.partner.com/items",
            throttle=True,
            namespace="remote",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["score"] == 0.75

    def test_occurrences_aggregated_across_traces(self):
        """Same faulted subsegment in 3 of 5 traces → occurrences=3."""
        faulted_doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "dynamodb.get_item",
            error=True,
            http_status=400,
        )
        healthy_doc = _root_segment_doc("payment-service", 0.0, 1.0)
        traces = _traces_from_docs(
            faulted_doc, faulted_doc, faulted_doc, healthy_doc, healthy_doc
        )

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["details"]["sampledTraceCount"] == 5
        assert result["details"]["faultedSubsegments"][0]["occurrences"] == 3

    def test_nested_subsegment_fault_found_recursively(self):
        """Fault on a subsegment nested inside another subsegment is detected."""
        nested_sub = {"name": "dynamodb.get_item", "error": True, "http": {"response": {"status": 400}}}
        outer_sub = {"name": "outer-call", "subsegments": [nested_sub]}
        doc = json.dumps({
            "name": "payment-service",
            "start_time": 0.0,
            "end_time": 1.0,
            "subsegments": [outer_sub],
        })
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        names = {e["name"] for e in result["details"]["faultedSubsegments"]}
        assert "dynamodb.get_item" in names

    def test_remote_namespace_http_fault_detected(self):
        """Outbound HTTP fault with namespace='remote' is detected (generality test)."""
        doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 1.0,
            "https://api.example.com",
            fault=True,
            http_status=502,
            namespace="remote",
        )
        traces = _traces_from_docs(doc)

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None
        assert result["details"]["faultedSubsegments"][0]["namespace"] == "remote"
        assert result["score"] == 0.90

    def test_lambda_two_document_structure_emits_fault_finding(self):
        """Real Lambda X-Ray structure: facade doc (no parent_id) + function segment
        doc (parent_id set) containing the faulted DynamoDB subsegment.

        Before the fix, the parent_id guard discarded the function segment doc,
        so _extract_faulted_subsegments was never called on the document that
        actually holds the subsegments, and the finding was silently suppressed.
        """
        facade_doc = json.dumps({
            "name": "payment-service",
            "id": "facade000000001",
            "start_time": 0.0,
            "end_time": 1.5,
            # No parent_id — this is the root facade segment.
        })
        function_segment_doc = json.dumps({
            "name": "payment-service",
            "id": "func000000002",
            "parent_id": "facade000000001",  # Points to the facade.
            "start_time": 0.1,
            "end_time": 1.4,
            "subsegments": [
                {
                    "name": "dynamodb.get_item",
                    "error": True,
                    "http": {"response": {"status": 400}},
                    "namespace": "aws",
                }
            ],
        })
        # Both documents come in as separate Segments of the same trace.
        traces = [{
            "Segments": [
                {"Document": facade_doc},
                {"Document": function_segment_doc},
            ]
        }]

        result = _build_dependency_fault_finding(
            evidence_id="ev-traces-003",
            trace_service="payment-service",
            traces=traces,
            incident_window=_INCIDENT_WINDOW,
        )

        assert result is not None, (
            "trace_dependency_fault was not emitted — the parent_id guard is "
            "incorrectly skipping the Lambda function segment document"
        )
        assert result["findingType"] == "trace_dependency_fault"
        faulted = result["details"]["faultedSubsegments"]
        assert len(faulted) == 1
        assert faulted[0]["name"] == "dynamodb.get_item"
        assert faulted[0]["error"] is True
        assert faulted[0]["httpStatus"] == 400
        assert result["score"] == 0.70  # error/4xx only

    def test_evidence_id_slot_2_correct_for_service_idx_0(self, mocker):
        """In collect_traces, the fault finding for service_idx=0 gets ev-traces-003."""
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 3.0)]},   # incident
            {"TraceSummaries": [_summary("b1", 0.5)]},   # baseline
        ]
        faulted_doc = _root_doc_with_subsegment(
            "payment-service", 0.0, 3.0,
            "dynamodb.get_item",
            error=True,
            http_status=400,
        )
        healthy_root = _root_segment_doc("payment-service", 0.0, 3.0)
        xray.batch_get_traces.return_value = {
            "Traces": [
                {"Segments": [{"Document": faulted_doc}]},
                {"Segments": [{"Document": healthy_root}]},
            ]
        }

        result = collect_traces(SCOPE, xray)

        fault_findings = [f for f in result["findings"] if f["findingType"] == "trace_dependency_fault"]
        assert len(fault_findings) == 1
        assert fault_findings[0]["evidenceId"] == "ev-traces-003"


# ---------------------------------------------------------------------------
# Tests for the new trace_error_status finding
# ---------------------------------------------------------------------------

_BASELINE_WINDOW = {"start": "2026-03-20T09:45:00Z", "end": "2026-03-20T10:00:00Z"}


def _status(
    trace_count: int,
    fault_count: int = 0,
    error_count: int = 0,
    throttle_count: int = 0,
) -> dict:
    """Build a _summarize_trace_status-shaped dict for test fixtures."""
    fault_rate = round(fault_count / trace_count, 4) if trace_count > 0 else 0.0
    error_rate = round(error_count / trace_count, 4) if trace_count > 0 else 0.0
    throttle_rate = round(throttle_count / trace_count, 4) if trace_count > 0 else 0.0
    return {
        "traceCount": trace_count,
        "faultCount": fault_count,
        "errorCount": error_count,
        "throttleCount": throttle_count,
        "faultRate": fault_rate,
        "errorRate": error_rate,
        "throttleRate": throttle_rate,
    }


class TestTraceErrorStatusFinding:

    def test_no_fault_or_error_in_incident_returns_none(self):
        """No fault/error in incident window → no finding."""
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(10),
            baseline_status=_status(10),
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is None

    def test_fault_in_incident_baseline_zero_emits_finding(self):
        """Fault in incident window, baseline had none → finding emitted."""
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(10, fault_count=8),
            baseline_status=_status(10),
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is not None
        assert result["findingType"] == "trace_error_status"
        assert result["evidenceId"] == "ev-traces-004"
        assert result["source"] == "traces"
        assert result["resourceType"] == "xray-service"
        assert result["details"]["signalType"] == "fault"

    def test_fault_rate_above_50pct_yields_score_0_85(self):
        """faultRate > 0.5 → score 0.85."""
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(10, fault_count=6),  # 60% fault rate
            baseline_status=_status(10),
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is not None
        assert result["score"] == 0.85

    def test_fault_rate_at_or_below_50pct_yields_score_0_70(self):
        """faultRate ≤ 0.5 → score 0.70."""
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(10, fault_count=5),  # exactly 50% fault rate
            baseline_status=_status(10),
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is not None
        assert result["score"] == 0.70

    def test_error_only_no_fault_yields_score_0_60(self):
        """error only (no fault) → score 0.60."""
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(10, error_count=4),
            baseline_status=_status(10),
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is not None
        assert result["score"] == 0.60
        assert result["details"]["signalType"] == "error"

    def test_baseline_similar_rate_suppresses_finding(self):
        """If baseline had similar fault rate, finding is suppressed (not a new signal)."""
        # Incident: 50% fault rate, baseline: 40% fault rate → ratio < 2.0 → suppress
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(10, fault_count=5),   # 50%
            baseline_status=_status(10, fault_count=4),   # 40%
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is None

    def test_baseline_much_lower_rate_emits_finding(self):
        """Incident fault rate is ≥ 2× baseline → finding emitted."""
        # Incident: 60% fault rate, baseline: 10% fault rate → ratio = 6.0 → emit
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(10, fault_count=6),   # 60%
            baseline_status=_status(10, fault_count=1),   # 10%
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is not None
        assert result["findingType"] == "trace_error_status"

    def test_zero_incident_traces_returns_none(self):
        """No incident traces → no finding (avoid division by zero)."""
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=_status(0),
            baseline_status=_status(10),
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is None

    def test_summary_contains_counts_and_service_name(self):
        """Summary string includes fault count, trace count, and service name."""
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="my-lambda-function",
            incident_status=_status(20, fault_count=14),
            baseline_status=_status(20),
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is not None
        assert "my-lambda-function" in result["summary"]
        assert "14/20" in result["summary"]
        assert "trace_error_status" in result["summary"]

    def test_details_include_both_status_windows(self):
        """Details include incidentTraceStatus, baselineTraceStatus, and both windows."""
        inc_status = _status(10, fault_count=7)
        base_status = _status(10)
        result = _build_trace_error_status_finding(
            evidence_id="ev-traces-004",
            trace_service="payment-service",
            incident_status=inc_status,
            baseline_status=base_status,
            incident_window=_INCIDENT_WINDOW,
            baseline_window=_BASELINE_WINDOW,
        )
        assert result is not None
        assert result["details"]["incidentTraceStatus"] == inc_status
        assert result["details"]["baselineTraceStatus"] == base_status
        assert result["details"]["incidentWindow"] == _INCIDENT_WINDOW
        assert result["details"]["baselineWindow"] == _BASELINE_WINDOW

    def test_collect_traces_emits_error_status_finding_for_lambda_timeout(self, mocker):
        """Integration: Lambda timeout scenario — HasFault=True on summaries → trace_error_status emitted."""
        xray = mocker.MagicMock()
        # Incident: all 5 traces have HasFault=True (Lambda timeout)
        xray.get_trace_summaries.side_effect = [
            {
                "TraceSummaries": [
                    _summary("t1", 6.0, has_fault=True),
                    _summary("t2", 6.0, has_fault=True),
                    _summary("t3", 6.0, has_fault=True),
                    _summary("t4", 6.0, has_fault=True),
                    _summary("t5", 6.0, has_fault=True),
                ]
            },
            {"TraceSummaries": [_summary("b1", 0.04), _summary("b2", 0.05)]},  # baseline: healthy
        ]
        # Segment documents have no faulted subsegments (Lambda timeout = no downstream fault)
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [{"Document": _root_segment_doc("payment-service", 0.0, 6.0)}]}]
        }

        result = collect_traces(SCOPE, xray)

        assert result["errors"] == []
        error_status_findings = [f for f in result["findings"] if f["findingType"] == "trace_error_status"]
        assert len(error_status_findings) == 1, (
            "trace_error_status finding must be emitted when Lambda timeout sets HasFault=True "
            "on trace summaries even though no faulted subsegments exist"
        )
        finding = error_status_findings[0]
        assert finding["evidenceId"] == "ev-traces-004"  # slot 3 for service_idx=0
        assert finding["score"] == 0.85  # fault rate = 100% > 50%
        assert finding["details"]["signalType"] == "fault"
        assert finding["details"]["incidentTraceStatus"]["faultCount"] == 5

    def test_collect_traces_logs_error_status_finding_emitted(self, mocker, caplog):
        """When trace_error_status is emitted, an INFO log breadcrumb is written."""
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 6.0, has_fault=True)]},
            {"TraceSummaries": [_summary("b1", 0.04)]},
        ]
        xray.batch_get_traces.return_value = {"Traces": []}

        with caplog.at_level("INFO"):
            collect_traces(SCOPE, xray)

        assert any(
            "traces_finding_emitted" in rec.message and "trace_error_status" in rec.message
            for rec in caplog.records
        )

    def test_collect_traces_logs_skipped_when_no_error_status(self, mocker, caplog):
        """When trace_error_status is skipped, a DEBUG log breadcrumb is written."""
        xray = mocker.MagicMock()
        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 3.0)]},  # no HasFault/HasError
            {"TraceSummaries": [_summary("b1", 0.5)]},
        ]
        xray.batch_get_traces.return_value = {"Traces": []}

        with caplog.at_level("DEBUG"):
            collect_traces(SCOPE, xray)

        assert any(
            "traces_error_status_finding_skipped" in rec.message
            for rec in caplog.records
        )


class TestExtractFaultedSubsegments:

    def test_returns_empty_for_no_subsegments(self):
        assert _extract_faulted_subsegments({}) == []

    def test_returns_empty_when_all_flags_false(self):
        doc = {"subsegments": [{"name": "healthy-call"}]}
        assert _extract_faulted_subsegments(doc) == []

    def test_extracts_error_subsegment(self):
        doc = {"subsegments": [{"name": "dynamodb.get_item", "error": True}]}
        result = _extract_faulted_subsegments(doc)
        assert len(result) == 1
        assert result[0]["name"] == "dynamodb.get_item"
        assert result[0]["error"] is True
        assert result[0]["fault"] is False
        assert result[0]["throttle"] is False

    def test_extracts_fault_subsegment_with_http_status(self):
        doc = {
            "subsegments": [{
                "name": "s3.PutObject",
                "fault": True,
                "namespace": "aws",
                "http": {"response": {"status": 500}},
            }]
        }
        result = _extract_faulted_subsegments(doc)
        assert len(result) == 1
        assert result[0]["httpStatus"] == 500
        assert result[0]["namespace"] == "aws"

    def test_skips_subsegment_without_name(self):
        doc = {"subsegments": [{"error": True}]}
        assert _extract_faulted_subsegments(doc) == []

    def test_recurses_into_nested_subsegments(self):
        nested = {"name": "inner-call", "fault": True}
        outer = {"name": "outer-call", "subsegments": [nested]}
        doc = {"subsegments": [outer]}
        result = _extract_faulted_subsegments(doc)
        names = {r["name"] for r in result}
        assert "inner-call" in names
        # outer-call itself has no fault flag, should not appear
        assert "outer-call" not in names


class TestIsRootService:

    def test_returns_true_when_service_is_root_segment(self):
        doc = json.dumps({"name": "my-lambda", "start_time": 0.0, "end_time": 1.0})
        traces = [{"Segments": [{"Document": doc}]}]
        assert _is_root_service(traces, "my-lambda") is True

    def test_returns_false_when_segment_has_parent_id(self):
        """A segment with parent_id is a child segment, not a root — even if the name matches."""
        doc = json.dumps({
            "name": "dynamodb-table",
            "parent_id": "abc123",
            "start_time": 0.0,
            "end_time": 0.1,
        })
        traces = [{"Segments": [{"Document": doc}]}]
        assert _is_root_service(traces, "dynamodb-table") is False

    def test_returns_false_for_empty_traces(self):
        assert _is_root_service([], "my-service") is False

    def test_returns_false_when_name_does_not_match(self):
        doc = json.dumps({"name": "other-service", "start_time": 0.0, "end_time": 1.0})
        traces = [{"Segments": [{"Document": doc}]}]
        assert _is_root_service(traces, "my-lambda") is False

    def test_returns_false_for_malformed_document(self):
        traces = [{"Segments": [{"Document": "not-valid-json"}]}]
        assert _is_root_service(traces, "my-service") is False

    def test_returns_true_when_match_found_in_any_trace(self):
        """Searches across all traces, not just the first."""
        other_doc = json.dumps({"name": "other-service", "start_time": 0.0, "end_time": 1.0})
        match_doc = json.dumps({"name": "my-lambda", "start_time": 1.0, "end_time": 2.0})
        traces = [
            {"Segments": [{"Document": other_doc}]},
            {"Segments": [{"Document": match_doc}]},
        ]
        assert _is_root_service(traces, "my-lambda") is True


class TestDownstreamServiceFindingSuppression:
    """Verify that downstream services (non-root in X-Ray) do not produce findings
    that are only valid for root services (trace_latency_spike, latency_dominant_segment).

    ResponseTime in X-Ray trace summaries is always the root trace's end-to-end duration.
    _aggregate_segment_durations_ms skips non-root segments.
    Both would produce misleading results if applied to downstream services.
    """

    def _lambda_doc_with_downstream(self, lambda_name: str, downstream_name: str) -> str:
        return json.dumps({
            "name": lambda_name,
            "start_time": 0.0,
            "end_time": 3.0,
            "subsegments": [{"name": downstream_name}],
        })

    def _downstream_doc(self, name: str) -> str:
        """Downstream services appear in trace documents with a parent_id."""
        return json.dumps({
            "name": name,
            "parent_id": "abc123",
            "start_time": 0.5,
            "end_time": 0.52,
        })

    def test_downstream_service_does_not_emit_latency_spike(self, mocker):
        """A service that only appears as a child segment must not emit trace_latency_spike.

        When X-Ray is queried for a downstream service (e.g. a DynamoDB table),
        the returned trace summaries contain the root Lambda traces — their
        ResponseTime is the Lambda's end-to-end duration, not DynamoDB's latency.
        Emitting trace_latency_spike from this data falsely implicates DynamoDB.
        """
        scope = {**SCOPE, "traceServices": ["lambda-fn", "dynamodb-table"]}
        xray = mocker.MagicMock()

        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 3.0)]},   # lambda-fn incident
            {"TraceSummaries": [_summary("b1", 0.1)]},   # lambda-fn baseline
            {"TraceSummaries": [_summary("t1", 3.0)]},   # dynamodb-table incident (same traces)
            {"TraceSummaries": [_summary("b1", 0.1)]},   # dynamodb-table baseline (same traces)
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [
                {"Document": self._lambda_doc_with_downstream("lambda-fn", "dynamodb-table")},
                {"Document": self._downstream_doc("dynamodb-table")},
            ]}]
        }

        result = collect_traces(scope, xray)

        assert result["errors"] == []
        dynamo_findings = [f for f in result["findings"] if f["resourceName"] == "dynamodb-table"]
        assert not any(f["findingType"] == "trace_latency_spike" for f in dynamo_findings)

    def test_downstream_service_does_not_emit_dominant_segment(self, mocker):
        """A service that only appears as a child segment must not emit latency_dominant_segment.

        _aggregate_segment_durations_ms only considers root segments, so the dominant
        segment for a downstream service's traces is always the root Lambda — identical
        to the Lambda's own finding and misleading when attributed to the downstream resource.
        """
        scope = {**SCOPE, "traceServices": ["lambda-fn", "dynamodb-table"]}
        xray = mocker.MagicMock()

        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 3.0)]},
            {"TraceSummaries": [_summary("b1", 0.1)]},
            {"TraceSummaries": [_summary("t1", 3.0)]},
            {"TraceSummaries": [_summary("b1", 0.1)]},
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [
                {"Document": self._lambda_doc_with_downstream("lambda-fn", "dynamodb-table")},
                {"Document": self._downstream_doc("dynamodb-table")},
            ]}]
        }

        result = collect_traces(scope, xray)

        assert result["errors"] == []
        dynamo_findings = [f for f in result["findings"] if f["resourceName"] == "dynamodb-table"]
        assert not any(f["findingType"] == "latency_dominant_segment" for f in dynamo_findings)

    def test_root_service_still_emits_latency_spike_and_dominant_segment(self, mocker):
        """Root services are unaffected — they still produce both findings normally."""
        scope = {**SCOPE, "traceServices": ["lambda-fn", "dynamodb-table"]}
        xray = mocker.MagicMock()

        xray.get_trace_summaries.side_effect = [
            {"TraceSummaries": [_summary("t1", 3.0)]},
            {"TraceSummaries": [_summary("b1", 0.1)]},
            {"TraceSummaries": [_summary("t1", 3.0)]},
            {"TraceSummaries": [_summary("b1", 0.1)]},
        ]
        xray.batch_get_traces.return_value = {
            "Traces": [{"Segments": [
                {"Document": self._lambda_doc_with_downstream("lambda-fn", "dynamodb-table")},
                {"Document": self._downstream_doc("dynamodb-table")},
            ]}]
        }

        result = collect_traces(scope, xray)

        assert result["errors"] == []
        lambda_findings = [f for f in result["findings"] if f["resourceName"] == "lambda-fn"]
        assert any(f["findingType"] == "trace_latency_spike" for f in lambda_findings)
        assert any(f["findingType"] == "latency_dominant_segment" for f in lambda_findings)

