"""
Unit tests for the Logs Worker.

Uses unittest.mock to patch the CloudWatch Logs client — no real AWS calls are made.
Schema validation uses jsonschema against schemas/worker-output.schema.json.

Run from repo root:
    python -m pytest backend/workers/logs/tests/ -v
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
import pytest
from botocore.exceptions import ClientError
from referencing import Registry, Resource

import logs_worker as logs_worker_module
from logs_worker import (
    APIGW_PATTERNS,
    PREDEFINED_PATTERNS,
    _TOTAL_PATTERN_SLOTS,
    collect_logs,
)

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
    "logGroups": ["/ecs/payment-service"],
}

# Unix timestamp for the start of the incident window (2026-03-20T10:00:00Z).
# Used to distinguish incident vs baseline queries by their startTime argument.
_INCIDENT_START_TS = int(datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc).timestamp())


def _cw_error(code: str = "AccessDeniedException") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}},
        "StartQuery",
    )


def _complete(count: int) -> dict:
    """Build a mock get_query_results response for a successfully completed query."""
    return {
        "status": "Complete",
        "results": [[{"field": "count", "value": str(count)}]],
    }


def _make_logs_mock(mocker, *, counts: dict | None = None):
    """Build a logs client mock that tracks queryId → (log_group, pattern, is_incident).

    counts: dict keyed by (log_group, pattern_name, is_incident) → int.
            Missing entries default to 0.
    """
    counts = counts or {}
    store: dict[str, tuple[str, str, bool]] = {}
    call_n = {"n": 0}

    client = mocker.MagicMock()

    def _start(**kwargs):
        qid = f"q{call_n['n']}"
        call_n["n"] += 1
        is_incident = kwargs["startTime"] >= _INCIDENT_START_TS
        lg = kwargs["logGroupName"]
        qs = kwargs["queryString"]
        # Identify pattern from both PREDEFINED_PATTERNS and APIGW_PATTERNS.
        matched = next(
            (p["name"] for p in [*PREDEFINED_PATTERNS, *APIGW_PATTERNS]
             if p["filter_expression"] in qs),
            None,
        )
        store[qid] = (lg, matched, is_incident)
        return {"queryId": qid}

    def _results(**kwargs):
        qid = kwargs["queryId"]
        key = store.get(qid)
        if key is None:
            return _complete(0)
        return _complete(counts.get(key, 0))

    client.start_query.side_effect = _start
    client.get_query_results.side_effect = _results
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCollectLogs:

    def test_happy_path_spike_produces_finding(self, mocker):
        """ERROR pattern 12x in incident vs baseline → error_log_spike finding."""
        client = _make_logs_mock(mocker, counts={
            ("/ecs/payment-service", "ERROR", True): 243,
            ("/ecs/payment-service", "ERROR", False): 20,
        })

        result = collect_logs(SCOPE, client)

        assert result["incidentId"] == "inc-test-001"
        assert result["source"] == "logs"
        assert result["errors"] == []
        assert len(result["findings"]) == 1

        f = result["findings"][0]
        # ERROR is PREDEFINED_PATTERNS index 0, lg index 0 → ev-logs-001
        assert f["evidenceId"] == "ev-logs-001"
        assert f["source"] == "logs"
        assert f["findingType"] == "error_log_spike"
        assert f["details"]["pattern"] == "ERROR"
        assert f["details"]["logGroup"] == "/ecs/payment-service"
        assert f["details"]["incidentCount"] == 243
        assert f["details"]["baselineCount"] == 20
        assert f["details"]["changeRatio"] == pytest.approx(243 / 20, rel=1e-3)
        assert f["details"]["incidentWindow"] == SCOPE["incidentWindow"]
        assert f["details"]["baselineWindow"] == SCOPE["baselineWindow"]
        assert 0.0 <= f["score"] <= 1.0

    def test_no_incident_count_returns_empty_findings(self, mocker):
        """All patterns return 0 in incident window → findings=[], errors=[]."""
        client = _make_logs_mock(mocker)

        result = collect_logs(SCOPE, client)

        assert result["findings"] == []
        assert result["errors"] == []

    def test_new_error_pattern_when_baseline_zero(self, mocker):
        """Pattern absent in baseline but present in incident → new_error_pattern."""
        client = _make_logs_mock(mocker, counts={
            ("/ecs/payment-service", "Exception", True): 50,
            # baseline returns 0 (default)
        })

        result = collect_logs(SCOPE, client)

        assert len(result["findings"]) == 1
        f = result["findings"][0]
        assert f["findingType"] == "new_error_pattern"
        assert f["details"]["incidentCount"] == 50
        assert f["details"]["baselineCount"] == 0
        assert f["details"]["changeRatio"] is None
        assert 0.0 <= f["score"] <= 1.0

    def test_change_below_threshold_no_finding(self, mocker):
        """Ratio 1.1x — below ANOMALY_THRESHOLD — produces no finding."""
        client = _make_logs_mock(mocker, counts={
            ("/ecs/payment-service", "ERROR", True): 11,
            ("/ecs/payment-service", "ERROR", False): 10,
        })

        result = collect_logs(SCOPE, client)

        assert result["findings"] == []
        assert result["errors"] == []

    def test_query_failed_status_appends_error_and_continues(self, mocker):
        """A query returning Failed appends to errors[]; remaining pairs still run."""
        client = mocker.MagicMock()
        call_n = {"n": 0}
        qids: list[str] = []

        def _start(**kwargs):
            qid = f"q{call_n['n']}"
            call_n["n"] += 1
            qids.append(qid)
            return {"queryId": qid}

        def _results(**kwargs):
            # Fail only the very first query (lg0/pat0 incident).
            if kwargs["queryId"] == "q0":
                return {"status": "Failed", "results": []}
            return _complete(0)

        client.start_query.side_effect = _start
        client.get_query_results.side_effect = _results

        result = collect_logs(SCOPE, client)

        assert len(result["errors"]) == 1
        assert "query_failed" in result["errors"][0]["reason"]
        # Remaining pairs still ran — more than one start_query call.
        assert client.start_query.call_count > 1

    def test_query_cancelled_status_appends_error(self, mocker):
        """A query returning Cancelled status is treated as an error."""
        client = mocker.MagicMock()
        client.start_query.return_value = {"queryId": "q0"}
        client.get_query_results.return_value = {"status": "Cancelled", "results": []}

        result = collect_logs(SCOPE, client)

        assert any("query_cancelled" in e["reason"] for e in result["errors"])

    def test_cloudwatch_error_on_start_query_appends_error_continues(self, mocker):
        """ClientError from start_query → errors[] populated, other pairs continue."""
        client = mocker.MagicMock()
        call_n = {"n": 0}

        def _start(**kwargs):
            call_n["n"] += 1
            if call_n["n"] == 1:
                raise _cw_error("AccessDeniedException")
            return {"queryId": f"q{call_n['n']}"}

        client.start_query.side_effect = _start
        client.get_query_results.return_value = _complete(0)

        result = collect_logs(SCOPE, client)

        assert len(result["errors"]) == 1
        assert "cloudwatch_error" in result["errors"][0]["reason"]
        assert "AccessDeniedException" in result["errors"][0]["reason"]
        assert client.start_query.call_count > 1

    def test_poll_timeout_appends_error(self, mocker):
        """Poll loop exceeding MAX_POLL_ITERATIONS raises _QueryError → errors[]."""
        mocker.patch("logs_worker.time.sleep")
        mocker.patch.object(logs_worker_module, "MAX_POLL_ITERATIONS", 2)

        client = mocker.MagicMock()
        client.start_query.return_value = {"queryId": "q0"}
        # Always returns Running — loop never terminates naturally.
        client.get_query_results.return_value = {"status": "Running", "results": []}

        result = collect_logs(SCOPE, client)

        assert any("query_poll_timeout" in e["reason"] for e in result["errors"])

    def test_empty_log_groups_returns_empty(self, mocker):
        """scope.logGroups = [] → findings=[], errors=[], no AWS calls made."""
        client = mocker.MagicMock()
        scope = {**SCOPE, "logGroups": []}

        result = collect_logs(scope, client)

        assert result["findings"] == []
        assert result["errors"] == []
        client.start_query.assert_not_called()

    def test_evidence_id_stable_by_position_across_log_groups(self, mocker):
        """Second log group's first pattern slot is offset by _TOTAL_PATTERN_SLOTS.

        With _TOTAL_PATTERN_SLOTS = 6 (4 predefined + 2 APIGW), the second group
        starts at slot 7: lg_idx=1, pat_idx=0 → ev-logs-{1*6+0+1:03d} = ev-logs-007.
        Slots 5 and 6 in the first group's range are silently reserved for APIGW
        patterns and skipped for non-API-Gateway log groups.
        """
        scope = {**SCOPE, "logGroups": ["/ecs/payment-service", "/ecs/auth-service"]}
        client = _make_logs_mock(mocker, counts={
            # Spike only on auth-service / ERROR (lg_idx=1, pat_idx=0).
            ("/ecs/auth-service", "ERROR", True): 80,
            # baseline returns 0 (default) → new_error_pattern
        })

        result = collect_logs(scope, client)

        assert len(result["findings"]) == 1
        f = result["findings"][0]
        # lg_idx=1, pat_idx=0 → ev-logs-{1*_TOTAL_PATTERN_SLOTS+0+1:03d}
        # With _TOTAL_PATTERN_SLOTS=6: ev-logs-007.
        expected_id = f"ev-logs-{1 * _TOTAL_PATTERN_SLOTS + 0 + 1:03d}"
        assert f["evidenceId"] == expected_id
        assert f["details"]["logGroup"] == "/ecs/auth-service"
        assert f["details"]["pattern"] == "ERROR"

    def test_evidence_id_position_within_single_log_group(self, mocker):
        """Patterns 0..3 in a single log group map to ev-logs-001..004 respectively."""
        # Make each pattern produce a spike to verify all four IDs.
        counts = {
            ("/ecs/payment-service", p["name"], True): 50
            for p in PREDEFINED_PATTERNS
        }
        client = _make_logs_mock(mocker, counts=counts)

        result = collect_logs(SCOPE, client)

        assert result["errors"] == []
        assert len(result["findings"]) == len(PREDEFINED_PATTERNS)
        ids = [f["evidenceId"] for f in result["findings"]]
        assert ids == ["ev-logs-001", "ev-logs-002", "ev-logs-003", "ev-logs-004"]

    def test_resource_name_is_service(self, mocker):
        """Finding resourceName comes from scope.service, not the log group path."""
        client = _make_logs_mock(mocker, counts={
            ("/ecs/payment-service", "ERROR", True): 100,
        })

        result = collect_logs(SCOPE, client)

        assert result["findings"][0]["resourceName"] == "payment-service"

    def test_lambda_log_group_is_classified_as_lambda_function(self, mocker):
        """/aws/lambda/<fn> log group should classify finding as lambda-function."""
        scope = {
            **SCOPE,
            "service": "fallback-service-name",
            "logGroups": ["/aws/lambda/simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI"],
        }
        client = _make_logs_mock(mocker, counts={
            (
                "/aws/lambda/simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI",
                "ERROR",
                True,
            ): 29,
        })

        result = collect_logs(scope, client)

        assert len(result["findings"]) == 1
        finding = result["findings"][0]
        assert finding["resourceType"] == "lambda-function"
        assert (
            finding["resourceName"]
            == "simple-crud-api-dev-GetItemsFunction-uh39T43nqmhI"
        )

    def test_multiple_errors_do_not_abort_processing(self, mocker):
        """Two patterns erroring still lets the remaining patterns run."""
        client = mocker.MagicMock()
        call_n = {"n": 0}

        def _start(**kwargs):
            n = call_n["n"]
            call_n["n"] += 1
            qid = f"q{n}"
            return {"queryId": qid}

        def _results(**kwargs):
            # Fail the first two incident queries (pat 0 and pat 1).
            if kwargs["queryId"] in ("q0", "q2"):
                return {"status": "Failed", "results": []}
            return _complete(0)

        client.start_query.side_effect = _start
        client.get_query_results.side_effect = _results

        result = collect_logs(SCOPE, client)

        # Exactly two errors (one per failed pair); the other patterns ran.
        assert len(result["errors"]) == 2
        assert client.start_query.call_count > 2

    def test_output_schema_valid(self, mocker):
        """Output envelope with findings must conform to schemas/worker-output.schema.json."""
        client = _make_logs_mock(mocker, counts={
            ("/ecs/payment-service", "ERROR", True): 100,
            ("/ecs/payment-service", "ERROR", False): 10,
        })

        result = collect_logs(SCOPE, client)

        schemas_dir = Path(SCHEMA_PATH).parent
        worker_schema = json.loads(Path(SCHEMA_PATH).read_text())
        evidence_schema = json.loads((schemas_dir / "evidence.schema.json").read_text())

        registry = Registry().with_resource(
            uri="evidence.schema.json",
            resource=Resource.from_contents(evidence_schema),
        )
        validator = jsonschema.Draft202012Validator(worker_schema, registry=registry)
        validator.validate(result)

    def test_output_schema_valid_empty(self, mocker):
        """Empty output (no findings) also conforms to schemas/worker-output.schema.json."""
        client = _make_logs_mock(mocker)

        result = collect_logs(SCOPE, client)

        with open(SCHEMA_PATH) as f:
            schema = json.load(f)
        jsonschema.validate(result, schema)


class TestExceptionSamples:
    """Tests for _query_sample_messages integration via collect_logs."""

    def _make_sampling_mock(self, mocker, *, count_for_error: int, sample_messages: list[str]):
        """Mock that returns a count for the ERROR pattern and @message rows for sample queries."""
        store: dict[str, dict] = {}  # queryId -> {"type": ..., "is_incident": ...}
        call_n = {"n": 0}

        client = mocker.MagicMock()

        def _start(**kwargs):
            qid = f"q{call_n['n']}"
            call_n["n"] += 1
            qs = kwargs["queryString"]
            is_incident = kwargs.get("startTime", 0) >= _INCIDENT_START_TS
            qtype = "samples" if "fields @message" in qs else "count"
            store[qid] = {"type": qtype, "is_incident": is_incident}
            return {"queryId": qid}

        def _results(**kwargs):
            qid = kwargs["queryId"]
            meta = store.get(qid, {"type": "count", "is_incident": False})
            if meta["type"] == "samples":
                return {
                    "status": "Complete",
                    "results": [
                        [{"field": "@message", "value": msg}, {"field": "@ptr", "value": "ptr"}]
                        for msg in sample_messages
                    ],
                }
            count = count_for_error if meta["is_incident"] else 0
            return {"status": "Complete", "results": [[{"field": "count", "value": str(count)}]]}

        client.start_query.side_effect = _start
        client.get_query_results.side_effect = _results
        return client

    def test_exception_samples_present_when_error_pattern_hits(self, mocker):
        """ERROR finding gets exceptionSamples when sample query returns @message rows."""
        samples = [
            "ValidationException: The provided key element does not match the schema",
            "Simulated error for testing (param1=1)",
        ]
        client = self._make_sampling_mock(mocker, count_for_error=50, sample_messages=samples)

        result = collect_logs(SCOPE, client)

        error_finding = next(f for f in result["findings"] if f["details"]["pattern"] == "ERROR")
        assert "exceptionSamples" in error_finding["details"]
        assert error_finding["details"]["exceptionSamples"] == samples

    def test_exception_samples_absent_when_sample_query_returns_nothing(self, mocker):
        """When sample query returns empty results, exceptionSamples is not set."""
        client = self._make_sampling_mock(mocker, count_for_error=20, sample_messages=[])

        result = collect_logs(SCOPE, client)

        error_finding = next(f for f in result["findings"] if f["details"]["pattern"] == "ERROR")
        assert "exceptionSamples" not in error_finding["details"]

    def test_exception_samples_truncated_to_max_length(self, mocker):
        """Each sample message is truncated to _MAX_EXCEPTION_MSG_LEN characters."""
        long_msg = "x" * 300  # longer than _MAX_EXCEPTION_MSG_LEN (200)
        client = self._make_sampling_mock(mocker, count_for_error=10, sample_messages=[long_msg])

        result = collect_logs(SCOPE, client)

        error_finding = next(f for f in result["findings"] if f["details"]["pattern"] == "ERROR")
        samples = error_finding["details"]["exceptionSamples"]
        assert len(samples) == 1
        assert len(samples[0]) == 200

    def test_sample_query_not_called_for_timeout_pattern(self, mocker):
        """The 'timeout' pattern is not in _SAMPLE_PATTERNS; no sample query is issued."""
        # Return a spike for timeout only, nothing for ERROR/Exception.
        counts = {
            ("/ecs/payment-service", "timeout", True): 50,
        }
        client = _make_logs_mock(mocker, counts=counts)

        result = collect_logs(SCOPE, client)

        assert len(result["findings"]) == 1
        assert result["findings"][0]["details"]["pattern"] == "timeout"
        # Verify no sample query (no "fields @message" query was issued).
        all_qs = [call.kwargs["queryString"] for call in client.start_query.call_args_list]
        assert not any("fields @message" in qs for qs in all_qs)

    def test_sample_query_not_called_for_connection_refused_pattern(self, mocker):
        """The 'connection refused' pattern is not in _SAMPLE_PATTERNS."""
        counts = {
            ("/ecs/payment-service", "connection refused", True): 30,
        }
        client = _make_logs_mock(mocker, counts=counts)

        result = collect_logs(SCOPE, client)

        assert len(result["findings"]) == 1
        all_qs = [call.kwargs["queryString"] for call in client.start_query.call_args_list]
        assert not any("fields @message" in qs for qs in all_qs)

    def test_sample_query_failure_does_not_abort_finding(self, mocker):
        """If the sample query raises an exception, the finding is still recorded."""
        call_n = {"n": 0}
        store: dict[str, bool] = {}  # queryId -> is_incident
        client = mocker.MagicMock()

        def _start(**kwargs):
            qid = f"q{call_n['n']}"
            call_n["n"] += 1
            qs = kwargs["queryString"]
            if "fields @message" in qs:
                raise Exception("sample query network error")
            is_incident = kwargs.get("startTime", 0) >= _INCIDENT_START_TS
            store[qid] = is_incident
            return {"queryId": qid}

        def _results(**kwargs):
            qid = kwargs["queryId"]
            is_incident = store.get(qid, False)
            count = 50 if is_incident else 0
            return {"status": "Complete", "results": [[{"field": "count", "value": str(count)}]]}

        client.start_query.side_effect = _start
        client.get_query_results.side_effect = _results

        result = collect_logs(SCOPE, client)

        # Finding is still produced despite sample query failure.
        error_finding = next(f for f in result["findings"] if f["details"]["pattern"] == "ERROR")
        assert error_finding is not None
        # exceptionSamples is simply absent.
        assert "exceptionSamples" not in error_finding["details"]
        # Errors list is clean (sample failures are silent).
        assert result["errors"] == []


class TestApiGatewayLogSupport:
    """Tests for API Gateway log group detection, pattern application, and evidence IDs."""

    _APIGW_LOG_GROUP = "/aws/apigateway/abc123def456"

    def _apigw_scope(self, extra_groups: list[str] | None = None) -> dict:
        groups = [self._APIGW_LOG_GROUP] + (extra_groups or [])
        return {**SCOPE, "logGroups": groups}

    def test_apigw_log_group_resource_classification(self, mocker):
        """/aws/apigateway/<api-id> is classified as api-gateway with the api-id as name."""
        client = _make_logs_mock(mocker, counts={
            (self._APIGW_LOG_GROUP, "HTTP 5XX", True): 40,
        })

        result = collect_logs(self._apigw_scope(), client)

        apigw_finding = next(
            (f for f in result["findings"] if f["details"]["pattern"] == "HTTP 5XX"),
            None,
        )
        assert apigw_finding is not None
        assert apigw_finding["resourceType"] == "api-gateway"
        assert apigw_finding["resourceName"] == "abc123def456"

    def test_apigw_log_group_4xx_pattern_produces_finding(self, mocker):
        """HTTP 4XX pattern on an API Gateway log group produces an error_log_spike finding."""
        client = _make_logs_mock(mocker, counts={
            (self._APIGW_LOG_GROUP, "HTTP 4XX", True): 120,
            (self._APIGW_LOG_GROUP, "HTTP 4XX", False): 5,
        })

        result = collect_logs(self._apigw_scope(), client)

        finding = next(
            (f for f in result["findings"] if f["details"]["pattern"] == "HTTP 4XX"),
            None,
        )
        assert finding is not None
        assert finding["findingType"] == "error_log_spike"
        assert finding["source"] == "logs"
        assert finding["details"]["incidentCount"] == 120
        assert finding["details"]["baselineCount"] == 5

    def test_apigw_log_group_5xx_pattern_produces_finding(self, mocker):
        """HTTP 5XX pattern on an API Gateway log group produces an error_log_spike finding."""
        client = _make_logs_mock(mocker, counts={
            (self._APIGW_LOG_GROUP, "HTTP 5XX", True): 60,
            # baseline absent \u2192 new_error_pattern
        })

        result = collect_logs(self._apigw_scope(), client)

        finding = next(
            (f for f in result["findings"] if f["details"]["pattern"] == "HTTP 5XX"),
            None,
        )
        assert finding is not None
        assert finding["findingType"] == "new_error_pattern"

    def test_non_apigw_log_group_skips_apigw_patterns(self, mocker):
        """ECS and Lambda log groups must NOT run APIGW_PATTERNS.

        A scope with one non-API-Gateway log group should issue exactly
        len(PREDEFINED_PATTERNS) * 2 count queries (2 windows per pattern),
        not more.
        """
        scope = {**SCOPE, "logGroups": ["/ecs/payment-service"]}
        client = _make_logs_mock(mocker)

        collect_logs(scope, client)

        expected_count_queries = len(PREDEFINED_PATTERNS) * 2
        assert client.start_query.call_count == expected_count_queries

    def test_evidence_id_slots_reserved_for_apigw_patterns(self, mocker):
        """Evidence ID slots for APIGW patterns are reserved even on non-APIGW groups.

        With _TOTAL_PATTERN_SLOTS = 6 (4 predefined + 2 APIGW):
          - APIGW group (lg_idx=0):
              HTTP 4XX \u2192 pat_idx=4 \u2192 ev-logs-005
              HTTP 5XX \u2192 pat_idx=5 \u2192 ev-logs-006
          - ECS group (lg_idx=1):
              ERROR    \u2192 pat_idx=0 \u2192 ev-logs-{1*_TOTAL_PATTERN_SLOTS+1:03d} = ev-logs-007
        Slots for lg_idx=1 PREDEFINED start at 7, not 5, because APIGW slots in
        lg_idx=0 are reserved regardless.
        """
        scope = {**SCOPE, "logGroups": [self._APIGW_LOG_GROUP, "/ecs/payment-service"]}
        client = _make_logs_mock(mocker, counts={
            (self._APIGW_LOG_GROUP, "HTTP 5XX", True): 50,   # APIGW group pat_idx=5
            ("/ecs/payment-service", "ERROR", True): 80,      # ECS group pat_idx=0
        })

        result = collect_logs(scope, client)

        ids = {f["details"]["pattern"]: f["evidenceId"] for f in result["findings"]}
        # APIGW: lg=0, pat_idx=5 (HTTP 5XX) \u2192 0*6+5+1 = 6 \u2192 ev-logs-006
        assert ids["HTTP 5XX"] == f"ev-logs-{0 * _TOTAL_PATTERN_SLOTS + len(PREDEFINED_PATTERNS) + 1 + 1:03d}"
        # ECS: lg=1, pat_idx=0 (ERROR) \u2192 1*6+0+1 = 7 \u2192 ev-logs-007
        assert ids["ERROR"] == f"ev-logs-{1 * _TOTAL_PATTERN_SLOTS + 0 + 1:03d}"
        # Verify no collision.
        assert ids["HTTP 5XX"] != ids["ERROR"]

    def test_userdefined_apigateway_prefix_is_detected(self, mocker):
        """apigateway/<name> (no /aws/ prefix) is treated as an API Gateway log group.

        This covers the common user-defined access-log destination pattern, e.g.
        apigateway/simple-crud-api-dev-access set as the stage access log ARN.
        The HTTP 5XX APIGW pattern must fire on such groups.
        """
        log_group = "apigateway/simple-crud-api-dev-access"
        client = _make_logs_mock(mocker, counts={
            (log_group, "HTTP 5XX", True): 75,
        })

        result = collect_logs({**SCOPE, "logGroups": [log_group]}, client)

        finding = next(
            (f for f in result["findings"] if f["details"]["pattern"] == "HTTP 5XX"),
            None,
        )
        assert finding is not None, "Expected HTTP 5XX finding for apigateway/ log group"
        assert finding["findingType"] == "new_error_pattern"
        assert finding["resourceType"] == "api-gateway"

    def test_userdefined_apigateway_prefix_resource_classification(self, mocker):
        """apigateway/<name> extracts the correct resourceName and resourceType.

        The name portion after 'apigateway/' must be used as resourceName.
        """
        log_group = "apigateway/simple-crud-api-dev-access"
        client = _make_logs_mock(mocker, counts={
            (log_group, "HTTP 4XX", True): 200,
            (log_group, "HTTP 4XX", False): 10,
        })

        result = collect_logs({**SCOPE, "logGroups": [log_group]}, client)

        finding = next(
            (f for f in result["findings"] if f["details"]["pattern"] == "HTTP 4XX"),
            None,
        )
        assert finding is not None
        assert finding["resourceType"] == "api-gateway"
        assert finding["resourceName"] == "simple-crud-api-dev-access"
        assert finding["findingType"] == "error_log_spike"
