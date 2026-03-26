"""
Unit tests for signals.py — the hypothesis signal normalization layer.

Tests normalize_findings() covering:
- each finding type classification
- detail field extraction (incidentCount, pattern, exceptionSamples)
- multi-source combinations
- edge cases: empty input, missing fields, malformed values

Run from repo root:
    pytest backend/orchestration/candidate_hypotheses_builder/tests/test_signals.py -v
"""


from signals import normalize_findings, TRACE_FAULT_HIGH_SCORE_THRESHOLD


# ---------------------------------------------------------------------------
# Minimal finding factories
# ---------------------------------------------------------------------------

def _finding(finding_type: str, evidence_id: str, **kwargs) -> dict:
    """Build a minimal finding dict."""
    return {
        "evidenceId": evidence_id,
        "findingType": finding_type,
        "score": kwargs.get("score", 0.5),
        "resourceName": kwargs.get("resourceName", "test-service"),
        "details": kwargs.get("details", {}),
        "summary": kwargs.get("summary", ""),
    }


# ---------------------------------------------------------------------------
# Empty / unknown input
# ---------------------------------------------------------------------------

class TestEmptyAndUnknown:

    def test_empty_list_returns_default_signals(self):
        sigs = normalize_findings([])
        assert not sigs.latency_signal_present
        assert not sigs.error_log_signal_present
        assert not sigs.error_rate_signal_present
        assert not sigs.trace_bottleneck_present
        assert not sigs.timeout_log_present
        assert sigs.resource_evidence_ids == []
        assert sigs.trace_fault_evidence_ids == []

    def test_unknown_finding_type_does_not_crash(self):
        sigs = normalize_findings([_finding("some_unknown_type", "ev-x-001")])
        assert not sigs.latency_signal_present
        assert sigs.resource_evidence_ids == []

    def test_finding_missing_evidence_id_does_not_add_empty_string(self):
        f = {"findingType": "latency_spike", "score": 0.9, "details": {}}
        sigs = normalize_findings([f])
        assert sigs.latency_signal_present
        assert sigs.latency_evidence_ids == []  # empty string not appended


# ---------------------------------------------------------------------------
# Latency signals
# ---------------------------------------------------------------------------

class TestLatencySignals:

    def test_latency_spike_classified_as_latency(self):
        sigs = normalize_findings([_finding("latency_spike", "ev-metrics-001")])
        assert sigs.latency_signal_present
        assert "ev-metrics-001" in sigs.latency_evidence_ids

    def test_trace_latency_spike_classified_as_latency(self):
        sigs = normalize_findings([_finding("trace_latency_spike", "ev-traces-001")])
        assert sigs.latency_signal_present
        assert "ev-traces-001" in sigs.latency_evidence_ids

    def test_metric_spike_with_latency_metric_name_classified_as_latency(self):
        sigs = normalize_findings([_finding("metric_spike", "ev-metrics-010",
                                            details={"metricName": "SuccessfulRequestLatency"})])
        assert sigs.latency_signal_present
        assert "ev-metrics-010" in sigs.latency_evidence_ids

    def test_metric_spike_with_response_keyword_in_summary(self):
        sigs = normalize_findings([_finding("metric_spike", "ev-metrics-011",
                                            summary="ResponseTime changed 2x vs baseline",
                                            details={})])
        assert sigs.latency_signal_present

    def test_metric_spike_without_latency_keyword_not_classified(self):
        sigs = normalize_findings([_finding("metric_spike", "ev-metrics-020",
                                            details={"metricName": "InvocationsCount"})])
        assert not sigs.latency_signal_present

    def test_two_latency_findings_sets_multi_latency(self):
        sigs = normalize_findings([
            _finding("latency_spike", "ev-metrics-001"),
            _finding("trace_latency_spike", "ev-traces-001"),
        ])
        assert sigs.multi_latency_present

    def test_duplicate_trace_latency_variants_do_not_set_multi_latency(self):
        """Equivalent duplicated trace-latency findings should not count as multi-latency corroboration."""
        sigs = normalize_findings([
            _finding("trace_latency_spike", "ev-traces-001"),
            _finding("trace_latency_spike", "ev-traces-004"),
        ])
        assert sigs.latency_signal_present
        assert not sigs.multi_latency_present

    def test_one_latency_finding_does_not_set_multi_latency(self):
        sigs = normalize_findings([_finding("latency_spike", "ev-metrics-001")])
        assert not sigs.multi_latency_present


# ---------------------------------------------------------------------------
# Error-rate signals
# ---------------------------------------------------------------------------

class TestErrorRateSignals:

    def test_error_rate_spike_sets_flag(self):
        sigs = normalize_findings([_finding("error_rate_spike", "ev-metrics-002")])
        assert sigs.error_rate_signal_present
        assert "ev-metrics-002" in sigs.error_rate_evidence_ids


# ---------------------------------------------------------------------------
# Error-log signals
# ---------------------------------------------------------------------------

class TestErrorLogSignals:

    def test_error_log_spike_sets_flag(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-001",
                                            details={"pattern": "Exception", "incidentCount": 10})])
        assert sigs.error_log_signal_present
        assert "ev-logs-001" in sigs.error_log_evidence_ids

    def test_new_error_pattern_sets_flag_and_new_pattern_ids(self):
        sigs = normalize_findings([_finding("new_error_pattern", "ev-logs-010",
                                            details={"pattern": "ERROR", "incidentCount": 5})])
        assert sigs.error_log_signal_present
        assert sigs.app_error_has_new_pattern
        assert "ev-logs-010" in sigs.new_error_pattern_evidence_ids
        assert "ev-logs-010" in sigs.error_log_evidence_ids

    def test_incident_count_summed_across_findings(self):
        f1 = _finding("error_log_spike", "ev-logs-001", details={"pattern": "ERROR", "incidentCount": 12})
        f2 = _finding("new_error_pattern", "ev-logs-002", details={"pattern": "Exception", "incidentCount": 8})
        sigs = normalize_findings([f1, f2])
        assert sigs.app_error_incident_count == 20

    def test_malformed_incident_count_skipped_gracefully(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-001",
                                            details={"pattern": "ERROR", "incidentCount": "bad"})])
        assert sigs.app_error_incident_count == 0

    def test_none_incident_count_treated_as_zero(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-001",
                                            details={"pattern": "ERROR", "incidentCount": None})])
        assert sigs.app_error_incident_count == 0

    def test_pattern_names_deduplicated_and_sorted(self):
        f1 = _finding("error_log_spike", "ev-logs-001", details={"pattern": "Exception"})
        f2 = _finding("new_error_pattern", "ev-logs-002", details={"pattern": "ERROR"})
        f3 = _finding("error_log_spike", "ev-logs-003", details={"pattern": "Exception"})
        sigs = normalize_findings([f1, f2, f3])
        assert sigs.error_log_pattern_names == ["ERROR", "Exception"]

    def test_exception_samples_collected_across_findings(self):
        f1 = _finding("error_log_spike", "ev-logs-001",
                      details={"pattern": "ERROR", "exceptionSamples": ["sample-a", "sample-b"]})
        f2 = _finding("error_log_spike", "ev-logs-002",
                      details={"pattern": "Exception", "exceptionSamples": ["sample-c"]})
        sigs = normalize_findings([f1, f2])
        assert sigs.app_error_exception_samples == ["sample-a", "sample-b", "sample-c"]

    def test_non_string_exception_samples_skipped(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-001",
                                            details={"pattern": "ERROR",
                                                     "exceptionSamples": [None, 42, "valid-sample"]})])
        assert sigs.app_error_exception_samples == ["valid-sample"]

    def test_missing_exception_samples_handled(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-001",
                                            details={"pattern": "ERROR"})])
        assert sigs.app_error_exception_samples == []


# ---------------------------------------------------------------------------
# Timeout / connectivity classification
# ---------------------------------------------------------------------------

class TestTimeoutLogClassification:

    def test_timeout_pattern_sets_timeout_flag(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-002",
                                            details={"pattern": "timeout"})])
        assert sigs.timeout_log_present
        assert "ev-logs-002" in sigs.timeout_log_evidence_ids
        # Also included in error_log evidence
        assert "ev-logs-002" in sigs.error_log_evidence_ids

    def test_connection_refused_pattern_sets_timeout_flag(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-002",
                                            details={"pattern": "connection refused"})])
        assert sigs.timeout_log_present

    def test_timeout_case_insensitive(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-002",
                                            details={"pattern": "ReadTimeout"})])
        assert sigs.timeout_log_present

    def test_non_timeout_pattern_does_not_set_flag(self):
        sigs = normalize_findings([_finding("error_log_spike", "ev-logs-001",
                                            details={"pattern": "Exception"})])
        assert not sigs.timeout_log_present

    def test_timeout_pattern_names_deduplicated(self):
        f1 = _finding("error_log_spike", "ev-logs-001", details={"pattern": "timeout"})
        f2 = _finding("error_log_spike", "ev-logs-002", details={"pattern": "timeout"})
        sigs = normalize_findings([f1, f2])
        assert sigs.timeout_log_pattern_names == ["timeout"]


# ---------------------------------------------------------------------------
# Trace signals
# ---------------------------------------------------------------------------

class TestTraceSignals:

    def test_latency_dominant_segment_sets_bottleneck(self):
        sigs = normalize_findings([_finding("latency_dominant_segment", "ev-traces-002")])
        assert sigs.trace_bottleneck_present
        assert "ev-traces-002" in sigs.trace_bottleneck_evidence_ids

    def test_trace_dependency_fault_strong(self):
        sigs = normalize_findings([
            _finding("trace_dependency_fault", "ev-traces-003",
                     score=TRACE_FAULT_HIGH_SCORE_THRESHOLD)
        ])
        assert sigs.trace_fault_strong_count == 1
        assert sigs.trace_fault_weak_count == 0
        assert "ev-traces-003" in sigs.trace_fault_evidence_ids

    def test_trace_dependency_fault_weak(self):
        sigs = normalize_findings([
            _finding("trace_dependency_fault", "ev-traces-003",
                     score=TRACE_FAULT_HIGH_SCORE_THRESHOLD - 0.01)
        ])
        assert sigs.trace_fault_strong_count == 0
        assert sigs.trace_fault_weak_count == 1

    def test_mixed_strong_and_weak_trace_faults(self):
        sigs = normalize_findings([
            _finding("trace_dependency_fault", "ev-traces-003", score=0.80),
            _finding("trace_dependency_fault", "ev-traces-006", score=0.40),
        ])
        assert sigs.trace_fault_strong_count == 1
        assert sigs.trace_fault_weak_count == 1

    def test_trace_error_status_sets_flag_and_evidence_id(self):
        """trace_error_status finding sets trace_error_status_present and adds evidence ID."""
        sigs = normalize_findings([
            _finding("trace_error_status", "ev-traces-004",
                     details={"signalType": "fault"}, score=0.85)
        ])
        assert sigs.trace_error_status_present
        assert "ev-traces-004" in sigs.trace_error_status_evidence_ids
        assert sigs.trace_error_status_signal_type == "fault"

    def test_trace_error_status_also_sets_error_rate_signal(self):
        """trace_error_status is also registered as an error-rate signal for existing rules."""
        sigs = normalize_findings([
            _finding("trace_error_status", "ev-traces-004",
                     details={"signalType": "error"}, score=0.60)
        ])
        assert sigs.error_rate_signal_present
        assert "ev-traces-004" in sigs.error_rate_evidence_ids

    def test_trace_error_status_fault_type_wins_over_error(self):
        """When both error and fault findings are present, signal_type should be 'fault'."""
        sigs = normalize_findings([
            _finding("trace_error_status", "ev-traces-004",
                     details={"signalType": "error"}, score=0.60),
            _finding("trace_error_status", "ev-traces-008",
                     details={"signalType": "fault"}, score=0.85),
        ])
        assert sigs.trace_error_status_signal_type == "fault"

    def test_trace_error_status_without_signal_type_field(self):
        """Missing signalType in details does not crash; flag is still set."""
        sigs = normalize_findings([
            _finding("trace_error_status", "ev-traces-004", details={}, score=0.70)
        ])
        assert sigs.trace_error_status_present
        assert sigs.trace_error_status_signal_type == ""


# ---------------------------------------------------------------------------
# Resource exhaustion signals
# ---------------------------------------------------------------------------

class TestResourceSignals:

    def test_cpu_spike_added_to_resource(self):
        sigs = normalize_findings([_finding("cpu_spike", "ev-metrics-003")])
        assert "ev-metrics-003" in sigs.resource_evidence_ids
        assert "cpu_spike" in sigs.resource_finding_types

    def test_memory_spike_added_to_resource(self):
        sigs = normalize_findings([_finding("memory_spike", "ev-metrics-004")])
        assert "memory_spike" in sigs.resource_finding_types

    def test_db_connection_spike_added_to_resource(self):
        sigs = normalize_findings([_finding("db_connection_spike", "ev-metrics-005")])
        assert "db_connection_spike" in sigs.resource_finding_types

    def test_resource_finding_types_deduplicated_and_sorted(self):
        sigs = normalize_findings([
            _finding("cpu_spike", "ev-metrics-003"),
            _finding("memory_spike", "ev-metrics-004"),
            _finding("cpu_spike", "ev-metrics-005"),
        ])
        assert sigs.resource_finding_types == ["cpu_spike", "memory_spike"]
        assert len(sigs.resource_evidence_ids) == 3   # IDs not deduplicated


# ---------------------------------------------------------------------------
# Multi-source combinations
# ---------------------------------------------------------------------------

class TestMultiSourceCombinations:

    def test_latency_and_error_signals(self):
        sigs = normalize_findings([
            _finding("latency_spike", "ev-metrics-001"),
            _finding("error_rate_spike", "ev-metrics-002"),
            _finding("error_log_spike", "ev-logs-001", details={"pattern": "Exception", "incidentCount": 5}),
        ])
        assert sigs.latency_signal_present
        assert sigs.error_rate_signal_present
        assert sigs.error_log_signal_present
        assert sigs.app_error_incident_count == 5

    def test_all_signals_present(self):
        sigs = normalize_findings([
            _finding("latency_spike", "ev-metrics-001"),
            _finding("trace_latency_spike", "ev-traces-001"),
            _finding("error_rate_spike", "ev-metrics-002"),
            _finding("error_log_spike", "ev-logs-001", details={"pattern": "Exception"}),
            _finding("new_error_pattern", "ev-logs-002", details={"pattern": "ERROR"}),
            _finding("error_log_spike", "ev-logs-003", details={"pattern": "timeout"}),
            _finding("latency_dominant_segment", "ev-traces-002"),
            _finding("trace_dependency_fault", "ev-traces-003", score=0.9),
            _finding("cpu_spike", "ev-metrics-003"),
        ])
        assert sigs.latency_signal_present
        assert sigs.multi_latency_present           # 2 latency findings
        assert sigs.error_rate_signal_present
        assert sigs.error_log_signal_present
        assert sigs.app_error_has_new_pattern
        assert sigs.timeout_log_present
        assert sigs.trace_bottleneck_present
        assert sigs.trace_fault_strong_count == 1
        assert len(sigs.resource_evidence_ids) == 1
