"""Signal normalization layer for the Candidate Hypotheses Builder.

Converts raw worker findings into a tightly-typed NormalizedSignals object
that hypothesis rules consume. Rules read from this object instead of
directly inspecting worker-specific findingType strings and details layouts.

Only the signals that current hypothesis rules actually need are normalised
here. This is intentionally thin — it is not a full taxonomy of all findings.

- ADR-002: deterministic, no AI
- ADR-007: evidence IDs preserved and grouped per signal type
"""

from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Finding-type sets — worker-specific strings are confined to this module.
# ---------------------------------------------------------------------------

_LATENCY_TYPES = frozenset({"latency_spike"})
_TRACE_LATENCY_TYPES = frozenset({"trace_latency_spike"})
_ERROR_RATE_TYPES = frozenset({"error_rate_spike"})
_ERROR_LOG_TYPES = frozenset({"error_log_spike", "new_error_pattern"})
_RESOURCE_TYPES = frozenset({"cpu_spike", "memory_spike", "db_connection_spike"})
_TRACE_BOTTLENECK_TYPES = frozenset({"latency_dominant_segment"})
_TRACE_FAULT_TYPES = frozenset({"trace_dependency_fault"})
_TRACE_ERROR_STATUS_TYPES = frozenset({"trace_error_status"})

# Keywords that identify a generic metric_spike finding as a latency signal.
_LATENCY_METRIC_NAME_KEYWORDS = ("latency", "response", "duration")

# Substrings that classify a log pattern name as a timeout/connectivity issue.
_TIMEOUT_SUBSTRINGS = ("timeout", "connection refused")

# Threshold separating strong trace faults (5xx/throttle) from weak (4xx) ones.
# Used in the downstream-dependency confidence boost calculation.
TRACE_FAULT_HIGH_SCORE_THRESHOLD = 0.75


# ---------------------------------------------------------------------------
# NormalizedSignals dataclass
# ---------------------------------------------------------------------------

@dataclass
class NormalizedSignals:
    """Normalised signal summary derived from raw worker findings.

    Decouples hypothesis rules from worker-specific findingType strings and
    details-field layouts. Confidence scoring and trigger conditions are driven
    by the presence flags and counts. Display-only data (rationale text inputs)
    is included as named fields to keep rules self-contained.
    """

    # --- Latency signals (metric latency, trace latency, metric_spike keyword match) ---
    latency_signal_present: bool = False
    latency_evidence_ids: list[str] = field(default_factory=list)
    multi_latency_present: bool = False     # True when 2+ latency findings present

    # --- Error-rate signals (metric 5xx / Lambda Errors spikes) ---
    error_rate_signal_present: bool = False
    error_rate_evidence_ids: list[str] = field(default_factory=list)

    # --- Error-log signals (error_log_spike, new_error_pattern) ---
    error_log_signal_present: bool = False
    error_log_evidence_ids: list[str] = field(default_factory=list)
    new_error_pattern_evidence_ids: list[str] = field(default_factory=list)
    error_log_pattern_names: list[str] = field(default_factory=list)    # display only

    # --- Trace bottleneck signals (latency_dominant_segment) ---
    trace_bottleneck_present: bool = False
    trace_bottleneck_evidence_ids: list[str] = field(default_factory=list)

    # --- Trace fault signals (trace_dependency_fault), split by severity ---
    trace_fault_evidence_ids: list[str] = field(default_factory=list)
    trace_fault_strong_count: int = 0       # score >= TRACE_FAULT_HIGH_SCORE_THRESHOLD
    trace_fault_weak_count: int = 0         # score <  TRACE_FAULT_HIGH_SCORE_THRESHOLD

    # --- Trace error-status signals (trace_error_status) ---
    # Emitted when X-Ray trace summaries show fault/error flags in the incident
    # window that were absent (or significantly lower) in the baseline.
    # Catches Lambda-level errors (timeouts, unhandled exceptions) that do not
    # produce faulted downstream subsegments.
    trace_error_status_present: bool = False
    trace_error_status_evidence_ids: list[str] = field(default_factory=list)
    trace_error_status_signal_type: str = ""   # "fault" or "error" from the finding

    # --- Timeout / connectivity log patterns ---
    timeout_log_present: bool = False
    timeout_log_evidence_ids: list[str] = field(default_factory=list)
    timeout_log_pattern_names: list[str] = field(default_factory=list)   # display only

    # --- Resource exhaustion signals ---
    resource_evidence_ids: list[str] = field(default_factory=list)
    resource_finding_types: list[str] = field(default_factory=list)      # display only

    # --- App-error enrichments (scoring triggers for the error-surge rule) ---
    app_error_incident_count: int = 0       # sum of details.incidentCount
    app_error_has_new_pattern: bool = False
    app_error_exception_samples: list[str] = field(default_factory=list) # raw sample strings

    # --- Generic lookup ---
    evidence_source_by_id: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Public normalisation function
# ---------------------------------------------------------------------------

def normalize_findings(all_findings: list[dict]) -> NormalizedSignals:
    """Build a NormalizedSignals from a flat list of worker findings.

    Call once at the start of hypothesis building. Each rule then reads
    signals from the returned object without inspecting raw findings directly.
    """
    sigs = NormalizedSignals()
    latency_modalities: set[str] = set()

    for f in all_findings:
        ft = f.get("findingType", "")
        ev = f.get("evidenceId", "")
        source = str(f.get("source", ""))
        details = f.get("details") or {}

        if ev and source:
            sigs.evidence_source_by_id[ev] = source

        # Error-rate
        if ft in _ERROR_RATE_TYPES:
            sigs.error_rate_signal_present = True
            if ev:
                sigs.error_rate_evidence_ids.append(ev)

        # Error-log (handles both error_log_spike and new_error_pattern)
        if ft in _ERROR_LOG_TYPES:
            sigs.error_log_signal_present = True
            if ev:
                sigs.error_log_evidence_ids.append(ev)

            if ft == "new_error_pattern":
                sigs.app_error_has_new_pattern = True
                if ev:
                    sigs.new_error_pattern_evidence_ids.append(ev)

            pattern_name = str(details.get("pattern", ""))
            if pattern_name and pattern_name not in sigs.error_log_pattern_names:
                sigs.error_log_pattern_names.append(pattern_name)

            try:
                sigs.app_error_incident_count += int(details.get("incidentCount", 0) or 0)
            except (ValueError, TypeError):
                pass

            for s in (details.get("exceptionSamples") or []):
                if isinstance(s, str):
                    sigs.app_error_exception_samples.append(s)

            # Classify as timeout / connectivity if pattern name matches.
            if _is_timeout_pattern(pattern_name):
                sigs.timeout_log_present = True
                if ev:
                    sigs.timeout_log_evidence_ids.append(ev)
                if pattern_name and pattern_name not in sigs.timeout_log_pattern_names:
                    sigs.timeout_log_pattern_names.append(pattern_name)

        # Latency signal (metric latency, trace latency, metric_spike keyword)
        if _is_latency_finding(ft, details, str(f.get("summary", ""))):
            sigs.latency_signal_present = True
            if ev:
                sigs.latency_evidence_ids.append(ev)
            modality = _latency_modality_key(ft, details, str(f.get("summary", "")))
            if modality:
                latency_modalities.add(modality)

        # Trace bottleneck
        if ft in _TRACE_BOTTLENECK_TYPES:
            sigs.trace_bottleneck_present = True
            if ev:
                sigs.trace_bottleneck_evidence_ids.append(ev)

        # Trace fault — split by severity for downstream confidence weighting
        if ft in _TRACE_FAULT_TYPES:
            if ev:
                sigs.trace_fault_evidence_ids.append(ev)
            score = f.get("score", 0.0)
            if score >= TRACE_FAULT_HIGH_SCORE_THRESHOLD:
                sigs.trace_fault_strong_count += 1
            else:
                sigs.trace_fault_weak_count += 1

        # Trace error-status — Lambda-level fault/error from X-Ray trace summaries
        # (e.g. Lambda timeout, unhandled exception) that does not produce a faulted
        # downstream subsegment. Treated as an error-rate corroborator.
        if ft in _TRACE_ERROR_STATUS_TYPES:
            sigs.trace_error_status_present = True
            if ev:
                sigs.trace_error_status_evidence_ids.append(ev)
            # Capture the signal type from the first finding (fault > error).
            signal_type = str(details.get("signalType", ""))
            if signal_type and (
                not sigs.trace_error_status_signal_type
                or signal_type == "fault"
            ):
                sigs.trace_error_status_signal_type = signal_type
            # Also register as an error-rate signal so existing rules that check
            # error_rate_signal_present (e.g. runtime latency regression) pick it up.
            sigs.error_rate_signal_present = True
            if ev:
                sigs.error_rate_evidence_ids.append(ev)

        # Resource exhaustion
        if ft in _RESOURCE_TYPES:
            if ev:
                sigs.resource_evidence_ids.append(ev)
            if ft not in sigs.resource_finding_types:
                sigs.resource_finding_types.append(ft)

    # Derived flags computed after full scan
    # Require corroboration across distinct latency modalities/surfaces,
    # not repeated variants of the same surface (e.g. duplicated trace services).
    sigs.multi_latency_present = len(latency_modalities) >= 2
    sigs.resource_finding_types.sort()
    sigs.error_log_pattern_names.sort()
    sigs.timeout_log_pattern_names.sort()

    return sigs


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_latency_finding(finding_type: str, details: dict, summary: str) -> bool:
    """Return True if a finding represents a latency signal."""
    if finding_type in (_LATENCY_TYPES | _TRACE_LATENCY_TYPES):
        return True
    if finding_type != "metric_spike":
        return False
    metric_name = str(details.get("metricName", "")).lower()
    summary_lower = summary.lower()
    return any(kw in metric_name or kw in summary_lower for kw in _LATENCY_METRIC_NAME_KEYWORDS)


def _is_timeout_pattern(pattern_name: str) -> bool:
    """Return True if the log pattern name indicates a timeout or connectivity issue."""
    lower = pattern_name.lower()
    return any(t in lower for t in _TIMEOUT_SUBSTRINGS)


def _latency_modality_key(finding_type: str, details: dict, summary: str) -> str | None:
    """Return a normalized latency modality key for corroboration logic.

    This separates *independent latency surfaces* from duplicate representations
    of the same surface.
    """
    if finding_type == "trace_latency_spike":
        return "trace_latency"
    if finding_type == "latency_spike":
        return "service_latency"
    if finding_type == "metric_spike":
        metric_name = str(details.get("metricName", "")).lower()
        summary_lower = summary.lower()
        if any(kw in metric_name or kw in summary_lower for kw in _LATENCY_METRIC_NAME_KEYWORDS):
            return "generic_metric_latency"
    return None
