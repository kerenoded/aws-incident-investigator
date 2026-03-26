"""
Candidate Hypotheses Builder — core logic.

Deterministic rule-based component that combines all worker findings and
constructs a shortlist of candidate root-cause hypotheses with evidence
references and confidence scores. No AI involved.

- ADR-002: deterministic, no AI
- ADR-007: references stable evidenceIds from worker findings
- ADR-009: handles missing or empty worker outputs gracefully
- ADR-019: scoring uses heuristic weights, not calibrated probabilities

Worker-specific finding types and details layouts are isolated in
signals.py. Rules consume NormalizedSignals and do not inspect raw
findings directly, except for display-only label enrichment.
"""

import logging
import re
from collections import Counter

from signals import NormalizedSignals, normalize_findings  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring constants — heuristic weights, not calibrated probabilities.
#
# Three distinct scoring concepts exist in this codebase (see ADR-019):
#   evidence score    (worker level):    0–1 anomaly magnitude per finding
#   hypothesis confidence (this layer):  heuristic rank among candidate causes
#   AI plausibility   (evaluator level): comparative Bedrock assessment
#
# Hypothesis confidence follows: base + sum(applied boosts), capped.
# Boosts add weight when corroborating signals are present. The cap prevents
# spurious high confidence from many individually weak signals.
# ---------------------------------------------------------------------------

# Runtime latency regression rule
_BASE_RUNTIME_LATENCY = 0.35
_BOOST_LATENCY_ERROR_SIGNAL = 0.15
_BOOST_LATENCY_TRACE_BOTTLENECK = 0.15
_BOOST_LATENCY_MULTI_LATENCY = 0.10
_BOOST_LATENCY_CROSS_EVIDENCE_BREADTH = 0.10
_CAP_RUNTIME_LATENCY = 0.85

# Downstream dependency latency rule
_BASE_DOWNSTREAM_DEPENDENCY = 0.15
_BOOST_DOWNSTREAM_TIMEOUT_LOG = 0.20      # per timeout-log finding
_BOOST_DOWNSTREAM_TRACE_FAULT_STRONG = 0.20   # per strong fault (score >= threshold)
_BOOST_DOWNSTREAM_TRACE_FAULT_WEAK = 0.10     # per weak fault (4xx, score < threshold)
_CAP_DOWNSTREAM_DEPENDENCY = 0.70

# Application error surge rule
_BASE_APP_ERROR_SURGE = 0.25
_BOOST_ERROR_SURGE_NEW_PATTERN = 0.10
_BOOST_ERROR_SURGE_COUNT_GE_THRESHOLD = 0.10
_BOOST_ERROR_SURGE_MULTI_PATTERN = 0.10
_BOOST_ERROR_SURGE_MULTI_SOURCE = 0.05
_BOOST_ERROR_SURGE_TRACE_CORROBORATION = 0.05   # weak trace fault (4xx) confirms app code made bad call
_CAP_APP_ERROR_SURGE = 0.70
_ERROR_SURGE_COUNT_THRESHOLD = 20

# Resource exhaustion rule
_BASE_RESOURCE_EXHAUSTION = 0.20
_BOOST_RESOURCE_EACH_ADDITIONAL = 0.25
_CAP_RESOURCE_EXHAUSTION = 0.70

# Unknown cause fallback
_UNKNOWN_CAUSE_CONFIDENCE = 0.05

# Short qualifiers for well-understood exception classes.
# Display-only: appended to the cause label when the exception class is known.
# Keep this list small — only add entries that carry clear, general meaning.
_EXCEPTION_QUALIFIERS: dict[str, str] = {
    "ValidationException": "key/schema mismatch",
    "AccessDeniedException": "access denied",
    "AccessDenied": "access denied",
    "ResourceNotFoundException": "resource not found",
    "ConditionalCheckFailedException": "write condition failed",
    "ThrottlingException": "request throttled",
    "ServiceUnavailableException": "service unavailable",
}

# Guard keywords: when a qualifier describes only one subtype of a broad exception
# class, list substrings that must appear in the exception sample text for the
# qualifier to be applied.  Exception classes absent from this dict always
# receive their qualifier (their qualifier is semantically identical to the
# class name itself).
_QUALIFIER_GUARDS: dict[str, list[str]] = {
    "ValidationException": ["key element", "schema", "attribute type"],
}

# Regex to extract an exception class name from AWS SDK exception messages.
# Primary: matches the parenthesised class in AWS SDK format, e.g.
#   ClientError: An error occurred (ValidationException) when calling ...
# Fallback: matches bare prefix format, e.g.  ValidationException: message
# Applied separately (primary first) so the parenthesised form always wins.
_EXCEPTION_CLASS_PRIMARY_RE = re.compile(r"\((\w+(?:Exception|Error|Fault|Denied))\)")
_EXCEPTION_CLASS_FALLBACK_RE = re.compile(r"\b(\w+(?:Exception|Error|Fault))\s*:")


def build_hypotheses(scope: dict, worker_outputs: dict) -> dict:
    """Build candidate root-cause hypotheses from combined worker findings.

    Applies named hypothesis rules against normalised signals and falls back
    to a single unknown_cause hypothesis when no rule fires.

    Args:
        scope:          Scope dict from the Scope Builder
                        (see schemas/scope.schema.json).
        worker_outputs: Dict keyed by source name ("metrics", "logs",
                        "traces"). Missing keys are treated as
                        empty (findings: [], errors: []).

    Returns:
        Hypothesis envelope: { incidentId, hypotheses: [...] }
        Each hypothesis conforms to schemas/hypothesis.schema.json.
    """
    incident_id = scope["incidentId"]
    all_findings = _canonicalize_findings(_flatten_findings(worker_outputs))
    sigs = normalize_findings(all_findings)

    candidates = []

    h = _rule_runtime_latency_regression(sigs)
    if h:
        candidates.append(h)

    h = _rule_application_error_surge(sigs)
    if h:
        candidates.append(h)

    h = _rule_downstream_dependency_latency(sigs)
    if h:
        candidates.append(h)

    h = _rule_resource_exhaustion(sigs)
    if h:
        candidates.append(h)

    if not candidates:
        final_confidence = _UNKNOWN_CAUSE_CONFIDENCE
        candidates.append({
            "cause": "unknown_cause",
            "rationale": "No strong signal detected in available worker findings.",
            "supportingEvidenceIds": [],
            "confidence": final_confidence,
            "confidenceBreakdown": _build_confidence_breakdown(
                base=_UNKNOWN_CAUSE_CONFIDENCE,
                boosts=[],
                cap=1.0,
                final=final_confidence,
            ),
        })

    candidates.sort(key=lambda c: c["confidence"], reverse=True)

    hypotheses = [
        {"id": f"h{i + 1}", **candidate}
        for i, candidate in enumerate(candidates[:3])
    ]

    return {
        "incidentId": incident_id,
        "hypotheses": hypotheses,
    }


# ---------------------------------------------------------------------------
# Hypothesis rules
# ---------------------------------------------------------------------------

def _rule_runtime_latency_regression(sigs: NormalizedSignals) -> dict | None:
    """Rule: broad runtime latency regression with corroborating evidence.

    Trigger conditions:
      - At least one latency signal (metric, latency finding, or trace latency)
      - Plus at least one corroborator:
          - error-rate/error-log signal, OR
          - trace bottleneck signal

    This rule intentionally bridges cross-worker evidence so findings that are
    currently disconnected from specific named rules can still contribute to a
    concrete, deterministic hypothesis.
    """
    if not sigs.latency_signal_present:
        return None

    has_error_signal = sigs.error_rate_signal_present or sigs.error_log_signal_present
    if not has_error_signal and not sigs.trace_bottleneck_present:
        return None

    error_boost_applied = has_error_signal
    trace_boost_applied = sigs.trace_bottleneck_present
    multi_latency_boost_applied = sigs.multi_latency_present
    supporting_sources = {
        sigs.evidence_source_by_id[evidence_id]
        for evidence_id in (sigs.latency_evidence_ids + sigs.trace_bottleneck_evidence_ids)
        if evidence_id in sigs.evidence_source_by_id
    }
    cross_evidence_breadth_boost_applied = (
        len(sigs.latency_evidence_ids + sigs.trace_bottleneck_evidence_ids) >= 4
        and len(supporting_sources) >= 2
    )

    confidence = _BASE_RUNTIME_LATENCY
    if error_boost_applied:
        confidence += _BOOST_LATENCY_ERROR_SIGNAL
    if trace_boost_applied:
        confidence += _BOOST_LATENCY_TRACE_BOTTLENECK
    if multi_latency_boost_applied:
        confidence += _BOOST_LATENCY_MULTI_LATENCY
    if cross_evidence_breadth_boost_applied:
        confidence += _BOOST_LATENCY_CROSS_EVIDENCE_BREADTH
    final_confidence = min(confidence, _CAP_RUNTIME_LATENCY)

    error_evidence_ids = sigs.error_rate_evidence_ids + sigs.error_log_evidence_ids
    supporting = (
        sigs.latency_evidence_ids
        + error_evidence_ids
        + sigs.trace_bottleneck_evidence_ids
    )

    rationale = ["Cross-source latency degradation signal detected."]
    if has_error_signal:
        rationale.append("Error signals co-occur with latency change.")
    if sigs.trace_bottleneck_present:
        rationale.append("Trace bottleneck evidence indicates a dominant slow segment.")

    return {
        "cause": "runtime latency regression",
        "rationale": " ".join(rationale),
        "supportingEvidenceIds": supporting,
        "confidence": round(final_confidence, 4),
        "confidenceBreakdown": _build_confidence_breakdown(
            base=_BASE_RUNTIME_LATENCY,
            boosts=[
                ("error_signal", _BOOST_LATENCY_ERROR_SIGNAL, error_boost_applied),
                ("trace_bottleneck_signal", _BOOST_LATENCY_TRACE_BOTTLENECK, trace_boost_applied),
                ("multiple_latency_signals", _BOOST_LATENCY_MULTI_LATENCY, multi_latency_boost_applied),
                (
                    "cross_evidence_breadth",
                    _BOOST_LATENCY_CROSS_EVIDENCE_BREADTH,
                    cross_evidence_breadth_boost_applied,
                ),
            ],
            cap=_CAP_RUNTIME_LATENCY,
            final=final_confidence,
        ),
    }


def _rule_downstream_dependency_latency(sigs: NormalizedSignals) -> dict | None:
    """Rule: downstream dependency failure signals from logs or X-Ray traces.

    Triggers on:
    - trace_dependency_fault findings from the traces worker (always sufficient).

    A timeout log alone is not sufficient: a Lambda self-timeout also produces a
    'timeout' log pattern but no faulted downstream subsegment. Requiring trace fault
    corroboration prevents false-positive downstream hypotheses in that case.
    Timeout logs still contribute their confidence boost when trace faults are present.

    Confidence boosts:
    - Log timeout pattern:       +0.20 per finding (only when trace faults present)
    - Trace fault (5xx/throttle, score >= threshold): +0.20 per finding
    - Trace error (4xx only, score < threshold):      +0.10 per finding
    Cap: 0.70.
    """
    if not sigs.trace_fault_evidence_ids:
        return None

    confidence = _BASE_DOWNSTREAM_DEPENDENCY
    confidence += _BOOST_DOWNSTREAM_TIMEOUT_LOG * len(sigs.timeout_log_evidence_ids)
    confidence += _BOOST_DOWNSTREAM_TRACE_FAULT_STRONG * sigs.trace_fault_strong_count
    confidence += _BOOST_DOWNSTREAM_TRACE_FAULT_WEAK * sigs.trace_fault_weak_count
    confidence = min(confidence, _CAP_DOWNSTREAM_DEPENDENCY)

    supporting = sigs.timeout_log_evidence_ids + sigs.trace_fault_evidence_ids

    rationale_parts = []
    if sigs.timeout_log_present:
        pattern_names = sorted(sigs.timeout_log_pattern_names)
        rationale_parts.append(
            f"Timeout or connectivity error patterns detected: {', '.join(pattern_names)}."
        )
    if sigs.trace_fault_strong_count > 0:
        rationale_parts.append(
            "Faulted or throttled downstream dependency detected in X-Ray traces (5xx/throttle)."
        )
    if sigs.trace_fault_weak_count > 0:
        rationale_parts.append(
            "X-Ray dependency subsegment shows client-side 4xx (for example a malformed downstream request). API edge metrics may still show 5xx when the upstream Lambda invocation fails."
        )

    # Cause label reflects signal strength.
    if sigs.trace_fault_strong_count > 0:
        cause = "downstream dependency failure"
    elif sigs.trace_fault_weak_count > 0 and not sigs.timeout_log_present:
        cause = "downstream dependency client error"
    else:
        cause = "downstream dependency latency"

    return {
        "cause": cause,
        "rationale": " ".join(rationale_parts),
        "supportingEvidenceIds": supporting,
        "confidence": round(confidence, 4),
        "confidenceBreakdown": _build_confidence_breakdown(
            base=_BASE_DOWNSTREAM_DEPENDENCY,
            boosts=[
                (
                    "timeout_or_connectivity_pattern_count",
                    round(_BOOST_DOWNSTREAM_TIMEOUT_LOG * len(sigs.timeout_log_evidence_ids), 4),
                    sigs.timeout_log_present,
                ),
                (
                    "trace_fault_strong",
                    round(_BOOST_DOWNSTREAM_TRACE_FAULT_STRONG * sigs.trace_fault_strong_count, 4),
                    sigs.trace_fault_strong_count > 0,
                ),
                (
                    "trace_fault_weak_4xx",
                    round(_BOOST_DOWNSTREAM_TRACE_FAULT_WEAK * sigs.trace_fault_weak_count, 4),
                    sigs.trace_fault_weak_count > 0,
                ),
            ],
            cap=_CAP_DOWNSTREAM_DEPENDENCY,
            final=round(confidence, 4),
        ),
    }


def _rule_application_error_surge(sigs: NormalizedSignals) -> dict | None:
    """Rule: runtime/application errors surged relative to baseline.

    Triggered by error log findings, including baseline-zero discoveries
    (new_error_pattern). This provides a general deterministic path for
    incidents where logs clearly indicate increased runtime errors but no
    latency/resource rule is matched.
    """
    if not sigs.error_log_signal_present:
        return None

    new_pattern_boost_applied = sigs.app_error_has_new_pattern
    incident_count_boost_applied = sigs.app_error_incident_count >= _ERROR_SURGE_COUNT_THRESHOLD
    multi_pattern_boost_applied = len(sigs.error_log_evidence_ids) >= 2
    trace_corroboration_boost_applied = bool(sigs.trace_fault_evidence_ids)
    supporting = (
        sigs.error_log_evidence_ids
        + sigs.error_rate_evidence_ids
        + sigs.trace_fault_evidence_ids
    )
    supporting_sources = {
        sigs.evidence_source_by_id[evidence_id]
        for evidence_id in supporting
        if evidence_id in sigs.evidence_source_by_id
    }
    multi_source_boost_applied = len(supporting_sources) >= 2

    confidence = _BASE_APP_ERROR_SURGE
    if new_pattern_boost_applied:
        confidence += _BOOST_ERROR_SURGE_NEW_PATTERN
    if incident_count_boost_applied:
        confidence += _BOOST_ERROR_SURGE_COUNT_GE_THRESHOLD
    if multi_pattern_boost_applied:
        confidence += _BOOST_ERROR_SURGE_MULTI_PATTERN
    if multi_source_boost_applied:
        confidence += _BOOST_ERROR_SURGE_MULTI_SOURCE
    if trace_corroboration_boost_applied:
        confidence += _BOOST_ERROR_SURGE_TRACE_CORROBORATION
    final_confidence = min(confidence, _CAP_APP_ERROR_SURGE)

    # Exception class extraction for cause label enrichment (display only).
    exception_class = _extract_exception_class(sigs.app_error_exception_samples)
    if exception_class:
        qualifier = _EXCEPTION_QUALIFIERS.get(exception_class)
        if qualifier:
            guards = _QUALIFIER_GUARDS.get(exception_class)
            if guards:
                combined = " ".join(sigs.app_error_exception_samples).lower()
                if not any(g in combined for g in guards):
                    qualifier = None
        cause = (
            f"application error surge — {exception_class} ({qualifier})"
            if qualifier
            else f"application error surge — {exception_class}"
        )
    else:
        cause = "application error surge"

    rationale_parts = [
        "Application/runtime error logs increased in the incident window versus baseline."
    ]
    if exception_class:
        rationale_parts.append(f"Dominant exception class in log samples: {exception_class}.")
    if sigs.app_error_has_new_pattern:
        rationale_parts.append("At least one error pattern was newly introduced in the incident window.")
    if sigs.app_error_incident_count > 0:
        rationale_parts.append(
            f"Observed aggregated error-pattern count in sampled findings: {sigs.app_error_incident_count}."
        )
    if sigs.error_log_pattern_names:
        rationale_parts.append(f"Patterns: {', '.join(p for p in sigs.error_log_pattern_names if p)}.")

    return {
        "cause": cause,
        "rationale": " ".join(rationale_parts),
        "supportingEvidenceIds": supporting,
        "confidence": round(final_confidence, 4),
        "confidenceBreakdown": _build_confidence_breakdown(
            base=_BASE_APP_ERROR_SURGE,
            boosts=[
                ("new_error_pattern_present", _BOOST_ERROR_SURGE_NEW_PATTERN, new_pattern_boost_applied),
                ("incident_error_count_ge_20", _BOOST_ERROR_SURGE_COUNT_GE_THRESHOLD, incident_count_boost_applied),
                ("multiple_error_log_findings", _BOOST_ERROR_SURGE_MULTI_PATTERN, multi_pattern_boost_applied),
                ("cross_source_corroboration", _BOOST_ERROR_SURGE_MULTI_SOURCE, multi_source_boost_applied),
                ("trace_fault_corroboration", _BOOST_ERROR_SURGE_TRACE_CORROBORATION, trace_corroboration_boost_applied),
            ],
            cap=_CAP_APP_ERROR_SURGE,
            final=final_confidence,
        ),
    }


def _rule_resource_exhaustion(sigs: NormalizedSignals) -> dict | None:
    """Rule: resource utilisation metrics indicate saturation.

    Triggered by cpu_spike, memory_spike, or db_connection_spike findings.
    Each additional qualifying finding adds 0.25 to confidence, capped at 0.70.
    """
    if not sigs.resource_evidence_ids:
        return None

    additional_count = max(len(sigs.resource_evidence_ids) - 1, 0)
    confidence = min(_BASE_RESOURCE_EXHAUSTION + _BOOST_RESOURCE_EACH_ADDITIONAL * additional_count,
                     _CAP_RESOURCE_EXHAUSTION)

    return {
        "cause": "resource exhaustion",
        "rationale": f"Resource saturation signals detected: {', '.join(sigs.resource_finding_types)}.",
        "supportingEvidenceIds": sigs.resource_evidence_ids,
        "confidence": round(confidence, 4),
        "confidenceBreakdown": _build_confidence_breakdown(
            base=_BASE_RESOURCE_EXHAUSTION,
            boosts=[
                (
                    "additional_resource_signals",
                    round(_BOOST_RESOURCE_EACH_ADDITIONAL * additional_count, 4),
                    additional_count > 0,
                )
            ],
            cap=_CAP_RESOURCE_EXHAUSTION,
            final=confidence,
        ),
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _flatten_findings(worker_outputs: dict) -> list:
    """Flatten all findings from all worker output envelopes into a single list."""
    findings = []
    for source in ("metrics", "logs", "traces"):
        findings.extend(worker_outputs.get(source, {}).get("findings", []))
    return findings


def _canonicalize_findings(findings: list[dict]) -> list[dict]:
    """Drop mirrored findings that represent the same semantic symptom.

    Keeps one representative per equivalence class so supportingEvidenceIds
    reflect orthogonal corroboration rather than duplicated surfaces.

    Handles:
    - Trace findings: deduplicated by finding type + window + key values.
    - Metric findings: deduplicated by (namespace, metricName, stat, currentValue,
      baselineValue) so equivalent dimension-set variants of the same metric
      don't inflate evidence counts.
    """
    canonical = []
    seen: set[tuple] = set()
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        key = _finding_equivalence_key(finding)
        if key is not None:
            if key in seen:
                continue
            seen.add(key)
        canonical.append(finding)
    return canonical


def _finding_equivalence_key(finding: dict) -> tuple | None:
    """Return a deduplication key for a finding, or None if no key applies."""
    source = finding.get("source")
    if source == "traces":
        return _trace_equivalence_key(finding)
    if source == "metrics":
        return _metric_equivalence_key(finding)
    return None


def _metric_equivalence_key(finding: dict) -> tuple | None:
    """Return a deduplication key for metric findings.

    Two metric findings are equivalent when they report the same metric
    (namespace + metricName + stat) with the same observed values
    (currentValue + baselineValue). Different dimension sets that happen
    to return identical data are collapsed to one representative.

    Returns None for metric findings that lack the expected detail fields,
    in which case no deduplication is applied (safe default).
    """
    details = finding.get("details")
    if not isinstance(details, dict):
        return None
    namespace = details.get("namespace")
    metric_name = details.get("metricName")
    stat = details.get("stat")
    current = details.get("currentValue")
    baseline = details.get("baselineValue")
    finding_type = finding.get("findingType")
    if not all(v is not None for v in (namespace, metric_name, stat, finding_type)):
        return None
    return (
        "metrics",
        finding_type,
        str(namespace),
        str(metric_name),
        str(stat),
        round(_as_float(current, default=-1.0) or -1.0, 4),
        round(_as_float(baseline, default=-1.0) or -1.0, 4),
    )


def _trace_equivalence_key(finding: dict) -> tuple | None:
    if finding.get("source") != "traces":
        return None

    finding_type = finding.get("findingType")
    details = finding.get("details", {}) if isinstance(finding.get("details"), dict) else {}
    incident = details.get("incidentWindow") if isinstance(details.get("incidentWindow"), dict) else {}
    baseline = details.get("baselineWindow") if isinstance(details.get("baselineWindow"), dict) else {}

    if finding_type == "trace_latency_spike":
        return (
            finding_type,
            incident.get("start"),
            incident.get("end"),
            baseline.get("start"),
            baseline.get("end"),
            round(_as_float(details.get("incidentAvgResponseTimeMs"), default=-1.0) or -1.0, 2),
            round(_as_float(details.get("baselineAvgResponseTimeMs"), default=-1.0) or -1.0, 2),
            round(_as_float(details.get("changeRatio"), default=-1.0) or -1.0, 4),
        )

    if finding_type == "latency_dominant_segment":
        return (
            finding_type,
            incident.get("start"),
            incident.get("end"),
            str(details.get("dominantSegment") or ""),
            round(_as_float(details.get("dominantSegmentPct"), default=-1.0) or -1.0, 3),
            round(_as_float(details.get("dominantSegmentMs"), default=-1.0) or -1.0, 3),
            int(_as_float(details.get("sampledTraceCount"), default=-1.0) or -1),
        )

    if finding_type == "trace_dependency_fault":
        faulted = details.get("faultedSubsegments")
        names = tuple(sorted(str(s.get("name", "")) for s in faulted)) if isinstance(faulted, list) else ()
        return (
            finding_type,
            incident.get("start"),
            incident.get("end"),
            names,
            round(finding.get("score", 0.0), 2),
        )

    return None


def _extract_exception_class(samples: list[str]) -> str | None:
    """Return the most-frequent exception class found in exception sample strings, or None.

    Applies two regex patterns to each sample string (primary first, fallback second):
    - Primary: AWS SDK parenthesised format ``(ExceptionClass)``
    - Fallback: bare prefix format ``ExceptionClass:``

    Returns the class name that appears most often, or ``None`` when no match is found.
    This is display-only; it does not affect confidence scores.
    """
    counts: Counter = Counter()
    for sample in samples:
        if not isinstance(sample, str):
            continue
        m = _EXCEPTION_CLASS_PRIMARY_RE.search(sample)
        if m:
            counts[m.group(1)] += 1
            continue
        m = _EXCEPTION_CLASS_FALLBACK_RE.search(sample)
        if m:
            counts[m.group(1)] += 1
    if not counts:
        return None
    return counts.most_common(1)[0][0]


def _build_confidence_breakdown(*, base: float, boosts: list[tuple[str, float, bool]], cap: float, final: float) -> dict:
    """Return deterministic confidence breakdown metadata for explainability."""
    total_before_cap = base + sum(value for _, value, applied in boosts if applied)
    return {
        "base": round(base, 4),
        "boosts": [
            {
                "name": name,
                "value": round(value, 4),
                "applied": applied,
            }
            for name, value, applied in boosts
        ],
        "totalBeforeCap": round(total_before_cap, 4),
        "cap": round(cap, 4),
        "final": round(final, 4),
    }


def _as_float(value, *, default: float | None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
