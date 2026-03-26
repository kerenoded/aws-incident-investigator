"""
Final Report Builder — core logic.

Deterministically assembles all investigation outputs (scope, worker evidence,
hypotheses, optional AI evaluation) into a structured
incident report.

- ADR-002: deterministic; AI evaluation is accepted as an optional parameter
           but deterministic hypotheses are the primary source for topHypotheses
- ADR-007: evidence IDs from workers flow through unchanged
- ADR-009: empty or missing worker outputs are handled gracefully

AI evaluation is accepted as a bounded additive input.
Deterministic hypotheses remain primary for ranking.
"""

import re

# Maximum number of evidence summaries to include in evidenceHighlights.
_MAX_EVIDENCE_HIGHLIGHTS = 5

_MAX_STRONGEST_EVIDENCE = 3
_ERROR_FINDING_TYPES = {"error_log_spike", "new_error_pattern", "error_rate_spike"}
_GENERIC_AI_MISSING_EVIDENCE = {
    "container restart count",
    "database connection pool metrics",
}
_GENERIC_AI_MISSING_EVIDENCE_SUBSTRINGS: tuple[str, ...] = (
    "detailed error logs",
    "specific stack traces",
    "x-ray traces with detailed downstream call failures",
    "x-ray traces with more detailed downstream request information",
    "x-ray traces with detailed downstream request and response payload",
    "x-ray traces with detailed downstream request/response payload",
    "more detailed logs",
    "detailed error messages",
)

# Finding types that are deduplicated per (resourceName, source) in evidence highlights.
# Keeps only the highest-scored finding when multiple log patterns fire for the same resource.
_LOG_DEDUP_TYPES = frozenset({"error_log_spike", "new_error_pattern"})
_EVIDENCE_ID_PATTERN = re.compile(r"^ev-[a-z0-9]+-\d+$")


def build_report(
    scope: dict,
    worker_outputs: dict,
    hypotheses: dict,
    ai_evaluation: dict | None = None,
) -> dict:
    """Assemble a final incident report from all investigation outputs.

    Uses deterministic hypotheses as the primary source for topHypotheses.
    ai_evaluation is accepted as a bounded additive input for explainability.

    Args:
        scope:            Scope dict from the Scope Builder.
        worker_outputs:   Dict keyed by source name ("metrics", "logs",
                          "changes", "traces"). Missing keys treated as empty.
        hypotheses:       Hypothesis envelope from the Candidate Hypotheses Builder.
        ai_evaluation:    Optional AI hypothesis evaluation output (null-safe).

    Returns:
        Final report dict conforming to schemas/final-report.schema.json.
    """
    top_hypotheses = _build_top_hypotheses(hypotheses)
    summary = _build_summary(top_hypotheses)
    evidence_highlights = _build_evidence_highlights(
        worker_outputs,
        top_hypotheses=top_hypotheses,
    )
    worker_errors = _collect_worker_errors(worker_outputs)
    confidence_explanation = _build_confidence_explanation(
        top_hypotheses=top_hypotheses,
        worker_outputs=worker_outputs,
        ai_evaluation=ai_evaluation,
        worker_errors=worker_errors,
    )
    operator_focus = _build_operator_focus(
        top_hypotheses=top_hypotheses,
        worker_outputs=worker_outputs,
    )
    report = {
        "incidentId": scope["incidentId"],
        "summary": summary,
        "topHypotheses": top_hypotheses,
        "evidenceHighlights": evidence_highlights,
        "incidentWindow": scope.get("incidentWindow"),
        "baselineWindow": scope.get("baselineWindow"),
        "workerErrors": worker_errors,
        "confidenceExplanation": confidence_explanation,
        "operatorFocus": operator_focus,
    }

    # Additive: include AI assessments when present. topHypotheses is unchanged.
    resource_hints = _build_hypothesis_resource_hints(top_hypotheses, worker_outputs)
    normalized_assessments = _normalize_ai_assessments(
        ai_evaluation,
        resource_hints=resource_hints,
    )
    if normalized_assessments:
        report["aiAssessments"] = normalized_assessments

    normalized_actions = _normalize_ai_next_best_actions(ai_evaluation)
    if normalized_actions:
        report["aiNextBestActions"] = normalized_actions

    ai_model_id = ai_evaluation.get("modelId") if isinstance(ai_evaluation, dict) else None
    if ai_model_id and (normalized_assessments or normalized_actions):
        report["aiMeta"] = {"modelId": ai_model_id}

    return report


# ---------------------------------------------------------------------------
# Assembly helpers
# ---------------------------------------------------------------------------

def _build_top_hypotheses(hypotheses: dict) -> list:
    """Convert hypothesis envelope entries to the topHypotheses report format."""
    mapped = []
    for h in hypotheses.get("hypotheses", []):
        item = {
            "cause": h.get("cause", "unknown"),
            "confidence": h.get("confidence", 0.0),
            "supportingEvidenceIds": h.get("supportingEvidenceIds", []),
        }
        confidence_breakdown = h.get("confidenceBreakdown")
        if isinstance(confidence_breakdown, dict):
            item["confidenceBreakdown"] = confidence_breakdown
        mapped.append(item)
    return mapped


def _deduplicate_log_findings(findings: list) -> list:
    """Keep only the highest-scored log error finding per (resourceName, source) group.

    Other finding types pass through unchanged. Assumes findings are sorted by
    score descending so the first occurrence per key is the strongest.
    """
    seen: set[tuple] = set()
    result = []
    for f in findings:
        if f.get("findingType") in _LOG_DEDUP_TYPES:
            key = (f.get("resourceName"), f.get("source"))
            if key in seen:
                continue
            seen.add(key)
        result.append(f)
    return result


def _build_summary(top_hypotheses: list) -> str:
    """Generate a one-sentence summary from the top hypothesis."""
    if not top_hypotheses:
        return "Investigation completed. No strong root-cause hypothesis identified."
    top = top_hypotheses[0]
    cause = top.get("cause", "unknown cause")
    confidence = top.get("confidence", 0.0)
    return (
        f"Most likely cause: {cause} "
        f"(confidence {confidence:.0%}). "
        "Review evidence highlights and supporting evidence IDs for details."
    )


def _build_evidence_highlights(
    worker_outputs: dict,
    *,
    top_hypotheses: list,
) -> list:
    """Build evidence highlights, prioritizing top-hypothesis supporting evidence.

    If the top hypothesis has supporting evidence IDs that resolve to findings,
    highlights are sourced from those findings only (score-descending). This
    keeps highlights aligned with the selected top cause.

    Fallback behavior (no resolvable supporting evidence): keep previous
    score-descending highlights across all findings.
    """
    by_evidence_id = _build_finding_lookup(worker_outputs)

    top_supporting_ids = []
    if top_hypotheses:
        top_supporting_ids = top_hypotheses[0].get("supportingEvidenceIds", []) or []

    top_supporting_findings = [
        by_evidence_id[evidence_id]
        for evidence_id in top_supporting_ids
        if evidence_id in by_evidence_id
    ]
    top_supporting_findings.sort(key=lambda f: f.get("score", 0.0), reverse=True)
    top_supporting_findings = _deduplicate_trace_equivalent_findings(top_supporting_findings)
    top_supporting_findings = _deduplicate_log_findings(top_supporting_findings)

    if top_supporting_findings:
        return [
            f["summary"]
            for f in top_supporting_findings[:_MAX_EVIDENCE_HIGHLIGHTS]
            if "summary" in f
        ]

    # Fallback: preserve previous global score-based behavior.
    all_findings = []
    for source in ("metrics", "logs", "traces"):
        all_findings.extend(worker_outputs.get(source, {}).get("findings", []))
    all_findings.sort(key=lambda f: f.get("score", 0.0), reverse=True)
    all_findings = _deduplicate_trace_equivalent_findings(all_findings)
    all_findings = _deduplicate_log_findings(all_findings)
    return [
        f["summary"]
        for f in all_findings[:_MAX_EVIDENCE_HIGHLIGHTS]
        if "summary" in f
    ]


def _collect_worker_errors(worker_outputs: dict) -> list:
    """Aggregate error entries from all worker outputs into a flat list."""
    errors = []
    for source in ("metrics", "logs", "traces"):
        errors.extend(worker_outputs.get(source, {}).get("errors", []))
    return errors


def _build_confidence_explanation(
    *,
    top_hypotheses: list,
    worker_outputs: dict,
    ai_evaluation: dict | None,
    worker_errors: list,
) -> dict:
    top_hypothesis = top_hypotheses[0] if top_hypotheses else {}
    top_cause = str(top_hypothesis.get("cause") or "unknown_cause")
    top_confidence = _as_float(top_hypothesis.get("confidence"), default=0.0)
    runner_up_confidence = None
    confidence_delta = None

    if len(top_hypotheses) > 1:
        runner_up_confidence = _as_float(top_hypotheses[1].get("confidence"), default=0.0)
        confidence_delta = round(top_confidence - runner_up_confidence, 4)

    strongest_evidence = _build_strongest_evidence(
        top_hypothesis=top_hypothesis,
        worker_outputs=worker_outputs,
    )
    supporting_source_count = _count_supporting_evidence_sources(
        top_hypothesis=top_hypothesis,
        worker_outputs=worker_outputs,
    )

    ai_identified_missing = _extract_ai_missing_evidence(ai_evaluation)
    resource_hints = _build_hypothesis_resource_hints(top_hypotheses, worker_outputs)
    ai_contribution = _build_ai_contribution(
        ai_evaluation=ai_evaluation,
        top_cause=top_cause,
        resource_hints=resource_hints,
    )

    return {
        "topHypothesisCause": top_cause,
        "whyRankedHighest": _build_ranked_highest_reason(
            top_hypothesis=top_hypothesis,
            strongest_evidence=strongest_evidence,
            supporting_source_count=supporting_source_count,
            top_confidence=top_confidence,
            runner_up_confidence=runner_up_confidence,
            confidence_delta=confidence_delta,
        ),
        "strongestEvidence": strongest_evidence,
        "missingEvidence": {
            "aiIdentified": ai_identified_missing,
            "collectionGaps": [
                {
                    "source": str(err.get("source", "unknown")),
                    "reason": str(err.get("reason", "unknown")),
                }
                for err in worker_errors
            ],
        },
        "contribution": {
            "rankingDriver": "deterministic",
            "deterministic": {
                "topConfidence": round(top_confidence, 4),
                "runnerUpConfidence": round(runner_up_confidence, 4)
                if runner_up_confidence is not None
                else None,
                "confidenceDelta": confidence_delta,
            },
            "ai": ai_contribution,
        },
    }


def _build_operator_focus(
    *,
    top_hypotheses: list,
    worker_outputs: dict,
) -> dict:
    top_hypothesis = top_hypotheses[0] if top_hypotheses else {}
    strongest_evidence = _build_strongest_evidence(
        top_hypothesis=top_hypothesis,
        worker_outputs=worker_outputs,
    )
    by_evidence_id = _build_finding_lookup(worker_outputs)

    primary_item = strongest_evidence[0] if strongest_evidence else None
    primary_finding = (
        by_evidence_id.get(primary_item["evidenceId"]) if primary_item else None
    )

    top_supporting_ids = top_hypothesis.get("supportingEvidenceIds") or []
    top_error = _build_top_error_pattern(
        supporting_evidence=strongest_evidence,
        by_evidence_id=by_evidence_id,
        all_supporting_ids=top_supporting_ids,
    )
    trace_dependency_hint = _build_trace_dependency_hint(worker_outputs)

    where_to_look_first = _build_where_to_look_first(
        top_hypothesis=top_hypothesis,
        primary_item=primary_item,
        primary_finding=primary_finding,
        top_error=top_error,
    )

    return {
        "mostLikelyAffectedComponent": _extract_component_name(primary_finding),
        "primaryImplicatedResource": _build_primary_resource(primary_item),
        "topErrorPattern": top_error,
        "traceDependencyHint": trace_dependency_hint,
        "whereToLookFirst": where_to_look_first,
    }


def _build_trace_dependency_hint(worker_outputs: dict) -> dict | None:
    """Build a compact trace dependency hint from the strongest trace fault finding.

    This is additive operator context so downstream/X-Ray evidence stays visible
    even when deterministic ranking is led by logs/metrics.
    """
    trace_findings = worker_outputs.get("traces", {}).get("findings", [])
    candidates = [
        finding
        for finding in trace_findings
        if finding.get("findingType") == "trace_dependency_fault"
    ]
    if not candidates:
        return None

    top = max(candidates, key=lambda finding: _as_float(finding.get("score"), default=0.0) or 0.0)
    details = top.get("details") if isinstance(top.get("details"), dict) else {}
    faulted = details.get("faultedSubsegments") if isinstance(details.get("faultedSubsegments"), list) else []
    subsegments = [subsegment for subsegment in faulted if isinstance(subsegment, dict)]
    primary_subsegment = max(subsegments, key=_trace_subsegment_sort_key) if subsegments else {}

    return {
        "evidenceId": top.get("evidenceId"),
        "resourceName": top.get("resourceName"),
        "summary": top.get("summary", ""),
        "score": _as_float(top.get("score"), default=0.0),
        "subsegmentName": primary_subsegment.get("name"),
        "namespace": primary_subsegment.get("namespace"),
        "httpStatus": primary_subsegment.get("httpStatus"),
        "occurrences": primary_subsegment.get("occurrences"),
        "sampledTraceCount": details.get("sampledTraceCount"),
    }


def _trace_subsegment_sort_key(subsegment: dict) -> tuple[int, int, int]:
    """Rank trace faulted subsegments by operator usefulness.

    Preference order:
    1) AWS namespace over local wrappers
    2) HTTP status present over null
    3) Higher occurrences
    """
    namespace = str(subsegment.get("namespace") or "").strip().lower()
    namespace_rank = 1 if namespace == "aws" else 0
    http_status_rank = 1 if subsegment.get("httpStatus") is not None else 0
    occurrences_rank = int(_as_float(subsegment.get("occurrences"), default=0.0) or 0)
    return (namespace_rank, http_status_rank, occurrences_rank)


def _build_primary_resource(primary_item: dict | None) -> dict | None:
    if not primary_item:
        return None
    return {
        "evidenceId": primary_item["evidenceId"],
        "source": primary_item.get("source"),
        "resourceType": primary_item.get("resourceType"),
        "resourceName": primary_item.get("resourceName"),
        "findingType": primary_item.get("findingType"),
        "score": primary_item.get("score"),
        "summary": primary_item.get("summary", ""),
    }


def _build_top_error_pattern(
    *,
    supporting_evidence: list[dict],
    by_evidence_id: dict[str, dict],
    all_supporting_ids: list[str] | None = None,
) -> dict | None:
    # Prefer log-derived patterns for operator guidance; fall back to any
    # error finding when log evidence is unavailable.
    #
    # Search order:
    #   1. Top-3 strongest evidence items (already ranked by score)
    #   2. All remaining supporting evidence IDs for the top hypothesis
    #      (covers lower-scored log findings that didn't make the top-3)
    candidates = [
        item
        for item in supporting_evidence
        if item.get("findingType") in _ERROR_FINDING_TYPES
    ]

    # Extend candidates from the full supporting evidence list when the top-3
    # strongest items don't include any error findings (e.g. all latency).
    if not candidates and all_supporting_ids:
        for evidence_id in all_supporting_ids:
            finding = by_evidence_id.get(evidence_id)
            if not finding:
                continue
            if finding.get("findingType") not in _ERROR_FINDING_TYPES:
                continue
            # Avoid duplicates already in supporting_evidence.
            already_present = any(
                item.get("evidenceId") == evidence_id for item in supporting_evidence
            )
            if already_present:
                continue
            candidates.append({
                "evidenceId": evidence_id,
                "source": finding.get("source"),
                "findingType": finding.get("findingType"),
                "resourceName": finding.get("resourceName"),
                "summary": finding.get("summary", ""),
                "score": finding.get("score", 0.0),
            })

    ordered = [item for item in candidates if item.get("source") == "logs"] + [
        item for item in candidates if item.get("source") != "logs"
    ]

    for item in ordered:
        finding = by_evidence_id.get(item.get("evidenceId"), {})
        details = finding.get("details", {}) if isinstance(finding.get("details"), dict) else {}
        pattern = details.get("pattern")
        if not isinstance(pattern, str) or not pattern.strip():
            pattern = item.get("summary", "")

        result = {
            "evidenceId": item.get("evidenceId"),
            "source": item.get("source"),
            "findingType": item.get("findingType"),
            "resourceName": item.get("resourceName"),
            "pattern": str(pattern),
            "summary": item.get("summary", ""),
            "incidentCount": details.get("incidentCount"),
            "baselineCount": details.get("baselineCount"),
            "changeRatio": details.get("changeRatio"),
        }
        exception_samples = details.get("exceptionSamples")
        if exception_samples:
            result["exceptionSamples"] = _dedupe_non_empty_strings(exception_samples)[:3]
        return result

    return None


def _build_where_to_look_first(
    *,
    top_hypothesis: dict,
    primary_item: dict | None,
    primary_finding: dict | None,
    top_error: dict | None,
) -> str:
    if primary_item:
        location = _extract_component_name(primary_finding) or "the primary implicated component"
        if top_error:
            top_error_location = str(top_error.get("resourceName") or "").strip()
            cross_component = bool(
                top_error.get("source") == "logs"
                and top_error_location
                and _normalize_cause(top_error_location) != _normalize_cause(location)
            )

            exception_samples = top_error.get("exceptionSamples") or []
            if exception_samples:
                if cross_component:
                    return (
                        f"Start with {top_error_location} logs — exception errors present; "
                        f"then correlate with {location} metrics in the incident window."
                    )
                return f"Check {location} — exception errors present; see sample messages below."

            pattern = top_error.get("pattern")
            if pattern:
                if top_error.get("source") == "logs":
                    if cross_component:
                        return (
                            f"Start with {top_error_location} logs — filter for '{pattern}' in the incident window, "
                            f"then correlate with {location} metrics."
                        )
                    return f"Check {location} — filter logs for '{pattern}' in the incident window."
                return f"Check {location} — inspect metrics for '{pattern}' in the incident window."

        return f"Check {location} — it has the strongest signal for the top hypothesis."

    cause = str(top_hypothesis.get("cause") or "top hypothesis")
    return (
        f"No concrete resource was linked to {cause}; start from the highest-scored evidence highlight."
    )


def _extract_component_name(finding: dict | None) -> str | None:
    if not isinstance(finding, dict):
        return None
    resource_name = finding.get("resourceName")
    if isinstance(resource_name, str) and resource_name.strip():
        return resource_name.strip()
    summary = finding.get("summary")
    if isinstance(summary, str) and summary.strip():
        return summary.strip()
    return None


def _build_ranked_highest_reason(
    *,
    top_hypothesis: dict,
    strongest_evidence: list,
    supporting_source_count: int,
    top_confidence: float,
    runner_up_confidence: float | None,
    confidence_delta: float | None,
) -> str:
    if not top_hypothesis:
        return "No deterministic hypothesis was ranked with meaningful confidence."

    if runner_up_confidence is None:
        evidence_count = len(strongest_evidence)
        return (
            f"Only one deterministic hypothesis was generated with confidence {top_confidence:.0%}; "
            f"it is supported by {evidence_count} strongest evidence item(s)"
            f" across {supporting_source_count} source(s)."
        )

    evidence_count = len(strongest_evidence)
    return (
        f"Deterministic ranking selected this hypothesis by confidence gap "
        f"({top_confidence:.0%} vs {runner_up_confidence:.0%}; delta {confidence_delta:.0%}) "
        f"with {evidence_count} strongest supporting evidence item(s)."
    )


def _build_strongest_evidence(
    *,
    top_hypothesis: dict,
    worker_outputs: dict,
) -> list[dict]:
    by_evidence_id = _build_finding_lookup(worker_outputs)
    supporting_ids = top_hypothesis.get("supportingEvidenceIds") or []
    ranked = []
    for evidence_id in supporting_ids:
        finding = by_evidence_id.get(evidence_id)
        if not finding:
            continue
        ranked.append(
            {
                "evidenceId": evidence_id,
                "source": finding.get("source"),
                "resourceType": finding.get("resourceType"),
                "resourceName": finding.get("resourceName"),
                "findingType": finding.get("findingType"),
                "score": _as_float(finding.get("score"), default=0.0),
                "summary": finding.get("summary", ""),
            }
        )
    ranked.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    ranked = _deduplicate_trace_equivalent_evidence_items(ranked, by_evidence_id=by_evidence_id)
    return ranked[:_MAX_STRONGEST_EVIDENCE]


def _count_supporting_evidence_sources(*, top_hypothesis: dict, worker_outputs: dict) -> int:
    by_evidence_id = _build_finding_lookup(worker_outputs)
    sources = {
        finding.get("source")
        for evidence_id in (top_hypothesis.get("supportingEvidenceIds") or [])
        for finding in [by_evidence_id.get(evidence_id)]
        if isinstance(finding, dict)
        and isinstance(finding.get("source"), str)
        and finding.get("source")
    }
    return len(sources)


def _build_finding_lookup(worker_outputs: dict) -> dict[str, dict]:
    lookup = {}
    for source in ("metrics", "logs", "traces"):
        for finding in worker_outputs.get(source, {}).get("findings", []):
            evidence_id = finding.get("evidenceId")
            if isinstance(evidence_id, str):
                lookup[evidence_id] = finding
    return lookup


def _deduplicate_trace_equivalent_findings(findings: list) -> list:
    """Drop mirrored trace findings that encode the same semantic signal.

    Some traces can surface equivalent latency spikes under multiple trace
    services/resources. We keep one representative so evidence highlights
    emphasize orthogonal signals rather than mirrored duplicates.
    """
    deduped = []
    seen = set()
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        key = _trace_equivalence_key(finding)
        if key is None:
            deduped.append(finding)
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(finding)
    return deduped


def _deduplicate_trace_equivalent_evidence_items(ranked_items: list[dict], *, by_evidence_id: dict[str, dict]) -> list[dict]:
    """Apply trace-equivalence dedupe over ranked strongest-evidence items."""
    deduped = []
    seen = set()
    for item in ranked_items:
        evidence_id = item.get("evidenceId")
        finding = by_evidence_id.get(evidence_id, {})
        key = _trace_equivalence_key(finding)
        if key is not None:
            if key in seen:
                continue
            seen.add(key)
        deduped.append(item)
    return deduped


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

    return None


def _extract_ai_missing_evidence(ai_evaluation: dict | None) -> list[str]:
    if not ai_evaluation:
        return []
    values = ai_evaluation.get("missingEvidence", [])
    if not isinstance(values, list):
        return []
    cleaned = []
    seen = set()
    for value in values:
        if not isinstance(value, str):
            continue
        normalized = " ".join(value.strip().lower().split())
        if not normalized:
            continue
        if _is_generic_ai_missing_evidence(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(value.strip())
    return cleaned


def _is_generic_ai_missing_evidence(normalized: str) -> bool:
    if normalized in _GENERIC_AI_MISSING_EVIDENCE:
        return True
    if any(sub in normalized for sub in _GENERIC_AI_MISSING_EVIDENCE_SUBSTRINGS):
        return True

    if "detailed stack traces" in normalized:
        # Keep stack-trace requests only when they are concise and scoped to
        # concrete exception/error occurrences, e.g.:
        # "Detailed stack traces for RuntimeError occurrences"
        has_specific_error_target = " for " in normalized and (
            "error" in normalized or "exception" in normalized
        )
        has_occurrence_scope = "occurrence" in normalized or "sample" in normalized
        has_generic_explanatory_tail = any(
            token in normalized
            for token in (
                "to identify",
                "to understand",
                "exact code path",
                "root cause",
            )
        )
        if not has_specific_error_target or not has_occurrence_scope or has_generic_explanatory_tail:
            return True

    has_incident_context = any(token in normalized for token in ("incident time", "incident window"))
    has_traffic_patterns_context = "traffic" in normalized and (
        "pattern" in normalized or "patterns" in normalized
    )
    has_user_requests_context = "user requests" in normalized
    if has_incident_context and (has_traffic_patterns_context or has_user_requests_context):
        return True

    # Filter broad, low-actionability requests that are incident-related but vague.
    if "additional metrics" in normalized and "downstream service" in normalized:
        return True
    if "more detailed logs" in normalized and "lambda" in normalized:
        return True
    if "detailed error messages" in normalized and "downstream" in normalized:
        return True

    return False


def _normalize_ai_assessments(
    ai_evaluation: dict | None,
    *,
    resource_hints: dict[str, list[str]] | None = None,
) -> list[dict]:
    if not isinstance(ai_evaluation, dict):
        return []
    raw = ai_evaluation.get("assessments", [])
    if not isinstance(raw, list):
        return []

    best_by_cause: dict[str, dict] = {}
    order: list[str] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        cause = str(item.get("cause") or "").strip()
        reason = str(item.get("reason") or "").strip()
        plausibility = _as_float(item.get("plausibility"), default=None)
        if not cause or not reason or plausibility is None:
            continue
        key = _normalize_cause(cause)
        normalized_item = {
            "cause": cause,
            "plausibility": plausibility,
            "reason": _ensure_reason_has_resource_hint(
                cause=cause,
                reason=reason,
                resource_hints=resource_hints or {},
            ),
        }
        if key not in best_by_cause:
            best_by_cause[key] = normalized_item
            order.append(key)
            continue
        if plausibility > _as_float(best_by_cause[key].get("plausibility"), default=-1.0):
            best_by_cause[key] = normalized_item

    return [best_by_cause[key] for key in order if key in best_by_cause]


def _dedupe_non_empty_strings(values: list) -> list[str]:
    deduped = []
    seen = set()
    for value in values:
        if not isinstance(value, str):
            continue
        normalized = value.strip()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _build_ai_contribution(
    *,
    ai_evaluation: dict | None,
    top_cause: str,
    resource_hints: dict[str, list[str]] | None = None,
) -> dict:
    assessments = _normalize_ai_assessments(
        ai_evaluation,
        resource_hints=resource_hints,
    )

    if not assessments:
        return {
            "assessmentAvailable": False,
            "topHypothesisMatch": None,
            "plausibility": None,
            "reason": None,
            "unavailableReason": _derive_ai_unavailable_reason(ai_evaluation),
        }

    ai_top = ""
    if isinstance(ai_evaluation, dict):
        ai_top = str(ai_evaluation.get("topHypothesis") or "")

    top_match = _normalize_cause(ai_top) == _normalize_cause(top_cause)
    matched_assessment = next(
        (
            assessment
            for assessment in assessments
            if _normalize_cause(str(assessment.get("cause") or "")) == _normalize_cause(top_cause)
        ),
        None,
    )

    return {
        "assessmentAvailable": True,
        "topHypothesisMatch": top_match,
        "plausibility": _as_float(matched_assessment.get("plausibility"), default=None)
        if matched_assessment
        else None,
        "reason": matched_assessment.get("reason") if matched_assessment else None,
        "unavailableReason": None,
    }


def _build_hypothesis_resource_hints(top_hypotheses: list, worker_outputs: dict) -> dict[str, list[str]]:
    """Build per-hypothesis concrete resource hints from supporting evidence IDs."""
    by_evidence_id = _build_finding_lookup(worker_outputs)
    hints: dict[str, list[str]] = {}
    for hypothesis in top_hypotheses:
        cause = str(hypothesis.get("cause") or "").strip()
        if not cause:
            continue
        resources = []
        seen = set()
        for evidence_id in hypothesis.get("supportingEvidenceIds") or []:
            finding = by_evidence_id.get(evidence_id) or {}
            resource_name = finding.get("resourceName")
            if isinstance(resource_name, str):
                normalized = resource_name.strip()
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    resources.append(normalized)
        if resources:
            hints[_normalize_cause(cause)] = resources
    return hints


def _ensure_reason_has_resource_hint(*, cause: str, reason: str, resource_hints: dict[str, list[str]]) -> str:
    """Ensure assessment reasons include at least one concrete resource when available."""
    clean_reason = reason.strip()
    if not clean_reason:
        return clean_reason

    cause_key = _normalize_cause(cause)
    hinted_resources = resource_hints.get(cause_key, [])
    if not hinted_resources:
        return clean_reason

    reason_l = clean_reason.lower()
    if "resource:" in reason_l:
        return clean_reason
    if any(resource.lower() in reason_l for resource in hinted_resources):
        return clean_reason

    suffix = f"Resource: {hinted_resources[0]}."
    if clean_reason.endswith((".", "!", "?")):
        return f"{clean_reason} {suffix}"
    return f"{clean_reason}. {suffix}"


def _derive_ai_unavailable_reason(ai_evaluation: dict | None) -> str:
    if not isinstance(ai_evaluation, dict) or not ai_evaluation:
        return "ai_evaluation_not_returned"
    value = ai_evaluation.get("unavailableReason")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "ai_assessments_missing"


def _normalize_ai_next_best_actions(ai_evaluation: dict | None) -> list[dict]:
    """Normalize optional AI next-best-actions payload for report output.

    This keeps output backward-compatible and bounded:
    - optional field only
    - max 3 items
    - each item must include action/why/evidenceIds/expectedSignal/confidence
    - evidenceIds must match ev-* format and be non-empty
    """
    if not isinstance(ai_evaluation, dict):
        return []
    raw = ai_evaluation.get("nextBestActions")
    if not isinstance(raw, list):
        return []

    normalized: list[dict] = []
    for item in raw:
        if len(normalized) >= 3:
            break
        if not isinstance(item, dict):
            continue

        action = item.get("action")
        why = item.get("why")
        expected_signal = item.get("expectedSignal")
        confidence = _as_float(item.get("confidence"), default=None)
        evidence_ids = item.get("evidenceIds")

        if not isinstance(action, str) or not action.strip():
            continue
        if not isinstance(why, str) or not why.strip():
            continue
        if not isinstance(expected_signal, str) or not expected_signal.strip():
            continue
        if confidence is None or confidence < 0.0 or confidence > 1.0:
            continue
        if not isinstance(evidence_ids, list):
            continue

        clean_ids: list[str] = []
        seen: set[str] = set()
        for evidence_id in evidence_ids:
            if not isinstance(evidence_id, str):
                continue
            normalized_id = evidence_id.strip()
            if not normalized_id:
                continue
            if not _EVIDENCE_ID_PATTERN.match(normalized_id):
                continue
            if normalized_id in seen:
                continue
            seen.add(normalized_id)
            clean_ids.append(normalized_id)

        if not clean_ids:
            continue

        normalized.append(
            {
                "action": action.strip(),
                "why": why.strip(),
                "evidenceIds": clean_ids,
                "expectedSignal": expected_signal.strip(),
                "confidence": confidence,
            }
        )

    return normalized


def _normalize_cause(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _as_float(value, *, default: float | None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


